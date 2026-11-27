//! Process-kill durability and statement-atomicity drill for the embedded SQL subset.
//!
//! Spawns a child process running deterministic autocommitted SQL statements against both
//! ordinary `Store` and `CheckpointStore` backends, sends `SIGKILL` via `Child::kill` at
//! pseudo-random acknowledgement counts, and verifies on reopen that:
//! - every acknowledged statement is durably present,
//! - the single statement potentially in flight at the kill is either entirely applied or
//!   entirely absent (never partially visible for multi-row INSERT/UPDATE/DELETE),
//! - no later statement is visible,
//! - the SQL catalog survives and the reopened store remains writable.

use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlOutcome, SqlValue, Store,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const ENV_ROLE: &str = "EMDA_SQL_CRASH_ROLE";
const ENV_DIR: &str = "EMDA_SQL_CRASH_DIR";
const ENV_BACKEND: &str = "EMDA_SQL_CRASH_BACKEND";
const ENV_SEED: &str = "EMDA_SQL_CRASH_SEED";
const ENV_START_SEQ: &str = "EMDA_SQL_CRASH_START_SEQ";

const KILLS_PER_BACKEND: usize = 10;
const ORDINARY_SEED: u64 = 0xE3DA_5C11_2026_0101;
const CHECKPOINT_SEED: u64 = 0xE3DA_5C11_2026_0202;

type Model = BTreeMap<i64, (i64, String)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackendKind {
    Ordinary,
    Checkpoint,
}

impl BackendKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Checkpoint => "checkpoint",
        }
    }

    fn from_str(raw: &str) -> Self {
        match raw {
            "ordinary" => Self::Ordinary,
            "checkpoint" => Self::Checkpoint,
            other => panic!("unknown backend: {other}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InFlightStatus {
    Absent,
    Applied,
}

struct SqlStatementPlan {
    sql: String,
    params: Vec<SqlValue>,
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn mix(seed: u64, seq: u64) -> u64 {
    splitmix64(seed ^ splitmix64(seq))
}

fn row_checksum(kind: &str, id: i64, seq: u64) -> String {
    let tag = mix(id as u64, seq ^ 0xA5A5_5A5A_1234_5678);
    format!("{kind}:id={id}:seq={seq}:chk={tag:016x}")
}

fn range_checksum(kind: &str, lo: i64, hi: i64, seq: u64) -> String {
    let tag = mix(
        (lo as u64).rotate_left(17) ^ (hi as u64),
        seq ^ 0x55AA_33CC_8765_4321,
    );
    format!("{kind}:lo={lo}:hi={hi}:seq={seq}:chk={tag:016x}")
}

/// Deterministic statement sequence indexed by `seq >= 1`.
///
/// Every 6-statement block operates on its own disjoint primary-key range `base_id..base_id+64`:
/// - phase 0: multi-row INSERT of 4..=16 rows (`base_id..base_id+n0`)
/// - phase 1: multi-row INSERT of 1..=16 rows (`base_id+20..base_id+20+n1`)
/// - phase 2: single-row UPDATE by primary key (`WHERE id = ?`)
/// - phase 3: multi-row UPDATE by primary-key range (`WHERE id >= ? AND id <= ?`)
/// - phase 4: multi-row UPDATE by non-PK predicate + PK bound (`WHERE a = ? AND id >= ?`)
/// - phase 5: DELETE (alternating single-row PK delete and 3-row range/predicate delete)
///
/// Every statement is guaranteed to succeed and strictly mutate at least one row (with
/// multi-row operations mutating multiple rows atomically), so `model_after(seq)` is always
/// distinct from `model_after(seq - 1)`.
fn build_statement(seed: u64, seq: u64) -> SqlStatementPlan {
    assert!(seq >= 1, "sequence numbers start at 1");
    let block = (seq - 1) / 6;
    let phase = (seq - 1) % 6;
    let base_id = (block as i64) * 64;
    let r = mix(seed, seq);

    match phase {
        0 => {
            let count = 4 + (r % 13) as i64; // 4..=16 rows
            build_multi_insert(base_id, count, seq, block % 2 == 0)
        }
        1 => {
            let count = 1 + (r % 16) as i64; // 1..=16 rows
            build_multi_insert(base_id + 20, count, seq, block % 2 == 1)
        }
        2 => {
            let target_id = base_id + (r % 4) as i64;
            let text = row_checksum("upd_pk", target_id, seq);
            SqlStatementPlan {
                sql: "UPDATE t SET a = ?, b = ? WHERE id = ?".to_string(),
                params: vec![
                    SqlValue::Integer(seq as i64),
                    SqlValue::Text(text),
                    SqlValue::Integer(target_id),
                ],
            }
        }
        3 => {
            let lo = base_id;
            let hi = base_id + 35;
            let text = range_checksum("upd_rng", lo, hi, seq);
            SqlStatementPlan {
                sql: "UPDATE t SET a = ?, b = ? WHERE id >= ? AND id <= ?".to_string(),
                params: vec![
                    SqlValue::Integer(seq as i64),
                    SqlValue::Text(text),
                    SqlValue::Integer(lo),
                    SqlValue::Integer(hi),
                ],
            }
        }
        4 => {
            let prev_seq = (seq - 1) as i64;
            let lo = base_id + 1;
            let hi = base_id + 35;
            let text = range_checksum("upd_pred", lo, hi, seq);
            SqlStatementPlan {
                sql: "UPDATE t SET a = ?, b = ? WHERE a = ? AND id >= ?".to_string(),
                params: vec![
                    SqlValue::Integer(seq as i64),
                    SqlValue::Text(text),
                    SqlValue::Integer(prev_seq),
                    SqlValue::Integer(lo),
                ],
            }
        }
        _ => {
            if block % 2 == 0 {
                SqlStatementPlan {
                    sql: "DELETE FROM t WHERE id = ?".to_string(),
                    params: vec![SqlValue::Integer(base_id)],
                }
            } else {
                let prev_seq = (seq - 1) as i64;
                SqlStatementPlan {
                    sql: "DELETE FROM t WHERE id >= ? AND id <= ? AND a = ?".to_string(),
                    params: vec![
                        SqlValue::Integer(base_id + 1),
                        SqlValue::Integer(base_id + 3),
                        SqlValue::Integer(prev_seq),
                    ],
                }
            }
        }
    }
}

fn build_multi_insert(start_id: i64, count: i64, seq: u64, use_params: bool) -> SqlStatementPlan {
    let mut sql = String::from("INSERT INTO t (id, a, b) VALUES ");
    let mut params = Vec::new();
    for i in 0..count {
        if i > 0 {
            sql.push_str(", ");
        }
        let id = start_id + i;
        let text = row_checksum("ins", id, seq);
        if use_params {
            sql.push_str("(?, ?, ?)");
            params.push(SqlValue::Integer(id));
            params.push(SqlValue::Integer(seq as i64));
            params.push(SqlValue::Text(text));
        } else {
            sql.push_str(&format!("({id}, {seq}, '{text}')"));
        }
    }
    SqlStatementPlan { sql, params }
}

fn apply_to_model(model: &mut Model, seed: u64, seq: u64) -> usize {
    assert!(seq >= 1, "sequence numbers start at 1");
    let block = (seq - 1) / 6;
    let phase = (seq - 1) % 6;
    let base_id = (block as i64) * 64;
    let r = mix(seed, seq);

    match phase {
        0 => {
            let count = 4 + (r % 13) as i64;
            for i in 0..count {
                let id = base_id + i;
                let prev = model.insert(id, (seq as i64, row_checksum("ins", id, seq)));
                assert!(prev.is_none(), "duplicate id {id} in model");
            }
            count as usize
        }
        1 => {
            let count = 1 + (r % 16) as i64;
            for i in 0..count {
                let id = base_id + 20 + i;
                let prev = model.insert(id, (seq as i64, row_checksum("ins", id, seq)));
                assert!(prev.is_none(), "duplicate id {id} in model");
            }
            count as usize
        }
        2 => {
            let target_id = base_id + (r % 4) as i64;
            let entry = model
                .get_mut(&target_id)
                .unwrap_or_else(|| panic!("expected live row {target_id} for seq={seq}"));
            *entry = (seq as i64, row_checksum("upd_pk", target_id, seq));
            1
        }
        3 => {
            let lo = base_id;
            let hi = base_id + 35;
            let text = range_checksum("upd_rng", lo, hi, seq);
            let mut updated = 0usize;
            for (&id, entry) in model.iter_mut() {
                if (lo..=hi).contains(&id) {
                    *entry = (seq as i64, text.clone());
                    updated += 1;
                }
            }
            assert!(updated >= 5, "expected multi-row range update at seq={seq}");
            updated
        }
        4 => {
            let prev_seq = (seq - 1) as i64;
            let lo = base_id + 1;
            let hi = base_id + 35;
            let text = range_checksum("upd_pred", lo, hi, seq);
            let mut updated = 0usize;
            for (&id, entry) in model.iter_mut() {
                if entry.0 == prev_seq && id >= lo {
                    *entry = (seq as i64, text.clone());
                    updated += 1;
                }
            }
            assert!(
                updated >= 4,
                "expected multi-row predicate update at seq={seq}"
            );
            updated
        }
        _ => {
            if block % 2 == 0 {
                let removed = model.remove(&base_id);
                assert!(
                    removed.is_some(),
                    "expected live row {base_id} at seq={seq}"
                );
                1
            } else {
                let prev_seq = (seq - 1) as i64;
                let keys: Vec<i64> = model
                    .iter()
                    .filter(|&(&id, entry)| {
                        (base_id + 1..=base_id + 3).contains(&id) && entry.0 == prev_seq
                    })
                    .map(|(&id, _)| id)
                    .collect();
                assert_eq!(keys.len(), 3, "expected 3-row delete at seq={seq}");
                for id in keys {
                    model.remove(&id);
                }
                3
            }
        }
    }
}

fn verify_recovered_state(
    outcome: &SqlOutcome,
    model_acked: &Model,
    model_in_flight: &Model,
) -> Result<InFlightStatus, String> {
    let SqlOutcome::Rows { columns, rows } = outcome else {
        return Err(format!("expected SqlOutcome::Rows, got {outcome:?}"));
    };
    let expected_cols = vec!["id".to_string(), "a".to_string(), "b".to_string()];
    if *columns != expected_cols {
        return Err(format!(
            "catalog columns mismatch: expected {expected_cols:?}, got {columns:?}"
        ));
    }

    let mut observed = Model::new();
    let mut prev_id: Option<i64> = None;
    for (idx, row) in rows.iter().enumerate() {
        if row.len() != 3 {
            return Err(format!("row {idx} has {} columns instead of 3", row.len()));
        }
        let (SqlValue::Integer(id), SqlValue::Integer(a), SqlValue::Text(b)) =
            (&row[0], &row[1], &row[2])
        else {
            return Err(format!("row {idx} has unexpected value types: {row:?}"));
        };
        if let Some(prev) = prev_id.filter(|prev| *id <= *prev) {
            return Err(format!(
                "SELECT primary-key ordering violated at row {idx}: {prev} followed by {id}"
            ));
        }
        prev_id = Some(*id);
        observed.insert(*id, (*a, b.clone()));
    }

    if observed == *model_acked {
        return Ok(InFlightStatus::Absent);
    }
    if observed == *model_in_flight {
        return Ok(InFlightStatus::Applied);
    }

    // Build a diagnostic describing how observed deviates from both valid endpoints.
    let missing_from_acked: Vec<i64> = model_acked
        .keys()
        .filter(|id| !observed.contains_key(id))
        .copied()
        .take(5)
        .collect();
    let extra_over_in_flight: Vec<i64> = observed
        .keys()
        .filter(|id| !model_in_flight.contains_key(id))
        .copied()
        .take(5)
        .collect();
    let mismatched_acked: Vec<i64> = model_acked
        .iter()
        .filter(|(id, val)| observed.get(id) != Some(*val))
        .map(|(&id, _)| id)
        .take(5)
        .collect();
    let mismatched_in_flight: Vec<i64> = model_in_flight
        .iter()
        .filter(|(id, val)| observed.get(id) != Some(*val))
        .map(|(&id, _)| id)
        .take(5)
        .collect();

    Err(format!(
        "recovered state matches neither acked ({} rows) nor in-flight ({} rows); \
         observed {} rows; missing_from_acked={missing_from_acked:?}, \
         extra_over_in_flight={extra_over_in_flight:?}, \
         mismatched_vs_acked={mismatched_acked:?}, \
         mismatched_vs_in_flight={mismatched_in_flight:?}",
        model_acked.len(),
        model_in_flight.len(),
        observed.len(),
    ))
}

fn retry_lock<T>(mut open: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match open() {
            Ok(value) => return Ok(value),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => return Err(err),
        }
    }
}

enum OpenBackend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl OpenBackend {
    fn sql(&self) -> SqlDatabase<'_> {
        match self {
            Self::Ordinary(store) => SqlDatabase::new(DocumentDatabase::new(store)),
            Self::Checkpoint(store) => {
                SqlDatabase::new(DocumentDatabase::with_checkpoint_store(store))
            }
        }
    }
}

fn store_path(dir: &Path) -> PathBuf {
    dir.join("store")
}

fn checkpoint_path(dir: &Path) -> PathBuf {
    dir.join("checkpoint")
}

fn digest_path(dir: &Path) -> PathBuf {
    dir.join("digest")
}

fn init_backend(dir: &Path, kind: BackendKind) {
    let sp = store_path(dir);
    match kind {
        BackendKind::Ordinary => {
            let store = Store::open(&sp).unwrap();
            let db = SqlDatabase::new(DocumentDatabase::new(&store));
            let res = db
                .execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL)",
                    &[],
                )
                .unwrap();
            assert_eq!(res, SqlOutcome::TableCreated);
        }
        BackendKind::Checkpoint => {
            let cp = checkpoint_path(dir);
            let initial = Store::open(&sp).unwrap();
            let digest = initial.checkpoint_to(&cp).unwrap();
            drop(initial);
            fs::write(digest_path(dir), digest).unwrap();

            let store = CheckpointStore::open(&sp, &cp, &digest).unwrap();
            let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
            let res = db
                .execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL)",
                    &[],
                )
                .unwrap();
            assert_eq!(res, SqlOutcome::TableCreated);
        }
    }
}

fn open_backend(dir: &Path, kind: BackendKind) -> OpenBackend {
    let sp = store_path(dir);
    match kind {
        BackendKind::Ordinary => OpenBackend::Ordinary(retry_lock(|| Store::open(&sp)).unwrap()),
        BackendKind::Checkpoint => {
            let cp = checkpoint_path(dir);
            let digest: [u8; 32] = fs::read(digest_path(dir)).unwrap().try_into().unwrap();
            OpenBackend::Checkpoint(
                retry_lock(|| CheckpointStore::open(&sp, &cp, &digest)).unwrap(),
            )
        }
    }
}

fn verify_reopened_writability(db: &SqlDatabase<'_>, round: usize) {
    // Confirm catalog metadata survived: duplicate CREATE TABLE fails, IF NOT EXISTS succeeds.
    assert!(matches!(
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
            .unwrap_err(),
        SqlError::TableExists(_)
    ));
    assert_eq!(
        db.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)", &[])
            .unwrap(),
        SqlOutcome::TableCreated
    );

    // Exercise INSERT, UPDATE, SELECT, DELETE on negative probe IDs to confirm writability
    // without disturbing the positive-ID drill model.
    let probe_a = -((round as i64) * 10 + 1);
    let probe_b = probe_a - 1;
    assert_eq!(
        db.execute(
            "INSERT INTO t (id, a, b) VALUES (?, ?, ?), (?, ?, ?)",
            &[
                SqlValue::Integer(probe_a),
                SqlValue::Integer(round as i64),
                SqlValue::Text(format!("probe-a-{round}")),
                SqlValue::Integer(probe_b),
                SqlValue::Integer(round as i64),
                SqlValue::Text(format!("probe-b-{round}")),
            ],
        )
        .unwrap(),
        SqlOutcome::Inserted { rows: 2 }
    );
    assert_eq!(
        db.execute(
            "UPDATE t SET a = ?, b = ? WHERE id = ?",
            &[
                SqlValue::Integer((round as i64) + 10_000),
                SqlValue::Text(format!("probe-upd-{round}")),
                SqlValue::Integer(probe_a),
            ],
        )
        .unwrap(),
        SqlOutcome::Updated { rows: 1 }
    );
    assert_eq!(
        db.execute("SELECT id, a, b FROM t WHERE id < 0", &[])
            .unwrap(),
        SqlOutcome::Rows {
            columns: vec!["id".into(), "a".into(), "b".into()],
            rows: vec![
                vec![
                    SqlValue::Integer(probe_b),
                    SqlValue::Integer(round as i64),
                    SqlValue::Text(format!("probe-b-{round}")),
                ],
                vec![
                    SqlValue::Integer(probe_a),
                    SqlValue::Integer((round as i64) + 10_000),
                    SqlValue::Text(format!("probe-upd-{round}")),
                ],
            ],
        }
    );
    assert_eq!(
        db.execute(
            "DELETE FROM t WHERE id >= ? AND id <= ?",
            &[SqlValue::Integer(probe_b), SqlValue::Integer(probe_a)],
        )
        .unwrap(),
        SqlOutcome::Deleted { rows: 2 }
    );
}

#[test]
fn sql_crash_worker() {
    if std::env::var(ENV_ROLE).ok().as_deref() != Some("worker") {
        return;
    }
    let dir = PathBuf::from(std::env::var(ENV_DIR).expect("missing dir"));
    let backend = BackendKind::from_str(&std::env::var(ENV_BACKEND).expect("missing backend"));
    let seed: u64 = std::env::var(ENV_SEED)
        .expect("missing seed")
        .parse()
        .expect("invalid seed");
    let start_seq: u64 = std::env::var(ENV_START_SEQ)
        .expect("missing start_seq")
        .parse()
        .expect("invalid start_seq");

    let opened = open_backend(&dir, backend);
    let db = opened.sql();
    let stdout = io::stdout();

    for seq in start_seq.. {
        let plan = build_statement(seed, seq);
        db.execute(&plan.sql, &plan.params)
            .unwrap_or_else(|err| panic!("worker backend={backend:?} seq={seq} failed: {err}"));
        let line = format!("ACK {seq}\n");
        let mut out = stdout.lock();
        if out.write_all(line.as_bytes()).is_err() || out.flush().is_err() {
            break;
        }
    }
}

fn run_crash_drill(kind: BackendKind, seed: u64) {
    let dir = tempfile::tempdir().unwrap();
    init_backend(dir.path(), kind);

    let mut model = Model::new();
    let mut committed_seq: u64 = 0;
    let mut total_acked: u64 = 0;
    let mut in_flight_applied: usize = 0;
    let mut in_flight_absent: usize = 0;

    for round in 0..KILLS_PER_BACKEND {
        let start_seq = committed_seq + 1;
        let round_mix = mix(seed ^ 0xDEAD_BEEF, round as u64);
        let target_acks = 3 + (round_mix % 10); // 3..=12 ACKs before kill

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "sql_crash_worker", "--nocapture"])
            .env(ENV_ROLE, "worker")
            .env(ENV_DIR, dir.path())
            .env(ENV_BACKEND, kind.as_str())
            .env(ENV_SEED, seed.to_string())
            .env(ENV_START_SEQ, start_seq.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let child_stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel::<u64>();
        let reader = std::thread::spawn(move || {
            let mut buf_reader = BufReader::new(child_stdout);
            let mut line = String::new();
            loop {
                line.clear();
                match buf_reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        // Only accept complete newline-terminated ACK lines.
                        let seq = line
                            .ends_with('\n')
                            .then(|| line.trim_end().strip_prefix("ACK "))
                            .flatten()
                            .and_then(|rest| rest.parse::<u64>().ok());
                        if let Some(seq) = seq {
                            let _ = tx.send(seq);
                        }
                    }
                }
            }
        });

        let deadline = Instant::now() + Duration::from_secs(20);
        let mut acks_this_round: u64 = 0;
        let mut last_ack: u64 = committed_seq;

        while acks_this_round < target_acks {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            match rx.recv_timeout(remaining) {
                Ok(seq) => {
                    assert_eq!(
                        seq,
                        last_ack + 1,
                        "non-monotonic ACK from worker: expected {}, got {seq}",
                        last_ack + 1
                    );
                    last_ack = seq;
                    acks_this_round += 1;
                }
                Err(_) => {
                    let _ = child.kill();
                    let output = child.wait_with_output().unwrap();
                    let _ = reader.join();
                    panic!(
                        "worker timed out or exited early on backend={kind:?} round={round} \
                         (got {acks_this_round}/{target_acks} ACKs); stderr={}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }
        }

        // Vary kill timing deterministically across rounds (0us up to ~2.5ms) so SIGKILL
        // lands at different points inside the next in-flight statement.
        let jitter_us = match round % 5 {
            0 => 0,
            1 => 250,
            2 => 750,
            3 => 1_500,
            _ => 2_500,
        };
        if jitter_us > 0 {
            std::thread::sleep(Duration::from_micros(jitter_us));
        }

        let _ = child.kill();
        let _status = child.wait().unwrap();
        reader.join().unwrap();

        // Drain any complete ACK lines the worker already flushed to the pipe before SIGKILL.
        while let Ok(seq) = rx.try_recv() {
            assert_eq!(
                seq,
                last_ack + 1,
                "non-monotonic buffered ACK: expected {}, got {seq}",
                last_ack + 1
            );
            last_ack = seq;
            acks_this_round += 1;
        }

        total_acked += acks_this_round;

        for seq in start_seq..=last_ack {
            apply_to_model(&mut model, seed, seq);
        }
        let mut model_in_flight = model.clone();
        apply_to_model(&mut model_in_flight, seed, last_ack + 1);
        assert_ne!(
            model,
            model_in_flight,
            "in-flight statement {} must mutate model",
            last_ack + 1
        );

        let reopened = open_backend(dir.path(), kind);
        let db = reopened.sql();
        let observed = db.execute("SELECT id, a, b FROM t", &[]).unwrap();
        let status =
            verify_recovered_state(&observed, &model, &model_in_flight).unwrap_or_else(|err| {
                panic!(
                    "durability/atomicity violation on backend={kind:?} round={round} \
                     last_ack={last_ack}: {err}"
                )
            });

        match status {
            InFlightStatus::Absent => {
                in_flight_absent += 1;
                committed_seq = last_ack;
            }
            InFlightStatus::Applied => {
                in_flight_applied += 1;
                model = model_in_flight;
                committed_seq = last_ack + 1;
            }
        }

        // Verify catalog survival and post-reopen writability, then re-verify model state.
        verify_reopened_writability(&db, round);
        let after_probe = db.execute("SELECT id, a, b FROM t", &[]).unwrap();
        assert_eq!(
            verify_recovered_state(&after_probe, &model, &model),
            Ok(InFlightStatus::Absent)
        );
        drop(reopened);
    }

    assert_eq!(in_flight_applied + in_flight_absent, KILLS_PER_BACKEND);
    assert!(total_acked >= (KILLS_PER_BACKEND as u64) * 3);
    println!(
        "[sql_crash] backend={} kills={} total_acked={} final_committed_seq={} \
         live_rows={} in_flight_applied={} in_flight_absent={}",
        kind.as_str(),
        KILLS_PER_BACKEND,
        total_acked,
        committed_seq,
        model.len(),
        in_flight_applied,
        in_flight_absent,
    );
}

#[test]
fn sql_crash_ordinary_store() {
    run_crash_drill(BackendKind::Ordinary, ORDINARY_SEED);
}

#[test]
fn sql_crash_checkpoint_store() {
    run_crash_drill(BackendKind::Checkpoint, CHECKPOINT_SEED);
}

#[test]
fn crash_verifier_rejects_missing_ack_partial_in_flight_and_future_state() {
    fn to_outcome(model: &Model) -> SqlOutcome {
        SqlOutcome::Rows {
            columns: vec!["id".into(), "a".into(), "b".into()],
            rows: model
                .iter()
                .map(|(&id, (a, b))| {
                    vec![
                        SqlValue::Integer(id),
                        SqlValue::Integer(*a),
                        SqlValue::Text(b.clone()),
                    ]
                })
                .collect(),
        }
    }

    let seed = ORDINARY_SEED;
    let mut m_acked = Model::new();
    apply_to_model(&mut m_acked, seed, 1); // multi-row insert (>= 4 rows)
    apply_to_model(&mut m_acked, seed, 2); // multi-row insert
    apply_to_model(&mut m_acked, seed, 3); // single-row PK update

    // In-flight statement 4 is a multi-row range UPDATE over >= 5 rows.
    let mut m_in_flight = m_acked.clone();
    apply_to_model(&mut m_in_flight, seed, 4);

    // Future statement 5.
    let mut m_future = m_in_flight.clone();
    apply_to_model(&mut m_future, seed, 5);

    // Valid endpoints pass.
    assert_eq!(
        verify_recovered_state(&to_outcome(&m_acked), &m_acked, &m_in_flight),
        Ok(InFlightStatus::Absent)
    );
    assert_eq!(
        verify_recovered_state(&to_outcome(&m_in_flight), &m_acked, &m_in_flight),
        Ok(InFlightStatus::Applied)
    );

    // 1. Missing acknowledged row must fail.
    let mut missing_ack = m_acked.clone();
    let first_id = *missing_ack.keys().next().unwrap();
    missing_ack.remove(&first_id);
    assert!(verify_recovered_state(&to_outcome(&missing_ack), &m_acked, &m_in_flight).is_err());

    // 2. Partially applied multi-row UPDATE must fail.
    let mut partial_update = m_acked.clone();
    let first_updated = m_in_flight.get(&first_id).unwrap().clone();
    partial_update.insert(first_id, first_updated);
    assert!(verify_recovered_state(&to_outcome(&partial_update), &m_acked, &m_in_flight).is_err());

    // 3. Future statement (last_ack + 2) must fail.
    assert!(verify_recovered_state(&to_outcome(&m_future), &m_acked, &m_in_flight).is_err());

    // 4. Partially applied multi-row INSERT must fail.
    let m_empty = Model::new();
    let mut m_after_1 = Model::new();
    apply_to_model(&mut m_after_1, seed, 1);
    let mut partial_insert = m_after_1.clone();
    let remove_key = *partial_insert.keys().next().unwrap();
    partial_insert.remove(&remove_key);
    assert!(verify_recovered_state(&to_outcome(&partial_insert), &m_empty, &m_after_1).is_err());

    // 5. Out-of-order PK rows must fail.
    let mut swapped = to_outcome(&m_acked);
    if let SqlOutcome::Rows { ref mut rows, .. } = swapped {
        rows.swap(0, 1);
    }
    assert!(verify_recovered_state(&swapped, &m_acked, &m_in_flight).is_err());
}

const ENV_INDEX_CRASH_UNIQUE: &str = "EMDA_SQL_INDEX_CRASH_UNIQUE";

#[test]
fn sql_index_build_crash_worker() {
    if std::env::var(ENV_ROLE).ok().as_deref() != Some("index_worker") {
        return;
    }
    let dir = PathBuf::from(std::env::var(ENV_DIR).expect("missing dir"));
    let backend = BackendKind::from_str(&std::env::var(ENV_BACKEND).expect("missing backend"));
    let unique = std::env::var(ENV_INDEX_CRASH_UNIQUE).ok().as_deref() == Some("1");
    let opened = open_backend(&dir, backend);
    let db = opened.sql();

    let mut out = io::stdout();
    out.write_all(b"READY\n").unwrap();
    out.flush().unwrap();

    let sql = if unique {
        "CREATE UNIQUE INDEX idx_crash ON t (b)"
    } else {
        "CREATE INDEX idx_crash ON t (a)"
    };
    db.execute(sql, &[]).unwrap();
    let _ = out.write_all(b"DONE\n");
    let _ = out.flush();
}

fn run_index_build_crash_drill(kind: BackendKind) {
    for (resume_via_create, unique) in [(true, false), (false, true)] {
        let mut delay_ms = 25u64;
        let mut hit_mid_build = false;

        for attempt in 0..5 {
            let dir = tempfile::tempdir().unwrap();
            init_backend(dir.path(), kind);
            {
                let opened = open_backend(dir.path(), kind);
                let db = opened.sql();
                let total_rows = 4_000i64;
                for chunk in (0..total_rows).collect::<Vec<_>>().chunks(250) {
                    let placeholders = vec!["(?, ?, ?)"; chunk.len()].join(", ");
                    let params: Vec<SqlValue> = chunk
                        .iter()
                        .flat_map(|&i| {
                            [
                                SqlValue::Integer(i),
                                SqlValue::Integer(i % 50),
                                SqlValue::Text(format!("u-{i}")),
                            ]
                        })
                        .collect();
                    db.execute(&format!("INSERT INTO t VALUES {placeholders}"), &params)
                        .unwrap();
                }
            }

            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "sql_index_build_crash_worker", "--nocapture"])
                .env(ENV_ROLE, "index_worker")
                .env(ENV_DIR, dir.path())
                .env(ENV_BACKEND, kind.as_str())
                .env(ENV_INDEX_CRASH_UNIQUE, if unique { "1" } else { "0" })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();

            let child_stdout = child.stdout.take().unwrap();
            let mut reader = BufReader::new(child_stdout);
            let mut line = String::new();
            let mut saw_ready = false;
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if line.trim_end() == "READY" {
                            saw_ready = true;
                            break;
                        }
                    }
                }
            }
            if !saw_ready {
                let output = child.wait_with_output().unwrap();
                panic!(
                    "index worker failed to signal READY on {kind:?} attempt={attempt}: stderr={}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            std::thread::sleep(Duration::from_millis(delay_ms));
            let _ = child.kill();
            let _ = child.wait();

            let opened = open_backend(dir.path(), kind);
            let db = opened.sql();

            // Whether killed before build started, mid-build, or at completion,
            // readers must return exact pre-index rows and never rely on an incomplete index.
            let check_q = if unique {
                "SELECT id FROM t WHERE b = 'u-123'"
            } else {
                "SELECT id FROM t WHERE a = 23 ORDER BY id"
            };
            assert_eq!(
                db.execute(check_q, &[]).unwrap(),
                db.execute_force_full_scan_for_test(check_q, &[]).unwrap()
            );

            let jobs = db.document_database().index_jobs("t").unwrap();
            if !jobs.is_empty() {
                hit_mid_build = true;
                if resume_via_create {
                    assert_eq!(
                        db.execute("CREATE INDEX idx_crash ON t (a)", &[]).unwrap(),
                        SqlOutcome::IndexCreated
                    );
                    assert!(db.document_database().index_jobs("t").unwrap().is_empty());
                    assert_eq!(
                        db.execute(check_q, &[]).unwrap(),
                        db.execute_force_full_scan_for_test(check_q, &[]).unwrap()
                    );
                } else {
                    assert_eq!(
                        db.execute("DROP INDEX idx_crash ON t", &[]).unwrap(),
                        SqlOutcome::IndexDropped
                    );
                    assert!(db.document_database().index_jobs("t").unwrap().is_empty());
                    assert!(db.document_database().indexes("t").unwrap().is_empty());
                    assert_eq!(
                        db.execute(check_q, &[]).unwrap(),
                        db.execute_force_full_scan_for_test(check_q, &[]).unwrap()
                    );
                }
                break;
            } else if db.document_database().indexes("t").unwrap().is_empty() {
                // Killed before start_index_build committed; lengthen delay slightly next attempt.
                delay_ms = delay_ms.saturating_mul(2);
            } else {
                // Finished before kill; shorten delay next attempt.
                delay_ms = (delay_ms / 2).max(5);
            }
            if attempt == 4 && !hit_mid_build {
                panic!(
                    "failed to catch {kind:?} CREATE INDEX mid-build within 5 attempts \
                     (unique={unique}, last delay_ms={delay_ms})"
                );
            }
        }
        assert!(hit_mid_build);
    }
}

#[test]
fn sql_index_build_crash_ordinary_store() {
    run_index_build_crash_drill(BackendKind::Ordinary);
}

#[test]
fn sql_index_build_crash_checkpoint_store() {
    run_index_build_crash_drill(BackendKind::Checkpoint);
}
