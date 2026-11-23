//! Real-time serial-history checks for document, index and raw-key transactions.
use database_engine::{
    CancellationToken, CheckpointStore, CollectionSchema, ControlledTransactionError,
    DocumentDatabase, DocumentTransaction, DocumentTransactionMode, IndexDefinition, IndexQuery,
    IndexValue, Store, TimedTransactionError,
};
use serde_json::json;
use std::io::{self, ErrorKind};
use std::num::NonZeroU64;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use database_engine::RetainedExportLimits;
use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    AuthorizedRetainedStore, FileRetainedAuthority, IndexLimits, ManagedRetainedStore,
    OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixLimits,
};

mod support;
use support::retry_lock;

#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    amounts: [i64; 2],
    claim: Option<&'static str>,
}

const INITIAL_STATE: State = State {
    amounts: [3, 2],
    claim: None,
};

const RETAINED_LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 65_536,
        entries: 256,
        key_bytes: 8_192,
        work_bytes: 65_537,
    },
    index: IndexLimits {
        blocks: 4,
        anchor_bytes: 1_024,
    },
    suffix: SuffixLimits {
        bytes: 65_536,
        operations: 256,
        work: 65_536,
    },
    overlay: OverlayLimits {
        entries: 256,
        key_bytes: 8_192,
    },
};
const RETAINED_EXPORT: RetainedExportLimits = RetainedExportLimits {
    max_log_bytes: 65_536,
    max_live_keys: 256,
    max_live_key_bytes: 8_192,
    max_output_bytes: 65_536,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Observation {
    amounts: [i64; 2],
    mirror: Vec<u8>,
    balance_index: Vec<(String, i64)>,
    claim_index: Vec<String>,
}

impl State {
    fn observation(&self) -> Observation {
        let mut balance_index: Vec<(String, i64)> =
            vec![("a".into(), self.amounts[0]), ("b".into(), self.amounts[1])];
        balance_index.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        Observation {
            amounts: self.amounts,
            mirror: mirror(self.amounts),
            balance_index,
            claim_index: self.claim.into_iter().map(str::to_owned).collect(),
        }
    }
}

fn mirror(amounts: [i64; 2]) -> Vec<u8> {
    format!("{},{}", amounts[0], amounts[1]).into_bytes()
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Transfer(usize),
    Reset,
    Observe,
    Claim(&'static str),
    Release,
}

const OPERATIONS: [Operation; 6] = [
    Operation::Transfer(0),
    Operation::Transfer(1),
    Operation::Reset,
    Operation::Observe,
    Operation::Claim("c"),
    Operation::Claim("d"),
];
const CHAINED_OPERATIONS: [Operation; 7] = [
    OPERATIONS[0],
    OPERATIONS[1],
    OPERATIONS[2],
    OPERATIONS[3],
    OPERATIONS[4],
    OPERATIONS[5],
    Operation::Release,
];

#[derive(Debug)]
enum Outcome {
    Conflict,
    Busy,
    Duplicate,
    Cancelled,
    Write(Observation),
    Read(Observation),
    Claimed,
}

#[derive(Debug)]
struct Event {
    operation: Operation,
    outcome: Outcome,
    start: usize,
    end: usize,
}

fn serializes(events: &[Event], final_state: &State) -> bool {
    serializes_from(&INITIAL_STATE, events, final_state)
}

fn serializes_from(initial: &State, events: &[Event], final_state: &State) -> bool {
    fn search(events: &[Event], final_state: &State, used: u64, state: State) -> bool {
        if used.count_ones() as usize == events.len() {
            return &state == final_state;
        }
        for (index, event) in events.iter().enumerate() {
            let bit = 1_u64 << index;
            if used & bit != 0
                || events.iter().enumerate().any(|(other, predecessor)| {
                    other != index && used & (1 << other) == 0 && predecessor.end < event.start
                })
            {
                continue;
            }
            let mut next = state.clone();
            match (&event.operation, &event.outcome) {
                (_, Outcome::Conflict | Outcome::Busy | Outcome::Cancelled) => {}
                (Operation::Claim(_), Outcome::Duplicate) if state.claim.is_some() => {}
                (Operation::Claim(id), Outcome::Claimed) if state.claim.is_none() => {
                    next.claim = Some(id);
                }
                (Operation::Observe, Outcome::Read(observed))
                    if *observed == state.observation() => {}
                (Operation::Transfer(from), Outcome::Write(observed))
                    if *observed == state.observation() =>
                {
                    if next.amounts[*from] > 0 {
                        next.amounts[*from] -= 1;
                        next.amounts[1 - *from] += 1;
                    }
                }
                (Operation::Reset, Outcome::Write(observed))
                    if *observed == state.observation() =>
                {
                    next.amounts = [4, 6];
                }
                (Operation::Release, Outcome::Write(observed))
                    if *observed == state.observation() =>
                {
                    next.claim = None;
                }
                _ => continue,
            }
            if search(events, final_state, used | bit, next) {
                return true;
            }
        }
        false
    }
    search(events, final_state, 0, initial.clone())
}

fn observe(tx: &DocumentTransaction<'_, '_>) -> io::Result<Observation> {
    let a = tx.get("accounts", "a")?.expect("account a");
    let b = tx.get("accounts", "b")?.expect("account b");
    let amounts = [
        a["balance"].as_i64().unwrap(),
        b["balance"].as_i64().unwrap(),
    ];
    let raw = tx.get_raw(b"mirror")?.expect("raw mirror");
    let balance_index = tx
        .query_index("accounts", "balance", &IndexQuery::default())?
        .documents
        .into_iter()
        .map(|entry| (entry.id, entry.document["balance"].as_i64().unwrap()))
        .collect();
    let claim_index = tx
        .query_index(
            "accounts",
            "handle",
            &IndexQuery {
                equal: vec![IndexValue::String("claim".into())],
                ..Default::default()
            },
        )?
        .documents
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    Ok(Observation {
        amounts,
        mirror: raw,
        balance_index,
        claim_index,
    })
}

fn write_amounts(tx: &mut DocumentTransaction<'_, '_>, amounts: [i64; 2]) -> io::Result<()> {
    tx.replace(
        "accounts",
        "a",
        &json!({"handle":"a", "balance":amounts[0]}),
        false,
    )?;
    std::thread::yield_now();
    tx.replace(
        "accounts",
        "b",
        &json!({"handle":"b", "balance":amounts[1]}),
        false,
    )?;
    tx.put_raw(b"mirror", &mirror(amounts))
}

fn execute(
    db: &DocumentDatabase<'_>,
    operation: Operation,
    mode: usize,
    retained: bool,
) -> Outcome {
    let action = |tx: &mut DocumentTransaction<'_, '_>| match operation {
        Operation::Observe => observe(tx).map(Outcome::Read),
        Operation::Claim(id) => {
            std::thread::yield_now();
            tx.insert("accounts", id, &json!({"handle":"claim"}))?;
            Ok(Outcome::Claimed)
        }
        Operation::Release => {
            let before = observe(tx)?;
            if let Some(id) = before.claim_index.first() {
                assert!(tx.delete("accounts", id)?);
            }
            Ok(Outcome::Write(before))
        }
        Operation::Transfer(from) => {
            let before = observe(tx)?;
            let mut amounts = before.amounts;
            if amounts[from] > 0 {
                amounts[from] -= 1;
                amounts[1 - from] += 1;
            }
            write_amounts(tx, amounts)?;
            Ok(Outcome::Write(before))
        }
        Operation::Reset => {
            let before = observe(tx)?;
            write_amounts(tx, [4, 6])?;
            Ok(Outcome::Write(before))
        }
    };
    let result = match mode % 6 {
        0 => db
            .transaction(action)
            .map_err(ControlledTransactionError::Operation),
        1 => db
            .transaction_with_read_set(action)
            .map_err(ControlledTransactionError::Operation),
        2 => db
            .transaction_until(
                Instant::now() + Duration::from_secs(30),
                DocumentTransactionMode::ReadSetFifo,
                action,
            )
            .map_err(|error| match error {
                TimedTransactionError::Operation(error) => {
                    ControlledTransactionError::Operation(error)
                }
                TimedTransactionError::DeadlineExceeded => panic!("model deadline expired"),
            }),
        3 => {
            let token = CancellationToken::new();
            token.cancel();
            db.transaction_controlled(&token, None, DocumentTransactionMode::Conservative, action)
        }
        4 => {
            let token = CancellationToken::new();
            db.transaction_controlled(&token, None, DocumentTransactionMode::ReadSetFifo, |tx| {
                let outcome = action(tx)?;
                token.cancel();
                Ok(outcome)
            })
        }
        _ => std::thread::scope(|scope| {
            let token = CancellationToken::new();
            let cancelling = token.clone();
            let worker = scope.spawn(move || {
                std::thread::yield_now();
                cancelling.cancel();
            });
            let result =
                db.transaction_controlled(&token, None, DocumentTransactionMode::ReadSet, action);
            worker.join().unwrap();
            result
        }),
    };
    match result {
        Ok(outcome) => outcome,
        Err(ControlledTransactionError::Cancelled) => Outcome::Cancelled,
        Err(ControlledTransactionError::Operation(error))
            if database_engine::is_transaction_conflict(&error) =>
        {
            Outcome::Conflict
        }
        Err(ControlledTransactionError::Operation(error))
            if retained
                && error.kind() == ErrorKind::WouldBlock
                && error.to_string() == "retained document transaction is already active" =>
        {
            Outcome::Busy
        }
        Err(ControlledTransactionError::Operation(error))
            if error.kind() == ErrorKind::AlreadyExists =>
        {
            Outcome::Duplicate
        }
        Err(error) => panic!("unexpected {operation:?} error: {error}"),
    }
}

fn initialize(db: &DocumentDatabase<'_>) {
    db.create_collection("accounts", CollectionSchema::default())
        .unwrap();
    for (name, unique) in [("balance", false), ("handle", true)] {
        db.create_index(
            "accounts",
            IndexDefinition {
                name: name.into(),
                fields: vec![vec![name.into()]],
                unique,
                partial: None,
            },
        )
        .unwrap();
    }
    db.insert("accounts", "a", &json!({"handle":"a", "balance":3}))
        .unwrap();
    db.insert("accounts", "b", &json!({"handle":"b", "balance":2}))
        .unwrap();
    db.transaction(|tx| tx.put_raw(b"mirror", &mirror([3, 2])))
        .unwrap();
}

fn reset(db: &DocumentDatabase<'_>) {
    db.transaction(|tx| {
        tx.delete("accounts", "c")?;
        tx.delete("accounts", "d")?;
        write_amounts(tx, [3, 2])
    })
    .unwrap();
}

fn read_state(db: &DocumentDatabase<'_>) -> State {
    let observed = db.transaction(|tx| observe(tx)).unwrap();
    let claim = match observed.claim_index.as_slice() {
        [] => None,
        [id] if id == "c" => Some("c"),
        [id] if id == "d" => Some("d"),
        other => panic!("invalid unique index: {other:?}"),
    };
    let state = State {
        amounts: observed.amounts,
        claim,
    };
    assert_eq!(observed, state.observation());
    let integrity = db.verify_collection("accounts").unwrap();
    assert_eq!(integrity.documents, 2 + usize::from(claim.is_some()));
    state
}

#[test]
fn history_checker_rejects_fractured_raw_or_index_data() {
    let state = State {
        amounts: [3, 2],
        claim: None,
    };
    let mut invalid = state.observation();
    invalid.mirror = b"2,3".to_vec();
    let events = [Event {
        operation: Operation::Observe,
        outcome: Outcome::Read(invalid),
        start: 0,
        end: 1,
    }];
    assert!(!serializes(&events, &state));
    let mut invalid = state.observation();
    invalid.balance_index.swap(0, 1);
    let events = [Event {
        operation: Operation::Observe,
        outcome: Outcome::Read(invalid),
        start: 0,
        end: 1,
    }];
    assert!(!serializes(&events, &state));
}

#[test]
fn history_checker_rejects_duplicate_unique_claims_and_lost_writes() {
    let state = State {
        amounts: [3, 2],
        claim: Some("c"),
    };
    let claims = [
        Event {
            operation: Operation::Claim("c"),
            outcome: Outcome::Claimed,
            start: 0,
            end: 3,
        },
        Event {
            operation: Operation::Claim("d"),
            outcome: Outcome::Claimed,
            start: 1,
            end: 2,
        },
    ];
    assert!(!serializes(&claims, &state));
    let start = State {
        amounts: [3, 2],
        claim: None,
    };
    let transfer = [Event {
        operation: Operation::Transfer(0),
        outcome: Outcome::Write(start.observation()),
        start: 0,
        end: 1,
    }];
    assert!(!serializes(&transfer, &start));
}

#[test]
fn history_checker_rejects_changes_attributed_only_to_cancelled_work() {
    let cancelled = [Event {
        operation: Operation::Reset,
        outcome: Outcome::Cancelled,
        start: 0,
        end: 1,
    }];
    assert!(serializes(
        &cancelled,
        &State {
            amounts: [3, 2],
            claim: None
        }
    ));
    assert!(!serializes(
        &cancelled,
        &State {
            amounts: [4, 6],
            claim: None
        }
    ));
}

#[test]
fn chained_checker_requires_recovered_unique_claim_before_release() {
    let initial = State {
        claim: Some("c"),
        ..INITIAL_STATE
    };
    let final_state = State {
        claim: Some("d"),
        ..INITIAL_STATE
    };
    let mut events = [
        Event {
            operation: Operation::Release,
            outcome: Outcome::Write(initial.observation()),
            start: 0,
            end: 1,
        },
        Event {
            operation: Operation::Claim("d"),
            outcome: Outcome::Claimed,
            start: 2,
            end: 3,
        },
    ];
    assert!(serializes_from(&initial, &events, &final_state));
    assert!(!serializes(&events, &final_state));
    events[0].outcome = Outcome::Cancelled;
    assert!(!serializes_from(&initial, &events, &final_state));
}

enum TestStore {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl TestStore {
    fn database(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }
}

fn record_history(
    db: &DocumentDatabase<'_>,
    iteration: usize,
    retained: bool,
    operations: &[Operation],
) -> Vec<Event> {
    let barrier = Barrier::new(operations.len() + 1);
    let clock = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for offset in 0..operations.len() {
            let operation = operations[(offset + iteration) % operations.len()];
            let (db, barrier, clock) = (db, &barrier, &clock);
            handles.push(scope.spawn(move || {
                barrier.wait();
                if (iteration + offset) % 3 == 0 {
                    std::thread::yield_now();
                }
                let start = clock.fetch_add(1, Ordering::SeqCst);
                // Vary mode independently of operation across iterations.
                let outcome = execute(db, operation, iteration * 2 + offset, retained);
                let end = clock.fetch_add(1, Ordering::SeqCst);
                Event {
                    operation,
                    outcome,
                    start,
                    end,
                }
            }));
        }
        barrier.wait();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    })
}

#[test]
fn concurrent_document_histories_serialize_and_recover_on_both_backends() {
    let histories = std::env::var("EMDA_DOCUMENT_MODEL_HISTORIES")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(128);
    assert!((1..=8192).contains(&histories));
    for checkpoint_mode in [false, true] {
        let mut cancelled = 0_usize;
        let mut completed = 0_usize;
        let mut rejected = 0_usize;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = retry_lock(|| Store::open(&path)).unwrap();
        initialize(&DocumentDatabase::new(&initial));
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        drop(initial);
        for iteration in 0..histories {
            let backend = if checkpoint_mode {
                TestStore::Checkpoint(
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap(),
                )
            } else {
                TestStore::Ordinary(retry_lock(|| Store::open(&path)).unwrap())
            };
            let db = backend.database();
            reset(&db);
            let events = record_history(&db, iteration, false, &OPERATIONS);
            let expected = read_state(&db);
            for event in &events {
                match &event.outcome {
                    Outcome::Cancelled => cancelled += 1,
                    Outcome::Conflict | Outcome::Busy | Outcome::Duplicate => rejected += 1,
                    _ => completed += 1,
                }
            }
            assert!(
                serializes(&events, &expected),
                "backend={checkpoint_mode} iteration={iteration} history={events:?} final={expected:?}"
            );
            drop(backend);
            let recovered = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                read_state(&DocumentDatabase::new(&recovered)),
                expected,
                "backend={checkpoint_mode} iteration={iteration} recovery"
            );
        }
        assert!(
            cancelled >= histories,
            "pre-cancelled operations must abort"
        );
        println!(
            "checkpoint={checkpoint_mode} histories={histories} cancelled={cancelled} completed={completed} rejected={rejected}"
        );
    }
}

#[test]
fn chained_document_histories_alternate_writable_recovery_backends() {
    let windows = std::env::var("EMDA_CHAINED_DOCUMENT_MODEL_WINDOWS")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(48);
    assert!((1..=4096).contains(&windows));

    for force_v2 in [false, true] {
        if force_v2 && !cfg!(target_os = "linux") {
            continue;
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let mut checkpoint = root.path().join("base");
        if force_v2 {
            std::fs::create_dir(&path).unwrap();
            std::fs::write(
                path.join("records.log"),
                include_bytes!("fixtures/v2-empty-records.log"),
            )
            .unwrap();
            std::fs::write(
                path.join("commit.state"),
                include_bytes!("fixtures/v2-empty-commit.state"),
            )
            .unwrap();
        }
        let ordinary = retry_lock(|| Store::open(&path)).unwrap();
        initialize(&DocumentDatabase::new(&ordinary));
        let mut digest = ordinary.checkpoint_to(&checkpoint).unwrap();
        drop(ordinary);
        let header = std::fs::read(path.join("records.log")).unwrap();
        let format = std::str::from_utf8(&header[..8]).unwrap();

        let mut expected = INITIAL_STATE;
        let (mut claims, mut releases, mut refreshes) = (0, 0, 0);
        for window in 0..windows {
            let checkpoint_writer = window % 2 != 0;
            let backend = if checkpoint_writer {
                TestStore::Checkpoint(
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap(),
                )
            } else {
                TestStore::Ordinary(retry_lock(|| Store::open(&path)).unwrap())
            };
            let db = backend.database();
            assert_eq!(read_state(&db), expected, "{format} window {window} start");
            let events = record_history(&db, window, false, &CHAINED_OPERATIONS);
            let observed = read_state(&db);
            assert!(
                serializes_from(&expected, &events, &observed),
                "{format} checkpoint_writer={checkpoint_writer} window={window} initial={expected:?} events={events:?} final={observed:?}"
            );
            for event in &events {
                match (&event.operation, &event.outcome) {
                    (Operation::Claim(_), Outcome::Claimed) => claims += 1,
                    (Operation::Release, Outcome::Write(before))
                        if !before.claim_index.is_empty() =>
                    {
                        releases += 1;
                    }
                    _ => {}
                }
            }
            expected = observed;
            drop(backend);

            let recovered =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            assert_eq!(
                read_state(&DocumentDatabase::with_checkpoint_store(&recovered)),
                expected,
                "{format} window {window} checkpoint recovery"
            );
            drop(recovered);
            let ordinary = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                read_state(&DocumentDatabase::new(&ordinary)),
                expected,
                "{format} window {window} full recovery"
            );
            if (window + 1) % 16 == 0 {
                checkpoint = root.path().join(format!("base-{window}"));
                digest = ordinary.checkpoint_to(&checkpoint).unwrap();
                refreshes += 1;
            }
        }
        println!(
            "format={format} windows={windows} operations={} recovery_checks={} claims={claims} releases={releases} checkpoint_refreshes={refreshes}",
            windows * CHAINED_OPERATIONS.len(),
            windows * 2,
        );
    }
}

#[test]
fn retained_document_histories_serialize_and_recover_across_reclamation() {
    let histories = std::env::var("EMDA_RETAINED_DOCUMENT_MODEL_HISTORIES")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(48);
    assert!((1..=8192).contains(&histories));
    let limits = RETAINED_LIMITS;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let catalog = root.path().join("catalog");
    let initial = retry_lock(|| Store::open(&source)).unwrap();
    initialize(&DocumentDatabase::new(&initial));
    ManagedRetainedStore::copy_from_store(initial, &source, &catalog, RETAINED_EXPORT, limits)
        .unwrap();

    let mut backend =
        ManagedRetainedStore::open_with_auto_reclaim(&catalog, limits, NonZeroU64::MIN).unwrap();
    let initial_generation = backend.generation_id().unwrap();
    backend.put(b"mirror", &mirror([3, 2])).unwrap();
    drop(backend);

    let (mut cancelled, mut busy, mut completed, mut rotations) = (0, 0, 0, 0);
    let mut generation = initial_generation;
    for iteration in 0..histories {
        let mut backend =
            ManagedRetainedStore::open_with_auto_reclaim(&catalog, limits, NonZeroU64::MIN)
                .unwrap();
        // A round can otherwise consist entirely of no-op, read, or rejected
        // transactions. Persist an epoch before the race to force rotation.
        let epoch = (iteration as u64).to_le_bytes();
        backend.put(b"model-epoch", &epoch).unwrap();
        let expected = {
            let db = DocumentDatabase::with_managed_retained_store(&mut backend);
            reset(&db);
            let events = record_history(&db, iteration, true, &OPERATIONS);
            let expected = read_state(&db);
            assert!(
                serializes(&events, &expected),
                "retained iteration={iteration} history={events:?} final={expected:?}"
            );
            for event in events {
                match event.outcome {
                    Outcome::Cancelled => cancelled += 1,
                    Outcome::Busy => busy += 1,
                    Outcome::Duplicate | Outcome::Conflict => {}
                    _ => completed += 1,
                }
            }
            expected
        };
        let next = backend.generation_id().unwrap();
        rotations += usize::from(next != generation);
        generation = next;
        drop(backend);
        let mut recovered = ManagedRetainedStore::open(&catalog, limits).unwrap();
        assert_eq!(
            recovered
                .get(b"model-epoch", epoch.len())
                .unwrap()
                .as_deref(),
            Some(epoch.as_slice()),
            "retained iteration={iteration} recovered epoch"
        );
        let db = DocumentDatabase::with_managed_retained_store(&mut recovered);
        assert_eq!(
            read_state(&db),
            expected,
            "retained iteration={iteration} recovery"
        );
    }
    assert_eq!(rotations, histories);
    assert!(cancelled >= histories);
    println!(
        "retained histories={histories} cancelled={cancelled} completed={completed} busy={busy} rotation_histories={rotations}"
    );
}

#[test]
fn authorized_document_histories_chain_across_rotation_and_reopen() {
    let windows = std::env::var("EMDA_AUTHORIZED_DOCUMENT_MODEL_WINDOWS")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(48);
    assert!((1..=4096).contains(&windows));
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let catalog = root.path().join("catalog");
    let authority = root.path().join("authority");
    let store = retry_lock(|| Store::open(&source)).unwrap();
    initialize(&DocumentDatabase::new(&store));
    let prepared = AuthorizedRetainedStore::<FileRetainedAuthority>::prepare_from_store(
        store,
        &source,
        &catalog,
        RETAINED_EXPORT,
        RETAINED_LIMITS,
    )
    .unwrap();
    let mut observer = FileRetainedAuthority::create(&authority).unwrap();
    assert!(observer.compare_and_swap(None, prepared).unwrap());
    let open = || {
        AuthorizedRetainedStore::open_with_auto_reclaim(
            &catalog,
            RETAINED_LIMITS,
            FileRetainedAuthority::open(&authority).unwrap(),
            NonZeroU64::MIN,
        )
        .unwrap()
    };
    let mut backend = open();
    backend.put(b"model-epoch", b"seed").unwrap();
    let mut generation = prepared.committed.generation;
    let mut expected = INITIAL_STATE;
    let (mut cancelled, mut busy, mut completed, mut claims, mut releases) = (0, 0, 0, 0, 0);
    for window in 0..windows {
        let epoch = (window as u64).to_le_bytes();
        backend.put(b"model-epoch", &epoch).unwrap();
        {
            let db = DocumentDatabase::with_authorized_retained_store(&mut backend);
            assert_eq!(
                read_state(&db),
                expected,
                "authorized window={window} start"
            );
            let events = record_history(&db, window, true, &CHAINED_OPERATIONS);
            let observed = read_state(&db);
            assert!(
                serializes_from(&expected, &events, &observed),
                "authorized window={window} initial={expected:?} events={events:?} final={observed:?}"
            );
            expected = observed;
            for event in events {
                match (event.operation, event.outcome) {
                    (_, Outcome::Cancelled) => cancelled += 1,
                    (_, Outcome::Busy) => busy += 1,
                    (_, Outcome::Duplicate | Outcome::Conflict) => {}
                    (Operation::Claim(_), Outcome::Claimed) => {
                        completed += 1;
                        claims += 1;
                    }
                    (Operation::Release, Outcome::Write(before))
                        if !before.claim_index.is_empty() =>
                    {
                        completed += 1;
                        releases += 1;
                    }
                    _ => completed += 1,
                }
            }
        }
        let committed = observer.read().unwrap().unwrap();
        assert!(committed.pending.is_none());
        assert_ne!(committed.committed.generation, generation);
        generation = committed.committed.generation;
        drop(backend);
        backend = open();
        let reopened = observer.read().unwrap().unwrap();
        assert!(reopened.pending.is_none());
        assert_eq!(reopened.committed, committed.committed);
        assert_eq!(
            backend.get(b"model-epoch", epoch.len()).unwrap().as_deref(),
            Some(epoch.as_slice()),
            "authorized window={window} recovered epoch"
        );
        assert_eq!(
            read_state(&DocumentDatabase::with_authorized_retained_store(
                &mut backend
            )),
            expected,
            "authorized window={window} recovery"
        );
    }
    assert!(cancelled >= windows);
    println!(
        "authorized windows={windows} calls={} reopens={windows} rotation_windows={windows} cancelled={cancelled} busy={busy} completed={completed} claims={claims} releases={releases}",
        windows * CHAINED_OPERATIONS.len(),
    );
}
