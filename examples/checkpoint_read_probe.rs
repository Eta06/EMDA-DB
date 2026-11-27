//! Focused probe for checkpoint-backed read, indexed query, and full-scan costs.
//!
//! usage: checkpoint_read_probe <directory> <rows> <ops>
use database_engine::{
    CheckpointStore, DocumentDatabase, DocumentTransactionMode, IndexBound, IndexQuery, IndexValue,
    MAX_SCAN_ENTRIES, SqlDatabase, SqlOutcome, SqlValue, Store,
};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct Samples(Vec<f64>);

impl Samples {
    fn summary(&mut self) -> (f64, f64, f64, f64) {
        self.0.sort_by(f64::total_cmp);
        let at = |q: f64| self.0[((self.0.len() - 1) as f64 * q).round() as usize];
        let avg = self.0.iter().sum::<f64>() / self.0.len() as f64;
        (avg, at(0.5), at(0.99), *self.0.last().unwrap())
    }

    fn json(mut self) -> String {
        let (avg, p50, p99, max) = self.summary();
        format!(
            "{{\"n\":{},\"avg_us\":{avg:.1},\"p50_us\":{p50:.1},\"p99_us\":{p99:.1},\"max_us\":{max:.1}}}",
            self.0.len()
        )
    }
}

fn time<T>(samples: &mut Vec<f64>, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let start = Instant::now();
    let out = f()?;
    samples.push(start.elapsed().as_secs_f64() * 1e6);
    Ok(out)
}

fn indexed_row_params(id: i64) -> Vec<SqlValue> {
    vec![
        SqlValue::Integer(id),
        SqlValue::Integer(id * 3 + 7),
        SqlValue::Text(format!("tag-{:04}", id % 200)),
        SqlValue::Boolean(id % 2 == 0),
        SqlValue::Real(id as f64 / 4.0),
    ]
}

fn exec_count(db: &SqlDatabase<'_>, sql: &str, params: &[SqlValue]) -> Result<usize> {
    Ok(match db.execute(sql, params)? {
        SqlOutcome::Rows { rows, .. } => rows.len(),
        SqlOutcome::Inserted { rows }
        | SqlOutcome::Updated { rows }
        | SqlOutcome::Deleted { rows } => rows,
        SqlOutcome::TableCreated | SqlOutcome::IndexCreated | SqlOutcome::IndexDropped => 0,
    })
}

fn populate_indexed_tables(db: &SqlDatabase<'_>, rows: i64) -> Result<()> {
    exec_count(
        db,
        "CREATE TABLE items1 (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, tag TEXT NOT NULL, active BOOLEAN NOT NULL, score REAL)",
        &[],
    )?;
    exec_count(db, "CREATE INDEX idx1_code ON items1 (code)", &[])?;
    let tuple = "(?, ?, ?, ?, ?)";
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        let sql = format!(
            "INSERT INTO items1 VALUES {}",
            vec![tuple; chunk.len()].join(", ")
        );
        let params = chunk
            .iter()
            .flat_map(|id| indexed_row_params(*id))
            .collect::<Vec<_>>();
        exec_count(db, &sql, &params)?;
    }

    exec_count(
        db,
        "CREATE TABLE items3 (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, tag TEXT NOT NULL, active BOOLEAN NOT NULL, score REAL)",
        &[],
    )?;
    exec_count(db, "CREATE INDEX idx3_code ON items3 (code)", &[])?;
    exec_count(db, "CREATE INDEX idx3_tag ON items3 (tag)", &[])?;
    exec_count(db, "CREATE INDEX idx3_active ON items3 (active)", &[])?;
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        let sql = format!(
            "INSERT INTO items3 VALUES {}",
            vec![tuple; chunk.len()].join(", ")
        );
        let params = chunk
            .iter()
            .flat_map(|id| indexed_row_params(*id))
            .collect::<Vec<_>>();
        exec_count(db, &sql, &params)?;
    }
    Ok(())
}

/// Reproduce the exact `sql_bench` `emda-checkpoint-indexed` loop (interleaved
/// eq + range + insert_1idx + insert_3idx with background worker) to diagnose
/// why sql_bench saw ~735 us eq / ~3480 us range.
fn measure_sql_bench_exact_repro(
    root: &Path,
    rows: i64,
    ops: i64,
    label: &str,
    cache_limit: usize,
    with_worker: bool,
    with_inserts: bool,
) -> Result<()> {
    let log_dir = root.join(format!("repro-{label}"));
    let base_ckpt = root.join(format!("repro-{label}-base"));
    drop(Store::open(&log_dir)?);
    CheckpointStore::rebuild_managed_selection(&log_dir, &base_ckpt)?;
    let store = Arc::new(CheckpointStore::open_managed(&log_dir)?);
    store.set_checkpoint_block_cache_limit(cache_limit)?;
    store.enable_automatic_checkpoints()?;
    let worker = store.start_checkpoint_worker(Duration::from_millis(5), 256 * 1024)?;

    let sql = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
    populate_indexed_tables(&sql, rows)?;
    let worker = if with_worker {
        Some(worker)
    } else {
        worker.stop()?;
        None
    };

    let eq_sql = "SELECT id, code, tag, active, score FROM items1 WHERE code = ?";
    let range_sql = "SELECT id, code, tag, active, score FROM items1 WHERE code >= ? AND code < ?";
    let ins1_sql = "INSERT INTO items1 VALUES (?, ?, ?, ?, ?)";
    let ins3_sql = "INSERT INTO items3 VALUES (?, ?, ?, ?, ?)";

    let mem_before = store.checkpoint_memory_status()?;
    let (mut idx_eq, mut idx_range, mut ins_1idx, mut ins_3idx) = (vec![], vec![], vec![], vec![]);
    let mut sha_after_ins = Vec::new();
    let sample_buf = vec![0x42u8; 32 * 1024];

    for op in 0..ops {
        let id = (op * 7919) % rows;
        let target_code = id * 3 + 7;
        time(&mut idx_eq, || {
            assert_eq!(
                exec_count(&sql, eq_sql, &[SqlValue::Integer(target_code)])?,
                1
            );
            Ok(())
        })?;
        let lo = ((op * 1049) % (rows - 10).max(1)) * 3 + 7;
        let hi = lo + 30;
        time(&mut idx_range, || {
            assert!(
                exec_count(
                    &sql,
                    range_sql,
                    &[SqlValue::Integer(lo), SqlValue::Integer(hi)]
                )? > 0
            );
            Ok(())
        })?;
        if with_inserts {
            time(&mut ins_1idx, || {
                exec_count(&sql, ins1_sql, &indexed_row_params(rows + op))?;
                Ok(())
            })?;
            time(&mut ins_3idx, || {
                exec_count(&sql, ins3_sql, &indexed_row_params(rows + op))?;
                Ok(())
            })?;
            time(&mut sha_after_ins, || {
                std::hint::black_box(Sha256::digest(&sample_buf));
                Ok(())
            })?;
        }
    }

    let mem_after = store.checkpoint_memory_status()?;
    let worker_ckpts = match worker {
        Some(w) => w.stop()?.checkpoints,
        None => 0,
    };

    println!(
        "{{\"phase\":\"sql_bench_repro\",\"label\":\"{label}\",\"cache_limit\":{cache_limit},\
\"with_worker\":{with_worker},\"with_inserts\":{with_inserts},\
\"overlay_before\":{},\"overlay_after\":{},\"worker_ckpts\":{worker_ckpts},\
\"hits_delta\":{},\"misses_delta\":{},\"cached_bytes\":{},\
\"idx_eq\":{},\"idx_range_10\":{},\"ins1\":{},\"ins3\":{},\"sha32k_post_ins\":{}}}",
        mem_before.overlay_keys,
        mem_after.overlay_keys,
        mem_after
            .block_cache
            .hits
            .saturating_sub(mem_before.block_cache.hits),
        mem_after
            .block_cache
            .misses
            .saturating_sub(mem_before.block_cache.misses),
        mem_after.block_cache.cached_bytes,
        Samples(idx_eq).json(),
        Samples(idx_range).json(),
        if ins_1idx.is_empty() {
            "null".to_string()
        } else {
            Samples(ins_1idx).json()
        },
        if ins_3idx.is_empty() {
            "null".to_string()
        } else {
            Samples(ins_3idx).json()
        },
        if sha_after_ins.is_empty() {
            "null".to_string()
        } else {
            Samples(sha_after_ins).json()
        },
    );
    Ok(())
}

fn measure_checkpoint_indexed(root: &Path, rows: i64, ops: i64) -> Result<()> {
    let log_dir = root.join("ckpt-indexed");
    let base_ckpt = root.join("ckpt-indexed-base");
    drop(Store::open(&log_dir)?);
    CheckpointStore::rebuild_managed_selection(&log_dir, &base_ckpt)?;
    let store = Arc::new(CheckpointStore::open_managed(&log_dir)?);
    store.enable_automatic_checkpoints()?;
    let worker = store.start_checkpoint_worker(Duration::from_millis(5), 256 * 1024)?;

    {
        let sql = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
        populate_indexed_tables(&sql, rows)?;
    }
    worker.stop()?;

    let final_ckpt = root.join("ckpt-indexed-final");
    store.disable_automatic_checkpoints()?;
    store.rollover_managed(&final_ckpt)?;

    let ckpt_len = std::fs::metadata(&final_ckpt)?.len();
    let log_len = std::fs::metadata(log_dir.join("records.log"))?.len();
    let mem = store.checkpoint_memory_status()?;
    println!(
        "{{\"phase\":\"geometry\",\"rows\":{rows},\"checkpoint_bytes\":{ckpt_len},\"log_bytes\":{log_len},\"directory_bytes\":{},\"overlay_keys\":{}}}",
        mem.directory_bytes, mem.overlay_keys
    );
    drop(store);

    let eq_sql = "SELECT id, code, tag, active, score FROM items1 WHERE code = ?";
    let range_sql = "SELECT id, code, tag, active, score FROM items1 WHERE code >= ? AND code < ?";
    let pk_sql = "SELECT id, code, tag, active, score FROM items1 WHERE id = ?";
    let full_sql = "SELECT id FROM items1 WHERE score >= ? AND active = TRUE";

    for &(label, cache_limit) in &[
        ("cache_0_off", 0usize),
        ("cache_1_count_misses", 1usize),
        ("cache_4m", 4 * 1024 * 1024),
        ("cache_8m", 8 * 1024 * 1024),
    ] {
        let fresh = CheckpointStore::open_managed(&log_dir)?;
        fresh.set_checkpoint_block_cache_limit(cache_limit)?;
        let doc_db = DocumentDatabase::with_checkpoint_store(&fresh);
        let sql_db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&fresh));

        let _ = exec_count(&sql_db, eq_sql, &[SqlValue::Integer(7)])?;
        let _ = exec_count(
            &sql_db,
            range_sql,
            &[SqlValue::Integer(7), SqlValue::Integer(37)],
        )?;
        let _ = exec_count(&sql_db, pk_sql, &[SqlValue::Integer(0)])?;

        let snap_stats = || -> Result<(u64, u64, usize, usize)> {
            let s = fresh.checkpoint_memory_status()?.block_cache;
            Ok((s.hits, s.misses, s.cached_bytes, s.cached_blocks))
        };

        let (h0, m0, _, _) = snap_stats()?;
        let mut s_pk = Vec::new();
        for op in 0..ops {
            let id = (op * 7919) % rows;
            time(&mut s_pk, || {
                assert_eq!(exec_count(&sql_db, pk_sql, &[SqlValue::Integer(id)])?, 1);
                Ok(())
            })?;
        }
        let (h1, m1, _, _) = snap_stats()?;

        let mut s_eq = Vec::new();
        for op in 0..ops {
            let id = (op * 7919) % rows;
            let code = id * 3 + 7;
            time(&mut s_eq, || {
                assert_eq!(exec_count(&sql_db, eq_sql, &[SqlValue::Integer(code)])?, 1);
                Ok(())
            })?;
        }
        let (h2, m2, _, _) = snap_stats()?;

        let mut s_range = Vec::new();
        for op in 0..ops {
            let lo = ((op * 1049) % (rows - 10).max(1)) * 3 + 7;
            let hi = lo + 30;
            time(&mut s_range, || {
                let n = exec_count(
                    &sql_db,
                    range_sql,
                    &[SqlValue::Integer(lo), SqlValue::Integer(hi)],
                )?;
                assert!(n > 0);
                Ok(())
            })?;
        }
        let (h3, m3, _, _) = snap_stats()?;

        let mut s_doc_eq_rs = Vec::new();
        let mut s_doc_range_rs = Vec::new();
        for op in 0..ops {
            let id = (op * 7919) % rows;
            let code = id * 3 + 7;
            time(&mut s_doc_eq_rs, || {
                let page = doc_db.transaction_with_read_set(|tx| {
                    tx.query_index(
                        "items1",
                        "idx1_code",
                        &IndexQuery {
                            equal: vec![IndexValue::Integer(code)],
                            lower: None,
                            upper: None,
                            after: None,
                            limit: 256,
                        },
                    )
                })?;
                assert_eq!(page.documents.len(), 1);
                Ok(())
            })?;
            let lo = ((op * 1049) % (rows - 10).max(1)) * 3 + 7;
            let hi = lo + 30;
            time(&mut s_doc_range_rs, || {
                let page = doc_db.transaction_with_read_set(|tx| {
                    tx.query_index(
                        "items1",
                        "idx1_code",
                        &IndexQuery {
                            equal: vec![],
                            lower: Some(IndexBound {
                                value: IndexValue::Integer(lo),
                                inclusive: true,
                            }),
                            upper: Some(IndexBound {
                                value: IndexValue::Integer(hi),
                                inclusive: false,
                            }),
                            after: None,
                            limit: 256,
                        },
                    )
                })?;
                assert!(!page.documents.is_empty());
                Ok(())
            })?;
        }
        let (h4, m4, _, _) = snap_stats()?;

        let mut s_full_sql = Vec::new();
        let mut s_full_doc = Vec::new();
        for _ in 0..5 {
            time(&mut s_full_sql, || {
                let n = exec_count(&sql_db, full_sql, &[SqlValue::Real(0.0)])?;
                assert!(n > 0);
                Ok(())
            })?;
            time(&mut s_full_doc, || {
                let total = doc_db
                    .transaction_detailed(DocumentTransactionMode::ReadSet, None, None, |tx| {
                        let mut count = 0usize;
                        let mut after: Option<String> = None;
                        loop {
                            let page =
                                tx.scan_documents("items1", after.as_deref(), MAX_SCAN_ENTRIES)?;
                            if page.is_empty() {
                                break;
                            }
                            count += page.len();
                            after = page.last().map(|e| e.id.clone());
                        }
                        Ok(count)
                    })
                    .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
                assert_eq!(total, rows as usize);
                Ok(())
            })?;
        }
        let (h5, m5, cached_bytes, cached_blocks) = snap_stats()?;

        println!(
            "{{\"phase\":\"ckpt_indexed\",\"label\":\"{label}\",\"limit\":{cache_limit},\
\"cached_bytes\":{cached_bytes},\"cached_blocks\":{cached_blocks},\
\"pk_sql\":{},\"pk_hits\":{},\"pk_misses\":{},\
\"eq_sql\":{},\"eq_hits\":{},\"eq_misses\":{},\
\"range_sql\":{},\"range_hits\":{},\"range_misses\":{},\
\"doc_eq_rs\":{},\"doc_range_rs\":{},\"doc_hits\":{},\"doc_misses\":{},\
\"full_sql\":{},\"full_doc\":{},\"full_hits\":{},\"full_misses\":{}}}",
            Samples(s_pk).json(),
            h1 - h0,
            m1 - m0,
            Samples(s_eq).json(),
            h2 - h1,
            m2 - m1,
            Samples(s_range).json(),
            h3 - h2,
            m3 - m2,
            Samples(s_doc_eq_rs).json(),
            Samples(s_doc_range_rs).json(),
            h4 - h3,
            m4 - m3,
            Samples(s_full_sql).json(),
            Samples(s_full_doc).json(),
            h5 - h4,
            m5 - m4
        );
        drop(fresh);
    }

    Ok(())
}

fn measure_full_scan_split(root: &Path, rows: i64) -> Result<()> {
    let store_dir = root.join("scan-store");
    let store = Store::open(&store_dir)?;
    let doc_db = DocumentDatabase::new(&store);
    let sql_db = SqlDatabase::new(DocumentDatabase::new(&store));

    exec_count(
        &sql_db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)",
        &[],
    )?;
    let tuple = "(?, ?, ?, ?)";
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        let sql = format!(
            "INSERT INTO items VALUES {}",
            vec![tuple; chunk.len()].join(", ")
        );
        let params = chunk
            .iter()
            .flat_map(|id| {
                vec![
                    SqlValue::Integer(*id),
                    SqlValue::Text(format!("name-{id:08}")),
                    SqlValue::Real(*id as f64 / 4.0),
                    SqlValue::Boolean(id % 3 == 0),
                ]
            })
            .collect::<Vec<_>>();
        exec_count(&sql_db, &sql, &params)?;
    }

    let doc_prefix = b"\0emda-document\x01D\x00\x05items".to_vec();
    let mut doc_end = doc_prefix.clone();
    *doc_end.last_mut().unwrap() += 1;

    // Also measure raw pread + crc32 directly over records.log (10k separate preads vs 64K sequential chunks).
    let log_path = store_dir.join("records.log");
    let log_file = File::open(&log_path)?;
    let log_len = log_file.metadata()?.len() as usize;

    let mut raw_store_scan = Vec::new();
    let mut tx_rs_scan = Vec::new();
    let mut doc_scan = Vec::new();
    let mut sql_full_select = Vec::new();
    let mut json_parse_only = Vec::new();
    let mut crc_only = Vec::new();
    let mut seq_64k_pread = Vec::new();
    let mut raw_values: Vec<Vec<u8>> = Vec::new();

    for iter in 0..10 {
        time(&mut raw_store_scan, || {
            let mut after = doc_prefix.clone();
            let mut count = 0usize;
            if iter == 0 {
                raw_values.clear();
            }
            loop {
                let page = store.scan(Some(&after), Some(&doc_end), MAX_SCAN_ENTRIES)?;
                if page.is_empty() {
                    break;
                }
                count += page.len();
                after = page.last().unwrap().0.clone();
                if iter == 0 {
                    for (_, v) in page {
                        raw_values.push(v);
                    }
                }
            }
            assert_eq!(count, rows as usize);
            Ok(())
        })?;

        time(&mut tx_rs_scan, || {
            let count = store.transaction_with_read_set(|tx| {
                let mut after = doc_prefix.clone();
                let mut count = 0usize;
                loop {
                    let page = tx.scan(Some(&after), Some(&doc_end), MAX_SCAN_ENTRIES)?;
                    if page.is_empty() {
                        break;
                    }
                    count += page.len();
                    after = page.last().unwrap().0.clone();
                }
                Ok(count)
            })?;
            assert_eq!(count, rows as usize);
            Ok(())
        })?;

        time(&mut doc_scan, || {
            let count = doc_db.transaction_with_read_set(|tx| {
                let mut after: Option<String> = None;
                let mut count = 0usize;
                loop {
                    let page = tx.scan_documents("items", after.as_deref(), MAX_SCAN_ENTRIES)?;
                    if page.is_empty() {
                        break;
                    }
                    count += page.len();
                    after = page.last().map(|e| e.id.clone());
                }
                Ok(count)
            })?;
            assert_eq!(count, rows as usize);
            Ok(())
        })?;

        time(&mut sql_full_select, || {
            let n = exec_count(
                &sql_db,
                "SELECT id FROM items WHERE score >= ? AND active = TRUE",
                &[SqlValue::Real(0.0)],
            )?;
            assert!(n > 0);
            Ok(())
        })?;

        time(&mut json_parse_only, || {
            let mut parsed = 0usize;
            for bytes in &raw_values {
                let v: serde_json::Value = serde_json::from_slice(bytes)?;
                std::hint::black_box(&v);
                parsed += 1;
            }
            assert_eq!(parsed, rows as usize);
            Ok(())
        })?;

        time(&mut crc_only, || {
            let mut acc = 0u32;
            for bytes in &raw_values {
                acc ^= crc32fast::hash(bytes);
            }
            std::hint::black_box(acc);
            Ok(())
        })?;

        time(&mut seq_64k_pread, || {
            let mut buf = vec![0u8; 64 * 1024];
            let mut off = 0usize;
            while off < log_len {
                let n = (log_len - off).min(buf.len());
                log_file.read_exact_at(&mut buf[..n], off as u64)?;
                std::hint::black_box(&buf[..n]);
                off += n;
            }
            Ok(())
        })?;
    }

    println!(
        "{{\"phase\":\"full_scan_split_store\",\"rows\":{rows},\"log_bytes\":{log_len},\"avg_value_bytes\":{:.1},\
\"raw_store_scan\":{},\"tx_rs_scan\":{},\"doc_scan\":{},\"sql_full_select\":{},\
\"json_parse_only\":{},\"crc_only\":{},\"seq_64k_pread\":{}}}",
        raw_values.iter().map(Vec::len).sum::<usize>() as f64 / raw_values.len() as f64,
        Samples(raw_store_scan).json(),
        Samples(tx_rs_scan).json(),
        Samples(doc_scan).json(),
        Samples(sql_full_select).json(),
        Samples(json_parse_only).json(),
        Samples(crc_only).json(),
        Samples(seq_64k_pread).json(),
    );

    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [dir, rows, ops] = args.as_slice() else {
        return Err("usage: checkpoint_read_probe <directory> <rows> <ops>".into());
    };
    let (rows, ops): (i64, i64) = (rows.parse()?, ops.parse()?);
    let root = tempfile::Builder::new()
        .prefix(".ckpt-read-probe-")
        .tempdir_in(dir)?;
    measure_sql_bench_exact_repro(root.path(), rows, ops, "exact_cache0", 0, true, true)?;
    measure_sql_bench_exact_repro(root.path(), rows, ops, "exact_cache1_cnt", 1, true, true)?;
    measure_sql_bench_exact_repro(
        root.path(),
        rows,
        ops,
        "exact_noworker_noins",
        1,
        false,
        false,
    )?;
    measure_sql_bench_exact_repro(
        root.path(),
        rows,
        ops,
        "exact_cache8m",
        8 * 1024 * 1024,
        true,
        true,
    )?;
    measure_checkpoint_indexed(root.path(), rows, ops)?;
    measure_full_scan_split(root.path(), rows)?;
    Ok(())
}
