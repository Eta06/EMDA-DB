//! Measures the embedded SQL subset against direct document calls and
//! SQLite with full device flushes. Prints one JSON line per backend.
//!
//! usage: sql_bench <directory> <rows> <operations>
//!
//! Each backend loads `rows` rows in 100-row INSERT statements, then times
//! `operations` of each: prepare (parse) only, primary-key SELECT, one-row
//! durable INSERT, primary-key UPDATE and DELETE, and one full-table SELECT
//! with a predicate. Final contents are checked after reopening.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, SqlDatabase, SqlOutcome, SqlValue, Store,
};
use serde_json::json;
use std::error::Error;
use std::path::Path;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct Samples(Vec<f64>);

impl Samples {
    fn json(mut self) -> String {
        self.0.sort_by(f64::total_cmp);
        let at = |q: f64| self.0[((self.0.len() - 1) as f64 * q).round() as usize];
        format!(
            "{{\"n\":{},\"p50_us\":{:.1},\"p99_us\":{:.1},\"max_us\":{:.1}}}",
            self.0.len(),
            at(0.5),
            at(0.99),
            self.0.last().unwrap()
        )
    }
}

fn time(samples: &mut Vec<f64>, f: impl FnOnce() -> Result<()>) -> Result<()> {
    let started = Instant::now();
    f()?;
    samples.push(started.elapsed().as_secs_f64() * 1e6);
    Ok(())
}

trait Backend {
    fn exec(&self, sql: &str, params: &[SqlValue]) -> Result<usize>;
    fn prepare_only(&self, sql: &str) -> Result<()>;
}

impl Backend for SqlDatabase<'_> {
    fn exec(&self, sql: &str, params: &[SqlValue]) -> Result<usize> {
        Ok(match self.execute(sql, params)? {
            SqlOutcome::Rows { rows, .. } => rows.len(),
            SqlOutcome::Inserted { rows }
            | SqlOutcome::Updated { rows }
            | SqlOutcome::Deleted { rows } => rows,
            _ => 0,
        })
    }
    fn prepare_only(&self, sql: &str) -> Result<()> {
        self.prepare(sql)?;
        Ok(())
    }
}

struct Sqlite(rusqlite::Connection);

impl Backend for Sqlite {
    fn exec(&self, sql: &str, params: &[SqlValue]) -> Result<usize> {
        let values = params
            .iter()
            .map(|value| match value {
                SqlValue::Null => rusqlite::types::Value::Null,
                SqlValue::Integer(n) => rusqlite::types::Value::Integer(*n),
                SqlValue::Real(f) => rusqlite::types::Value::Real(*f),
                SqlValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
                SqlValue::Boolean(b) => rusqlite::types::Value::Integer(*b as i64),
            })
            .collect::<Vec<_>>();
        let mut statement = self.0.prepare_cached(sql)?;
        let params = rusqlite::params_from_iter(values);
        if statement.readonly() {
            let mut rows = statement.query(params)?;
            let mut count = 0;
            while rows.next()?.is_some() {
                count += 1;
            }
            Ok(count)
        } else {
            Ok(statement.execute(params)?)
        }
    }
    fn prepare_only(&self, sql: &str) -> Result<()> {
        self.0.prepare(sql)?;
        Ok(())
    }
}

fn row_params(id: i64) -> Vec<SqlValue> {
    vec![
        SqlValue::Integer(id),
        SqlValue::Text(format!("name-{id:08}")),
        SqlValue::Real(id as f64 / 4.0),
        SqlValue::Boolean(id % 3 == 0),
    ]
}

fn workload(name: &str, db: &dyn Backend, rows: i64, ops: i64) -> Result<String> {
    db.exec(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)",
        &[],
    )?;
    let started = Instant::now();
    let tuple = "(?, ?, ?, ?)";
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        let sql = format!(
            "INSERT INTO items VALUES {}",
            vec![tuple; chunk.len()].join(", ")
        );
        let params = chunk
            .iter()
            .flat_map(|id| row_params(*id))
            .collect::<Vec<_>>();
        db.exec(&sql, &params)?;
    }
    let load_ms = started.elapsed().as_secs_f64() * 1e3;

    let select = "SELECT id, name, score, active FROM items WHERE id = ?";
    let insert = "INSERT INTO items VALUES (?, ?, ?, ?)";
    let update = "UPDATE items SET score = ? WHERE id = ?";
    let delete = "DELETE FROM items WHERE id = ?";
    let scan = "SELECT id FROM items WHERE score >= ? AND active = TRUE";
    let (mut parse, mut get, mut put, mut set, mut del, mut full) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for op in 0..ops {
        let id = (op * 7919) % rows;
        time(&mut parse, || db.prepare_only(update))?;
        time(&mut get, || {
            (db.exec(select, &[SqlValue::Integer(id)])? == 1)
                .then_some(())
                .ok_or_else(|| "point select missed".into())
        })?;
        time(&mut put, || {
            db.exec(insert, &row_params(rows + op)).map(|_| ())
        })?;
        time(&mut set, || {
            (db.exec(update, &[SqlValue::Real(-1.0), SqlValue::Integer(id)])? == 1)
                .then_some(())
                .ok_or_else(|| "update missed".into())
        })?;
        time(&mut del, || {
            (db.exec(delete, &[SqlValue::Integer(rows + op)])? == 1)
                .then_some(())
                .ok_or_else(|| "delete missed".into())
        })?;
    }
    let mut matched = 0;
    for _ in 0..ops.min(20) {
        time(&mut full, || {
            matched = db.exec(scan, &[SqlValue::Real(0.0)])?;
            Ok(())
        })?;
    }
    Ok(format!(
        "{{\"backend\":\"{name}\",\"rows\":{rows},\"load_ms\":{load_ms:.1},\"parse\":{},\
\"pk_select\":{},\"insert\":{},\"pk_update\":{},\"pk_delete\":{},\"full_select\":{},\
\"full_select_matched\":{matched}}}",
        Samples(parse).json(),
        Samples(get).json(),
        Samples(put).json(),
        Samples(set).json(),
        Samples(del).json(),
        Samples(full).json()
    ))
}

fn check(db: &dyn Backend, rows: i64) -> Result<()> {
    let count = db.exec("SELECT id FROM items", &[])?;
    if count != rows as usize {
        return Err(format!("expected {rows} rows after reopen, found {count}").into());
    }
    Ok(())
}

/// Direct document calls for the point operations, to isolate SQL overhead.
fn documents(db: &DocumentDatabase<'_>, rows: i64, ops: i64) -> Result<String> {
    db.create_collection("items", CollectionSchema::default())?;
    let row = |id: i64| json!({"id": id, "name": format!("name-{id:08}"), "score": id as f64 / 4.0, "active": id % 3 == 0});
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        db.transaction(|tx| {
            for id in chunk {
                tx.insert("items", &format!("{id:016x}"), &row(*id))?;
            }
            Ok(())
        })?;
    }
    let (mut get, mut put, mut del) = (vec![], vec![], vec![]);
    for op in 0..ops {
        let id = (op * 7919) % rows;
        time(&mut get, || {
            db.get("items", &format!("{id:016x}"))?.ok_or("missing")?;
            Ok(())
        })?;
        time(&mut put, || {
            db.insert("items", &format!("{:016x}", rows + op), &row(rows + op))?;
            Ok(())
        })?;
        time(&mut del, || {
            db.transaction(|tx| tx.delete("items", &format!("{:016x}", rows + op)))?;
            Ok(())
        })?;
    }
    Ok(format!(
        "{{\"backend\":\"documents\",\"rows\":{rows},\"get\":{},\"insert\":{},\"delete\":{}}}",
        Samples(get).json(),
        Samples(put).json(),
        Samples(del).json()
    ))
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

fn indexed_workload(name: &str, db: &dyn Backend, rows: i64, ops: i64) -> Result<String> {
    // 1-index table: equality + short range + single-row durable insert cost
    db.exec(
        "CREATE TABLE items1 (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, tag TEXT NOT NULL, active BOOLEAN NOT NULL, score REAL)",
        &[],
    )?;
    db.exec("CREATE INDEX idx1_code ON items1 (code)", &[])?;

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
        db.exec(&sql, &params)?;
    }

    // 3-index table: single-row durable insert cost with 3 secondary indexes
    db.exec(
        "CREATE TABLE items3 (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, tag TEXT NOT NULL, active BOOLEAN NOT NULL, score REAL)",
        &[],
    )?;
    db.exec("CREATE INDEX idx3_code ON items3 (code)", &[])?;
    db.exec("CREATE INDEX idx3_tag ON items3 (tag)", &[])?;
    db.exec("CREATE INDEX idx3_active ON items3 (active)", &[])?;
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        let sql = format!(
            "INSERT INTO items3 VALUES {}",
            vec![tuple; chunk.len()].join(", ")
        );
        let params = chunk
            .iter()
            .flat_map(|id| indexed_row_params(*id))
            .collect::<Vec<_>>();
        db.exec(&sql, &params)?;
    }

    let eq_sql = "SELECT id, code, tag, active, score FROM items1 WHERE code = ?";
    let range_sql = "SELECT id, code, tag, active, score FROM items1 WHERE code >= ? AND code < ?";
    let ins1_sql = "INSERT INTO items1 VALUES (?, ?, ?, ?, ?)";
    let ins3_sql = "INSERT INTO items3 VALUES (?, ?, ?, ?, ?)";
    let (mut idx_eq, mut idx_range, mut ins_1idx, mut ins_3idx) = (vec![], vec![], vec![], vec![]);

    for op in 0..ops {
        let id = (op * 7919) % rows;
        let target_code = id * 3 + 7;
        time(&mut idx_eq, || {
            (db.exec(eq_sql, &[SqlValue::Integer(target_code)])? == 1)
                .then_some(())
                .ok_or_else(|| "indexed eq missed".into())
        })?;
        let lo = ((op * 1049) % (rows - 10).max(1)) * 3 + 7;
        let hi = lo + 30;
        time(&mut idx_range, || {
            let n = db.exec(range_sql, &[SqlValue::Integer(lo), SqlValue::Integer(hi)])?;
            if n == 0 {
                return Err("indexed range missed".into());
            }
            Ok(())
        })?;
        time(&mut ins_1idx, || {
            db.exec(ins1_sql, &indexed_row_params(rows + op))
                .map(|_| ())
        })?;
        time(&mut ins_3idx, || {
            db.exec(ins3_sql, &indexed_row_params(rows + op))
                .map(|_| ())
        })?;
    }

    Ok(format!(
        "{{\"backend\":\"{name}-indexed\",\"rows\":{rows},\"idx_eq\":{},\"idx_range_10\":{},\"insert_1idx\":{},\"insert_3idx\":{}}}",
        Samples(idx_eq).json(),
        Samples(idx_range).json(),
        Samples(ins_1idx).json(),
        Samples(ins_3idx).json(),
    ))
}

fn agg_row_params(id: i64) -> Vec<SqlValue> {
    vec![
        SqlValue::Integer(id),
        SqlValue::Integer(id % 100),
        SqlValue::Integer(id * 3 + 1),
        SqlValue::Real(id as f64 / 4.0),
        SqlValue::Boolean(id % 3 == 0),
    ]
}

fn aggregate_workload(name: &str, db: &dyn Backend, rows: i64, ops: i64) -> Result<String> {
    db.exec(
        "CREATE TABLE agg_items (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, val INTEGER NOT NULL, score REAL, active BOOLEAN NOT NULL)",
        &[],
    )?;
    let tuple = "(?, ?, ?, ?, ?)";
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        let sql = format!(
            "INSERT INTO agg_items VALUES {}",
            vec![tuple; chunk.len()].join(", ")
        );
        let params = chunk
            .iter()
            .flat_map(|id| agg_row_params(*id))
            .collect::<Vec<_>>();
        db.exec(&sql, &params)?;
    }

    let count_sql = "SELECT COUNT(*) FROM agg_items";
    let sum_where_sql = "SELECT SUM(val) FROM agg_items WHERE active = TRUE AND score >= ?";
    let group_sql = "SELECT grp, COUNT(*), SUM(val), AVG(score) FROM agg_items GROUP BY grp";
    let (mut count_samples, mut sum_where_samples, mut group_samples) = (vec![], vec![], vec![]);
    let iters = ops.clamp(1, 20);
    let expected_groups = (rows as usize).min(100);

    for _ in 0..iters {
        time(&mut count_samples, || {
            (db.exec(count_sql, &[])? == 1)
                .then_some(())
                .ok_or_else(|| "COUNT(*) unexpected row count".into())
        })?;
        time(&mut sum_where_samples, || {
            (db.exec(sum_where_sql, &[SqlValue::Real(0.0)])? == 1)
                .then_some(())
                .ok_or_else(|| "SUM WHERE unexpected row count".into())
        })?;
        time(&mut group_samples, || {
            (db.exec(group_sql, &[])? == expected_groups)
                .then_some(())
                .ok_or_else(|| "GROUP BY unexpected group count".into())
        })?;
    }

    Ok(format!(
        "{{\"backend\":\"{name}-agg\",\"rows\":{rows},\"groups\":{expected_groups},\"count_star\":{},\"sum_where\":{},\"group_by_100\":{}}}",
        Samples(count_samples).json(),
        Samples(sum_where_samples).json(),
        Samples(group_samples).json(),
    ))
}

fn sqlite(path: &Path) -> Result<Sqlite> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON;",
    )?;
    Ok(Sqlite(conn))
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [dir, rows, ops] = args.as_slice() else {
        return Err("usage: sql_bench <directory> <rows> <operations>".into());
    };
    let (rows, ops): (i64, i64) = (rows.parse()?, ops.parse()?);
    let root = tempfile::Builder::new()
        .prefix(".sql-bench-")
        .tempdir_in(dir)?;
    let root = root.path();

    let store = Store::open(root.join("store"))?;
    println!(
        "{}",
        workload(
            "emda-store",
            &SqlDatabase::new(DocumentDatabase::new(&store)),
            rows,
            ops
        )?
    );
    drop(store);
    let store = Store::open(root.join("store"))?;
    check(&SqlDatabase::new(DocumentDatabase::new(&store)), rows)?;
    drop(store);

    let initial = Store::open(root.join("checkpoint-log"))?;
    let digest = initial.checkpoint_to(root.join("checkpoint"))?;
    drop(initial);
    let store = CheckpointStore::open(
        root.join("checkpoint-log"),
        root.join("checkpoint"),
        &digest,
    )?;
    println!(
        "{}",
        workload(
            "emda-checkpoint",
            &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store)),
            rows,
            ops
        )?
    );
    drop(store);
    let store = CheckpointStore::open(
        root.join("checkpoint-log"),
        root.join("checkpoint"),
        &digest,
    )?;
    check(
        &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store)),
        rows,
    )?;
    drop(store);

    let store = Store::open(root.join("documents"))?;
    println!("{}", documents(&DocumentDatabase::new(&store), rows, ops)?);
    drop(store);

    let db = sqlite(&root.join("sqlite.db"))?;
    println!("{}", workload("sqlite", &db, rows, ops)?);
    drop(db);
    check(&sqlite(&root.join("sqlite.db"))?, rows)?;

    let store_idx = Store::open(root.join("store-idx"))?;
    match indexed_workload(
        "emda-store",
        &SqlDatabase::new(DocumentDatabase::new(&store_idx)),
        rows,
        ops,
    ) {
        Ok(line) => println!("{line}"),
        Err(err) => println!(
            "{{\"backend\":\"emda-store-indexed\",\"rows\":{rows},\"skipped\":{:?}}}",
            err.to_string()
        ),
    }
    drop(store_idx);

    if rows <= 2_500 {
        let initial_idx = Store::open(root.join("checkpoint-idx-log"))?;
        let digest_idx = initial_idx.checkpoint_to(root.join("checkpoint-idx"))?;
        drop(initial_idx);
        let ckpt_idx = CheckpointStore::open(
            root.join("checkpoint-idx-log"),
            root.join("checkpoint-idx"),
            &digest_idx,
        )?;
        println!(
            "{}",
            indexed_workload(
                "emda-checkpoint",
                &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt_idx)),
                rows,
                ops
            )?
        );
        drop(ckpt_idx);
    } else {
        let log_idx = root.join("checkpoint-idx-log");
        let base_idx = root.join("checkpoint-idx");
        drop(Store::open(&log_idx)?);
        CheckpointStore::rebuild_managed_selection(&log_idx, &base_idx)?;
        let ckpt_idx = std::sync::Arc::new(CheckpointStore::open_managed(&log_idx)?);
        ckpt_idx.enable_automatic_checkpoints()?;
        let worker =
            ckpt_idx.start_checkpoint_worker(std::time::Duration::from_millis(5), 256 * 1024)?;
        println!(
            "{}",
            indexed_workload(
                "emda-checkpoint",
                &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt_idx)),
                rows,
                ops
            )?
        );
        worker.stop()?;
        drop(ckpt_idx);
    }

    let sqlite_idx = sqlite(&root.join("sqlite-idx.db"))?;
    println!("{}", indexed_workload("sqlite", &sqlite_idx, rows, ops)?);
    drop(sqlite_idx);

    let store_agg = Store::open(root.join("store-agg"))?;
    match aggregate_workload(
        "emda-store",
        &SqlDatabase::new(DocumentDatabase::new(&store_agg)),
        rows,
        ops,
    ) {
        Ok(line) => println!("{line}"),
        Err(err) => println!(
            "{{\"backend\":\"emda-store-agg\",\"rows\":{rows},\"skipped\":{:?}}}",
            err.to_string()
        ),
    }
    drop(store_agg);

    let initial_agg = Store::open(root.join("checkpoint-agg-log"))?;
    let digest_agg = initial_agg.checkpoint_to(root.join("checkpoint-agg"))?;
    drop(initial_agg);
    let ckpt_agg = CheckpointStore::open(
        root.join("checkpoint-agg-log"),
        root.join("checkpoint-agg"),
        &digest_agg,
    )?;
    println!(
        "{}",
        aggregate_workload(
            "emda-checkpoint",
            &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt_agg)),
            rows,
            ops
        )?
    );
    drop(ckpt_agg);

    let sqlite_agg = sqlite(&root.join("sqlite-agg.db"))?;
    println!("{}", aggregate_workload("sqlite", &sqlite_agg, rows, ops)?);
    drop(sqlite_agg);

    let store_tx = Store::open(root.join("store-tx"))?;
    println!(
        "{}",
        tx_workload(
            "emda-store",
            &SqlDatabase::new(DocumentDatabase::new(&store_tx)),
            rows,
            ops
        )?
    );
    drop(store_tx);

    let initial_tx = Store::open(root.join("checkpoint-tx-log"))?;
    let digest_tx = initial_tx.checkpoint_to(root.join("checkpoint-tx"))?;
    drop(initial_tx);
    let ckpt_tx = CheckpointStore::open(
        root.join("checkpoint-tx-log"),
        root.join("checkpoint-tx"),
        &digest_tx,
    )?;
    println!(
        "{}",
        tx_workload(
            "emda-checkpoint",
            &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt_tx)),
            rows,
            ops
        )?
    );
    drop(ckpt_tx);

    let sqlite_tx = sqlite(&root.join("sqlite-tx.db"))?;
    println!("{}", sqlite_tx_workload(&sqlite_tx, rows, ops)?);
    drop(sqlite_tx);
    Ok(())
}

const TX_UPDATES_PER_TX: i64 = 10;

fn tx_workload(name: &str, db: &SqlDatabase<'_>, rows: i64, ops: i64) -> Result<String> {
    db.exec(
        "CREATE TABLE tx_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)",
        &[],
    )?;
    let tuple = "(?, ?, ?, ?)";
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        let sql = format!(
            "INSERT INTO tx_items VALUES {}",
            vec![tuple; chunk.len()].join(", ")
        );
        let params = chunk
            .iter()
            .flat_map(|id| row_params(*id))
            .collect::<Vec<_>>();
        db.exec(&sql, &params)?;
    }

    let update_stmt = db.prepare("UPDATE tx_items SET score = ? WHERE id = ?")?;
    let mut samples = Vec::new();
    for op in 0..ops {
        time(&mut samples, || {
            db.transaction(|tx| {
                for step in 0..TX_UPDATES_PER_TX {
                    let id = (op * 7919 + step * 101) % rows;
                    let score = (op * TX_UPDATES_PER_TX + step) as f64;
                    tx.execute_prepared(
                        &update_stmt,
                        &[SqlValue::Real(score), SqlValue::Integer(id)],
                    )?;
                }
                Ok(())
            })?;
            Ok(())
        })?;
    }

    Ok(format!(
        "{{\"backend\":\"{name}-tx\",\"rows\":{rows},\"stmts_per_tx\":{TX_UPDATES_PER_TX},\"tx_10_updates\":{}}}",
        Samples(samples).json(),
    ))
}

fn sqlite_tx_workload(db: &Sqlite, rows: i64, ops: i64) -> Result<String> {
    db.exec(
        "CREATE TABLE tx_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)",
        &[],
    )?;
    let tuple = "(?, ?, ?, ?)";
    for chunk in (0..rows).collect::<Vec<_>>().chunks(100) {
        let sql = format!(
            "INSERT INTO tx_items VALUES {}",
            vec![tuple; chunk.len()].join(", ")
        );
        let params = chunk
            .iter()
            .flat_map(|id| row_params(*id))
            .collect::<Vec<_>>();
        db.exec(&sql, &params)?;
    }

    let update_sql = "UPDATE tx_items SET score = ?1 WHERE id = ?2";
    let mut samples = Vec::new();
    for op in 0..ops {
        time(&mut samples, || {
            db.0.execute_batch("BEGIN IMMEDIATE")?;
            {
                let mut stmt = db.0.prepare_cached(update_sql)?;
                for step in 0..TX_UPDATES_PER_TX {
                    let id = (op * 7919 + step * 101) % rows;
                    let score = (op * TX_UPDATES_PER_TX + step) as f64;
                    stmt.execute(rusqlite::params![score, id])?;
                }
            }
            db.0.execute_batch("COMMIT")?;
            Ok(())
        })?;
    }

    Ok(format!(
        "{{\"backend\":\"sqlite-tx\",\"rows\":{rows},\"stmts_per_tx\":{TX_UPDATES_PER_TX},\"tx_10_updates\":{}}}",
        Samples(samples).json(),
    ))
}
