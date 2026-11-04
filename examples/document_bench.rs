//! Small single-client JSON document comparison; not a general database ranking.
use database_engine::{
    CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue, Store,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const WARMUP: usize = 20;

fn samples(values: &mut [u128]) -> Value {
    values.sort_unstable();
    let at = |percent: usize| -> f64 {
        values[(values.len() * percent).div_ceil(100) - 1] as f64 / 1000.0
    };
    json!({"count":values.len(),"p50_us":at(50),"p95_us":at(95),"p99_us":at(99),"max_us":*values.last().unwrap() as f64/1000.0})
}
fn timed<T>(
    latencies: &mut BTreeMap<&'static str, Vec<u128>>,
    name: &'static str,
    record: bool,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let start = Instant::now();
    let result = operation()?;
    let elapsed = start.elapsed().as_nanos();
    if record {
        latencies.entry(name).or_default().push(elapsed);
    }
    Ok(result)
}

fn prototype(parent: &Path, iterations: usize) -> Result<Value> {
    let root = tempfile::tempdir_in(parent)?;
    let path = root.path().join("store");
    let store = Store::open(&path)?;
    let db = DocumentDatabase::new(&store);
    db.create_collection("docs", CollectionSchema::default())?;
    db.create_index(
        "docs",
        IndexDefinition {
            name: "email".into(),
            fields: vec![vec!["email".into()]],
            unique: true,
            partial: None,
        },
    )?;
    let mut times = BTreeMap::new();
    let mut measured = 0_u128;
    for sequence in 0..iterations + WARMUP {
        let record = sequence >= WARMUP;
        let id = format!("id-{sequence}");
        let email = format!("user-{sequence}@example.test");
        let first = json!({"email":email,"age":20,"active":true});
        let second = json!({"email":email,"age":21,"active":true});
        let query = IndexQuery {
            equal: vec![IndexValue::String(email.clone())],
            limit: 2,
            ..Default::default()
        };
        let start = Instant::now();
        timed(&mut times, "create", record, || {
            Ok(db.insert("docs", &id, &first)?)
        })?;
        let found = timed(&mut times, "read", record, || Ok(db.get("docs", &id)?))?;
        if found.as_ref() != Some(&first) {
            return Err("prototype read mismatch".into());
        }
        timed(&mut times, "update", record, || {
            Ok(db.replace("docs", &id, &second, false)?)
        })?;
        let found = timed(&mut times, "indexed_query", record, || {
            Ok(db.query_index("docs", "email", &query)?)
        })?;
        if found.documents.len() != 1
            || found.documents[0].id != id
            || found.documents[0].document != second
        {
            return Err("prototype indexed query mismatch".into());
        }
        if !timed(&mut times, "delete", record, || Ok(db.delete("docs", &id)?))? {
            return Err("prototype delete mismatch".into());
        }
        if record {
            measured += start.elapsed().as_nanos();
        }
    }
    drop(store);
    let reopened = Store::open(&path)?;
    let db = DocumentDatabase::new(&reopened);
    let verified = db.verify_collection("docs")?;
    if verified.documents != 0 || verified.index_keys != 0 {
        return Err("prototype recovery verification failed".into());
    }
    let result = times
        .iter_mut()
        .map(|(name, values)| ((*name).to_owned(), samples(values)))
        .collect::<BTreeMap<_, _>>();
    Ok(
        json!({"engine":"document-prototype","cycles":iterations,"warmup_cycles":WARMUP,"wall_ops_per_sec":iterations as f64*5.0*1e9/measured as f64,"latency":result,"reopen_verified":true,"durability":"underlying witnessed store sync before acknowledgement","transport":"in-process Rust"}),
    )
}

fn sqlite(parent: &Path, iterations: usize) -> Result<Value> {
    let root = tempfile::tempdir_in(parent)?;
    let path = root.path().join("sqlite.db");
    let connection = Connection::open(&path)?;
    let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    let synchronous: i64 = connection.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if mode != "wal" || synchronous != 2 {
        return Err("SQLite WAL/FULL required".into());
    }
    connection.execute_batch("CREATE TABLE docs(id TEXT PRIMARY KEY, body TEXT NOT NULL CHECK(json_valid(body))) WITHOUT ROWID; CREATE UNIQUE INDEX docs_email ON docs(json_extract(body,'$.email')); ")?;
    let index_plan: String = connection.query_row(
        "EXPLAIN QUERY PLAN SELECT id,body FROM docs WHERE json_extract(body,'$.email')=?1",
        ["probe@example.test"],
        |row| row.get(3),
    )?;
    if !index_plan.contains("USING INDEX docs_email") {
        return Err("SQLite did not choose the email index".into());
    }
    let mut times = BTreeMap::new();
    let mut measured = 0_u128;
    for sequence in 0..iterations + WARMUP {
        let record = sequence >= WARMUP;
        let id = format!("id-{sequence}");
        let email = format!("user-{sequence}@example.test");
        let first = json!({"email":email,"age":20,"active":true}).to_string();
        let second = json!({"email":email,"age":21,"active":true}).to_string();
        let start = Instant::now();
        timed(&mut times, "create", record, || {
            Ok(connection
                .prepare_cached("INSERT INTO docs(id,body) VALUES(?1,?2)")?
                .execute(params![id, first])?)
        })?;
        let found: String = timed(&mut times, "read", record, || {
            Ok(connection
                .prepare_cached("SELECT body FROM docs WHERE id=?1")?
                .query_row([&id], |row| row.get(0))?)
        })?;
        if found != first {
            return Err("SQLite read mismatch".into());
        }
        let changed = timed(&mut times, "update", record, || {
            Ok(connection
                .prepare_cached("UPDATE docs SET body=?2 WHERE id=?1")?
                .execute(params![id, second])?)
        })?;
        if changed != 1 {
            return Err("SQLite update mismatch".into());
        }
        let found: (String, String) = timed(&mut times, "indexed_query", record, || {
            Ok(connection
                .prepare_cached("SELECT id,body FROM docs WHERE json_extract(body,'$.email')=?1")?
                .query_row([&email], |row| Ok((row.get(0)?, row.get(1)?)))?)
        })?;
        if found.0 != id || found.1 != second {
            return Err("SQLite indexed query mismatch".into());
        }
        let deleted = timed(&mut times, "delete", record, || {
            Ok(connection
                .prepare_cached("DELETE FROM docs WHERE id=?1")?
                .execute([&id])?)
        })?;
        if deleted != 1 {
            return Err("SQLite delete mismatch".into());
        }
        if record {
            measured += start.elapsed().as_nanos();
        }
    }
    drop(connection);
    let reopened = Connection::open(&path)?;
    let count: i64 = reopened.query_row("SELECT count(*) FROM docs", [], |row| row.get(0))?;
    let integrity: String = reopened.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if count != 0 || integrity != "ok" {
        return Err("SQLite recovery verification failed".into());
    }
    let result = times
        .iter_mut()
        .map(|(name, values)| ((*name).to_owned(), samples(values)))
        .collect::<BTreeMap<_, _>>();
    Ok(
        json!({"engine":"sqlite-json","version":rusqlite::version(),"query_plan":index_plan,"cycles":iterations,"warmup_cycles":WARMUP,"wall_ops_per_sec":iterations as f64*5.0*1e9/measured as f64,"latency":result,"reopen_verified":true,"durability":"WAL synchronous=FULL, autocommit","transport":"in-process Rust"}),
    )
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [engine, parent, iterations] = args.as_slice() else {
        return Err(
            "usage: document_bench <prototype|sqlite> <existing-directory> <cycles>".into(),
        );
    };
    let parent = Path::new(parent);
    if !parent.is_dir() {
        return Err("parent directory must exist".into());
    }
    let iterations = iterations.parse::<usize>()?;
    if iterations == 0 || iterations > 100_000 {
        return Err("cycles must be 1..=100000".into());
    }
    let result = match engine.as_str() {
        "prototype" => prototype(parent, iterations)?,
        "sqlite" => sqlite(parent, iterations)?,
        _ => return Err("unknown engine".into()),
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
