//! Closed-loop concurrent document updates and indexed reads with clean reopen verification.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentTransaction, DocumentTransactionMode, IndexDefinition, IndexQuery,
    Store, TransactionFailure, is_transaction_conflict,
};
use rusqlite::{Connection, ErrorCode, params};
use serde_json::{Value, json};
use std::{
    error::Error,
    fs,
    path::Path,
    sync::Barrier,
    thread,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const INDEX_LOOKUP: &str = "SELECT id, body FROM docs WHERE json_extract(body,'$.email')=?1";
const RETRY_TIMEOUT: Duration = Duration::from_secs(30);

fn id(number: usize) -> String {
    format!("doc-{number:06}")
}

fn document(id: &str, version: usize) -> Value {
    json!({"email": format!("{id}-{version:06}@example.test"), "version": version, "active": true})
}

struct Samples {
    updates_ns: Vec<u128>,
    queries_ns: Vec<u128>,
    update_retries: usize,
    query_retries: usize,
}

fn cycles(
    client: usize,
    count: usize,
    mut update: impl FnMut(&str, &Value) -> Result<usize>,
    mut query: impl FnMut(&str, &Value) -> Result<usize>,
) -> Result<Samples> {
    let mut samples = Samples {
        updates_ns: Vec::with_capacity(count),
        queries_ns: Vec::with_capacity(count),
        update_retries: 0,
        query_retries: 0,
    };
    // Each client updates one document, sharing the log and the same unique
    // index with the other clients. No client writes another client's row.
    let id = id(client);
    for version in 1..=count {
        let expected = document(&id, version);
        let start = Instant::now();
        samples.update_retries += update(&id, &expected)?;
        samples.updates_ns.push(start.elapsed().as_nanos());

        let start = Instant::now();
        samples.query_retries += query(&id, &expected)?;
        samples.queries_ns.push(start.elapsed().as_nanos());
    }
    Ok(samples)
}

fn run_clients<C: Send>(
    clients: Vec<C>,
    work: impl Fn(C, usize) -> Result<Samples> + Sync,
) -> Result<(Duration, Vec<Samples>)> {
    let barrier = Barrier::new(clients.len() + 1);
    thread::scope(|scope| {
        let work = &work;
        let barrier = &barrier;
        let handles: Vec<_> = clients
            .into_iter()
            .enumerate()
            .map(|(index, client)| {
                scope.spawn(move || {
                    barrier.wait();
                    work(client, index)
                })
            })
            .collect();
        let start = Instant::now();
        barrier.wait();
        let mut samples = Vec::with_capacity(handles.len());
        for handle in handles {
            samples.push(handle.join().map_err(|_| "benchmark client panicked")??);
        }
        Ok((start.elapsed(), samples))
    })
}

fn retry<T>(mut attempt: impl FnMut(Instant) -> Result<Option<T>>) -> Result<(T, usize)> {
    let deadline = Instant::now() + RETRY_TIMEOUT;
    let mut retries = 0;
    loop {
        if let Some(value) = attempt(deadline)? {
            return Ok((value, retries));
        }
        retries += 1;
        if Instant::now() >= deadline {
            return Err("operation exhausted its retry window".into());
        }
        thread::yield_now();
    }
}

fn retry_document<T>(
    db: &DocumentDatabase<'_>,
    mut operation: impl FnMut(&mut DocumentTransaction<'_, '_>) -> std::io::Result<T>,
) -> Result<(T, usize)> {
    retry(|deadline| {
        match db.transaction_detailed(
            DocumentTransactionMode::ReadSet,
            Some(deadline),
            None,
            &mut operation,
        ) {
            Ok(value) => Ok(Some(value)),
            Err(TransactionFailure::Rejected(error)) if is_transaction_conflict(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    })
}

fn run_prototype(
    db: &DocumentDatabase<'_>,
    clients: usize,
    count: usize,
) -> Result<(Duration, Vec<Samples>)> {
    run_clients(vec![db; clients], |db, client| {
        cycles(
            client,
            count,
            |id, value| {
                let (_, retries) = retry_document(db, |tx| tx.replace("docs", id, value, false))?;
                Ok(retries)
            },
            |id, value| {
                let query = DocumentQuery {
                    filter: DocumentFilter::Equal {
                        path: vec!["email".into()],
                        value: value["email"].clone(),
                    },
                    limit: 2,
                    ..Default::default()
                };
                let (result, retries) = retry_document(db, |tx| tx.query_planned("docs", &query))?;
                if result.access
                    != (DocumentQueryAccess::UniqueEqualityIndex {
                        index: "email".into(),
                    })
                    || result.result.documents.len() != 1
                    || result.result.documents[0].id != id
                    || result.result.documents[0].document != *value
                {
                    return Err(
                        "prototype indexed result differs from the acknowledged update".into(),
                    );
                }
                Ok(retries)
            },
        )
    })
}

fn expected_row(number: usize, clients: usize, count: usize) -> (String, Value) {
    let id = id(number);
    let value = document(&id, if number < clients { count } else { 0 });
    (id, value)
}

fn verify_prototype(
    db: &DocumentDatabase<'_>,
    rows: usize,
    clients: usize,
    count: usize,
) -> Result<()> {
    let mut query = IndexQuery {
        limit: 256,
        ..Default::default()
    };
    let mut observed = 0;
    loop {
        let page = db.query_index("docs", "email", &query)?;
        if page.documents.is_empty() {
            break;
        }
        for entry in page.documents {
            let expected = expected_row(observed, clients, count);
            if observed == rows || (entry.id, entry.document) != expected {
                return Err("prototype recovered index does not match expected rows".into());
            }
            observed += 1;
        }
        query.after = page.next;
    }
    if observed != rows {
        return Err("prototype recovered index row count mismatch".into());
    }
    for number in 0..rows {
        let (id, expected) = expected_row(number, clients, count);
        if db.get("docs", &id)?.as_ref() != Some(&expected) {
            return Err("prototype recovered primary document mismatch".into());
        }
    }
    Ok(())
}

fn prototype(
    root: &Path,
    rows: usize,
    clients: usize,
    count: usize,
    checkpointed: bool,
) -> Result<(Duration, Vec<Samples>, Value)> {
    let path = root.join("store");
    let checkpoint = root.join("checkpoint");
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
    for start in (0..rows).step_by(32) {
        db.transaction(|tx| {
            for number in start..(start + 32).min(rows) {
                let (id, value) = expected_row(number, 0, 0);
                tx.insert("docs", &id, &value)?;
            }
            Ok(())
        })?;
    }

    let (elapsed, samples) = if checkpointed {
        let digest = store.checkpoint_to(&checkpoint)?;
        drop(store);
        let store = CheckpointStore::open(&path, &checkpoint, &digest)?;
        let result = run_prototype(
            &DocumentDatabase::with_checkpoint_store(&store),
            clients,
            count,
        )?;
        drop(store);
        let reopened = CheckpointStore::open(&path, &checkpoint, &digest)?;
        verify_prototype(
            &DocumentDatabase::with_checkpoint_store(&reopened),
            rows,
            clients,
            count,
        )?;
        result
    } else {
        let result = run_prototype(&db, clients, count)?;
        drop(store);
        verify_prototype(
            &DocumentDatabase::new(&Store::open(&path)?),
            rows,
            clients,
            count,
        )?;
        result
    };
    use std::io::Read;
    let mut header = [0; 8];
    fs::File::open(path.join("records.log"))?.read_exact(&mut header)?;
    let format = match &header {
        b"EMDALOG2" => "V2",
        b"EMDALOG3" => "V3",
        _ => return Err("unexpected prototype log format".into()),
    };
    Ok((
        elapsed,
        samples,
        json!({"witness": format, "durability": "synchronized witnessed commits"}),
    ))
}

fn open_sqlite(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_millis(50))?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    let mode: String = connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    let synchronous: i64 = connection.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if mode != "wal" || synchronous != 2 {
        return Err("SQLite WAL/FULL is required on each connection".into());
    }
    Ok(connection)
}

fn verify_sqlite_index(connection: &Connection, id: &str, expected: &Value) -> Result<()> {
    let email = expected["email"].as_str().expect("fixture email");
    let (actual_id, body): (String, String) = connection
        .prepare_cached(INDEX_LOOKUP)?
        .query_row([email], |row| Ok((row.get(0)?, row.get(1)?)))?;
    if actual_id != id || serde_json::from_str::<Value>(&body)? != *expected {
        return Err("SQLite indexed row mismatch".into());
    }
    Ok(())
}

fn sqlite(
    root: &Path,
    rows: usize,
    clients: usize,
    count: usize,
) -> Result<(Duration, Vec<Samples>, Value)> {
    let path = root.join("sqlite.db");
    Connection::open(&path)?
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))?;
    let mut connection = open_sqlite(&path)?;
    connection.execute_batch(
        "CREATE TABLE docs(id TEXT PRIMARY KEY, body TEXT NOT NULL CHECK(json_valid(body))) WITHOUT ROWID;
         CREATE UNIQUE INDEX docs_email ON docs(json_extract(body,'$.email'));",
    )?;
    let plan: String = connection.query_row(
        &format!("EXPLAIN QUERY PLAN {INDEX_LOOKUP}"),
        ["probe"],
        |row| row.get(3),
    )?;
    if !plan.contains("USING INDEX docs_email") {
        return Err("SQLite did not select the email expression index".into());
    }
    let transaction = connection.transaction()?;
    for number in 0..rows {
        let (id, value) = expected_row(number, 0, 0);
        transaction
            .prepare_cached("INSERT INTO docs VALUES (?1, ?2)")?
            .execute(params![id, value.to_string()])?;
    }
    transaction.commit()?;

    let handles = (0..clients)
        .map(|_| open_sqlite(&path))
        .collect::<Result<Vec<_>>>()?;
    let (elapsed, samples) = run_clients(handles, |connection, client| {
        cycles(
            client,
            count,
            |id, value| {
                let body = value.to_string();
                let (_, retries) = retry(|_| {
                    match connection
                        .prepare_cached("UPDATE docs SET body=?2 WHERE id=?1")
                        .and_then(|mut stmt| stmt.execute(params![id, &body]))
                    {
                        Ok(1) => Ok(Some(())),
                        Ok(_) => Err("SQLite update did not affect exactly one row".into()),
                        Err(rusqlite::Error::SqliteFailure(error, _))
                            if error.code == ErrorCode::DatabaseBusy =>
                        {
                            Ok(None)
                        }
                        Err(error) => Err(error.into()),
                    }
                })?;
                Ok(retries)
            },
            |id, expected| {
                verify_sqlite_index(&connection, id, expected)?;
                Ok(0)
            },
        )
    })?;
    drop(connection);

    let reopened = open_sqlite(&path)?;
    let actual_rows: usize =
        reopened.query_row("SELECT count(*) FROM docs", [], |row| row.get(0))?;
    let integrity: String = reopened.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if actual_rows != rows || integrity != "ok" {
        return Err("SQLite failed clean reopen verification".into());
    }
    for number in 0..rows {
        let (id, expected) = expected_row(number, clients, count);
        let body: String = reopened
            .prepare_cached("SELECT body FROM docs WHERE id=?1")?
            .query_row([&id], |row| row.get(0))?;
        if serde_json::from_str::<Value>(&body)? != expected {
            return Err("SQLite recovered primary document mismatch".into());
        }
        verify_sqlite_index(&reopened, &id, &expected)?;
    }
    Ok((
        elapsed,
        samples,
        json!({"durability": "WAL synchronous=FULL", "sqlite_version": rusqlite::version(), "query_plan": plan}),
    ))
}

fn peak_rss_kib() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [engine, parent, clients, count, rows] = args.as_slice() else {
        return Err("usage: document_contention_bench <store|checkpoint|sqlite> <directory> <clients> <cycles-per-client> <documents>".into());
    };
    let (clients, count, rows): (usize, usize, usize) =
        (clients.parse()?, count.parse()?, rows.parse()?);
    if !(1..=8).contains(&clients)
        || !(1..=1000).contains(&count)
        || !(clients..=1000).contains(&rows)
    {
        return Err(
            "clients must be 1..=8, cycles 1..=1000, and documents between clients and 1000".into(),
        );
    }
    let directory = tempfile::tempdir_in(parent)?;
    let (elapsed, samples, configuration) = match engine.as_str() {
        "store" | "checkpoint" => prototype(
            directory.path(),
            rows,
            clients,
            count,
            engine == "checkpoint",
        )?,
        "sqlite" => sqlite(directory.path(), rows, clients, count)?,
        _ => return Err("unknown benchmark engine".into()),
    };
    let samples: Vec<_> = samples
        .into_iter()
        .map(|samples| json!({"update_ns": samples.updates_ns, "query_ns": samples.queries_ns, "update_retries": samples.update_retries, "query_retries": samples.query_retries}))
        .collect();
    println!(
        "{}",
        json!({"engine": engine, "clients": clients, "cycles_per_client": count, "documents": rows,
            "elapsed_ns": elapsed.as_nanos(), "ops_per_sec": (clients * count * 2) as f64 / elapsed.as_secs_f64(),
            "configuration": configuration, "clients_samples": samples, "reopen_verified": true, "peak_rss_kib": peak_rss_kib()})
    );
    Ok(())
}
