//! Single-client transaction scanning one page from a bounded range.
use database_engine::{BatchOperation, CheckpointStore, Store};
use rusqlite::{Connection, TransactionBehavior, params};
use std::error::Error;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn key(index: u32) -> Vec<u8> {
    let mut key = vec![b'k'];
    key.extend_from_slice(&index.to_be_bytes());
    key
}
fn decode(value: &[u8]) -> io::Result<u64> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| io::Error::other("invalid counter"))?;
    Ok(u64::from_le_bytes(bytes))
}
fn sqlite_connection(path: &Path) -> BenchResult<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(30))?;
    connection.execute_batch("PRAGMA synchronous=FULL;")?;
    Ok(connection)
}
fn sqlite_read(connection: &Connection, key: &[u8]) -> BenchResult<u64> {
    let mut statement = connection.prepare_cached("SELECT value, checksum FROM kv WHERE key=?1")?;
    let (value, checksum): (Vec<u8>, u32) =
        statement.query_row([key], |row| Ok((row.get(0)?, row.get(1)?)))?;
    if crc32fast::hash(&value) != checksum {
        return Err("SQLite checksum mismatch".into());
    }
    Ok(decode(&value)?)
}
fn sqlite_scan(connection: &Connection, upper: &[u8]) -> BenchResult<()> {
    let mut statement = connection.prepare_cached(
        "SELECT key, value, checksum FROM kv WHERE key > ?1 AND key < ?2 ORDER BY key LIMIT 1",
    )?;
    let (found, value, checksum): (Vec<u8>, Vec<u8>, u32) = statement
        .query_row(params![b"k".as_slice(), upper], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if found != key(0) || crc32fast::hash(&value) != checksum || decode(&value)? != 0 {
        return Err("SQLite scan verification failed".into());
    }
    Ok(())
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [mode, total, width, operations, parent] = args.as_slice() else {
        return Err("usage: checkpoint_range_bench <checkpoint|sqlite> <total-keys> <range-width> <operations> <existing-directory>".into());
    };
    let total: usize = total.parse()?;
    let width: usize = width.parse()?;
    let operations: usize = operations.parse()?;
    if !matches!(mode.as_str(), "checkpoint" | "sqlite")
        || !(1..=60_000).contains(&total)
        || !(1..=total).contains(&width)
        || !(1..=10_000).contains(&operations)
        || !Path::new(parent).is_dir()
    {
        return Err("invalid benchmark configuration".into());
    }
    let dir = tempfile::Builder::new()
        .prefix(".checkpoint-range-bench-")
        .tempdir_in(parent)?;
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let sqlite_path = dir.path().join("sqlite.db");
    let upper = key(width as u32);
    let zero = 0_u64.to_le_bytes();
    let mut samples = Vec::with_capacity(operations);
    let started;
    if mode == "checkpoint" {
        let ordinary = Store::open(&path)?;
        let keys = (0..total)
            .map(|index| key(index as u32))
            .collect::<Vec<_>>();
        for chunk in keys.chunks(1024) {
            let operations = chunk
                .iter()
                .map(|key| BatchOperation::Put { key, value: &zero })
                .collect::<Vec<_>>();
            ordinary.write_batch(&operations)?;
        }
        ordinary.put(b"z-counter", &zero)?;
        let digest = ordinary.checkpoint_to(&checkpoint)?;
        drop(ordinary);
        let store = CheckpointStore::open(&path, &checkpoint, &digest)?;
        store.transaction_with_read_set(|tx| {
            let page = tx.scan(Some(b"k"), Some(&upper), 1)?;
            if page.len() != 1 || page[0].0 != key(0) || decode(&page[0].1)? != 0 {
                return Err(io::Error::other("checkpoint warm-up scan mismatch"));
            }
            Ok(())
        })?;
        started = Instant::now();
        for _ in 0..operations {
            let iteration = Instant::now();
            store.transaction_with_read_set(|tx| {
                let page = tx.scan(Some(b"k"), Some(&upper), 1)?;
                if page.len() != 1 || page[0].0 != key(0) || decode(&page[0].1)? != 0 {
                    return Err(io::Error::other("checkpoint scan mismatch"));
                }
                let old = tx
                    .get(b"z-counter")?
                    .ok_or_else(|| io::Error::other("counter missing"))?;
                let next = decode(&old)?
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("counter overflow"))?;
                tx.put(b"z-counter", &next.to_le_bytes())
            })?;
            samples.push(iteration.elapsed().as_nanos());
        }
        let elapsed = started.elapsed().as_nanos();
        drop(store);
        let reopened = Store::open(&path)?;
        if decode(
            &reopened
                .get(b"z-counter")?
                .ok_or("missing recovered counter")?,
        )? != operations as u64
        {
            return Err("checkpoint recovery verification failed".into());
        }
        emit(mode, total, width, operations, elapsed, &samples);
    } else {
        let mut conn = sqlite_connection(&sqlite_path)?;
        let journal: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if journal != "wal"
            || conn.query_row("PRAGMA synchronous", [], |row| row.get::<_, u32>(0))? != 2
        {
            return Err("SQLite WAL and synchronous=FULL required".into());
        }
        conn.execute_batch("CREATE TABLE kv(key BLOB PRIMARY KEY, value BLOB NOT NULL, checksum INTEGER NOT NULL) WITHOUT ROWID;")?;
        {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            {
                let mut insert =
                    tx.prepare_cached("INSERT INTO kv(key,value,checksum) VALUES(?1,?2,?3)")?;
                for index in 0..total {
                    insert.execute(params![
                        key(index as u32),
                        &zero[..],
                        crc32fast::hash(&zero)
                    ])?;
                }
                insert.execute(params![
                    b"z-counter".as_slice(),
                    &zero[..],
                    crc32fast::hash(&zero)
                ])?;
            }
            tx.commit()?;
        }
        sqlite_scan(&conn, &upper)?;
        started = Instant::now();
        for _ in 0..operations {
            let iteration = Instant::now();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            sqlite_scan(&tx, &upper)?;
            let next = sqlite_read(&tx, b"z-counter")?
                .checked_add(1)
                .ok_or("counter overflow")?
                .to_le_bytes();
            {
                let mut update =
                    tx.prepare_cached("UPDATE kv SET value=?1,checksum=?2 WHERE key=?3")?;
                if update.execute(params![
                    &next[..],
                    crc32fast::hash(&next),
                    b"z-counter".as_slice()
                ])? != 1
                {
                    return Err("SQLite update affected wrong row count".into());
                }
            }
            tx.commit()?;
            samples.push(iteration.elapsed().as_nanos());
        }
        let elapsed = started.elapsed().as_nanos();
        drop(conn);
        let reopened = sqlite_connection(&sqlite_path)?;
        if sqlite_read(&reopened, b"z-counter")? != operations as u64 {
            return Err("SQLite recovery verification failed".into());
        }
        emit(mode, total, width, operations, elapsed, &samples);
    }
    Ok(())
}
fn emit(
    mode: &str,
    total: usize,
    width: usize,
    operations: usize,
    elapsed: u128,
    samples: &[u128],
) {
    let version = if mode == "sqlite" {
        rusqlite::version()
    } else {
        env!("CARGO_PKG_VERSION")
    };
    let samples = samples
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "backend={mode} version={version} total={total} width={width} operations={operations} elapsed_ns={elapsed} samples_ns={samples}"
    );
}
