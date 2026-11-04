//! Disposable serializable two-key transaction benchmark.
use database_engine::{CheckpointStore, CheckpointTransaction, Store, Transaction};
use rusqlite::{Connection, TransactionBehavior, params};
use std::error::Error;
use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy)]
enum Mode {
    Conservative,
    ReadSet,
    ReadSetFifo,
    Checkpoint,
    CheckpointFifo,
    CheckpointReadSet,
    CheckpointReadSetFifo,
    Sqlite,
}
enum Worker {
    Engine(Arc<Store>),
    Checkpoint(Arc<CheckpointStore>),
    Sqlite(Connection),
}

trait CounterTransaction {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()>;
}

impl CounterTransaction for Transaction<'_> {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        Transaction::get(self, key)
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        Transaction::put(self, key, value)
    }
}

impl CounterTransaction for CheckpointTransaction<'_> {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        CheckpointTransaction::get(self, key)
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        CheckpointTransaction::put(self, key, value)
    }
}

fn key(prefix: u8, id: usize) -> Vec<u8> {
    let mut key = vec![prefix];
    key.extend_from_slice(&(id as u32).to_be_bytes());
    key
}

fn decode(value: &[u8]) -> io::Result<u64> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| io::Error::other("invalid counter"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn increment(tx: &mut impl CounterTransaction, first: &[u8], second: &[u8]) -> io::Result<()> {
    let left = tx
        .get(first)?
        .ok_or_else(|| io::Error::other("missing counter"))?;
    let right = tx
        .get(second)?
        .ok_or_else(|| io::Error::other("missing mirror"))?;
    let value = decode(&left)?;
    if decode(&right)? != value {
        return Err(io::Error::other("counter mismatch"));
    }
    let next = value
        .checked_add(1)
        .ok_or_else(|| io::Error::other("counter overflow"))?
        .to_le_bytes();
    tx.put(first, &next)?;
    tx.put(second, &next)
}

fn retry(
    mut action: impl FnMut() -> io::Result<()>,
    conflicts: &mut usize,
    operations: usize,
) -> BenchResult<()> {
    loop {
        match action() {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                *conflicts += 1;
                if *conflicts > operations * 100_000 {
                    return Err("retry limit exceeded".into());
                }
                thread::yield_now();
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn connection(path: &Path) -> BenchResult<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    conn.execute_batch("PRAGMA synchronous=FULL;")?;
    Ok(conn)
}

fn sqlite_read(conn: &Connection, key: &[u8]) -> BenchResult<u64> {
    let mut statement = conn.prepare_cached("SELECT value, checksum FROM kv WHERE key=?1")?;
    let (value, checksum): (Vec<u8>, u32) =
        statement.query_row([key], |row| Ok((row.get(0)?, row.get(1)?)))?;
    if crc32fast::hash(&value) != checksum {
        return Err("SQLite value checksum mismatch".into());
    }
    Ok(decode(&value)?)
}

fn sqlite_increment(conn: &mut Connection, first: &[u8], second: &[u8]) -> BenchResult<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let value = sqlite_read(&tx, first)?;
    if sqlite_read(&tx, second)? != value {
        return Err("SQLite counter mismatch".into());
    }
    let next = value
        .checked_add(1)
        .ok_or("counter overflow")?
        .to_le_bytes();
    let checksum = crc32fast::hash(&next);
    {
        let mut statement =
            tx.prepare_cached("UPDATE kv SET value=?1, checksum=?2 WHERE key=?3")?;
        for key in [first, second] {
            if statement.execute(params![&next[..], checksum, key])? != 1 {
                return Err("SQLite update affected the wrong number of rows".into());
            }
        }
    }
    tx.commit()?;
    Ok(())
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [backend, clients, operations, workload, parent] = args.as_slice() else {
        return Err("usage: transaction_bench <conservative|readset|readset-fifo|checkpoint|checkpoint-fifo|checkpoint-readset|checkpoint-readset-fifo|sqlite> <clients> <operations-per-client> <disjoint|shared> <existing-directory>".into());
    };
    let mode = match backend.as_str() {
        "conservative" => Mode::Conservative,
        "readset" => Mode::ReadSet,
        "readset-fifo" => Mode::ReadSetFifo,
        "checkpoint" => Mode::Checkpoint,
        "checkpoint-fifo" => Mode::CheckpointFifo,
        "checkpoint-readset" => Mode::CheckpointReadSet,
        "checkpoint-readset-fifo" => Mode::CheckpointReadSetFifo,
        "sqlite" => Mode::Sqlite,
        _ => return Err("unknown backend".into()),
    };
    let clients: usize = clients.parse()?;
    let operations: usize = operations.parse()?;
    if !(1..=8).contains(&clients)
        || !(1..=10_000).contains(&operations)
        || !matches!(workload.as_str(), "disjoint" | "shared")
        || !Path::new(parent).is_dir()
    {
        return Err("invalid workload configuration".into());
    }
    let directory = tempfile::Builder::new()
        .prefix(".transaction-bench-")
        .tempdir_in(parent)?;
    let path = directory.path().join("store");
    let sqlite_path = directory.path().join("sqlite.db");
    let pairs = if workload == "shared" { 1 } else { clients };
    let mut store = None;
    let mut checkpoint_store = None;
    let mut sqlite_keeper = None;
    let mut workers = Vec::new();
    if matches!(mode, Mode::Sqlite) {
        let conn = connection(&sqlite_path)?;
        let journal: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if journal != "wal"
            || conn.query_row("PRAGMA synchronous", [], |row| row.get::<_, u32>(0))? != 2
        {
            return Err("SQLite WAL and synchronous=FULL required".into());
        }
        conn.execute_batch("CREATE TABLE kv(key BLOB PRIMARY KEY, value BLOB NOT NULL, checksum INTEGER NOT NULL) WITHOUT ROWID;")?;
        let zero = 0_u64.to_le_bytes();
        for id in 0..pairs {
            for key in [key(b'a', id), key(b'b', id)] {
                conn.execute(
                    "INSERT INTO kv(key,value,checksum) VALUES(?1,?2,?3)",
                    params![key, &zero[..], crc32fast::hash(&zero)],
                )?;
            }
        }
        for _ in 0..clients {
            workers.push(Worker::Sqlite(connection(&sqlite_path)?));
        }
        sqlite_keeper = Some(conn);
    } else {
        let opened = Store::open(&path)?;
        for id in 0..pairs {
            opened.put(&key(b'a', id), &0_u64.to_le_bytes())?;
            opened.put(&key(b'b', id), &0_u64.to_le_bytes())?;
        }
        if matches!(
            mode,
            Mode::Checkpoint
                | Mode::CheckpointFifo
                | Mode::CheckpointReadSet
                | Mode::CheckpointReadSetFifo
        ) {
            let checkpoint = directory.path().join("checkpoint");
            let digest = opened.checkpoint_to(&checkpoint)?;
            drop(opened);
            let opened = Arc::new(CheckpointStore::open(&path, &checkpoint, &digest)?);
            for _ in 0..clients {
                workers.push(Worker::Checkpoint(Arc::clone(&opened)));
            }
            checkpoint_store = Some(opened);
        } else {
            let opened = Arc::new(opened);
            for _ in 0..clients {
                workers.push(Worker::Engine(Arc::clone(&opened)));
            }
            store = Some(opened);
        }
    }
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut threads = Vec::new();
    for (client, mut worker) in workers.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let id = if workload == "shared" { 0 } else { client };
        threads.push(thread::spawn(move || -> BenchResult<(usize, Vec<u128>)> {
            let first = key(b'a', id);
            let second = key(b'b', id);
            let mut conflicts = 0;
            let mut times = Vec::with_capacity(operations);
            barrier.wait();
            for _ in 0..operations {
                let started = Instant::now();
                match &mut worker {
                    Worker::Sqlite(conn) => sqlite_increment(conn, &first, &second)?,
                    Worker::Engine(store) => retry(
                        || match mode {
                            Mode::Conservative => {
                                store.transaction(|tx| increment(tx, &first, &second))
                            }
                            Mode::ReadSet => {
                                store.transaction_with_read_set(|tx| increment(tx, &first, &second))
                            }
                            Mode::ReadSetFifo => store.transaction_with_read_set_fifo(|tx| {
                                increment(tx, &first, &second)
                            }),
                            Mode::Checkpoint
                            | Mode::CheckpointFifo
                            | Mode::CheckpointReadSet
                            | Mode::CheckpointReadSetFifo
                            | Mode::Sqlite => {
                                unreachable!()
                            }
                        },
                        &mut conflicts,
                        operations,
                    )?,
                    Worker::Checkpoint(store) => {
                        retry(
                            || match mode {
                                Mode::Checkpoint => {
                                    store.transaction(|tx| increment(tx, &first, &second))
                                }
                                Mode::CheckpointFifo => {
                                    store.transaction_with_fifo(|tx| increment(tx, &first, &second))
                                }
                                Mode::CheckpointReadSet => store
                                    .transaction_with_read_set(|tx| increment(tx, &first, &second)),
                                Mode::CheckpointReadSetFifo => store
                                    .transaction_with_read_set_fifo(|tx| {
                                        increment(tx, &first, &second)
                                    }),
                                _ => unreachable!(),
                            },
                            &mut conflicts,
                            operations,
                        )?
                    }
                }
                times.push(started.elapsed().as_nanos());
            }
            Ok((conflicts, times))
        }));
    }
    let started = Instant::now();
    barrier.wait();
    let mut conflicts = Vec::with_capacity(clients);
    let mut samples = Vec::with_capacity(clients * operations);
    let mut failure = None;
    for handle in threads {
        match handle.join() {
            Ok(Ok((count, timings))) => {
                conflicts.push(count);
                samples.extend(timings);
            }
            Ok(Err(error)) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
            Err(_) => {
                if failure.is_none() {
                    failure = Some("benchmark worker panicked".into());
                }
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    let elapsed = started.elapsed().as_nanos();
    drop(store);
    drop(checkpoint_store);
    drop(sqlite_keeper);
    if matches!(mode, Mode::Sqlite) {
        let conn = connection(&sqlite_path)?;
        for id in 0..pairs {
            let expected = if workload == "shared" {
                clients * operations
            } else {
                operations
            } as u64;
            if sqlite_read(&conn, &key(b'a', id))? != expected
                || sqlite_read(&conn, &key(b'b', id))? != expected
            {
                return Err("SQLite post-reopen verification failed".into());
            }
        }
    } else {
        let reopened = Store::open(&path)?;
        for id in 0..pairs {
            let expected = if workload == "shared" {
                clients * operations
            } else {
                operations
            } as u64;
            for key in [key(b'a', id), key(b'b', id)] {
                let value = reopened.get(&key)?.ok_or("missing recovered counter")?;
                if decode(&value)? != expected {
                    return Err("post-reopen verification failed".into());
                }
            }
        }
    }
    let samples = samples
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let conflicts = conflicts
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let version = if matches!(mode, Mode::Sqlite) {
        rusqlite::version()
    } else {
        env!("CARGO_PKG_VERSION")
    };
    println!(
        "backend={backend} version={version} clients={clients} operations={operations} workload={workload} elapsed_ns={elapsed} conflicts={conflicts} samples_ns={samples}"
    );
    Ok(())
}
