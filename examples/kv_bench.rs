//! Small, closed-loop single-process baseline; see docs/BENCHMARKS.md.

use database_engine::Store;
use rusqlite::{Connection, params};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const DEFAULT_VALUE_SIZE: usize = 64;
const MAX_VALUE_SIZE: usize = 16 * 1024 * 1024;

struct Config {
    backend: String,
    clients: usize,
    operations: usize,
    keys: usize,
    write_percent: u64,
    directory: PathBuf,
    value_size: usize,
}

impl Config {
    fn parse() -> BenchResult<Self> {
        Self::parse_args(&std::env::args().skip(1).collect::<Vec<_>>())
    }

    fn parse_args(args: &[String]) -> BenchResult<Self> {
        if !(6..=7).contains(&args.len()) {
            return Err("usage: kv_bench <prototype|sqlite> <clients> <ops-per-client> <keys> <write-percent> <existing-directory> [value-bytes]".into());
        }
        let result = Self {
            backend: args[0].clone(),
            clients: args[1].parse()?,
            operations: args[2].parse()?,
            keys: args[3].parse()?,
            write_percent: args[4].parse()?,
            directory: PathBuf::from(&args[5]),
            value_size: args.get(6).map_or(Ok(DEFAULT_VALUE_SIZE), |s| s.parse())?,
        };
        if !matches!(result.backend.as_str(), "prototype" | "sqlite")
            || !(1..=64).contains(&result.clients)
            || !(1..=100_000).contains(&result.operations)
            || !(1..=1_000_000).contains(&result.keys)
            || result.write_percent > 100
            || !(1..=MAX_VALUE_SIZE).contains(&result.value_size)
            || !result.directory.is_dir()
        {
            return Err("invalid parameters: clients 1..64, operations 1..100000, keys 1..1000000, write percentage 0..100, value bytes 1..16777216, and an existing directory are required".into());
        }
        Ok(result)
    }
}

enum Worker {
    Prototype(Arc<Store>),
    Sqlite(Connection),
}

impl Worker {
    fn read(&mut self, key: &[u8], value_size: usize) -> BenchResult<()> {
        let value = match self {
            Self::Prototype(store) => store.get(key)?.ok_or("missing key")?,
            Self::Sqlite(connection) => {
                let mut statement =
                    connection.prepare_cached("SELECT value, checksum FROM kv WHERE key = ?1")?;
                let (value, expected): (Vec<u8>, u32) =
                    statement.query_row([key], |row| Ok((row.get(0)?, row.get(1)?)))?;
                if crc32fast::hash(&value) != expected {
                    return Err("SQLite value checksum mismatch".into());
                }
                value
            }
        };
        if value.len() != value_size || value.iter().any(|byte| *byte != value[0]) {
            return Err("read returned an invalid value".into());
        }
        Ok(())
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> BenchResult<()> {
        match self {
            Self::Prototype(store) => store.put(key, value)?,
            Self::Sqlite(connection) => {
                let mut statement = connection.prepare_cached(
                    "INSERT INTO kv(key, value, checksum) VALUES (?1, ?2, ?3) \
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value, checksum = excluded.checksum",
                )?;
                statement.execute(params![key, value, crc32fast::hash(value)])?;
            }
        }
        Ok(())
    }
}

fn sqlite_connection(path: &Path) -> BenchResult<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(30))?;
    connection.execute_batch("PRAGMA synchronous = FULL;")?;
    Ok(connection)
}

fn workers(config: &Config, directory: &Path) -> BenchResult<Vec<Worker>> {
    let initial_value = vec![0; config.value_size];
    if config.backend == "prototype" {
        let store = Arc::new(Store::open(directory.join("store"))?);
        let mut initializer = Worker::Prototype(Arc::clone(&store));
        for key in 0..config.keys {
            initializer.write(&(key as u64).to_be_bytes(), &initial_value)?;
        }
        return Ok((0..config.clients)
            .map(|_| Worker::Prototype(Arc::clone(&store)))
            .collect());
    }
    let path = directory.join("sqlite.db");
    let connection = sqlite_connection(&path)?;
    let journal: String =
        connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if journal != "wal" {
        return Err("SQLite WAL mode is unavailable".into());
    }
    if connection.query_row("PRAGMA synchronous", [], |row| row.get::<_, u32>(0))? != 2 {
        return Err("SQLite synchronous=FULL is unavailable".into());
    }
    connection.execute_batch("CREATE TABLE kv(key BLOB PRIMARY KEY, value BLOB NOT NULL, checksum INTEGER NOT NULL) WITHOUT ROWID;")?;
    let mut initializer = Worker::Sqlite(connection);
    for key in 0..config.keys {
        initializer.write(&(key as u64).to_be_bytes(), &initial_value)?;
    }
    drop(initializer);
    (0..config.clients)
        .map(|_| sqlite_connection(&path).map(Worker::Sqlite))
        .collect()
}

#[derive(Default)]
struct Samples {
    reads: Vec<u64>,
    writes: Vec<u64>,
    elapsed: Duration,
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn workload(
    mut worker: Worker,
    barrier: Arc<Barrier>,
    id: usize,
    operations: usize,
    keys: usize,
    write_percent: u64,
    value_size: usize,
) -> BenchResult<(Samples, Worker)> {
    let mut random = 0x9e37_79b9_7f4a_7c15_u64 ^ (id as u64 + 1);
    let mut samples = Samples::default();
    let mut value = vec![0; value_size];
    barrier.wait();
    let started = Instant::now();
    for _ in 0..operations {
        let write = next_random(&mut random) % 100 < write_percent;
        let key = (next_random(&mut random) % keys as u64).to_be_bytes();
        value.fill(next_random(&mut random) as u8);
        let started = Instant::now();
        if write {
            worker.write(&key, &value)?;
            samples
                .writes
                .push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        } else {
            worker.read(&key, value_size)?;
            samples
                .reads
                .push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }
    }
    samples.elapsed = started.elapsed();
    // Keep connections alive until the measurement stops so SQLite's last
    // connection cleanup/checkpoint is not included in operation throughput.
    Ok((samples, worker))
}

fn show_latency(label: &str, samples: &mut [u64]) {
    if samples.is_empty() {
        println!("{label}: count=0");
        return;
    }
    samples.sort_unstable();
    let p = |per_mille: u64| -> f64 { nearest_rank(samples, per_mille) as f64 / 1_000.0 };
    println!(
        "{label}: count={} p50_us={:.1} p95_us={:.1} p99_us={:.1} p99_9_us={:.1} max_us={:.1}",
        samples.len(),
        p(500),
        p(950),
        p(990),
        p(999),
        p(1000)
    );
}

// `samples` must already be sorted, and per_mille must be between 1 and 1000.
fn nearest_rank(samples: &[u64], per_mille: u64) -> u64 {
    debug_assert!(!samples.is_empty() && (1..=1000).contains(&per_mille));
    // Config caps the sample count at 6.4 million, so the u64 product cannot
    // overflow, including on targets where usize is 32-bit.
    let rank = (samples.len() as u64 * per_mille).div_ceil(1000);
    samples[(rank - 1) as usize]
}

fn main() -> BenchResult<()> {
    let config = Config::parse()?;
    let directory = tempfile::Builder::new()
        .prefix(".db-bench-")
        .tempdir_in(&config.directory)?;
    let mut workers = workers(&config, directory.path())?;
    // Warm a small identical key range before measuring; this is not a cold-cache test.
    for key in 0..config.keys.min(100) {
        workers[0].read(&(key as u64).to_be_bytes(), config.value_size)?;
    }
    let barrier = Arc::new(Barrier::new(config.clients + 1));
    let mut threads = Vec::new();
    for (id, worker) in workers.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let operations = config.operations;
        let keys = config.keys;
        let write_percent = config.write_percent;
        let value_size = config.value_size;
        threads.push(thread::spawn(move || {
            workload(
                worker,
                barrier,
                id,
                operations,
                keys,
                write_percent,
                value_size,
            )
        }));
    }
    let started = Instant::now();
    barrier.wait();
    let mut combined = Samples::default();
    let mut held_workers = Vec::with_capacity(config.clients);
    let mut client_summaries = Vec::with_capacity(config.clients);
    let mut failure: Option<Box<dyn Error + Send + Sync>> = None;
    for (id, worker) in threads.into_iter().enumerate() {
        match worker.join() {
            Ok(Ok((samples, connection))) => {
                client_summaries.push((
                    id,
                    samples.elapsed,
                    samples.reads.len(),
                    samples.writes.len(),
                    samples.reads.iter().copied().max().unwrap_or(0),
                    samples.writes.iter().copied().max().unwrap_or(0),
                ));
                combined.reads.extend(samples.reads);
                combined.writes.extend(samples.writes);
                held_workers.push(connection);
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
    let elapsed = started.elapsed();
    if let Some(error) = failure {
        return Err(error);
    }
    let completed = combined.reads.len() + combined.writes.len();
    println!(
        "backend={} clients={} keys={} value_bytes={} write_percent={} completed={} elapsed_ms={:.1} ops_per_sec={:.1}",
        config.backend,
        config.clients,
        config.keys,
        config.value_size,
        config.write_percent,
        completed,
        elapsed.as_secs_f64() * 1000.0,
        completed as f64 / elapsed.as_secs_f64()
    );
    println!(
        "mode=closed-loop seed=per-client-fixed warmup=up-to-100-reads durability=sync-before-ack sqlite=WAL-FULL sqlite_version={} debug_assertions={}",
        rusqlite::version(),
        cfg!(debug_assertions)
    );
    show_latency("read", &mut combined.reads);
    show_latency("write", &mut combined.writes);
    for (id, elapsed, reads, writes, max_read_ns, max_write_ns) in client_summaries {
        println!(
            "client={id} elapsed_ms={:.1} reads={reads} writes={writes} max_read_ms={:.3} max_write_ms={:.3}",
            elapsed.as_secs_f64() * 1000.0,
            max_read_ns as f64 / 1_000_000.0,
            max_write_ns as f64 / 1_000_000.0
        );
    }
    drop(held_workers);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Config, DEFAULT_VALUE_SIZE, MAX_VALUE_SIZE, nearest_rank};

    #[test]
    fn accepts_configurable_value_sizes_and_rejects_out_of_range() {
        let args = ["prototype", "2", "20", "16", "50", "."]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        assert_eq!(
            Config::parse_args(&args).unwrap().value_size,
            DEFAULT_VALUE_SIZE
        );
        let mut sized = args.clone();
        sized.push("4096".into());
        assert_eq!(Config::parse_args(&sized).unwrap().value_size, 4096);
        *sized.last_mut().unwrap() = "0".into();
        assert!(Config::parse_args(&sized).is_err());
        *sized.last_mut().unwrap() = (MAX_VALUE_SIZE + 1).to_string();
        assert!(Config::parse_args(&sized).is_err());
    }

    #[test]
    fn percentile_reports_rare_outliers_and_maximum() {
        let samples: Vec<_> = (1..=1000).collect();
        assert_eq!(nearest_rank(&samples, 500), 500);
        assert_eq!(nearest_rank(&samples, 990), 990);
        assert_eq!(nearest_rank(&samples, 999), 999);
        assert_eq!(nearest_rank(&samples, 1000), 1000);
        assert_eq!(nearest_rank(&[10, 20], 999), 20);
    }
}
