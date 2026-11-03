//! Small, closed-loop single-process baseline; see docs/BENCHMARKS.md.

mod support;

use database_engine::Store;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
#[cfg(test)]
use support::nearest_rank;
use support::{BenchResult, Worker, show_latency, sqlite_connection};

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
