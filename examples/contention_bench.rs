//! Small durable writes competing with a stream of large durable writes.
//! See docs/BENCHMARKS.md for usage, controls, and measurement limitations.
mod support;

use database_engine::Store;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use support::{BenchResult, Worker, show_latency, sqlite_connection};

const MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_BULK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const SMALL_VALUE_BYTES: usize = 64;

struct Config {
    backend: String,
    bulk_bytes: usize,
    bulk_writes: usize,
    small_clients: usize,
    small_ops: usize,
    directory: PathBuf,
}

impl Config {
    fn parse(args: &[String]) -> BenchResult<Self> {
        if args.len() != 6 {
            return Err("usage: contention_bench <prototype|sqlite> <bulk-bytes> <bulk-writes> <small-clients> <small-ops-per-client> <existing-directory>".into());
        }
        let config = Self {
            backend: args[0].clone(),
            bulk_bytes: args[1].parse()?,
            bulk_writes: args[2].parse()?,
            small_clients: args[3].parse()?,
            small_ops: args[4].parse()?,
            directory: PathBuf::from(&args[5]),
        };
        if !matches!(config.backend.as_str(), "prototype" | "sqlite")
            || !(1..=MAX_VALUE_BYTES).contains(&config.bulk_bytes)
            || !(1..=1000).contains(&config.bulk_writes)
            || !(1..=32).contains(&config.small_clients)
            || !(1..=100_000).contains(&config.small_ops)
            || (config.bulk_writes as u64 + 1) * config.bulk_bytes as u64 > MAX_BULK_BYTES
            || !config.directory.is_dir()
        {
            return Err("invalid parameters: bulk values 1..16777216 bytes, 1..1000 bulk writes, 1..32 small clients, 1..100000 operations per client, at most 8 GiB of bulk values, and an existing directory are required".into());
        }
        Ok(config)
    }
}

fn workers(config: &Config, directory: &Path) -> BenchResult<(Worker, Vec<Worker>)> {
    let (mut validator, workers) = if config.backend == "prototype" {
        let store = Arc::new(Store::open(directory.join("store"))?);
        (
            Worker::Prototype(Arc::clone(&store)),
            (0..config.small_clients + 1)
                .map(|_| Worker::Prototype(Arc::clone(&store)))
                .collect::<Vec<_>>(),
        )
    } else {
        let path = directory.join("sqlite.db");
        let connection = sqlite_connection(&path)?;
        let journal: String =
            connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if journal != "wal"
            || connection.query_row("PRAGMA synchronous", [], |row| row.get::<_, u32>(0))? != 2
        {
            return Err("SQLite WAL mode and synchronous=FULL are required".into());
        }
        connection.execute_batch("CREATE TABLE kv(key BLOB PRIMARY KEY, value BLOB NOT NULL, checksum INTEGER NOT NULL) WITHOUT ROWID;")?;
        let workers = (0..config.small_clients + 1)
            .map(|_| sqlite_connection(&path).map(Worker::Sqlite))
            .collect::<BenchResult<Vec<_>>>()?;
        (Worker::Sqlite(connection), workers)
    };
    validator.write(b"bulk", &vec![0; config.bulk_bytes])?;
    validator.read(b"bulk", config.bulk_bytes)?;
    for client in 0..config.small_clients {
        let key = (client as u64).to_be_bytes();
        validator.write(&key, &[0; SMALL_VALUE_BYTES])?;
        validator.read(&key, SMALL_VALUE_BYTES)?;
    }
    Ok((validator, workers))
}

enum Samples {
    Bulk { values: Vec<u64>, elapsed_ns: u64 },
    Small { all: Vec<u64>, overlap: Vec<u64> },
}

// Clear the overlap marker even if the bulk worker returns an error or panics.
struct BulkActivity(Arc<AtomicBool>);
impl Drop for BulkActivity {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn nanos(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn main() -> BenchResult<()> {
    let config = Config::parse(&std::env::args().skip(1).collect::<Vec<_>>())?;
    let directory = tempfile::Builder::new()
        .prefix(".db-contention-bench-")
        .tempdir_in(&config.directory)?;
    let (mut validator, mut workers) = workers(&config, directory.path())?;
    let bulk_worker = workers.pop().ok_or("bulk worker missing")?;
    let barrier = Arc::new(Barrier::new(config.small_clients + 2));
    let active = Arc::new(AtomicBool::new(true));
    let bulk_barrier = Arc::clone(&barrier);
    let bulk_active = Arc::clone(&active);
    let bulk_bytes = config.bulk_bytes;
    let bulk_writes = config.bulk_writes;
    let mut threads = Vec::new();
    threads.push(thread::spawn(move || -> BenchResult<(Samples, Worker)> {
        let _active = BulkActivity(bulk_active);
        let mut worker = bulk_worker;
        let mut value = vec![0; bulk_bytes];
        let mut samples = Vec::with_capacity(bulk_writes);
        bulk_barrier.wait();
        let stream_started = Instant::now();
        for sequence in 0..bulk_writes {
            value.fill(sequence as u8);
            let started = Instant::now();
            worker.write(b"bulk", &value)?;
            samples.push(nanos(started));
        }
        Ok((
            Samples::Bulk {
                values: samples,
                elapsed_ns: nanos(stream_started),
            },
            worker,
        ))
    }));
    for (id, mut worker) in workers.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let active = Arc::clone(&active);
        let small_ops = config.small_ops;
        threads.push(thread::spawn(move || -> BenchResult<(Samples, Worker)> {
            let key = (id as u64).to_be_bytes();
            let mut all = Vec::with_capacity(small_ops);
            let mut overlap = Vec::with_capacity(small_ops);
            let mut value = [0; SMALL_VALUE_BYTES];
            barrier.wait();
            for sequence in 0..small_ops {
                value.fill(sequence as u8);
                let overlapping = active.load(Ordering::Acquire);
                let started = Instant::now();
                worker.write(&key, &value)?;
                let elapsed = nanos(started);
                all.push(elapsed);
                // Keep writes that finish after the stream so its longest
                // waits are not silently omitted from the measured sample.
                if overlapping {
                    overlap.push(elapsed);
                }
            }
            Ok((Samples::Small { all, overlap }, worker))
        }));
    }
    let started = Instant::now();
    barrier.wait();
    let mut small_all = Vec::new();
    let mut small_overlap = Vec::new();
    let mut bulk = Vec::new();
    let mut bulk_elapsed_ns = 0;
    let mut held_workers = Vec::new();
    let mut failure = None;
    for handle in threads {
        match handle.join() {
            Ok(Ok((samples, worker))) => {
                match samples {
                    Samples::Bulk { values, elapsed_ns } => {
                        bulk = values;
                        bulk_elapsed_ns = elapsed_ns;
                    }
                    Samples::Small { all, overlap } => {
                        small_all.extend(all);
                        small_overlap.extend(overlap);
                    }
                }
                held_workers.push(worker);
            }
            Ok(Err(error)) => {
                failure.get_or_insert(error);
            }
            Err(_) => {
                failure.get_or_insert_with(|| "benchmark worker panicked".into());
            }
        }
    }
    let elapsed = started.elapsed();
    if let Some(error) = failure {
        return Err(error);
    }
    // Validate the last value written by every worker, outside the timers.
    validator.read(b"bulk", config.bulk_bytes)?;
    let value = validator.read_value(b"bulk")?;
    if value
        .iter()
        .any(|byte| *byte != (config.bulk_writes - 1) as u8)
    {
        return Err("bulk value mismatch".into());
    }
    for id in 0..config.small_clients {
        let key = (id as u64).to_be_bytes();
        validator.read(&key, SMALL_VALUE_BYTES)?;
        let value = validator.read_value(&key)?;
        if value
            .iter()
            .any(|byte| *byte != (config.small_ops - 1) as u8)
        {
            return Err("small value mismatch".into());
        }
    }
    println!(
        "backend={} bulk_bytes={} bulk_writes={} small_clients={} small_ops_per_client={} elapsed_ms={:.1} bulk_elapsed_ms={:.1} sqlite_version={}",
        config.backend,
        config.bulk_bytes,
        config.bulk_writes,
        config.small_clients,
        config.small_ops,
        elapsed.as_secs_f64() * 1000.0,
        bulk_elapsed_ns as f64 / 1_000_000.0,
        rusqlite::version()
    );
    println!(
        "mode=closed-loop durability=sync-before-ack sqlite=WAL-FULL overlap=small-write-starts-during-bulk-stream"
    );
    show_latency("small_all", &mut small_all);
    show_latency("small_during_bulk", &mut small_overlap);
    show_latency("bulk", &mut bulk);
    drop(held_workers);
    Ok(())
}
