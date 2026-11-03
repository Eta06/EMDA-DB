//! Warm-cache reopen benchmark for a generated, checksummed store.
//!
//! See docs/BENCHMARKS.md for setup, measurement limits, and safe usage.
use database_engine::Store;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const DEFAULT_VALUE_SIZE: usize = 64;
const MAX_VALUE_SIZE: usize = 16 * 1024 * 1024;
const PREPARE_WORKERS: u64 = 8;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn count(input: &str) -> BenchResult<u64> {
    let count = input.parse()?;
    if !(1..=1_000_000).contains(&count) {
        return Err("record count must be between 1 and 1000000".into());
    }
    Ok(count)
}

fn value_size(input: &str) -> BenchResult<usize> {
    let size = input.parse()?;
    if !(1..=MAX_VALUE_SIZE).contains(&size) {
        return Err("value bytes must be between 1 and 16777216".into());
    }
    Ok(size)
}

fn prepare(path: &Path, records: u64, value_size: usize) -> BenchResult<()> {
    // Never reuse or overwrite an existing path, even if it is an empty directory.
    fs::create_dir(path)?;
    let store = Arc::new(Store::open(path)?);
    let workers = records.min(PREPARE_WORKERS);
    let mut handles = Vec::new();
    for worker in 0..workers {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || -> io::Result<()> {
            let mut id = worker;
            let mut value = vec![0; value_size];
            while id < records {
                value.fill(id as u8);
                store.put(&id.to_be_bytes(), &value)?;
                id += workers;
            }
            Ok(())
        }));
    }
    // Join every worker before returning, including after an error.
    let mut failure = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
            Err(_) => {
                if failure.is_none() {
                    failure = Some(io::Error::other("fixture writer panicked"));
                }
            }
        }
    }
    if let Some(error) = failure {
        return Err(error.into());
    }
    println!(
        "prepared_records={records} value_bytes={value_size} log_bytes={}",
        fs::metadata(path.join("records.log"))?.len()
    );
    Ok(())
}

fn measure(path: &Path, records: u64, repetitions: usize, value_size: usize) -> BenchResult<()> {
    if !(1..=100).contains(&repetitions)
        || !path.join("records.log").is_file()
        || !path.join("store.lock").is_file()
    {
        return Err("an existing prepared store and 1..100 repetitions are required".into());
    }
    let mut samples = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let started = Instant::now();
        let store = Store::open(path)?;
        samples.push(started.elapsed().as_micros());
        // Opening verifies every record checksum. These additional samples
        // check that the recovered index contains the expected values.
        for id in [0, records / 2, records - 1] {
            let value = store.get(&id.to_be_bytes())?.ok_or("sample key missing")?;
            if value.len() != value_size || value.iter().any(|byte| *byte != id as u8) {
                return Err("sample value mismatch".into());
            }
        }
    }
    samples.sort_unstable();
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1];
    println!(
        "expected_records={records} repetitions={repetitions} value_bytes={value_size} log_bytes={} open_p50_us={} open_p95_us={} open_p99_us={} open_max_us={}",
        fs::metadata(path.join("records.log"))?.len(),
        percentile(50),
        percentile(95),
        percentile(99),
        samples[samples.len() - 1]
    );
    println!("open_samples_us={samples:?}");
    Ok(())
}

fn main() -> BenchResult<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [mode, path, records] if mode == "prepare" => {
            prepare(Path::new(path), count(records)?, DEFAULT_VALUE_SIZE)
        }
        [mode, path, records, size] if mode == "prepare" => {
            prepare(Path::new(path), count(records)?, value_size(size)?)
        }
        [mode, path, records, repetitions] if mode == "measure" => {
            measure(Path::new(path), count(records)?, repetitions.parse()?, DEFAULT_VALUE_SIZE)
        }
        [mode, path, records, repetitions, size] if mode == "measure" => {
            measure(Path::new(path), count(records)?, repetitions.parse()?, value_size(size)?)
        }
        _ => Err("usage: recovery_bench prepare <new-store> <records> [value-bytes] | measure <existing-store> <records> <repetitions> [value-bytes]".into()),
    }
}
