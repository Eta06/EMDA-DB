//! Closed-loop reader and transaction contention over a preloaded index.
use database_engine::{BatchOperation, Store};
use serde_json::json;
use std::error::Error;
use std::io::{self, ErrorKind, Read};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const MAX_READER_SAMPLES: usize = 20_000;

fn key(prefix: u8, number: usize) -> Vec<u8> {
    let mut result = vec![prefix];
    result.extend_from_slice(&(number as u64).to_be_bytes());
    result
}

fn counter(bytes: &[u8]) -> io::Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| io::Error::other("invalid counter"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [parent, base_keys, writers, operations] = args.as_slice() else {
        return Err("usage: snapshot_contention_bench <directory> <base-keys> <writers> <operations-per-writer>".into());
    };
    let base_keys: usize = base_keys.parse()?;
    let writers: usize = writers.parse()?;
    let operations: usize = operations.parse()?;
    if !Path::new(parent).is_dir()
        || !(1..=10_000).contains(&base_keys)
        || !(1..=8).contains(&writers)
        || !(1..=1000).contains(&operations)
    {
        return Err("invalid workload arguments".into());
    }
    let directory = tempfile::Builder::new()
        .prefix(".snapshot-contention-")
        .tempdir_in(parent)?;
    let path = directory.path().join("store");
    let store = Arc::new(Store::open(&path)?);
    let mut header = [0_u8; 8];
    std::fs::File::open(path.join("records.log"))?.read_exact(&mut header)?;
    let format = match &header {
        b"EMDALOG3" => "V3",
        b"EMDALOG2" => "V2",
        _ => return Err("unexpected newly created store format".into()),
    };
    for start in (0..base_keys).step_by(512) {
        let keys = (start..(start + 512).min(base_keys))
            .map(|n| key(b'x', n))
            .collect::<Vec<_>>();
        let batch = keys
            .iter()
            .map(|key| BatchOperation::Put {
                key,
                value: b"baseline",
            })
            .collect::<Vec<_>>();
        store.write_batch(&batch)?;
    }
    for number in 0..writers {
        store.put(&key(b'a', number), &0_u64.to_le_bytes())?;
        store.put(&key(b'b', number), &0_u64.to_le_bytes())?;
    }
    let barrier = Arc::new(Barrier::new(writers + 2));
    let stop = Arc::new(AtomicBool::new(false));
    let reader_store = Arc::clone(&store);
    let reader_barrier = Arc::clone(&barrier);
    let reader_stop = Arc::clone(&stop);
    let reader = thread::spawn(move || -> Result<(Vec<u128>, u64, u128)> {
        let mut samples = Vec::new();
        let mut reads = 0_u64;
        let mut maximum = 0_u128;
        let mut random = 0x9e37_79b9_7f4a_7c15_u64;
        reader_barrier.wait();
        while !reader_stop.load(Ordering::Acquire) {
            let start = Instant::now();
            let value = reader_store.get(&key(b'x', 0))?;
            let elapsed = start.elapsed().as_nanos();
            if value.as_deref() != Some(b"baseline") {
                return Err("reader value mismatch".into());
            }
            reads += 1;
            maximum = maximum.max(elapsed);
            if samples.len() < MAX_READER_SAMPLES {
                samples.push(elapsed);
            } else {
                // Sample the whole run, not just its beginning.
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let selected = random % reads;
                if selected < MAX_READER_SAMPLES as u64 {
                    samples[selected as usize] = elapsed;
                }
            }
        }
        Ok((samples, reads, maximum))
    });
    let mut threads = Vec::new();
    for number in 0..writers {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || -> Result<(Vec<u128>, u64)> {
            let first = key(b'a', number);
            let second = key(b'b', number);
            let mut samples = Vec::with_capacity(operations);
            let mut conflicts = 0_u64;
            barrier.wait();
            for _ in 0..operations {
                let start = Instant::now();
                loop {
                    match store.transaction(|tx| {
                        let a = tx
                            .get(&first)?
                            .ok_or_else(|| io::Error::other("counter missing"))?;
                        let b = tx
                            .get(&second)?
                            .ok_or_else(|| io::Error::other("mirror missing"))?;
                        let value = counter(&a)?;
                        if counter(&b)? != value {
                            return Err(io::Error::other("counter mismatch"));
                        }
                        let next = value
                            .checked_add(1)
                            .ok_or_else(|| io::Error::other("counter overflow"))?
                            .to_le_bytes();
                        tx.put(&first, &next)?;
                        tx.put(&second, &next)
                    }) {
                        Ok(()) => break,
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            conflicts += 1;
                            if conflicts > (operations as u64) * 100_000 {
                                return Err("retry limit exceeded".into());
                            }
                            thread::yield_now();
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                samples.push(start.elapsed().as_nanos());
            }
            Ok((samples, conflicts))
        }));
    }
    let start = Instant::now();
    barrier.wait();
    let mut writer_samples = Vec::with_capacity(writers * operations);
    let mut conflicts = 0_u64;
    let mut failure = None;
    for thread in threads {
        match thread.join() {
            Ok(Ok((samples, count))) => {
                writer_samples.extend(samples);
                conflicts += count;
            }
            Ok(Err(error)) => failure = Some(error),
            Err(_) => failure = Some("writer panicked".into()),
        }
    }
    let elapsed = start.elapsed().as_nanos();
    stop.store(true, Ordering::Release);
    let (reader_samples, reads, reader_max_ns) = reader.join().map_err(|_| "reader panicked")??;
    if let Some(error) = failure {
        return Err(error);
    }
    if writer_samples.len() != writers * operations || reader_samples.is_empty() {
        return Err("incomplete samples".into());
    }
    for number in 0..writers {
        for prefix in *b"ab" {
            let value = store
                .get(&key(prefix, number))?
                .ok_or("final counter missing")?;
            if counter(&value)? != operations as u64 {
                return Err("final counter mismatch".into());
            }
        }
    }
    drop(store);
    let reopened = Store::open(&path)?;
    for number in 0..writers {
        for prefix in *b"ab" {
            let value = reopened
                .get(&key(prefix, number))?
                .ok_or("recovered counter missing")?;
            if counter(&value)? != operations as u64 {
                return Err("recovered counter mismatch".into());
            }
        }
    }
    for start in (0..base_keys).step_by(512) {
        let keys = (start..(start + 512).min(base_keys))
            .map(|number| key(b'x', number))
            .collect::<Vec<_>>();
        let borrowed = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
        if reopened
            .get_many(&borrowed)?
            .iter()
            .any(|value| value.as_deref() != Some(b"baseline"))
        {
            return Err("recovered baseline mismatch".into());
        }
    }
    println!(
        "{}",
        json!({"base_keys":base_keys,"writers":writers,"operations_per_writer":operations,"elapsed_ns":elapsed,"writer_samples_ns":writer_samples,"conflicts":conflicts,"reader_samples_ns":reader_samples,"reader_reads":reads,"reader_sample_cap":MAX_READER_SAMPLES,"reader_sampling":"bounded reservoir","reader_max_ns":reader_max_ns,"log_format":format,"reopen_verified":true})
    );
    Ok(())
}
