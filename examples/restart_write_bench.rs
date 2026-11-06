//! Linux resource diagnostic for concurrent writes and fresh-process recovery.
use database_engine::{
    CheckpointStore, MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS, Store,
};
use serde_json::json;
use std::error::Error;
use std::io;
use std::path::Path;
use std::sync::Barrier;
use std::time::Instant;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const VALUE_BYTES: usize = 64;

fn key(number: usize, length: usize) -> Vec<u8> {
    let mut key = (number as u64).to_be_bytes().to_vec();
    key.resize(length, 0x5a);
    key
}

fn parse(value: &str, min: usize, max: usize) -> BenchResult<usize> {
    let value = value.parse()?;
    if !(min..=max).contains(&value) {
        return Err("benchmark argument out of range".into());
    }
    Ok(value)
}

fn token(hex: &str) -> BenchResult<[u8; 32]> {
    if hex.len() != 64 || !hex.is_ascii() {
        return Err("invalid checkpoint digest".into());
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)?;
    }
    Ok(bytes)
}

fn proc_field(path: &str, field: &str) -> BenchResult<u64> {
    let content = std::fs::read_to_string(path)?;
    Ok(content
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or("missing process metric")?
        .split_whitespace()
        .nth(1)
        .ok_or("invalid process metric")?
        .parse()?)
}

struct ProcessIo {
    rchar: u64,
    wchar: u64,
    read_bytes: u64,
    write_bytes: u64,
    read_calls: u64,
    write_calls: u64,
}

impl ProcessIo {
    fn now() -> BenchResult<Self> {
        let field = |name| proc_field("/proc/self/io", name);
        Ok(Self {
            rchar: field("rchar:")?,
            wchar: field("wchar:")?,
            read_bytes: field("read_bytes:")?,
            write_bytes: field("write_bytes:")?,
            read_calls: field("syscr:")?,
            write_calls: field("syscw:")?,
        })
    }

    fn delta(&self, before: &Self) -> serde_json::Value {
        json!({
            "rchar": self.rchar.saturating_sub(before.rchar),
            "wchar": self.wchar.saturating_sub(before.wchar),
            "read_bytes": self.read_bytes.saturating_sub(before.read_bytes),
            "write_bytes": self.write_bytes.saturating_sub(before.write_bytes),
            "read_calls": self.read_calls.saturating_sub(before.read_calls),
            "write_calls": self.write_calls.saturating_sub(before.write_calls),
        })
    }
}

enum Engine {
    Full(Store),
    Checkpoint(CheckpointStore),
}

impl Engine {
    fn open(parent: &Path, mode: &str, token: &[u8; 32]) -> BenchResult<Self> {
        Ok(match mode {
            "full" => Self::Full(Store::open(parent.join("store"))?),
            "checkpoint" => Self::Checkpoint(CheckpointStore::open(
                parent.join("store"),
                parent.join("index"),
                token,
            )?),
            _ => return Err("unknown engine mode".into()),
        })
    }

    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Full(store) => store.get(key),
            Self::Checkpoint(store) => store.get(key),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        match self {
            Self::Full(store) => store.put(key, value),
            Self::Checkpoint(store) => store.put(key, value),
        }
    }
}

fn verify(store: &Engine, number: usize, key_len: usize) -> BenchResult<()> {
    let value = store.get(&key(number, key_len))?.ok_or("missing value")?;
    if value.len() != VALUE_BYTES || value.iter().any(|byte| *byte != (number % 251) as u8) {
        return Err(format!("incorrect value for key {number}").into());
    }
    Ok(())
}

fn run(args: &[String]) -> BenchResult<()> {
    let [
        parent,
        base,
        tail,
        key_len,
        mode,
        digest,
        workers,
        operations,
        epoch,
        phase,
    ] = args
    else {
        return Err("usage: restart_write_bench <parent> <base-keys> <tail-keys> <key-bytes> <full|checkpoint> <digest> <workers> <writes-per-worker> <epoch> <write|verify>".into());
    };
    let base = parse(base, 1, 1_000_000)?;
    let tail = parse(tail, 0, 4096)?;
    let key_len = parse(key_len, 8, 1024)?;
    let workers = parse(workers, 1, 32)?;
    let operations = parse(operations, 1, 1024)?;
    let epoch = parse(epoch, 0, 32)?;
    if phase != "write" && phase != "verify" {
        return Err("unknown phase".into());
    }
    let per_epoch = workers * operations;
    let prior = epoch * per_epoch;
    let current = if phase == "write" { per_epoch } else { 0 };
    if tail + prior + current > MAX_RECOVERED_VIEW_TAIL_KEYS
        || (tail + prior + current) * key_len > MAX_RECOVERED_VIEW_TAIL_KEY_BYTES
    {
        return Err("benchmark would exceed checkpoint overlay limits".into());
    }
    let start_index = base + tail;
    let io_before = ProcessIo::now()?;
    let start = Instant::now();
    let store = Engine::open(Path::new(parent), mode, &token(digest)?)?;
    let open_us = start.elapsed().as_micros();
    let rss_after_open_kib = proc_field("/proc/self/status", "VmRSS:")?;
    for i in 0..16 {
        verify(&store, i * (base - 1) / 15, key_len)?;
        if tail > 0 {
            verify(&store, base + i * (tail - 1) / 15, key_len)?;
        }
    }
    for number in start_index..start_index + prior {
        verify(&store, number, key_len)?;
    }
    let mut latencies_ns = Vec::with_capacity(current);
    let mut elapsed_ns = 0;
    if phase == "write" {
        let barrier = Barrier::new(workers + 1);
        std::thread::scope(|scope| -> BenchResult<()> {
            let mut threads = Vec::new();
            for worker in 0..workers {
                let store = &store;
                let barrier = &barrier;
                threads.push(scope.spawn(move || -> BenchResult<Vec<u128>> {
                    let mut samples = Vec::with_capacity(operations);
                    barrier.wait();
                    for op in 0..operations {
                        let number = start_index + prior + worker * operations + op;
                        let value = vec![(number % 251) as u8; VALUE_BYTES];
                        let key = key(number, key_len);
                        let start = Instant::now();
                        store.put(&key, &value)?;
                        samples.push(start.elapsed().as_nanos());
                    }
                    Ok(samples)
                }));
            }
            let start = Instant::now();
            barrier.wait();
            for thread in threads {
                latencies_ns.extend(thread.join().map_err(|_| "write worker panicked")??);
            }
            elapsed_ns = start.elapsed().as_nanos();
            Ok(())
        })?;
        for number in start_index + prior..start_index + prior + current {
            verify(&store, number, key_len)?;
        }
    }
    let io_after = ProcessIo::now()?;
    let peak_rss_kib = proc_field("/proc/self/status", "VmHWM:")?;
    println!(
        "{}",
        json!({
            "mode": mode, "phase": phase, "epoch": epoch, "base_keys": base,
            "tail_keys": tail, "key_bytes": key_len, "workers": workers,
            "writes_per_worker": operations, "prior_verified": prior,
            "current_verified": current, "open_us": open_us, "write_elapsed_ns": elapsed_ns,
            "write_latencies_ns": latencies_ns, "rss_after_open_kib": rss_after_open_kib,
            "peak_rss_kib": peak_rss_kib, "process_io": io_after.delta(&io_before),
        })
    );
    Ok(())
}

fn main() -> BenchResult<()> {
    run(&std::env::args().skip(1).collect::<Vec<_>>())
}
