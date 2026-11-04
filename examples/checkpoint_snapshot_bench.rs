//! Linux process-RSS and creation-time comparison for read-only snapshots.
use database_engine::{CheckpointStore, MAX_SNAPSHOT_KEY_BYTES, MAX_SNAPSHOT_KEYS, Store};
use std::error::Error;
use std::path::Path;
use std::time::Instant;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const VALUE_BYTES: usize = 64;

fn key(number: usize, length: usize) -> Vec<u8> {
    let mut key = (number as u64).to_be_bytes().to_vec();
    key.resize(length, 0x5a);
    key
}
fn token_from_hex(hex: &str) -> BenchResult<[u8; 32]> {
    if hex.len() != 64 || !hex.is_ascii() {
        return Err("invalid checkpoint digest".into());
    }
    let mut token = [0; 32];
    for (index, byte) in token.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)?;
    }
    Ok(token)
}
fn memory_kib(field: &str) -> BenchResult<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or("memory field unavailable")?;
    Ok(line
        .split_whitespace()
        .nth(1)
        .ok_or("invalid memory field")?
        .parse()?)
}
fn verify(value: Option<Vec<u8>>, number: usize) -> BenchResult<()> {
    let value = value.ok_or("missing snapshot value")?;
    if value.len() != VALUE_BYTES || value.iter().any(|byte| *byte != (number % 251) as u8) {
        return Err("incorrect snapshot value".into());
    }
    Ok(())
}
fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [parent, count, key_len, tail_count, snapshots, mode, digest] = args.as_slice() else {
        return Err("usage: checkpoint_snapshot_bench <parent> <base-keys> <key-bytes> <tail-keys> <snapshots> <ordinary|checkpoint> <digest>".into());
    };
    let count: usize = count.parse()?;
    let key_len: usize = key_len.parse()?;
    let tail_count: usize = tail_count.parse()?;
    let snapshots: usize = snapshots.parse()?;
    if count == 0
        || count > 1_000_000
        || !(8..=1024).contains(&key_len)
        || tail_count > 4096
        || !(1..=16).contains(&snapshots)
        || !matches!(mode.as_str(), "ordinary" | "checkpoint")
        || !Path::new(parent).is_dir()
    {
        return Err("invalid benchmark configuration".into());
    }
    let token = token_from_hex(digest)?;
    let path = Path::new(parent).join("store");
    let checkpoint = Path::new(parent).join("index");
    let open = Instant::now();
    let (open_us, before_rss, after_rss, peak_rss, durations) = if mode == "ordinary" {
        let store = Store::open_with_checkpoint(&path, &checkpoint, &token)?;
        let open_us = open.elapsed().as_micros();
        let before = memory_kib("VmRSS:")?;
        let mut durations = Vec::new();
        let mut views = Vec::new();
        for _ in 0..snapshots {
            let started = Instant::now();
            views.push(store.snapshot(MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_KEY_BYTES)?);
            durations.push(started.elapsed().as_nanos());
        }
        let after = memory_kib("VmRSS:")?;
        let peak = memory_kib("VmHWM:")?;
        for view in &views {
            verify(view.get(&key(0, key_len))?, 0)?;
            verify(view.get(&key(count - 1, key_len))?, count - 1)?;
            if tail_count > 0 {
                verify(
                    view.get(&key(count + tail_count - 1, key_len))?,
                    count + tail_count - 1,
                )?;
            }
        }
        (open_us, before, after, peak, durations)
    } else {
        let store = CheckpointStore::open(&path, &checkpoint, &token)?;
        let open_us = open.elapsed().as_micros();
        let before = memory_kib("VmRSS:")?;
        let mut durations = Vec::new();
        let mut views = Vec::new();
        for _ in 0..snapshots {
            let started = Instant::now();
            views.push(store.snapshot()?);
            durations.push(started.elapsed().as_nanos());
        }
        let after = memory_kib("VmRSS:")?;
        let peak = memory_kib("VmHWM:")?;
        for view in &views {
            verify(view.get(&key(0, key_len))?, 0)?;
            verify(view.get(&key(count - 1, key_len))?, count - 1)?;
            if tail_count > 0 {
                verify(
                    view.get(&key(count + tail_count - 1, key_len))?,
                    count + tail_count - 1,
                )?;
            }
        }
        (open_us, before, after, peak, durations)
    };
    let durations = durations
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "mode={mode} base_keys={count} key_bytes={key_len} tail_keys={tail_count} snapshots={snapshots} open_us={open_us} before_rss_kib={before_rss} after_rss_kib={after_rss} peak_rss_kib={peak_rss} snapshot_ns={durations}"
    );
    Ok(())
}
