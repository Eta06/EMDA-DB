//! Linux-only experimental memory/open/lookup measurement for checkpoint readers.
use database_engine::{
    BatchOperation, CheckpointStore, CheckpointView, RecoveredCheckpointView, Store,
};
use std::error::Error;
use std::io;
use std::path::Path;
use std::time::Instant;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const VALUE_BYTES: usize = 64;
const LOOKUPS: usize = 16;

fn key(number: usize, length: usize) -> Vec<u8> {
    let mut key = (number as u64).to_be_bytes().to_vec();
    key.resize(length, 0x5a);
    key
}

fn parse_size(value: &str, min: usize, max: usize) -> BenchResult<usize> {
    let parsed = value.parse()?;
    if !(min..=max).contains(&parsed) {
        return Err("benchmark argument out of range".into());
    }
    Ok(parsed)
}

fn prepare(parent: &Path, count: usize, key_len: usize, tail_count: usize) -> BenchResult<()> {
    let path = parent.join("store");
    let checkpoint = parent.join("index");
    if !parent.is_dir() {
        return Err("parent must exist and fixture paths must not exist".into());
    }
    for target in [&path, &checkpoint] {
        match std::fs::symlink_metadata(target) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err("fixture path already exists".into()),
        }
    }
    let store = Store::open(&path)?;
    for start in (0..count).step_by(512) {
        let end = (start + 512).min(count);
        let keys = (start..end).map(|n| key(n, key_len)).collect::<Vec<_>>();
        let values = (start..end)
            .map(|n| vec![(n % 251) as u8; VALUE_BYTES])
            .collect::<Vec<_>>();
        let operations = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| BatchOperation::Put { key, value })
            .collect::<Vec<_>>();
        store.write_batch(&operations)?;
    }
    let token = store.checkpoint_to(&checkpoint)?;
    let prefix_bytes = std::fs::metadata(path.join("records.log"))?.len();
    for start in (0..tail_count).step_by(512) {
        let end = (start + 512).min(tail_count);
        let keys = (start..end)
            .map(|n| key(count + n, key_len))
            .collect::<Vec<_>>();
        let values = (start..end)
            .map(|n| vec![((count + n) % 251) as u8; VALUE_BYTES])
            .collect::<Vec<_>>();
        let operations = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| BatchOperation::Put { key, value })
            .collect::<Vec<_>>();
        store.write_batch(&operations)?;
    }
    drop(store);
    let log_bytes = std::fs::metadata(path.join("records.log"))?.len();
    print!(
        "keys={count} key_bytes={key_len} tail_keys={tail_count} tail_bytes={} log_bytes={} checkpoint_bytes={} token=",
        log_bytes - prefix_bytes,
        log_bytes,
        std::fs::metadata(checkpoint)?.len(),
    );
    for byte in token {
        print!("{byte:02x}");
    }
    println!();
    Ok(())
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

fn io_counter(field: &str) -> BenchResult<u64> {
    let status = std::fs::read_to_string("/proc/self/io")?;
    let line = status
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or("I/O field unavailable")?;
    Ok(line
        .split_whitespace()
        .nth(1)
        .ok_or("invalid I/O field")?
        .parse()?)
}

fn lookups(
    first: usize,
    count: usize,
    key_len: usize,
    mut get: impl FnMut(&[u8]) -> io::Result<Option<Vec<u8>>>,
) -> BenchResult<Vec<u128>> {
    let mut durations = Vec::with_capacity(LOOKUPS);
    for index in 0..LOOKUPS {
        let number = first + index * (count - 1) / (LOOKUPS - 1);
        let key = key(number, key_len);
        let start = Instant::now();
        let value = get(&key)?.ok_or("missing benchmark value")?;
        durations.push(start.elapsed().as_nanos());
        if value.len() != VALUE_BYTES || value.iter().any(|v| *v != (number % 251) as u8) {
            return Err("incorrect benchmark value".into());
        }
    }
    Ok(durations)
}

fn measure(
    parent: &Path,
    count: usize,
    key_len: usize,
    mode: &str,
    token: &[u8; 32],
    tail_count: usize,
) -> BenchResult<()> {
    let path = parent.join("store");
    let checkpoint = parent.join("index");
    let rchar_before = io_counter("rchar:")?;
    let disk_read_before = io_counter("read_bytes:")?;
    let read_calls_before = io_counter("syscr:")?;
    let start = Instant::now();
    let (open_us, rss_kib, after_rss_kib, blocks, overlay_keys, lookup_ns, tail_lookup_ns) =
        match mode {
            "store" | "store-full" => {
                let store = if mode == "store" {
                    Store::open_with_checkpoint(&path, &checkpoint, token)?
                } else {
                    Store::open(&path)?
                };
                let open_us = start.elapsed().as_micros();
                let rss_kib = memory_kib("VmRSS:")?;
                let lookup_ns = lookups(0, count, key_len, |key| store.get(key))?;
                let tail_lookup_ns = if tail_count > 0 {
                    lookups(count, tail_count, key_len, |key| store.get(key))?
                } else {
                    Vec::new()
                };
                let after_rss_kib = memory_kib("VmRSS:")?;
                (
                    open_us,
                    rss_kib,
                    after_rss_kib,
                    0,
                    0,
                    lookup_ns,
                    tail_lookup_ns,
                )
            }
            "checkpoint-store" => {
                let store = CheckpointStore::open(&path, &checkpoint, token)?;
                let open_us = start.elapsed().as_micros();
                let rss_kib = memory_kib("VmRSS:")?;
                let overlay_keys = store.overlay_keys()?;
                if overlay_keys != tail_count {
                    return Err("unexpected writable checkpoint overlay count".into());
                }
                let lookup_ns = lookups(0, count, key_len, |key| store.get(key))?;
                let tail_lookup_ns = if tail_count > 0 {
                    lookups(count, tail_count, key_len, |key| store.get(key))?
                } else {
                    Vec::new()
                };
                let after_rss_kib = memory_kib("VmRSS:")?;
                (
                    open_us,
                    rss_kib,
                    after_rss_kib,
                    0,
                    overlay_keys,
                    lookup_ns,
                    tail_lookup_ns,
                )
            }
            "view" => {
                let view = CheckpointView::open(&path, &checkpoint, token)?;
                let open_us = start.elapsed().as_micros();
                let rss_kib = memory_kib("VmRSS:")?;
                let blocks = view.block_count();
                let lookup_ns = lookups(0, count, key_len, |key| view.get(key))?;
                if tail_count > 0 && view.get(&key(count, key_len))?.is_some() {
                    return Err("historical view unexpectedly included a suffix key".into());
                }
                let after_rss_kib = memory_kib("VmRSS:")?;
                (
                    open_us,
                    rss_kib,
                    after_rss_kib,
                    blocks,
                    0,
                    lookup_ns,
                    Vec::new(),
                )
            }
            "recovered" => {
                let view = RecoveredCheckpointView::open(&path, &checkpoint, token)?;
                let open_us = start.elapsed().as_micros();
                let rss_kib = memory_kib("VmRSS:")?;
                let blocks = view.checkpoint_block_count();
                let overlay_keys = view.overlay_keys();
                if overlay_keys != tail_count {
                    return Err("unexpected recovered overlay count".into());
                }
                let lookup_ns = lookups(0, count, key_len, |key| view.get(key))?;
                let tail_lookup_ns = if tail_count > 0 {
                    lookups(count, tail_count, key_len, |key| view.get(key))?
                } else {
                    Vec::new()
                };
                let after_rss_kib = memory_kib("VmRSS:")?;
                (
                    open_us,
                    rss_kib,
                    after_rss_kib,
                    blocks,
                    overlay_keys,
                    lookup_ns,
                    tail_lookup_ns,
                )
            }
            _ => return Err("unknown measurement mode".into()),
        };
    let lookups = lookup_ns
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let tail_lookups = if tail_lookup_ns.is_empty() {
        "none".to_owned()
    } else {
        tail_lookup_ns
            .iter()
            .map(u128::to_string)
            .collect::<Vec<_>>()
            .join(",")
    };
    let peak_rss_kib = memory_kib("VmHWM:")?;
    let read_chars = io_counter("rchar:")?.saturating_sub(rchar_before);
    let disk_read_bytes = io_counter("read_bytes:")?.saturating_sub(disk_read_before);
    let read_calls = io_counter("syscr:")?.saturating_sub(read_calls_before);
    println!(
        "mode={mode} open_us={open_us} rss_kib={rss_kib} after_rss_kib={after_rss_kib} peak_rss_kib={peak_rss_kib} read_chars={read_chars} disk_read_bytes={disk_read_bytes} read_calls={read_calls} blocks={blocks} overlay_keys={overlay_keys} lookups_ns={lookups} tail_lookups_ns={tail_lookups}"
    );
    Ok(())
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, parent, count, key_len] if command == "prepare" => prepare(
            Path::new(parent),
            parse_size(count, 1, 1_000_000)?,
            parse_size(key_len, 8, 1024)?,
            0,
        ),
        [command, parent, count, key_len, tail_count] if command == "prepare" => prepare(
            Path::new(parent),
            parse_size(count, 1, 1_000_000)?,
            parse_size(key_len, 8, 1024)?,
            parse_size(tail_count, 0, 4096)?,
        ),
        [command, parent, count, key_len, mode, token] if command == "measure" => measure(
            Path::new(parent),
            parse_size(count, 1, 1_000_000)?,
            parse_size(key_len, 8, 1024)?,
            mode,
            &token_from_hex(token)?,
            0,
        ),
        [command, parent, count, key_len, mode, token, tail_count] if command == "measure" => measure(
            Path::new(parent),
            parse_size(count, 1, 1_000_000)?,
            parse_size(key_len, 8, 1024)?,
            mode,
            &token_from_hex(token)?,
            parse_size(tail_count, 0, 4096)?,
        ),
        _ => Err("usage: index_memory_bench prepare <empty-parent> <keys> <key-bytes> [tail-keys] | measure <parent> <keys> <key-bytes> <store-full|store|checkpoint-store|view|recovered> <digest> [tail-keys]".into()),
    }
}
