//! Compare equivalent verified checkpoints from the two writable store paths.
use database_engine::{CheckpointStore, RecoveredCheckpointView, Store};
use std::error::Error;
use std::io;
use std::path::Path;
use std::time::Instant;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const VALUE_BYTES: usize = 64;

enum Backend {
    Store(Store),
    Checkpoint(CheckpointStore),
}

impl Backend {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Store(store) => store.get(key),
            Self::Checkpoint(store) => store.get(key),
        }
    }

    fn checkpoint(&self, destination: &Path) -> io::Result<[u8; 32]> {
        match self {
            Self::Store(store) => store.checkpoint_to(destination),
            Self::Checkpoint(store) => store.rollover_to(destination),
        }
    }
}

fn parse(value: &str, min: usize, max: usize) -> BenchResult<usize> {
    let value = value.parse()?;
    if !(min..=max).contains(&value) {
        return Err("benchmark argument out of range".into());
    }
    Ok(value)
}

fn digest(hex: &str) -> BenchResult<[u8; 32]> {
    if hex.len() != 64 || !hex.is_ascii() {
        return Err("invalid checkpoint digest".into());
    }
    let mut result = [0; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)?;
    }
    Ok(result)
}

fn key(number: usize, length: usize) -> Vec<u8> {
    let mut key = (number as u64).to_be_bytes().to_vec();
    key.resize(length, 0x5a);
    key
}

fn verify(
    count: usize,
    key_len: usize,
    mut get: impl FnMut(&[u8]) -> io::Result<Option<Vec<u8>>>,
) -> BenchResult<()> {
    for index in 0..16 {
        let number = index * (count - 1) / 15;
        let value = get(&key(number, key_len))?.ok_or("missing sampled key")?;
        if value.len() != VALUE_BYTES || value.iter().any(|byte| *byte != (number % 251) as u8) {
            return Err("incorrect sampled value".into());
        }
    }
    Ok(())
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

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [parent, keys, key_len, tail, mode, token] = args.as_slice() else {
        return Err("usage: checkpoint_rollover_bench <fixture-parent> <base-keys> <key-bytes> <tail-keys> <store|checkpoint> <trusted-digest>".into());
    };
    let count = parse(keys, 1, 1_000_000)?;
    let key_len = parse(key_len, 8, 1024)?;
    let tail = parse(tail, 0, 4096)?;
    let token = digest(token)?;
    let parent = Path::new(parent);
    let path = parent.join("store");
    let source = parent.join("index");
    let output = parent.join("rollover-output");
    if std::fs::symlink_metadata(&output).is_ok() {
        return Err("output path already exists".into());
    }
    let backend = match mode.as_str() {
        "store" => Backend::Store(Store::open_with_checkpoint(&path, &source, &token)?),
        "checkpoint" => Backend::Checkpoint(CheckpointStore::open(&path, &source, &token)?),
        _ => return Err("unknown backend".into()),
    };
    verify(count + tail, key_len, |key| backend.get(key))?;
    let before_rss = memory_kib("VmRSS:")?;
    let started = Instant::now();
    let new_digest = backend.checkpoint(&output)?;
    let elapsed_us = started.elapsed().as_micros();
    let after_rss = memory_kib("VmRSS:")?;
    let peak_rss = memory_kib("VmHWM:")?;
    verify(count + tail, key_len, |key| backend.get(key))?;
    drop(backend);
    let reopened = RecoveredCheckpointView::open(&path, &output, &new_digest)?;
    if reopened.tail_bytes() != 0 || reopened.overlay_keys() != 0 {
        return Err("generated checkpoint did not cover the full log".into());
    }
    verify(count + tail, key_len, |key| reopened.get(key))?;
    drop(reopened);
    let file_bytes = std::fs::metadata(&output)?.len();
    std::fs::remove_file(&output)?;
    print!(
        "mode={mode} elapsed_us={elapsed_us} before_rss_kib={before_rss} after_rss_kib={after_rss} peak_rss_kib={peak_rss} checkpoint_bytes={file_bytes} digest="
    );
    for byte in new_digest {
        print!("{byte:02x}");
    }
    println!();
    Ok(())
}
