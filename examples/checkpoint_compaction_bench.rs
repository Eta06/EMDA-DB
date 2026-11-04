//! Compare separate-copy compactors on the same checked fixture.
use database_engine::{BatchOperation, COMPACTED_CHECKPOINT_FILE, CheckpointStore, Store};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Instant;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn parse(value: &str, min: usize, max: usize) -> BenchResult<usize> {
    let value = value.parse()?;
    if !(min..=max).contains(&value) {
        return Err("argument out of range".into());
    }
    Ok(value)
}

fn digest(hex: &str) -> BenchResult<[u8; 32]> {
    if hex.len() != 64 || !hex.is_ascii() {
        return Err("invalid digest".into());
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

fn verify(
    count: usize,
    key_len: usize,
    tail: usize,
    mut get: impl FnMut(&[u8]) -> io::Result<Option<Vec<u8>>>,
) -> BenchResult<()> {
    for index in 0..16 {
        let number = index * (count - 1) / 15;
        let actual = get(&key(number, key_len))?;
        let expected = if number < tail && number % 2 == 1 {
            None
        } else {
            Some(vec![
                ((number + usize::from(number < tail)) % 251) as u8;
                64
            ])
        };
        if actual != expected {
            return Err("incorrect sampled value".into());
        }
    }
    Ok(())
}

fn record_digest(path: &Path) -> BenchResult<[u8; 32]> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(28))?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hash.finalize().into())
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [mode, parent, keys, key_bytes, tail] if mode == "prepare" => {
            let count = parse(keys, 1, 1_000_000)?;
            let key_len = parse(key_bytes, 8, 1024)?;
            let tail = parse(tail, 0, 4096)?.min(count);
            let parent = Path::new(parent);
            let path = parent.join("store");
            let checkpoint = parent.join("index");
            if !parent.is_dir() { return Err("fixture parent must exist".into()); }
            for target in [&path, &checkpoint] {
                match std::fs::symlink_metadata(target) {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                    Ok(_) => return Err("fixture paths must be absent".into()),
                }
            }
            let store = Store::open(&path)?;
            for start in (0..count).step_by(512) {
                let end = (start + 512).min(count);
                let keys = (start..end).map(|n| key(n, key_len)).collect::<Vec<_>>();
                let values = (start..end).map(|n| vec![(n % 251) as u8; 64]).collect::<Vec<_>>();
                let operations = keys.iter().zip(&values).map(|(key, value)| BatchOperation::Put { key, value }).collect::<Vec<_>>();
                store.write_batch(&operations)?;
            }
            let token = store.checkpoint_to(&checkpoint)?;
            for start in (0..tail).step_by(512) {
                let end = (start + 512).min(tail);
                let keys = (start..end).map(|n| key(n, key_len)).collect::<Vec<_>>();
                let values = (start..end).map(|n| vec![((n + 1) % 251) as u8; 64]).collect::<Vec<_>>();
                let operations = keys.iter().zip(&values).enumerate().map(|(offset, (key, value))| {
                    if (start + offset) % 2 == 0 { BatchOperation::Put { key, value } } else { BatchOperation::Delete { key } }
                }).collect::<Vec<_>>();
                store.write_batch(&operations)?;
            }
            verify(count, key_len, tail, |key| store.get(key))?;
            drop(store);
            print!("keys={count} key_bytes={key_len} tail_keys={tail} source_bytes={} token=", std::fs::metadata(path.join("records.log"))?.len());
            for byte in token { print!("{byte:02x}"); }
            println!();
        }
        [mode, parent, keys, key_bytes, tail, backend, token] if mode == "measure" => {
            let count = parse(keys, 1, 1_000_000)?;
            let key_len = parse(key_bytes, 8, 1024)?;
            let tail = parse(tail, 0, 4096)?.min(count);
            let token = digest(token)?;
            let parent = Path::new(parent);
            let path = parent.join("store");
            let output = parent.join("compact-output");
            let (elapsed_us, before_rss, after_rss, peak_rss, checkpoint_digest) = match backend.as_str() {
                "store" => {
                    let store = Store::open_with_checkpoint(&path, parent.join("index"), &token)?;
                    verify(count, key_len, tail, |key| store.get(key))?;
                    let before = memory_kib("VmRSS:")?;
                    let started = Instant::now();
                    store.compact_to(&output)?;
                    let elapsed = started.elapsed().as_micros();
                    (elapsed, before, memory_kib("VmRSS:")?, memory_kib("VmHWM:")?, None)
                }
                "checkpoint" => {
                    let store = CheckpointStore::open(&path, parent.join("index"), &token)?;
                    verify(count, key_len, tail, |key| store.get(key))?;
                    let before = memory_kib("VmRSS:")?;
                    let started = Instant::now();
                    let digest = store.compact_to(&output)?;
                    let elapsed = started.elapsed().as_micros();
                    (elapsed, before, memory_kib("VmRSS:")?, memory_kib("VmHWM:")?, Some(digest))
                }
                _ => return Err("unknown backend".into()),
            };
            if let Some(digest) = checkpoint_digest {
                let copy = CheckpointStore::open(&output, output.join(COMPACTED_CHECKPOINT_FILE), &digest)?;
                verify(count, key_len, tail, |key| copy.get(key))?;
            } else {
                let copy = Store::open(&output)?;
                verify(count, key_len, tail, |key| copy.get(key))?;
            }
            let log = output.join("records.log");
            let target_bytes = std::fs::metadata(&log)?.len();
            let record_hash = record_digest(&log)?;
            let checkpoint_bytes = if checkpoint_digest.is_some() { std::fs::metadata(output.join(COMPACTED_CHECKPOINT_FILE))?.len() } else { 0 };
            std::fs::remove_dir_all(&output)?;
            print!("mode={backend} elapsed_us={elapsed_us} before_rss_kib={before_rss} after_rss_kib={after_rss} peak_rss_kib={peak_rss} target_bytes={target_bytes} checkpoint_bytes={checkpoint_bytes} record_digest=");
            for byte in record_hash { print!("{byte:02x}"); }
            println!();
        }
        _ => return Err("usage: checkpoint_compaction_bench prepare <parent> <keys> <key-bytes> <tail-keys> | measure <parent> <keys> <key-bytes> <tail-keys> <store|checkpoint> <digest>".into()),
    }
    Ok(())
}
