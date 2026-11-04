//! Linux benchmark for full-log backup restore and validation memory.
use database_engine::Store;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::io::{self, Read};
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

fn hash_file(path: &Path) -> BenchResult<[u8; 32]> {
    let mut file = std::fs::File::open(path)?;
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

fn key(number: usize, length: usize) -> Vec<u8> {
    let mut key = (number as u64).to_be_bytes().to_vec();
    key.resize(length, 0x5a);
    key
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [mode, parent, token] if mode == "make-backup" => {
            let parent = Path::new(parent);
            let store = Store::open_with_checkpoint(parent.join("store"), parent.join("index"), &digest(token)?)?;
            store.backup(parent.join("backup"))?;
            println!("prepared=true");
        }
        [mode, parent, keys, key_bytes, tail_keys] if mode == "restore" => {
            let parent = Path::new(parent);
            let count = parse(keys, 1, 1_000_000)? + parse(tail_keys, 0, 4096)?;
            let key_bytes = parse(key_bytes, 8, 1024)?;
            let output = parent.join("restore-output");
            match std::fs::symlink_metadata(&output) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
                Ok(_) => return Err("restore destination already exists".into()),
            }
            let before_rss = memory_kib("VmRSS:")?;
            let started = Instant::now();
            Store::restore(parent.join("backup"), &output)?;
            let elapsed_us = started.elapsed().as_micros();
            let after_rss = memory_kib("VmRSS:")?;
            let peak_rss = memory_kib("VmHWM:")?;
            let actual_digest = hash_file(&output.join("records.log"))?;
            if actual_digest != hash_file(&parent.join("store/records.log"))? {
                return Err("restored log differs from source".into());
            }
            let store = Store::open(&output)?;
            for index in 0..16 {
                let number = index * (count - 1) / 15;
                let value = store.get(&key(number, key_bytes))?.ok_or("missing sampled key")?;
                if value.len() != 64 || value.iter().any(|byte| *byte != (number % 251) as u8) {
                    return Err("incorrect sampled value".into());
                }
            }
            drop(store);
            std::fs::remove_dir_all(&output)?;
            print!("elapsed_us={elapsed_us} before_rss_kib={before_rss} after_rss_kib={after_rss} peak_rss_kib={peak_rss} digest=");
            for byte in actual_digest { print!("{byte:02x}"); }
            println!();
        }
        _ => return Err("usage: backup_validation_bench make-backup <fixture-parent> <trusted-digest> | restore <fixture-parent> <base-keys> <key-bytes> <tail-keys>".into()),
    }
    Ok(())
}
