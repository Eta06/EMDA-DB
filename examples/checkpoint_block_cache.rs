//! Disposable workload for the verified checkpoint block cache. `prepare`
//! builds a managed checkpoint of 200,000 keys under `<directory>/fixture`;
//! `read <limit-bytes>` reopens it in a fresh process and performs 200,000
//! point reads over a hot 10% of the keys with the given cache limit,
//! printing throughput and cache counters as JSON. Run each read under
//! `/usr/bin/time` to record peak RSS. A fresh process does not imply a cold
//! file cache.
use database_engine::{BatchOperation, CheckpointStore, Store};
use std::error::Error;
use std::path::Path;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const KEYS: u64 = 200_000;
const READS: u64 = 200_000;
const HOT: u64 = KEYS / 10;

fn key(index: u64) -> Vec<u8> {
    format!("key-{index:08}").into_bytes()
}

fn value(index: u64) -> Vec<u8> {
    vec![(index % 251) as u8; 64]
}

fn prepare(fixture: &Path) -> Result<()> {
    let source = fixture.join("store");
    let ordinary = Store::open(&source)?;
    let pairs = (0..KEYS)
        .map(|index| (key(index), value(index)))
        .collect::<Vec<_>>();
    for chunk in pairs.chunks(1000) {
        let batch = chunk
            .iter()
            .map(|(key, value)| BatchOperation::Put { key, value })
            .collect::<Vec<_>>();
        ordinary.write_batch(&batch)?;
    }
    let digest = ordinary.checkpoint_to(fixture.join("base"))?;
    drop(ordinary);
    let store = CheckpointStore::open(&source, fixture.join("base"), &digest)?;
    store.rollover_managed(fixture.join("managed"))?;
    Ok(())
}

fn read(fixture: &Path, limit: usize) -> Result<()> {
    let store = CheckpointStore::open_managed(fixture.join("store"))?;
    store.set_checkpoint_block_cache_limit(limit)?;
    // A fixed multiplicative sequence over the hot set.
    let mut state = 0x9e37_79b9_u64;
    let start = Instant::now();
    for _ in 0..READS {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let index = (state >> 33) % HOT * 10;
        if store.get(&key(index))? != Some(value(index)) {
            return Err(format!("value {index} mismatch").into());
        }
    }
    let elapsed = start.elapsed();
    let memory = store.checkpoint_memory_status()?;
    let cache = memory.block_cache;
    println!(
        "{{\"limit_bytes\":{limit},\"reads\":{READS},\"elapsed_ms\":{},\"reads_per_s\":{:.0},\
\"hits\":{},\"misses\":{},\"cached_bytes\":{},\"cached_blocks\":{},\"directory_bytes\":{}}}",
        elapsed.as_millis(),
        READS as f64 / elapsed.as_secs_f64(),
        cache.hits,
        cache.misses,
        cache.cached_bytes,
        cache.cached_blocks,
        memory.directory_bytes
    );
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [mode, directory] if mode == "prepare" => {
            let fixture = Path::new(directory).join("fixture");
            if fixture.exists() {
                return Err("fixture already exists".into());
            }
            std::fs::create_dir(&fixture)?;
            prepare(&fixture)
        }
        [mode, directory, limit] if mode == "read" => {
            read(&Path::new(directory).join("fixture"), limit.parse()?)
        }
        _ => Err("usage: checkpoint_block_cache prepare <dir> | read <dir> <limit-bytes>".into()),
    }
}
