//! Disposable peak-RSS workload for the online checkpoint handoff.
//!
//! `setup` builds a managed checkpoint store of large values, opens it
//! through `OnlineCheckpointStore` and checks every value. `compact` does the
//! same and then runs `compact_and_reclaim` and checks every value again
//! after reopening. Values are regenerated rather than kept in a model, so
//! the difference between the two modes' peak RSS isolates the handoff,
//! including its copy, catch-up and final verification. Run each mode in a
//! fresh process under `/usr/bin/time`.
use database_engine::{CheckpointStore, OnlineCheckpointStore, Store};
use std::error::Error;
use std::path::Path;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const VALUE_BYTES: usize = 1024 * 1024;

fn key(index: usize) -> Vec<u8> {
    format!("value-{index:05}").into_bytes()
}

fn value(index: usize, round: usize) -> Vec<u8> {
    let seed = (index * 31 + round * 7) as u8;
    (0..VALUE_BYTES)
        .map(|offset| (offset as u8).wrapping_mul(13).wrapping_add(seed))
        .collect()
}

fn check(store: &CheckpointStore, count: usize) -> Result<()> {
    for index in 0..count {
        if store.get(&key(index))? != Some(value(index, 1)) {
            return Err(format!("value {index} mismatch").into());
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [mode, parent, count] = args.as_slice() else {
        return Err(
            "usage: online_checkpoint_verify_rss <setup|compact> <directory> <values>".into(),
        );
    };
    let count: usize = count.parse()?;
    if !["setup", "compact"].contains(&mode.as_str())
        || !Path::new(parent).is_dir()
        || !(1..=1024).contains(&count)
    {
        return Err("invalid mode, directory or value count".into());
    }
    let root = tempfile::Builder::new()
        .prefix(".online-checkpoint-verify-rss-")
        .tempdir_in(parent)?;
    let source = root.path().join("source");
    let ordinary = Store::open(&source)?;
    // Two rounds leave one overwritten copy of every value in the log.
    for round in 0..2 {
        for index in 0..count {
            ordinary.put(&key(index), &value(index, round))?;
        }
    }
    let digest = ordinary.checkpoint_to(root.path().join("base"))?;
    drop(ordinary);
    let checkpoint = CheckpointStore::open(&source, root.path().join("base"), &digest)?;
    checkpoint.rollover_managed(root.path().join("managed"))?;
    let online = OnlineCheckpointStore::from_store(checkpoint)?;
    online.with_store(|store| check(store, count).map_err(std::io::Error::other))?;
    if mode == "compact" {
        let report =
            online.compact_and_reclaim(root.path().join("previous"), &Default::default())?;
        if report.compacted_log_bytes >= report.final_log_bytes {
            return Err("compaction did not shrink the log".into());
        }
        drop(online);
        let reopened = CheckpointStore::open_managed(&source)?;
        check(&reopened, count)?;
    }
    println!(
        "{{\"mode\":\"{mode}\",\"values\":{count},\"value_bytes\":{VALUE_BYTES},\"checked\":true}}"
    );
    Ok(())
}
