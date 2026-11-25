//! Disposable peak-RSS workload for the online checkpoint handoff.
//!
//! `setup` builds a managed checkpoint store of large values, opens it
//! through `OnlineCheckpointStore` and checks every value. `compact` does the
//! same and then runs `compact_and_reclaim` and checks every value again
//! after reopening. Values are regenerated rather than kept in a model, so
//! the difference between the two modes' peak RSS isolates the handoff,
//! including its copy, catch-up and final verification. Run each mode in a
//! fresh process under `/usr/bin/time`.
//!
//! `prepare` builds the same managed fixture under `<directory>/fixture`,
//! which must not exist, and exits. `handoff` then runs in a separate fresh
//! process, so its peak covers only its own work: it reopens the managed
//! store, captures a snapshot, commits a few small acknowledged writes, runs
//! `compact_and_reclaim`, checks the snapshot, drops everything, reopens and
//! verifies every key byte for byte, with no extra keys. Each phase is timed.
//! A fresh process does not imply a cold OS file cache.
use database_engine::{CheckpointStore, OnlineCheckpointStore, Store};
use std::error::Error;
use std::path::Path;
use std::time::Instant;

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

const SMALL_WRITES: usize = 8;

fn small_key(index: usize) -> Vec<u8> {
    format!("small-{index:02}").into_bytes()
}

fn small_value(index: usize) -> Vec<u8> {
    format!("acknowledged-{index:02}").into_bytes()
}

// Builds the managed fixture: every value written twice, checkpointed, then
// selected through a managed rollover. Returns the log length.
fn build_fixture(root: &Path, count: usize) -> Result<u64> {
    let source = root.join("source");
    let ordinary = Store::open(&source)?;
    // Two rounds leave one overwritten copy of every value in the log.
    for round in 0..2 {
        for index in 0..count {
            ordinary.put(&key(index), &value(index, round))?;
        }
    }
    let digest = ordinary.checkpoint_to(root.join("base"))?;
    drop(ordinary);
    let checkpoint = CheckpointStore::open(&source, root.join("base"), &digest)?;
    checkpoint.rollover_managed(root.join("managed"))?;
    Ok(std::fs::metadata(source.join("records.log"))?.len())
}

fn handoff(root: &Path, count: usize) -> Result<()> {
    let source = root.join("source");
    let original_log_bytes = std::fs::metadata(source.join("records.log"))?.len();
    let start = Instant::now();
    let online = OnlineCheckpointStore::from_store(CheckpointStore::open_managed(&source)?)?;
    let reopen_us = start.elapsed().as_micros();
    let snapshot = online.snapshot()?;
    for index in 0..SMALL_WRITES {
        online.with_store(|store| store.put(&small_key(index), &small_value(index)))?;
    }
    let start = Instant::now();
    let report = online.compact_and_reclaim(root.join("previous"), &Default::default())?;
    let handoff_us = start.elapsed().as_micros();
    if report.compacted_log_bytes >= report.final_log_bytes || root.join("previous").exists() {
        return Err("compaction did not shrink and reclaim the log".into());
    }
    // The snapshot still reads its captured state from the retired generation.
    let captured = snapshot.as_snapshot()?;
    if captured.get(&key(count - 1))? != Some(value(count - 1, 1))
        || captured.get(&small_key(0))?.is_some()
    {
        return Err("snapshot changed across the handoff".into());
    }
    drop(snapshot);
    drop(online);
    let compacted_log_bytes = std::fs::metadata(source.join("records.log"))?.len();
    let start = Instant::now();
    let reopened = CheckpointStore::open_managed(&source)?;
    // One ordered pass, one entry per page, compared byte for byte with a
    // streamed expected sequence: the small keys sort before the values.
    let mut expected = (0..SMALL_WRITES)
        .map(|index| (small_key(index), small_value(index)))
        .chain((0..count).map(|index| (key(index), value(index, 1))));
    let mut entries = 0;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page = reopened.scan(after.as_deref(), None, 1)?;
        let Some((observed_key, observed_value)) = page.into_iter().next() else {
            break;
        };
        match expected.next() {
            Some((key, value)) if key == observed_key && value == observed_value => {}
            _ => return Err(format!("unexpected entry at position {entries}").into()),
        }
        entries += 1;
        after = Some(observed_key);
    }
    if expected.next().is_some() {
        return Err(format!("only {entries} live keys were found").into());
    }
    let verify_us = start.elapsed().as_micros();
    println!(
        "{{\"mode\":\"handoff\",\"values\":{count},\"value_bytes\":{VALUE_BYTES},\
\"small_writes\":{SMALL_WRITES},\"original_log_bytes\":{original_log_bytes},\
\"final_log_bytes\":{},\"compacted_log_bytes\":{compacted_log_bytes},\
\"verified_entries\":{entries},\"reopen_us\":{reopen_us},\"handoff_us\":{handoff_us},\
\"verify_us\":{verify_us},\"snapshot_checked\":true,\"checked\":true}}",
        report.final_log_bytes
    );
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [mode, parent, count] = args.as_slice() else {
        return Err(
            "usage: online_checkpoint_verify_rss <setup|compact|prepare|handoff> <directory> <values>"
                .into(),
        );
    };
    let count: usize = count.parse()?;
    if !["setup", "compact", "prepare", "handoff"].contains(&mode.as_str())
        || !Path::new(parent).is_dir()
        || !(1..=1024).contains(&count)
    {
        return Err("invalid mode, directory or value count".into());
    }
    let fixture = Path::new(parent).join("fixture");
    if mode == "prepare" {
        if fixture.exists() {
            return Err("fixture directory already exists".into());
        }
        std::fs::create_dir(&fixture)?;
        let log_bytes = build_fixture(&fixture, count)?;
        println!("{{\"mode\":\"prepare\",\"values\":{count},\"log_bytes\":{log_bytes}}}");
        return Ok(());
    }
    if mode == "handoff" {
        return handoff(&fixture, count);
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
