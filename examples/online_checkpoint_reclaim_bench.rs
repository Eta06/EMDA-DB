//! Disposable online checkpoint-store compaction and reclamation measurement.
//!
//! A managed checkpoint store with overwritten and deleted history is
//! compacted while one thread keeps committing synchronized puts and an owned
//! snapshot pins the previous generation. The output records writer latency
//! before and during compaction, filesystem free space while the snapshot
//! pins the retired generation and after it is dropped, and an exact reopen.
//! Free space is a filesystem-wide reading and can include unrelated activity.
use database_engine::{BatchOperation, CheckpointStore, OnlineCheckpointStore, Store};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Distinct keys each concurrent writer cycles through. Two writer phases
/// stay below the default 16,384-key checkpoint suffix cap.
const WRITER_KEYS: u64 = 4096;
// Acknowledged entries and each synchronized put's latency in microseconds.
type WriterOutput = (Vec<(Vec<u8>, Vec<u8>)>, Vec<u128>);

fn key(index: usize) -> [u8; 8] {
    (index as u64).to_be_bytes()
}

fn fill(db: &Store, count: usize) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut model = BTreeMap::new();
    for round in 0..3 {
        for start in (0..count).step_by(512) {
            let end = (start + 512).min(count);
            let keys = (start..end).map(key).collect::<Vec<_>>();
            let values = (start..end)
                .map(|i| vec![((i + round) % 251) as u8; 64])
                .collect::<Vec<_>>();
            let operations = keys
                .iter()
                .zip(&values)
                .map(|(key, value)| BatchOperation::Put { key, value })
                .collect::<Vec<_>>();
            db.write_batch(&operations)?;
            for (key, value) in keys.into_iter().zip(values) {
                model.insert(key.to_vec(), value);
            }
        }
    }
    for start in (count - count / 8..count).step_by(512) {
        let keys = (start..(start + 512).min(count))
            .map(key)
            .collect::<Vec<_>>();
        let operations = keys
            .iter()
            .map(|key| BatchOperation::Delete { key })
            .collect::<Vec<_>>();
        db.write_batch(&operations)?;
        for key in keys {
            model.remove(key.as_slice());
        }
    }
    Ok(model)
}

// Open descriptors of this process, to show which phase holds old files.
fn open_descriptors() -> Result<usize> {
    let directory = if Path::new("/proc/self/fd").is_dir() {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    Ok(std::fs::read_dir(directory)?.count())
}

// Free space after asking the filesystem to settle. Block release after an
// unlink of the last reference can be asynchronous.
fn settled_free_space(path: &Path) -> Result<u64> {
    std::process::Command::new("sync").status()?;
    thread::sleep(Duration::from_secs(3));
    Ok(fs2::available_space(path)?)
}

fn percentile(sorted: &[u128], fraction: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}

fn summary(mut latencies: Vec<u128>) -> serde_json::Value {
    latencies.sort_unstable();
    json!({
        "writes": latencies.len(),
        "p50_us": percentile(&latencies, 0.50),
        "p99_us": percentile(&latencies, 0.99),
        "p999_us": percentile(&latencies, 0.999),
        "max_us": latencies.last().copied().unwrap_or(0),
    })
}

fn verify(store: &CheckpointStore, model: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<()> {
    let mut after: Option<Vec<u8>> = None;
    let mut expected = model.iter();
    loop {
        let page = store.scan(after.as_deref(), None, 1024)?;
        let Some((last, _)) = page.last() else {
            break;
        };
        after = Some(last.clone());
        for (observed_key, observed_value) in page {
            match expected.next() {
                Some((key, value)) if *key == observed_key && *value == observed_value => {}
                _ => return Err("reopened content mismatch".into()),
            }
        }
    }
    if expected.next().is_some() {
        return Err("reopened store is missing entries".into());
    }
    Ok(())
}

// Writes synchronized puts until stopped, returning acknowledged entries
// and each put's latency.
fn writer(
    store: Arc<OnlineCheckpointStore>,
    stop: Arc<AtomicBool>,
    prefix: &'static str,
) -> thread::JoinHandle<Result<WriterOutput>> {
    thread::spawn(move || {
        let mut written = Vec::new();
        let mut latencies = Vec::new();
        let mut index = 0_u64;
        while !stop.load(Ordering::Acquire) {
            // A fixed key set keeps the suffix within the default
            // distinct-key cap however fast the host is; the model keeps
            // the latest acknowledged value for each key.
            let key = format!("{prefix}-{:05}", index % WRITER_KEYS).into_bytes();
            let value = index.to_le_bytes().repeat(8);
            let start = Instant::now();
            store.with_store(|db| db.put(&key, &value))?;
            latencies.push(start.elapsed().as_micros());
            written.push((key, value));
            index += 1;
        }
        Ok((written, latencies))
    })
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [parent, count] = args.as_slice() else {
        return Err("usage: online_checkpoint_reclaim_bench <existing-directory> <keys>".into());
    };
    let count: usize = count.parse()?;
    if !Path::new(parent).is_dir() || !(1024..=2_000_000).contains(&count) {
        return Err("invalid parent or key count".into());
    }
    let root = tempfile::Builder::new()
        .prefix(".online-checkpoint-reclaim-bench-")
        .tempdir_in(parent)?;
    let source = root.path().join("source");
    let previous = root.path().join("previous");
    let ordinary = Store::open(&source)?;
    let mut model = fill(&ordinary, count)?;
    let digest = ordinary.checkpoint_to(root.path().join("base"))?;
    drop(ordinary);
    let checkpoint = CheckpointStore::open(&source, root.path().join("base"), &digest)?;
    checkpoint.rollover_managed(root.path().join("managed"))?;
    let store = Arc::new(OnlineCheckpointStore::from_store(checkpoint)?);
    let source_log_bytes = std::fs::metadata(source.join("records.log"))?.len();

    // Baseline writer latency without maintenance.
    let stop = Arc::new(AtomicBool::new(false));
    let baseline = writer(Arc::clone(&store), Arc::clone(&stop), "baseline");
    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Release);
    let (written, baseline_latencies) = baseline.join().map_err(|_| "writer panicked")??;
    model.extend(written);

    // Pin the current generation, then compact under concurrent writes.
    let snapshot = store.snapshot()?;
    let pinned_keys = model.len();
    let descriptors_before = open_descriptors()?;
    let free_before = settled_free_space(root.path())?;
    let stop = Arc::new(AtomicBool::new(false));
    let concurrent = writer(Arc::clone(&store), Arc::clone(&stop), "during");
    thread::sleep(Duration::from_millis(200));
    let start = Instant::now();
    let report = store.compact_and_reclaim(&previous, &Default::default())?;
    let elapsed_us = start.elapsed().as_micros();
    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Release);
    let (written, during_latencies) = concurrent.join().map_err(|_| "writer panicked")??;
    model.extend(written);
    if previous.exists() {
        return Err("previous generation still has a name".into());
    }
    let descriptors_pinned = open_descriptors()?;
    let free_pinned = settled_free_space(root.path())?;
    // The pinned view still reads the retired generation.
    let mut seen = 0_usize;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page = snapshot.as_snapshot()?.scan(after.as_deref(), None, 1024)?;
        let Some((last, _)) = page.last() else {
            break;
        };
        after = Some(last.clone());
        seen += page.len();
    }
    if seen != pinned_keys {
        return Err("pinned snapshot changed".into());
    }
    drop(snapshot);
    let descriptors_released = open_descriptors()?;
    let free_released = settled_free_space(root.path())?;
    let compacted_log_bytes = std::fs::metadata(source.join("records.log"))?.len();
    let staging_clean = !std::fs::read_dir(root.path())?.any(|entry| {
        entry
            .map(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".emda-stage-v1-")
            })
            .unwrap_or(true)
    });
    if !staging_clean {
        return Err("staging path remains after reclamation".into());
    }
    drop(store);
    let reopened = CheckpointStore::open_managed(&source)?;
    verify(&reopened, &model)?;
    println!(
        "{}",
        json!({
            "keys": count,
            "final_live_keys": model.len(),
            "source_log_bytes_at_start": source_log_bytes,
            "report": {
                "initial_log_bytes": report.initial_log_bytes,
                "final_log_bytes": report.final_log_bytes,
                "concurrent_replay_bytes": report.concurrent_replay_bytes,
                "final_replay_bytes": report.final_replay_bytes,
                "compacted_log_bytes": report.compacted_log_bytes,
            },
            "compacted_log_bytes_after_writes": compacted_log_bytes,
            "compaction_elapsed_us": elapsed_us,
            "writer_baseline": summary(baseline_latencies),
            "writer_during_compaction": summary(during_latencies),
            "open_descriptors": {
                "before_compaction": descriptors_before,
                "snapshot_pins_retired": descriptors_pinned,
                "after_snapshot_release": descriptors_released,
            },
            "free_bytes_before": free_before,
            "free_bytes_while_snapshot_pins_retired": free_pinned,
            "free_bytes_after_snapshot_release": free_released,
            "freed_after_release": i128::from(free_released) - i128::from(free_pinned),
            "staging_clean": staging_clean,
            "reopen_exact": true,
        })
    );
    Ok(())
}
