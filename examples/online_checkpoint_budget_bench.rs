//! Disposable disk-budget accounting measurement for online checkpoint-store
//! maintenance.
//!
//! A managed checkpoint store with overwritten and deleted history gets a
//! persistent disk budget. While one thread commits synchronized puts and an
//! owned snapshot pins the current generation, the store is compacted and
//! the previous generation reclaimed. A sampler records charged bytes and the
//! observed logical length of every file the store owns. Quiescent phase
//! readings are exact; concurrent samples are not atomic with file changes.
//! All sizes are logical file lengths, not filesystem allocation.
use database_engine::{
    BatchOperation, CheckpointStore, DiskBudgetPending, OnlineCheckpointStore,
    OnlineReclaimFailure, Store,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Distinct keys each concurrent writer cycles through. Two writer phases
/// stay below the default 16,384-key checkpoint suffix cap.
const WRITER_KEYS: u64 = 4096;

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

// Logical length of regular files under `path`; a missing path is zero.
fn tree_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    let mut total = 0;
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    for entry in entries {
        match entry.and_then(|entry| tree_bytes(&entry.path())) {
            Ok(bytes) => total += bytes,
            // A concurrent rename or removal can race the walk.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(total)
}

// Every named file the store owns: the store, an unselected or retired
// previous generation, staging in the parent and the selected external
// checkpoint until compaction replaces it.
fn observed(root: &Path, selected_external: Option<&Path>) -> std::io::Result<u64> {
    let mut total = tree_bytes(&root.join("source"))? + tree_bytes(&root.join("previous"))?;
    for entry in std::fs::read_dir(root)? {
        let name = entry?.file_name();
        if name.to_string_lossy().starts_with(".emda-stage-v1-") {
            total += tree_bytes(&root.join(name))?;
        }
    }
    if let Some(path) = selected_external {
        total += tree_bytes(path)?;
    }
    Ok(total)
}

fn status_json(
    store: &OnlineCheckpointStore,
    root: &Path,
    external: Option<&Path>,
) -> Result<serde_json::Value> {
    let files = observed(root, external)?;
    let status = store.disk_budget_status()?.ok_or("budget missing")?;
    Ok(json!({
        "charged": status.charged,
        "live_bytes": status.live_bytes,
        "maintenance_reserved": status.maintenance_reserved,
        "retired_bytes": status.retired_bytes,
        "pinned_bytes": status.pinned_bytes,
        "pending": format!("{:?}", status.pending),
        "observed_named_file_bytes": files,
        "charged_minus_observed": i128::from(status.charged) - i128::from(files),
    }))
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [parent, count] = args.as_slice() else {
        return Err("usage: online_checkpoint_budget_bench <existing-directory> <keys>".into());
    };
    let count: usize = count.parse()?;
    if !Path::new(parent).is_dir() || !(1024..=2_000_000).contains(&count) {
        return Err("invalid parent or key count".into());
    }
    let root = tempfile::Builder::new()
        .prefix(".online-checkpoint-budget-bench-")
        .tempdir_in(parent)?;
    let root_path = std::fs::canonicalize(root.path())?;
    let source = root_path.join("source");
    let previous = root_path.join("previous");
    let ordinary = Store::open(&source)?;
    let mut model = fill(&ordinary, count)?;
    let digest = ordinary.checkpoint_to(root_path.join("base"))?;
    drop(ordinary);
    let checkpoint = CheckpointStore::open(&source, root_path.join("base"), &digest)?;
    let managed: PathBuf = root_path.join("managed");
    checkpoint.rollover_managed(&managed)?;
    let store = Arc::new(OnlineCheckpointStore::from_store(checkpoint)?);
    store.set_disk_budget(Some(u64::MAX))?;
    let mut phases = serde_json::Map::new();
    phases.insert(
        "after_fill".into(),
        status_json(&store, &root_path, Some(&managed))?,
    );

    // A limit at the current charge refuses compaction before publication.
    let charged = store.disk_budget_status()?.ok_or("budget missing")?.charged;
    store.set_disk_budget(Some(charged))?;
    let refused = match store.compact_and_reclaim_detailed(&previous, &Default::default()) {
        Err(OnlineReclaimFailure::Compaction(failure)) => {
            let text = failure.to_string();
            !previous.exists() && text.contains("disk budget")
        }
        _ => false,
    };
    let refused_put = store
        .with_store(|db| db.put(b"refused", b"value"))
        .is_err_and(|error| error.kind() == std::io::ErrorKind::StorageFull);
    store.set_disk_budget(Some(u64::MAX))?;
    phases.insert(
        "after_refusals".into(),
        status_json(&store, &root_path, Some(&managed))?,
    );

    let snapshot = store.snapshot()?;
    let pinned_keys = model.len();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (store, stop) = (Arc::clone(&store), Arc::clone(&stop));
        thread::spawn(move || -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let mut written = Vec::new();
            let mut index = 0_u64;
            while !stop.load(Ordering::Acquire) {
                // A fixed key set keeps the suffix within the default
                // distinct-key cap; the model keeps the latest value.
                let key = format!("during-{:05}", index % WRITER_KEYS).into_bytes();
                let value = index.to_le_bytes().repeat(8);
                store.with_store(|db| db.put(&key, &value))?;
                written.push((key, value));
                index += 1;
            }
            Ok(written)
        })
    };
    let sampler = {
        let (store, stop, root_path, managed) = (
            Arc::clone(&store),
            Arc::clone(&stop),
            root_path.clone(),
            managed.clone(),
        );
        thread::spawn(move || -> Result<serde_json::Value> {
            let (mut samples, mut peak_charged, mut peak_observed, mut peak_reserved) =
                (0, 0, 0, 0);
            let (mut observed_above_charged, mut max_excess) = (0, 0_u64);
            let (mut all_above_charged, mut all_max_excess) = (0, 0_u64);
            while !stop.load(Ordering::Acquire) {
                // The external checkpoint is charged only while selected; the
                // replacement's own checkpoint takes over at the exchange.
                // Count it only if it was selected both before the walk
                // and after the status read, so the exchange cannot fall
                // between the two readings.
                let replaced = || root_path.join("source/index.checkpoint").exists();
                let before = replaced();
                // Read files first: admitted bytes are charged before writing.
                let all = observed(&root_path, Some(&managed))?;
                let without = observed(&root_path, None)?;
                let status = store.disk_budget_status()?.ok_or("budget missing")?;
                let files = if !before && !replaced() { all } else { without };
                samples += 1;
                peak_charged = peak_charged.max(status.charged);
                peak_observed = peak_observed.max(files);
                peak_reserved = peak_reserved.max(status.maintenance_reserved);
                if files > status.charged {
                    observed_above_charged += 1;
                    max_excess = max_excess.max(files - status.charged);
                }
                if all > status.charged {
                    all_above_charged += 1;
                    all_max_excess = all_max_excess.max(all - status.charged);
                }
                thread::sleep(Duration::from_millis(2));
            }
            Ok(json!({
                "samples": samples,
                "peak_charged": peak_charged,
                "peak_observed_named_file_bytes": peak_observed,
                "peak_maintenance_reserved": peak_reserved,
                "samples_with_observed_above_charged": observed_above_charged,
                "max_observed_excess": max_excess,
                "samples_including_unselected_external_above_charged": all_above_charged,
                "max_excess_including_unselected_external": all_max_excess,
                "external_checkpoint_bytes": tree_bytes(&managed)?,
            }))
        })
    };
    thread::sleep(Duration::from_millis(200));
    let start = Instant::now();
    let report = store.compact_and_reclaim(&previous, &Default::default())?;
    let elapsed_us = start.elapsed().as_micros();
    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Release);
    model.extend(writer.join().map_err(|_| "writer panicked")??);
    let sampled = sampler.join().map_err(|_| "sampler panicked")??;
    // The external managed checkpoint is no longer selected after compaction.
    phases.insert(
        "pinned_after_reclaim".into(),
        status_json(&store, &root_path, None)?,
    );
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
    phases.insert(
        "after_snapshot_release".into(),
        status_json(&store, &root_path, None)?,
    );
    let final_status = store.disk_budget_status()?.ok_or("budget missing")?;
    if final_status.pending != DiskBudgetPending::None || final_status.pinned_bytes != 0 {
        return Err("budget still charges maintenance after release".into());
    }
    drop(store);
    let reopened = OnlineCheckpointStore::open_managed(&source)?;
    let reopened_status = reopened.disk_budget_status()?.ok_or("budget missing")?;
    let mut after: Option<Vec<u8>> = None;
    let mut expected = model.iter();
    loop {
        let page = reopened.with_store(|db| db.scan(after.as_deref(), None, 1024))?;
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
    println!(
        "{}",
        json!({
            "keys": count,
            "final_live_keys": model.len(),
            "refused_compaction_at_charge_limit": refused,
            "refused_put_at_charge_limit": refused_put,
            "compaction_elapsed_us": elapsed_us,
            "report": {
                "initial_log_bytes": report.initial_log_bytes,
                "final_log_bytes": report.final_log_bytes,
                "compacted_log_bytes": report.compacted_log_bytes,
            },
            "phases": phases,
            "during_compaction": sampled,
            "reopened_charged": reopened_status.charged,
            "charged_unchanged_by_reopen": reopened_status.charged == final_status.charged,
            "reopen_exact": true,
        })
    );
    Ok(())
}
