//! Disposable online-compaction and retired-log reclamation comparison.
use database_engine::{BatchOperation, OnlineCompactionOptions, OnlineStore, Store};
use serde_json::json;
use std::{error::Error, io::Read, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn key(index: usize) -> [u8; 8] {
    (index as u64).to_be_bytes()
}

fn fill(db: &Store, count: usize) -> Result<()> {
    for round in 0..2 {
        let limit = if round == 0 { count } else { count / 2 };
        for start in (0..limit).step_by(512) {
            let end = (start + 512).min(limit);
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
    }
    Ok(())
}

fn verify(db: &Store, count: usize) -> Result<()> {
    let live = count - count / 8;
    let mut after = None;
    let mut seen = 0;
    loop {
        let page = db.scan(after.as_deref(), None, 1024)?;
        if page.is_empty() {
            break;
        }
        for (observed_key, value) in page {
            let expected = ((seen + usize::from(seen < count / 2)) % 251) as u8;
            if observed_key != key(seen) || value != vec![expected; 64] {
                return Err("live content mismatch".into());
            }
            after = Some(observed_key);
            seen += 1;
        }
    }
    if seen != live || db.get(&key(live))?.is_some() {
        return Err("live count or deleted key mismatch".into());
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [mode, parent, count] = args.as_slice() else {
        return Err(
            "usage: online_reclaim_bench <retain|reclaim> <existing-directory> <keys>".into(),
        );
    };
    let count: usize = count.parse()?;
    if !Path::new(parent).is_dir()
        || !(16..=16_000).contains(&count)
        || !["retain", "reclaim"].contains(&mode.as_str())
    {
        return Err("invalid mode, parent or key count".into());
    }
    let root = tempfile::Builder::new()
        .prefix(".online-reclaim-bench-")
        .tempdir_in(parent)?;
    let source = root.path().join("source");
    let previous = root.path().join("previous");
    let store = OnlineStore::open(&source)?;
    store.with_store(|db| fill(db, count).map_err(std::io::Error::other))?;
    let mut header = [0_u8; 8];
    std::fs::File::open(source.join("records.log"))?.read_exact(&mut header)?;
    let format = match &header {
        b"EMDALOG3" => "V3",
        b"EMDALOG2" => "V2",
        _ => return Err("unexpected newly created store format".into()),
    };
    let before = std::fs::metadata(source.join("records.log"))?.len();
    let start = Instant::now();
    let report = if mode == "retain" {
        store.compact_and_replace(&previous, &OnlineCompactionOptions::default())?
    } else {
        store.compact_and_reclaim(&previous, &OnlineCompactionOptions::default())?
    };
    let elapsed_us = start.elapsed().as_micros();
    if report.final_log_bytes != before {
        return Err("unexpected source log length".into());
    }
    if std::fs::read_dir(root.path())?.any(|entry| {
        entry
            .map(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".emda-stage-v1-")
            })
            .unwrap_or(true)
    }) {
        return Err("unexpected staging path after successful compaction".into());
    }
    let after = std::fs::metadata(source.join("records.log"))?.len();
    if after != report.compacted_log_bytes {
        return Err("unexpected compacted log length".into());
    }
    if previous.exists() != (mode == "retain") {
        return Err("unexpected previous path state".into());
    }
    let previous_bytes = if previous.exists() {
        std::fs::metadata(previous.join("records.log"))?.len()
    } else {
        0
    };
    if previous_bytes != if mode == "retain" { before } else { 0 } {
        return Err("previous log length mismatch".into());
    }
    store.with_store(|db| verify(db, count).map_err(std::io::Error::other))?;
    drop(store);
    let reopened = Store::open(&source)?;
    verify(&reopened, count)?;
    if mode == "retain" {
        verify(&Store::open(&previous)?, count)?;
    }
    println!(
        "{}",
        json!({"mode":mode,"log_format":format,"keys":count,"live_keys":count-count/8,
        "source_log_bytes":before,"compacted_log_bytes":after,"previous_log_bytes":previous_bytes,
        "total_log_bytes_after":after+previous_bytes,"elapsed_us":elapsed_us,"staging_clean":true,"reopen_verified":true})
    );
    Ok(())
}
