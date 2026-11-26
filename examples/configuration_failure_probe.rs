//! Disposable fixtures for automatic-checkpoint configuration fault checks.
use database_engine::{
    CheckpointStore, DiskBudgetPending, Entry, OnlineCheckpointStore, PublicationFailure, Store,
    cleanup_abandoned_staging,
};
use serde_json::json;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn expected() -> Vec<Entry> {
    vec![
        (b"old".to_vec(), b"original".to_vec()),
        (b"post-selection".to_vec(), b"acknowledged-managed".to_vec()),
        (b"safe".to_vec(), b"acknowledged".to_vec()),
        (b"tail".to_vec(), b"acknowledged-tail".to_vec()),
    ]
}

fn setup(root: &Path, format: &str, operation: &str) -> ProbeResult<()> {
    if !root.is_dir() || fs::read_dir(root)?.next().is_some() {
        return Err("fixture root must be an existing empty directory".into());
    }
    if !matches!(
        operation,
        "initial-enable" | "budgeted-enable" | "re-enable" | "disable"
    ) {
        return Err("unknown operation".into());
    }
    let source = root.join("source");
    fs::create_dir(&source)?;
    match format {
        "v2" => {
            fs::write(
                source.join("records.log"),
                include_bytes!("../tests/fixtures/v2-empty-records.log"),
            )?;
            fs::write(
                source.join("commit.state"),
                include_bytes!("../tests/fixtures/v2-empty-commit.state"),
            )?;
        }
        "v3" => {}
        _ => return Err("unknown format".into()),
    }
    let store = Store::open(&source)?;
    store.put(b"safe", b"acknowledged")?;
    store.put(b"old", b"original")?;
    let digest = store.checkpoint_to(root.join("checkpoint"))?;
    fs::write(root.join("checkpoint.digest"), digest)?;
    store.put(b"tail", b"acknowledged-tail")?;
    drop(store);
    let store = CheckpointStore::open(&source, root.join("checkpoint"), &digest)?;
    store.rollover_managed(root.join("selected"))?;
    store.put(b"post-selection", b"acknowledged-managed")?;
    store.backup(root.join("reference-backup"))?;
    if operation == "budgeted-enable" {
        // A first enable under an installed disk budget.
        let online = OnlineCheckpointStore::from_store(store)?;
        online.set_disk_budget(Some(u64::MAX))?;
        drop(online);
        let log = fs::read(source.join("records.log"))?;
        let prefix: &[u8] = if format == "v2" {
            b"EMDALOG2"
        } else {
            b"EMDALOG3"
        };
        if !log.starts_with(prefix) {
            return Err("fixture format differs from requested format".into());
        }
        println!(
            "{}",
            json!({"status":"ready", "format":format, "operation":operation})
        );
        return Ok(());
    }
    if operation != "initial-enable" {
        store.enable_automatic_checkpoints()?;
    }
    if operation == "re-enable" {
        store.disable_automatic_checkpoints()?;
    }
    drop(store);
    let log = fs::read(source.join("records.log"))?;
    let prefix: &[u8] = if format == "v2" {
        b"EMDALOG2"
    } else {
        b"EMDALOG3"
    };
    if !log.starts_with(prefix) {
        return Err("fixture format differs from requested format".into());
    }
    println!(
        "{}",
        json!({"status":"ready", "format":format, "operation":operation})
    );
    Ok(())
}

fn attempt(root: &Path, operation: &str) -> ProbeResult<()> {
    let mut handle = "unopened";
    let outcome = match CheckpointStore::open_managed(root.join("source")) {
        Ok(store) => {
            handle = "opened";
            let result = match operation {
                "initial-enable" | "budgeted-enable" | "re-enable" => {
                    store.enable_automatic_checkpoints_detailed()
                }
                "disable" => store.disable_automatic_checkpoints_detailed(),
                _ => return Err("unknown operation".into()),
            };
            if result.is_err() {
                let read_blocked = store.get(b"safe").is_err();
                handle = if read_blocked { "blocked" } else { "readable" };
                if matches!(&result, Err(PublicationFailure::OutcomeUnknown(_))) {
                    let write_blocked = store.put(b"unexpected", b"must-not-write").is_err();
                    if !read_blocked || !write_blocked {
                        return Err("uncertain outcome did not block handle".into());
                    }
                }
            }
            result
        }
        Err(error) => Err(PublicationFailure::NotPublished(error)),
    };
    match outcome {
        Ok(()) => println!(
            "{}",
            json!({"status":"success", "operation":operation, "classification":null, "handle":handle})
        ),
        Err(error) => {
            let (classification, error) = match error {
                PublicationFailure::NotPublished(error) => ("not_published", error),
                PublicationFailure::OutcomeUnknown(error) => ("unknown", error),
                other => ("unknown", io::Error::other(other)),
            };
            println!(
                "{}",
                json!({"status":"error", "operation":operation, "classification":classification,
                "kind":format!("{:?}",error.kind()), "message":error.to_string(), "handle":handle})
            );
        }
    }
    Ok(())
}

fn verify(root: &Path, operation: &str, order: &str) -> ProbeResult<()> {
    let paths = match order {
        "ordinary-first" => ["ordinary", "managed"],
        "managed-first" => ["managed", "ordinary"],
        _ => return Err("unknown order".into()),
    };
    let budgeted = operation == "budgeted-enable";
    let mut enabled = None;
    for path in paths {
        if path == "ordinary" {
            match Store::open(root.join("source")) {
                // An ordinary open refuses a budgeted directory unchanged.
                Err(error) if budgeted && error.kind() == io::ErrorKind::Unsupported => {}
                Err(error) => return Err(error.into()),
                Ok(_) if budgeted => {
                    return Err("ordinary open accepted a budgeted store".into());
                }
                Ok(store) => {
                    if store.scan(None, None, 16)? != expected() {
                        return Err("ordinary recovery changed fixture".into());
                    }
                }
            }
        } else {
            let store = CheckpointStore::open_managed(root.join("source"))?;
            if store.scan(None, None, 16)? != expected() {
                return Err("managed recovery changed fixture".into());
            }
            enabled = Some(store.automatic_checkpoints_enabled()?);
            if budgeted {
                check_budget(root, &store)?;
            }
        }
    }
    Store::restore(root.join("reference-backup"), root.join("restored"))?;
    if Store::open(root.join("restored"))?.scan(None, None, 16)? != expected() {
        return Err("reference backup did not recover the acknowledged fixture".into());
    }
    cleanup_abandoned_staging(root)?;
    cleanup_abandoned_staging(root.join("source"))?;
    let automatic = root.join("source/.emda-auto-checkpoints-v1");
    if automatic.exists() {
        cleanup_abandoned_staging(automatic)?;
    }
    println!(
        "{}",
        json!({"status":"verified", "operation":operation, "order":order,
        "enabled":enabled.ok_or("managed path was not checked")?, "backup_restored":true})
    );
    Ok(())
}

// The reopened budget charges exactly the store tree, the external selected
// checkpoint and one spare record, with nothing pending or reserved.
fn check_budget(root: &Path, store: &CheckpointStore) -> ProbeResult<()> {
    fn tree(path: &Path) -> ProbeResult<u64> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.is_file() {
            return Ok(metadata.len());
        }
        let mut total = 0;
        for entry in fs::read_dir(path)? {
            total += tree(&entry?.path())?;
        }
        Ok(total)
    }
    let status = store
        .disk_budget_status()?
        .ok_or("budget record disappeared")?;
    let measured = tree(&root.join("source"))? + tree(&root.join("selected"))? + 4096;
    if status.pending != DiskBudgetPending::None
        || status.automatic_reserved != 0
        || status.charged != measured
    {
        return Err(format!("reopened budget charge {status:?} differs from {measured}").into());
    }
    Ok(())
}

fn main() -> ProbeResult<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    match arguments.as_slice() {
        [command, root, format, operation] if command == "setup" => setup(Path::new(root), format, operation),
        [command, root, operation] if command == "attempt-publication" => attempt(Path::new(root), operation),
        [command, root, operation, order] if command == "verify" => verify(Path::new(root), operation, order),
        _ => Err("usage: setup ROOT FORMAT OPERATION | attempt-publication ROOT OPERATION | verify ROOT OPERATION ORDER".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifier_rejects_changed_acknowledged_data_in_either_order() {
        for order in ["ordinary-first", "managed-first"] {
            let root = tempfile::tempdir().unwrap();
            setup(root.path(), "v2", "re-enable").unwrap();
            let store = Store::open(root.path().join("source")).unwrap();
            store.put(b"safe", b"changed").unwrap();
            drop(store);
            assert!(verify(root.path(), "re-enable", order).is_err());
        }
    }
}
