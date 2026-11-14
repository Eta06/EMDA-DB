//! Disposable fixtures for detailed online-reclamation fault campaigns.
use database_engine::{OnlineHandoffFailure, OnlineReclaimFailure, OnlineStore, Store};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn expected() -> Vec<database_engine::Entry> {
    vec![
        (b"old".to_vec(), b"original".to_vec()),
        (b"safe".to_vec(), b"acknowledged".to_vec()),
        (b"tail".to_vec(), b"acknowledged-tail".to_vec()),
    ]
}

fn setup(root: &Path, format: &str) -> ProbeResult<()> {
    if !root.is_dir() || fs::read_dir(root)?.next().is_some() {
        return Err("fixture root must be an existing empty directory".into());
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
    store.put(b"old", b"obsolete")?;
    store.put(b"old", b"original")?;
    let digest = store.checkpoint_to(root.join("checkpoint"))?;
    fs::write(root.join("checkpoint.digest"), digest)?;
    store.put(b"tail", b"acknowledged-tail")?;
    store.backup(root.join("reference-backup"))?;
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
    fs::write(root.join("original-log.sha256"), Sha256::digest(&log))?;
    fs::create_dir(root.join("restore-area"))?;
    println!("{}", json!({"status":"ready", "format":format}));
    Ok(())
}

fn attempt(root: &Path, operation: &str) -> ProbeResult<()> {
    if operation != "reclaim" {
        return Err("unknown operation".into());
    }
    let mut handle = "unopened";
    let outcome = match OnlineStore::open(root.join("source")) {
        Ok(store) => {
            handle = "opened";
            let result =
                store.compact_and_reclaim_detailed(root.join("previous"), &Default::default());
            if matches!(
                &result,
                Err(OnlineReclaimFailure::Compaction(
                    OnlineHandoffFailure::ExchangeOutcomeUnknown(_)
                ))
            ) {
                let read_blocked = store.with_store(|db| db.get(b"safe")).is_err();
                let write_blocked = store
                    .with_store(|db| db.put(b"unexpected", b"must-not-write"))
                    .is_err();
                if !read_blocked || !write_blocked {
                    return Err("uncertain exchange did not block handle".into());
                }
                handle = "blocked";
            } else if matches!(&result, Err(OnlineReclaimFailure::Reclamation { .. })) {
                if store.with_store(|db| db.get(b"safe"))?.as_deref() != Some(&b"acknowledged"[..])
                {
                    return Err("reclamation failure left live handle unreadable".into());
                }
                handle = "readable";
            }
            result.map(|_| ())
        }
        Err(error) => Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::NotPublished(error),
        )),
    };
    match outcome {
        Ok(()) => println!(
            "{}",
            json!({"status":"success", "operation":operation, "classification":null, "handle":handle})
        ),
        Err(error) => {
            let (classification, kind, message) = match error {
                OnlineReclaimFailure::Compaction(OnlineHandoffFailure::NotPublished(error)) => {
                    ("not_published", error.kind(), error.to_string())
                }
                OnlineReclaimFailure::Compaction(
                    OnlineHandoffFailure::CandidateOutcomeUnknown(error),
                ) => ("candidate_unknown", error.kind(), error.to_string()),
                OnlineReclaimFailure::Compaction(OnlineHandoffFailure::ExchangeOutcomeUnknown(
                    error,
                )) => ("exchange_unknown", error.kind(), error.to_string()),
                OnlineReclaimFailure::Reclamation { error, .. } => {
                    ("reclamation", error.kind(), error.to_string())
                }
                other => ("unknown", io::ErrorKind::Other, other.to_string()),
            };
            println!(
                "{}",
                json!({"status":"error", "operation":operation, "classification":classification,
                "kind":format!("{kind:?}"), "message":message, "handle":handle})
            );
        }
    }
    Ok(())
}

fn verify(root: &Path, classification: &str, order: &str) -> ProbeResult<()> {
    let order = match order {
        "source-first" => ["source", "backup"],
        "backup-first" => ["backup", "source"],
        _ => return Err("unknown verification order".into()),
    };
    for path in order {
        let store = if path == "source" {
            Store::open(root.join("source"))?
        } else {
            let restored = root.join("restore-area/output");
            Store::restore(root.join("reference-backup"), &restored)?;
            Store::open(restored)?
        };
        if store.scan(None, None, 16)? != expected() {
            return Err("source or restored backup has incorrect values".into());
        }
    }
    if matches!(classification, "candidate_unknown" | "exchange_unknown")
        && root.join("previous").exists()
    {
        let previous = Store::open(root.join("previous"))?;
        if previous.scan(None, None, 16)? != expected() {
            return Err("published sibling has incorrect values".into());
        }
    }
    println!(
        "{}",
        json!({"status":"verified", "classification":classification,
        "order":if order[0]=="source" {"source-first"} else {"backup-first"}, "backup_restored":true})
    );
    Ok(())
}

fn main() -> ProbeResult<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    match arguments.as_slice() {
        [command, root, format] if command == "setup" => setup(Path::new(root), format),
        [command, root, operation] if command == "attempt-publication" => attempt(Path::new(root), operation),
        [command, root, classification, order] if command == "verify" => verify(Path::new(root), classification, order),
        _ => Err("usage: setup ROOT FORMAT | attempt-publication ROOT reclaim | verify ROOT CLASSIFICATION ORDER".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifier_rejects_changed_source_in_both_orders() {
        for order in ["source-first", "backup-first"] {
            let root = tempfile::tempdir().unwrap();
            setup(root.path(), "v2").unwrap();
            let store = Store::open(root.path().join("source")).unwrap();
            store.put(b"safe", b"changed").unwrap();
            drop(store);
            assert!(verify(root.path(), "not_published", order).is_err());
        }
    }
}
