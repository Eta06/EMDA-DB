//! Disposable fixtures for compaction-handoff filesystem fault campaigns.
use database_engine::{
    CheckpointStore, Entry, HandoffFailure, OnlineHandoffFailure, OnlineStore, Store,
    cleanup_abandoned_staging,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn fixture_digest(root: &Path) -> ProbeResult<[u8; 32]> {
    fs::read(root.join("checkpoint.digest"))?
        .try_into()
        .map_err(|_| "invalid fixture checkpoint digest".into())
}

fn setup(root: &Path, format: &str, backend: &str) -> ProbeResult<()> {
    if !root.is_dir() || fs::read_dir(root)?.next().is_some() {
        return Err("fixture root must be an existing empty directory".into());
    }
    if !matches!(backend, "ordinary" | "checkpoint" | "managed" | "online") {
        return Err("unknown backend".into());
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
    if backend == "managed" {
        let store = CheckpointStore::open(&source, root.join("checkpoint"), &digest)?;
        store.rollover_managed(root.join("managed-base"))?;
        store.enable_automatic_checkpoints()?;
        store.put(b"managed-tail", b"acknowledged-managed-tail")?;
    }
    let log = fs::read(source.join("records.log"))?;
    let expected = if format == "v2" {
        b"EMDALOG2"
    } else {
        b"EMDALOG3"
    };
    if !log.starts_with(expected) {
        return Err("fixture format differs from requested format".into());
    }
    fs::write(root.join("original-log.sha256"), Sha256::digest(&log))?;
    println!(
        "{}",
        json!({"status":"ready", "format":format, "backend":backend})
    );
    Ok(())
}

fn attempt(root: &Path, backend: &str) -> ProbeResult<()> {
    if backend == "online" {
        return attempt_online(root);
    }
    let source = root.join("source");
    let target = root.join("result");
    let outcome = match backend {
        "ordinary" => Store::open(&source)
            .map_err(HandoffFailure::NotPublished)
            .and_then(|store| {
                store
                    .compact_and_replace_detailed(&source, &target)
                    .map(|_| ())
            }),
        "checkpoint" => {
            let store = fs::read(root.join("checkpoint.digest"))
                .and_then(|bytes| {
                    bytes.try_into().map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid fixture digest")
                    })
                })
                .and_then(|digest: [u8; 32]| {
                    CheckpointStore::open(&source, root.join("checkpoint"), &digest)
                });
            store
                .map_err(HandoffFailure::NotPublished)
                .and_then(|store| store.compact_and_replace_detailed(&target).map(|_| ()))
        }
        "managed" => CheckpointStore::open_managed(&source)
            .map_err(HandoffFailure::NotPublished)
            .and_then(|store| store.compact_and_replace_detailed(&target).map(|_| ())),
        _ => return Err("unknown backend".into()),
    };
    match outcome {
        Ok(()) => println!(
            "{}",
            json!({"status":"success", "operation":backend, "classification":null})
        ),
        Err(error) => {
            let (classification, error) = match error {
                HandoffFailure::NotPublished(error) => ("not_published", error),
                HandoffFailure::CandidateOutcomeUnknown(error) => ("candidate_unknown", error),
                HandoffFailure::ExchangeOutcomeUnknown(error) => ("exchange_unknown", error),
                other => ("exchange_unknown", io::Error::other(other)),
            };
            println!(
                "{}",
                json!({"status":"error", "operation":backend, "classification":classification,
                "kind":format!("{:?}",error.kind()), "message":error.to_string()})
            );
        }
    }
    Ok(())
}

fn attempt_online(root: &Path) -> ProbeResult<()> {
    let mut handle_after_exchange_failure = None;
    let outcome = match OnlineStore::open(root.join("source")) {
        Ok(store) => {
            let outcome =
                store.compact_and_replace_detailed(root.join("result"), &Default::default());
            if matches!(
                &outcome,
                Err(OnlineHandoffFailure::ExchangeOutcomeUnknown(_))
            ) {
                let read_blocked = store.with_store(|db| db.get(b"safe")).is_err();
                let write_blocked = store
                    .with_store(|db| db.put(b"after-exchange-error", b"must-not-write"))
                    .is_err();
                if !read_blocked || !write_blocked {
                    return Err("exchange failure did not block online handle access".into());
                }
                handle_after_exchange_failure = Some("blocked");
            }
            outcome.map(|_| ())
        }
        Err(error) => Err(OnlineHandoffFailure::NotPublished(error)),
    };
    match outcome {
        Ok(()) => println!(
            "{}",
            json!({"status":"success", "operation":"online", "classification":null})
        ),
        Err(error) => {
            let (classification, error) = match error {
                OnlineHandoffFailure::NotPublished(error) => ("not_published", error),
                OnlineHandoffFailure::CandidateOutcomeUnknown(error) => {
                    ("candidate_unknown", error)
                }
                OnlineHandoffFailure::ExchangeOutcomeUnknown(error) => ("exchange_unknown", error),
                other => ("exchange_unknown", io::Error::other(other)),
            };
            println!(
                "{}",
                json!({"status":"error", "operation":"online", "classification":classification,
                "kind":format!("{:?}",error.kind()), "message":error.to_string(),
                "handle_after_exchange_failure":handle_after_exchange_failure})
            );
        }
    }
    Ok(())
}

fn check_values(
    get: impl Fn(&[u8]) -> io::Result<Option<Vec<u8>>>,
    scan: impl FnOnce() -> io::Result<Vec<Entry>>,
    backend: &str,
) -> ProbeResult<()> {
    for (key, value) in [
        (b"safe".as_slice(), b"acknowledged".as_slice()),
        (b"old".as_slice(), b"original".as_slice()),
        (b"tail".as_slice(), b"acknowledged-tail".as_slice()),
    ] {
        if get(key)?.as_deref() != Some(value) {
            return Err("acknowledged fixture data changed".into());
        }
    }
    if backend == "managed"
        && get(b"managed-tail")?.as_deref() != Some(b"acknowledged-managed-tail")
    {
        return Err("acknowledged managed suffix changed".into());
    }
    let mut expected = vec![
        (b"safe".to_vec(), b"acknowledged".to_vec()),
        (b"old".to_vec(), b"original".to_vec()),
        (b"tail".to_vec(), b"acknowledged-tail".to_vec()),
    ];
    if backend == "managed" {
        expected.push((
            b"managed-tail".to_vec(),
            b"acknowledged-managed-tail".to_vec(),
        ));
    }
    expected.sort();
    if scan()? != expected {
        return Err("handoff recovered unexpected or missing keys".into());
    }
    Ok(())
}

fn verify(root: &Path, backend: &str, order: &str) -> ProbeResult<()> {
    let source = root.join("source");
    let target = root.join("result");
    let target_visible = target.exists();
    let original_hash = fs::read(root.join("original-log.sha256"))?;
    let is_original = |path: &Path| -> ProbeResult<bool> {
        Ok(
            Sha256::digest(fs::read(path.join("records.log"))?).as_slice()
                == original_hash.as_slice(),
        )
    };
    let source_original = is_original(&source)?;
    let target_original = target_visible && is_original(&target)?;
    if source_original == target_original {
        return Err("expected exactly one original log".into());
    }
    let paths = match order {
        "source-first" => [&source, &target],
        "target-first" => [&target, &source],
        _ => return Err("unknown recovery order".into()),
    };
    for path in paths {
        if path.exists() {
            let store = Store::open(path)?;
            check_values(|key| store.get(key), || store.scan(None, None, 64), backend)?;
        }
    }
    let original = if source_original { &source } else { &target };
    let digest = fixture_digest(root)?;
    let original_cp = if backend == "managed" {
        CheckpointStore::open_managed(original)?
    } else {
        CheckpointStore::open(original, root.join("checkpoint"), &digest)?
    };
    check_values(
        |key| original_cp.get(key),
        || original_cp.scan(None, None, 64),
        backend,
    )?;
    drop(original_cp);
    if backend == "managed" {
        let live = CheckpointStore::open_managed(&source)?;
        check_values(|key| live.get(key), || live.scan(None, None, 64), backend)?;
        if !live.automatic_checkpoints_enabled()? {
            return Err("managed automatic setting changed".into());
        }
    }
    cleanup_abandoned_staging(root)?;
    cleanup_abandoned_staging(&source)?;
    if target_visible {
        cleanup_abandoned_staging(&target)?;
    }
    println!(
        "{}",
        json!({"status":"verified", "operation":backend, "order":order,
        "target_visible":target_visible, "source_original":source_original, "target_original":target_original})
    );
    Ok(())
}

fn main() -> ProbeResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, root, format, backend] if command == "setup" => setup(Path::new(root), format, backend),
        [command, root, backend] if command == "attempt-publication" => attempt(Path::new(root), backend),
        [command, root, backend, order] if command == "verify" => verify(Path::new(root), backend, order),
        _ => Err("usage: handoff_failure_probe setup <root> <v2|v3> <backend> | attempt-publication <root> <backend> | verify <root> <backend> <source-first|target-first>".into()),
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn verifier_rejects_changed_values_for_all_handoff_backends() {
        for backend in ["ordinary", "checkpoint", "managed", "online"] {
            let root = tempfile::tempdir().unwrap();
            setup(root.path(), "v2", backend).unwrap();
            attempt(root.path(), backend).unwrap();
            assert!(root.path().join("result").exists());
            verify(root.path(), backend, "source-first").unwrap();
            verify(root.path(), backend, "target-first").unwrap();
            let store = Store::open(root.path().join("source")).unwrap();
            store.put(b"unexpected", b"extra").unwrap();
            drop(store);
            let error = verify(root.path(), backend, "source-first").unwrap_err();
            assert!(error.to_string().contains("unexpected or missing keys"));
            let store = Store::open(root.path().join("source")).unwrap();
            store.delete(b"unexpected").unwrap();
            store.put(b"safe", b"changed").unwrap();
            drop(store);
            assert!(verify(root.path(), backend, "source-first").is_err());
        }
    }
}
