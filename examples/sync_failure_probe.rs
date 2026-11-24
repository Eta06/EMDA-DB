//! Disposable fixture operations for the Linux syscall EIO injection driver.
use database_engine::{
    BatchOperation, BoundedFullRebuildLimits, BoundedRebuildReadLimits, BoundedSuffixRebuildLimits,
    COMPACTED_CHECKPOINT_FILE, CheckpointStore, CheckpointView, Store, cleanup_abandoned_staging,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::io;
use std::path::Path;

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn check_values(store: &Store) -> ProbeResult<()> {
    if store.get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
        || store.get(b"old")?.as_deref() != Some(&b"original"[..])
    {
        return Err("acknowledged fixture values changed".into());
    }
    Ok(())
}

fn setup(root: &Path, format: &str) -> ProbeResult<()> {
    if !root.is_dir() || std::fs::read_dir(root)?.next().is_some() {
        return Err("fixture root must be an existing empty directory".into());
    }
    let source = root.join("source");
    std::fs::create_dir(&source)?;
    match format {
        "v2" => {
            std::fs::write(
                source.join("records.log"),
                include_bytes!("../tests/fixtures/v2-empty-records.log"),
            )?;
            std::fs::write(
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
    let expected: &[u8] = if format == "v2" {
        b"EMDALOG2"
    } else {
        b"EMDALOG3"
    };
    if &std::fs::read(source.join("records.log"))?[..8] != expected {
        return Err("unexpected fixture log format".into());
    }
    let checkpoint_digest = store.checkpoint_to(root.join("checkpoint"))?;
    std::fs::write(root.join("checkpoint.digest"), checkpoint_digest)?;
    store.backup(root.join("reference-backup"))?;
    println!("{}", json!({"status":"ready","format":format}));
    Ok(())
}

fn setup_legacy(root: &Path, operation: &str) -> ProbeResult<()> {
    let bytes: &[u8] = match operation {
        "upgrade-legacy" => include_bytes!("../tests/fixtures/db01-legacy-record.bin"),
        "upgrade-empty-legacy" => &[],
        _ => return Err("unknown legacy operation".into()),
    };
    let legacy = root.join("legacy");
    std::fs::create_dir(&legacy)?;
    std::fs::write(legacy.join("records.log"), bytes)?;
    println!("{}", json!({"status":"ready", "operation":operation}));
    Ok(())
}

fn setup_managed(root: &Path, operation: &str) -> ProbeResult<()> {
    checkpoint_store(root)?.rollover_managed(root.join("previous-selected"))?;
    println!("{}", json!({"status":"ready", "operation":operation}));
    Ok(())
}

fn setup_managed_tail(root: &Path, operation: &str) -> ProbeResult<()> {
    let store = if operation.ends_with("-existing") {
        CheckpointStore::open_managed(root.join("source"))?
    } else {
        checkpoint_store(root)?
    };
    store.put(b"tail", b"acknowledged-tail")?;
    let next_digest = store.checkpoint_to(root.join("expected-next"))?;
    std::fs::write(root.join("next.digest"), next_digest)?;
    println!("{}", json!({"status":"ready", "operation":operation}));
    Ok(())
}

fn action(root: &Path, operation: &str) -> io::Result<()> {
    let source = root.join("source");
    let target = root.join("result");
    match operation {
        "write" => Store::open(source)?.put(b"new", b"attempted"),
        "batch" => Store::open(source)?.write_batch(&batch()),
        "backup" => Store::open(source)?.backup(target),
        "restore" => Store::restore(root.join("reference-backup"), target),
        "upgrade" => Store::open(source)?.upgrade_format(target),
        "upgrade-legacy" => Store::open(root.join("legacy"))?.upgrade_legacy(target),
        "upgrade-empty-legacy" => Store::upgrade_empty_legacy(root.join("legacy"), target),
        "checkpoint" => Store::open(source)?.checkpoint_to(target).map(|_| ()),
        "checkpoint-copy" => checkpoint_store(root)?.checkpoint_to(target).map(|_| ()),
        "compaction" => Store::open(source)?.compact_to(target),
        "checkpoint-write" => checkpoint_store(root)?.put(b"new", b"attempted"),
        "checkpoint-batch" => checkpoint_store(root)?.write_batch(&batch()),
        "checkpoint-backup" => checkpoint_store(root)?.backup(target),
        "checkpoint-compaction" => checkpoint_store(root)?.compact_to(target).map(|_| ()),
        "checkpoint-rollover" => checkpoint_store(root)?.rollover_to(target).map(|_| ()),
        "checkpoint-managed-rollover" | "checkpoint-managed-rollover-tail" => {
            checkpoint_store(root)?.rollover_managed(target).map(|_| ())
        }
        "checkpoint-managed-rollover-existing" | "checkpoint-managed-rollover-tail-existing" => {
            CheckpointStore::open_managed(source)?
                .rollover_managed(target)
                .map(|_| ())
        }
        "checkpoint-managed-rebuild"
        | "checkpoint-managed-rebuild-existing"
        | "checkpoint-managed-rebuild-tail"
        | "checkpoint-managed-rebuild-tail-existing" => {
            CheckpointStore::rebuild_managed_selection(source, target).map(|_| ())
        }
        "initialize" => Store::open(target).map(|_| ()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unknown operation",
        )),
    }
}

fn batch() -> [BatchOperation<'static>; 3] {
    [
        BatchOperation::Put {
            key: b"new",
            value: b"attempted",
        },
        BatchOperation::Put {
            key: b"new-peer",
            value: b"paired",
        },
        BatchOperation::Delete { key: b"old" },
    ]
}

fn checkpoint_store(root: &Path) -> io::Result<CheckpointStore> {
    let digest: [u8; 32] = std::fs::read(root.join("checkpoint.digest"))?
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid checkpoint digest"))?;
    CheckpointStore::open(root.join("source"), root.join("checkpoint"), &digest)
}

fn attempt(root: &Path, operation: &str) -> ProbeResult<()> {
    let result = action(root, operation);
    match result {
        Ok(()) => println!("{}", json!({"status":"success","operation":operation})),
        Err(error) => println!(
            "{}",
            json!({"status":"error","operation":operation,"kind":format!("{:?}",error.kind()),"message":error.to_string()})
        ),
    }
    Ok(())
}

fn attempt_publication(root: &Path, operation: &str) -> ProbeResult<()> {
    use database_engine::PublicationFailure;
    if matches!(
        operation,
        "checkpoint-managed-rebuild"
            | "checkpoint-managed-rebuild-existing"
            | "checkpoint-managed-rebuild-tail"
            | "checkpoint-managed-rebuild-tail-existing"
    ) {
        return attempt_rebuild_publication(root, operation, "ordinary");
    }
    if matches!(
        operation,
        "checkpoint-managed-rollover"
            | "checkpoint-managed-rollover-existing"
            | "checkpoint-managed-rollover-tail"
            | "checkpoint-managed-rollover-tail-existing"
    ) {
        return attempt_managed_publication(root, operation);
    }
    let source = root.join("source");
    let target = root.join("result");
    let outcome = match operation {
        "backup" => Store::open(source)
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.backup_detailed(target)),
        "checkpoint-backup" => checkpoint_store(root)
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.backup_detailed(target)),
        "restore" => Store::restore_detailed(root.join("reference-backup"), target),
        "checkpoint" => Store::open(source)
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.checkpoint_to_detailed(target).map(|_| ())),
        "checkpoint-copy" => checkpoint_store(root)
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.checkpoint_to_detailed(target).map(|_| ())),
        "compaction" => Store::open(source)
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.compact_to_detailed(target)),
        "checkpoint-compaction" => checkpoint_store(root)
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.compact_to_detailed(target).map(|_| ())),
        "upgrade" => Store::open(source)
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.upgrade_format_detailed(target)),
        "upgrade-legacy" => Store::open(root.join("legacy"))
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.upgrade_legacy_detailed(target)),
        "upgrade-empty-legacy" => Store::upgrade_empty_legacy_detailed(root.join("legacy"), target),
        "checkpoint-rollover" => checkpoint_store(root)
            .map_err(PublicationFailure::NotPublished)
            .and_then(|store| store.rollover_to_detailed(target).map(|_| ())),
        _ => return Err("unknown publication operation".into()),
    };
    match outcome {
        Ok(()) => println!(
            "{}",
            json!({"status":"success", "operation":operation, "classification":null})
        ),
        Err(error) => {
            let (classification, error) = match error {
                PublicationFailure::NotPublished(error) => ("not_published", error),
                PublicationFailure::OutcomeUnknown(error) => ("unknown", error),
                other => ("unknown", io::Error::other(other)),
            };
            println!(
                "{}",
                json!({"status":"error", "operation":operation,
                "classification":classification, "kind":format!("{:?}",error.kind()), "message":error.to_string()})
            );
        }
    }
    Ok(())
}

fn attempt_rebuild_publication(root: &Path, operation: &str, rebuild: &str) -> ProbeResult<()> {
    use database_engine::ManagedRebuildFailure;
    let source = root.join("source");
    let target = root.join("result");
    let log_bytes = std::fs::metadata(source.join("records.log"))?.len();
    let reads = BoundedRebuildReadLimits {
        max_source_log_read_bytes: Some(3 * log_bytes + 7 * 36),
        ..Default::default()
    };
    let outcome = match rebuild {
        "ordinary" => CheckpointStore::rebuild_managed_selection_detailed(source, target),
        "full-bounded" => CheckpointStore::rebuild_managed_selection_full_bounded_with_read_limits(
            source,
            target,
            BoundedFullRebuildLimits {
                max_log_bytes: 4096,
                max_operations: 16,
                max_live_keys: 8,
                max_live_key_bytes: 128,
                max_chunk_keys: 1,
                max_chunk_key_bytes: 16,
                max_spill_bytes: 4096,
                max_merge_fan_in: 2,
            },
            Default::default(),
            None,
            reads,
        ),
        "suffix-bounded" => {
            let digest = std::fs::read(root.join("checkpoint.digest"))?
                .try_into()
                .map_err(|_| "invalid checkpoint digest")?;
            CheckpointStore::rebuild_managed_selection_bounded_with_read_limits(
                source,
                target,
                root.join("checkpoint"),
                &digest,
                BoundedSuffixRebuildLimits {
                    max_log_bytes: 4096,
                    max_operations: 16,
                    max_chunk_keys: 1,
                    max_chunk_key_bytes: 16,
                    max_spill_bytes: 4096,
                    max_merge_fan_in: 2,
                    base_checkpoint: Default::default(),
                    output_checkpoint: Default::default(),
                },
                Default::default(),
                reads,
            )
        }
        _ => return Err("unknown rebuild path".into()),
    };
    match outcome {
        Ok(_) => println!(
            "{}",
            json!({"status":"success", "operation":operation, "classification":null})
        ),
        Err(error) => {
            let (classification, error) = match error {
                ManagedRebuildFailure::NotPublished(error) => ("not_published", error),
                ManagedRebuildFailure::TailAdoptionOutcomeUnknown(error) => {
                    ("tail_adoption_unknown", error)
                }
                ManagedRebuildFailure::CheckpointOutcomeUnknown(error) => {
                    ("checkpoint_unknown", error)
                }
                ManagedRebuildFailure::SelectionOutcomeUnknown(error) => {
                    ("selection_unknown", error)
                }
                other => ("selection_unknown", io::Error::other(other)),
            };
            println!(
                "{}",
                json!({"status":"error", "operation":operation,
                "classification":classification, "kind":format!("{:?}",error.kind()), "message":error.to_string()})
            );
        }
    }
    Ok(())
}

fn attempt_recovery_open(root: &Path, mode: &str) -> ProbeResult<()> {
    use database_engine::{RecoveryOpenFailure, StoreRecoveryLimits};

    let source = root.join("source");
    let result = match mode {
        "full-log-open" => Store::open_with_recovery_limits_detailed(
            source,
            StoreRecoveryLimits {
                max_recovery_operations: Some(16),
                ..Default::default()
            },
        )
        .map(|store| store.get(b"tail")),
        "managed-checkpoint-open" => {
            CheckpointStore::open_managed_with_options_detailed(source, Default::default(), Some(1))
                .map(|store| store.get(b"tail"))
        }
        "explicit-checkpoint-open" => {
            let digest = std::fs::read(root.join("checkpoint.digest"))?
                .try_into()
                .map_err(|_| "invalid checkpoint digest")?;
            CheckpointStore::open_with_options_detailed(
                source,
                root.join("checkpoint"),
                &digest,
                Default::default(),
                Some(1),
            )
            .map(|store| store.get(b"tail"))
        }
        _ => return Err("unknown recovery open mode".into()),
    };
    let outcome = match result {
        Ok(value) => {
            if value?.as_deref() != Some(&b"acknowledged-tail"[..]) {
                return Err("recovered tail value mismatch".into());
            }
            json!({"status":"success"})
        }
        Err(failure) => {
            let (classification, error) = match failure {
                RecoveryOpenFailure::NotAdopted(error) => ("not_adopted", error),
                RecoveryOpenFailure::TailAdoptionOutcomeUnknown(error) => {
                    ("tail_adoption_unknown", error)
                }
                RecoveryOpenFailure::TailAdopted(error) => ("tail_adopted", error),
                other => return Err(other.into()),
            };
            json!({"status":"error", "classification":classification,
                "kind":format!("{:?}",error.kind()), "message":error.to_string()})
        }
    };
    println!("{outcome}");
    Ok(())
}

fn attempt_managed_publication(root: &Path, operation: &str) -> ProbeResult<()> {
    use database_engine::ManagedRolloverFailure;
    let store = if operation.ends_with("-existing") {
        CheckpointStore::open_managed(root.join("source"))
    } else {
        checkpoint_store(root)
    };
    let mut handle_after_selection_failure = None;
    let outcome = match store {
        Ok(store) => {
            let outcome = store.rollover_managed_detailed(root.join("result"));
            if matches!(
                &outcome,
                Err(ManagedRolloverFailure::SelectionOutcomeUnknown(_))
            ) {
                let read_blocked = store.get(b"safe").is_err();
                let write_blocked = store
                    .put(b"after-selection-error", b"must-not-write")
                    .is_err();
                if !read_blocked || !write_blocked {
                    return Err("selection failure did not block later handle access".into());
                }
                handle_after_selection_failure = Some("blocked");
            }
            outcome
        }
        Err(error) => Err(ManagedRolloverFailure::NotPublished(error)),
    };
    match outcome {
        Ok(_) => println!(
            "{}",
            json!({"status":"success", "operation":operation, "classification":null})
        ),
        Err(error) => {
            let (classification, error) = match error {
                ManagedRolloverFailure::NotPublished(error) => ("not_published", error),
                ManagedRolloverFailure::CheckpointOutcomeUnknown(error) => {
                    ("checkpoint_unknown", error)
                }
                ManagedRolloverFailure::SelectionOutcomeUnknown(error) => {
                    ("selection_unknown", error)
                }
                other => ("selection_unknown", io::Error::other(other)),
            };
            println!(
                "{}",
                json!({"status":"error", "operation":operation,
                "classification":classification, "kind":format!("{:?}",error.kind()), "message":error.to_string(),
                "handle_after_selection_failure":handle_after_selection_failure})
            );
        }
    }
    Ok(())
}

fn report_handle(
    operation: &str,
    outcome: io::Result<()>,
    read_safe: impl FnOnce() -> io::Result<Option<Vec<u8>>>,
    try_write: impl FnOnce() -> io::Result<()>,
) -> ProbeResult<()> {
    match outcome {
        Ok(()) => {
            if read_safe()?.as_deref() != Some(&b"acknowledged"[..]) {
                return Err("successful operation lost acknowledged data".into());
            }
            println!("{}", json!({"status":"success", "operation":operation}));
        }
        Err(error) => {
            let read_blocked = read_safe().is_err();
            let write_blocked = try_write().is_err();
            if !read_blocked || !write_blocked {
                return Err("failed operation did not block subsequent access".into());
            }
            println!(
                "{}",
                json!({"status":"error", "operation":operation,
                       "kind":format!("{:?}", error.kind()), "message":error.to_string(),
                       "read_blocked":read_blocked, "write_blocked":write_blocked})
            );
        }
    }
    Ok(())
}

fn attempt_with_handle_check(root: &Path, operation: &str) -> ProbeResult<()> {
    match operation {
        "write" | "batch" => {
            let store = Store::open(root.join("source"))?;
            let outcome = if operation == "write" {
                store.put(b"new", b"attempted")
            } else {
                store.write_batch(&batch())
            };
            report_handle(
                operation,
                outcome,
                || store.get(b"safe"),
                || store.put(b"after-error", b"must-not-write"),
            )
        }
        "checkpoint-write" | "checkpoint-batch" => {
            let store = checkpoint_store(root)?;
            let outcome = if operation == "checkpoint-write" {
                store.put(b"new", b"attempted")
            } else {
                store.write_batch(&batch())
            };
            report_handle(
                operation,
                outcome,
                || store.get(b"safe"),
                || store.put(b"after-error", b"must-not-write"),
            )
        }
        _ => Err("unknown handle-check operation".into()),
    }
}

fn inspect_source(
    get: impl Fn(&[u8]) -> io::Result<Option<Vec<u8>>>,
    operation: &str,
    status: &str,
) -> ProbeResult<bool> {
    if get(b"safe")?.as_deref() != Some(&b"acknowledged"[..]) {
        return Err("acknowledged fixture values changed".into());
    }
    if (operation.contains("-rollover-tail") || operation.contains("-rebuild-tail"))
        && get(b"tail")?.as_deref() != Some(&b"acknowledged-tail"[..])
    {
        return Err("acknowledged managed suffix changed".into());
    }
    let old_value = get(b"old")?;
    let new_value = get(b"new")?;
    let peer_value = get(b"new-peer")?;
    if matches!(operation, "batch" | "checkpoint-batch") {
        let before = old_value.as_deref() == Some(&b"original"[..])
            && new_value.is_none()
            && peer_value.is_none();
        let after = old_value.is_none()
            && new_value.as_deref() == Some(&b"attempted"[..])
            && peer_value.as_deref() == Some(&b"paired"[..]);
        if !before && !after {
            return Err("atomic batch recovered a fractured state".into());
        }
        if status == "success" && !after {
            return Err("successful batch missing after reopen".into());
        }
        return Ok(after);
    }
    if old_value.as_deref() != Some(&b"original"[..]) || peer_value.is_some() {
        return Err("acknowledged fixture values changed".into());
    }
    if matches!(operation, "write" | "checkpoint-write") {
        if status == "success" && new_value.as_deref() != Some(&b"attempted"[..]) {
            return Err("successful write missing after reopen".into());
        }
        if new_value
            .as_deref()
            .is_some_and(|value| value != b"attempted")
        {
            return Err("unexpected recovered value".into());
        }
    } else if new_value.is_some() {
        return Err("copy operation changed source values".into());
    }
    Ok(new_value.is_some())
}

fn verify(root: &Path, operation: &str, status: &str, order: &str) -> ProbeResult<()> {
    if !matches!(status, "success" | "error") {
        return Err("unknown outcome".into());
    }
    let order = match order {
        "ordinary-first" => ["ordinary", "checkpoint"],
        "checkpoint-first" => ["checkpoint", "ordinary"],
        _ => return Err("unknown recovery order".into()),
    };
    let digest: [u8; 32] = std::fs::read(root.join("checkpoint.digest"))?
        .try_into()
        .map_err(|_| "invalid checkpoint digest")?;
    let managed = matches!(
        operation,
        "checkpoint-managed-rollover"
            | "checkpoint-managed-rollover-existing"
            | "checkpoint-managed-rollover-tail"
            | "checkpoint-managed-rollover-tail-existing"
            | "checkpoint-managed-rebuild"
            | "checkpoint-managed-rebuild-existing"
            | "checkpoint-managed-rebuild-tail"
            | "checkpoint-managed-rebuild-tail-existing"
    );
    let has_tail = operation.contains("-rollover-tail") || operation.contains("-rebuild-tail");
    let next_digest: [u8; 32] = if has_tail {
        std::fs::read(root.join("next.digest"))?
            .try_into()
            .map_err(|_| "invalid next digest")?
    } else {
        digest
    };
    let selection_visible = managed.then(|| root.join("source/checkpoint.selection").exists());
    let mut new_present = None;
    for backend in order {
        let present = if backend == "ordinary" {
            let source = Store::open(root.join("source"))?;
            inspect_source(|key| source.get(key), operation, status)?
        } else {
            let source = if selection_visible == Some(true) {
                CheckpointStore::open_managed(root.join("source"))?
            } else {
                CheckpointStore::open(root.join("source"), root.join("checkpoint"), &digest)?
            };
            inspect_source(|key| source.get(key), operation, status)?
        };
        if new_present.is_some_and(|previous| previous != present) {
            return Err("recovery paths disagree about the write".into());
        }
        new_present = Some(present);
    }
    let target = root.join("result");
    let visible = target.exists();
    let legacy_expected: Option<&[u8]> = match operation {
        "upgrade-legacy" => Some(include_bytes!("../tests/fixtures/db01-legacy-record.bin")),
        "upgrade-empty-legacy" => Some(&[]),
        _ => None,
    };
    if let Some(expected) = legacy_expected {
        if std::fs::read(root.join("legacy/records.log"))? != expected {
            return Err("legacy source log changed".into());
        }
    }
    if !matches!(
        operation,
        "write" | "checkpoint-write" | "batch" | "checkpoint-batch"
    ) && status == "success"
        && !visible
    {
        return Err("successful copy was not published".into());
    }
    let mut initialization_state = None;
    if visible {
        if operation == "initialize" {
            match Store::open(&target) {
                Ok(store) => {
                    if store.get(b"safe")?.is_some()
                        || store.get(b"old")?.is_some()
                        || store.get(b"new")?.is_some()
                    {
                        return Err("initialized store is not empty".into());
                    }
                    initialization_state = Some("openable");
                }
                Err(error) if status == "error" && error.kind() == io::ErrorKind::InvalidData => {
                    initialization_state = Some("fail-closed");
                }
                Err(error) => return Err(error.into()),
            }
        } else if matches!(operation, "backup" | "checkpoint-backup") {
            let restored = root.join("verify-copy");
            Store::restore(&target, &restored)?;
            check_values(&Store::open(restored)?)?;
        } else if legacy_expected.is_some() {
            let copy = Store::open(&target)?;
            let rows = copy.scan(None, None, 16)?;
            let expected = if operation == "upgrade-legacy" {
                vec![(b"legacy".to_vec(), b"persisted".to_vec())]
            } else {
                Vec::new()
            };
            if rows != expected {
                return Err("published legacy upgrade has incorrect values".into());
            }
        } else if matches!(
            operation,
            "restore" | "upgrade" | "compaction" | "checkpoint-compaction"
        ) {
            check_values(&Store::open(&target)?)?;
            if operation == "checkpoint-compaction" {
                let checkpoint = target.join(COMPACTED_CHECKPOINT_FILE);
                let bytes = std::fs::read(&checkpoint)?;
                let length = bytes
                    .len()
                    .checked_sub(32)
                    .ok_or("short compacted checkpoint")?;
                let observed: [u8; 32] = Sha256::digest(&bytes[..length]).into();
                if bytes[length..] != observed {
                    return Err("compacted checkpoint digest mismatch".into());
                }
                let view = CheckpointView::open(&target, &checkpoint, &observed)?;
                if view.get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
                    || view.get(b"old")?.as_deref() != Some(&b"original"[..])
                {
                    return Err("compacted checkpoint has incorrect values".into());
                }
            }
        } else if matches!(
            operation,
            "checkpoint"
                | "checkpoint-copy"
                | "checkpoint-rollover"
                | "checkpoint-managed-rollover"
                | "checkpoint-managed-rollover-existing"
                | "checkpoint-managed-rollover-tail"
                | "checkpoint-managed-rollover-tail-existing"
                | "checkpoint-managed-rebuild"
                | "checkpoint-managed-rebuild-existing"
                | "checkpoint-managed-rebuild-tail"
                | "checkpoint-managed-rebuild-tail-existing"
        ) {
            // This digest was independently saved before the attempted copy.
            let view = CheckpointView::open(root.join("source"), &target, &next_digest)?;
            if view.get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
                || view.get(b"old")?.as_deref() != Some(&b"original"[..])
            {
                return Err("published checkpoint has incorrect values".into());
            }
            if has_tail && view.get(b"tail")?.as_deref() != Some(&b"acknowledged-tail"[..]) {
                return Err("published checkpoint lost the acknowledged suffix".into());
            }
        } else {
            return Err("unexpected result path".into());
        }
    }
    if managed && operation.ends_with("-existing") {
        let previous =
            CheckpointView::open(root.join("source"), root.join("previous-selected"), &digest)?;
        if previous.get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
            || previous.get(b"old")?.as_deref() != Some(&b"original"[..])
            || (has_tail && previous.get(b"tail")?.is_some())
        {
            return Err("previous managed checkpoint has incorrect values".into());
        }
    }
    if has_tail {
        let expected = CheckpointView::open(
            root.join("source"),
            root.join("expected-next"),
            &next_digest,
        )?;
        if expected.get(b"tail")?.as_deref() != Some(&b"acknowledged-tail"[..])
            || expected.get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
            || expected.get(b"old")?.as_deref() != Some(&b"original"[..])
        {
            return Err("trusted suffix checkpoint has incorrect values".into());
        }
    }
    let cleanup = cleanup_abandoned_staging(root)?;
    let selection_cleanup = if managed {
        Some(cleanup_abandoned_staging(root.join("source"))?)
    } else {
        None
    };
    println!(
        "{}",
        json!({"status":"verified","operation":operation,"target_visible":visible,
        "new_present":new_present.unwrap(),"recovery_order":order,"initialization_state":initialization_state,"cleanup_removed_markers":cleanup.removed_markers,
        "cleanup_removed_payloads":cleanup.removed_payloads,"cleanup_skipped":cleanup.skipped,
        "selection_visible":selection_visible,
        "selection_cleanup_removed_markers":selection_cleanup.as_ref().map(|value| value.removed_markers),
        "selection_cleanup_removed_payloads":selection_cleanup.as_ref().map(|value| value.removed_payloads),
        "selection_cleanup_skipped":selection_cleanup.as_ref().map(|value| value.skipped)})
    );
    Ok(())
}

// A torn log or witness must be rejected on both writable paths. The caller
// snapshots the underlying files and metadata before and after this check.
fn verify_rejected(root: &Path, order: &str) -> ProbeResult<()> {
    let order = match order {
        "ordinary-first" => ["ordinary", "checkpoint"],
        "checkpoint-first" => ["checkpoint", "ordinary"],
        _ => return Err("unknown recovery order".into()),
    };
    let digest: [u8; 32] = std::fs::read(root.join("checkpoint.digest"))?
        .try_into()
        .map_err(|_| "invalid checkpoint digest")?;
    for backend in order {
        let result = if backend == "ordinary" {
            Store::open(root.join("source")).map(|_| ())
        } else {
            CheckpointStore::open(root.join("source"), root.join("checkpoint"), &digest).map(|_| ())
        };
        match result {
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {}
            Err(error) => {
                return Err(format!("{backend} returned unexpected error: {error}").into());
            }
            Ok(()) => return Err(format!("{backend} accepted torn evidence").into()),
        }
    }
    let restored = root.join("verified-reference");
    Store::restore(root.join("reference-backup"), &restored)?;
    check_values(&Store::open(restored)?)?;
    println!(
        "{}",
        json!({"status":"rejected", "recovery_order":order, "backup_verified":true})
    );
    Ok(())
}

fn main() -> ProbeResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, root, format] if command == "setup" => setup(Path::new(root), format),
        [command, root, operation] if command == "setup-legacy" => setup_legacy(Path::new(root), operation),
        [command, root, operation] if command == "setup-managed" => setup_managed(Path::new(root), operation),
        [command, root, operation] if command == "setup-managed-tail" => setup_managed_tail(Path::new(root), operation),
        [command, root, operation] if command == "attempt" => attempt(Path::new(root), operation),
        [command, root, operation] if command == "attempt-publication" => attempt_publication(Path::new(root), operation),
        [command, root, operation, rebuild] if command == "attempt-rebuild-publication" => attempt_rebuild_publication(Path::new(root), operation, rebuild),
        [command, root, mode] if command == "attempt-recovery-open" => attempt_recovery_open(Path::new(root), mode),
        [command, root, operation] if command == "attempt-with-handle-check" => attempt_with_handle_check(Path::new(root), operation),
        [command, root, operation, status, order] if command == "verify" => verify(Path::new(root), operation, status, order),
        [command, root, order] if command == "verify-rejected" => verify_rejected(Path::new(root), order),
        _ => Err("usage: sync_failure_probe setup <empty-root> <v2|v3> | attempt <root> <operation> | verify <root> <operation> <success|error> <ordinary-first|checkpoint-first>".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_check_rejects_access_after_uncertain_failure() {
        let failure = || io::Error::other("injected uncertain result");
        assert!(
            report_handle(
                "write",
                Err(failure()),
                || Ok(Some(b"acknowledged".to_vec())),
                || Err(failure()),
            )
            .is_err()
        );
        assert!(report_handle("write", Err(failure()), || Err(failure()), || Ok(()),).is_err());
        assert!(
            report_handle(
                "write",
                Err(failure()),
                || Err(failure()),
                || Err(failure()),
            )
            .is_ok()
        );
    }

    #[test]
    fn verifier_rejects_missing_data_and_partial_publication() {
        let root = tempfile::tempdir().unwrap();
        setup(root.path(), "v2").unwrap();
        assert!(verify(root.path(), "restore", "success", "ordinary-first").is_err());
        std::fs::write(root.path().join("result"), b"partial backup").unwrap();
        assert!(verify(root.path(), "backup", "error", "checkpoint-first").is_err());
        assert!(verify(root.path(), "checkpoint", "error", "ordinary-first").is_err());
        std::fs::remove_file(root.path().join("result")).unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        store.put(b"safe", b"changed").unwrap();
        drop(store);
        assert!(verify(root.path(), "write", "error", "ordinary-first").is_err());
    }

    #[test]
    fn legacy_upgrade_verifier_rejects_changed_source_and_unexpected_rows() {
        for operation in ["upgrade-legacy", "upgrade-empty-legacy"] {
            let root = tempfile::tempdir().unwrap();
            setup(root.path(), "v2").unwrap();
            setup_legacy(root.path(), operation).unwrap();
            action(root.path(), operation).unwrap();
            verify(root.path(), operation, "success", "ordinary-first").unwrap();
            let copy = Store::open(root.path().join("result")).unwrap();
            copy.put(b"unexpected", b"value").unwrap();
            drop(copy);
            let error = verify(root.path(), operation, "success", "checkpoint-first").unwrap_err();
            assert!(error.to_string().contains("incorrect values"));
            std::fs::write(root.path().join("legacy/records.log"), b"changed").unwrap();
            let error = verify(root.path(), operation, "success", "ordinary-first").unwrap_err();
            assert!(error.to_string().contains("legacy source log changed"));
        }
    }

    #[test]
    fn managed_rollover_verifier_rejects_a_corrupt_visible_selection() {
        for operation in [
            "checkpoint-managed-rollover",
            "checkpoint-managed-rollover-existing",
            "checkpoint-managed-rollover-tail",
            "checkpoint-managed-rollover-tail-existing",
            "checkpoint-managed-rebuild",
            "checkpoint-managed-rebuild-existing",
            "checkpoint-managed-rebuild-tail",
            "checkpoint-managed-rebuild-tail-existing",
        ] {
            let root = tempfile::tempdir().unwrap();
            setup(root.path(), "v2").unwrap();
            if operation.ends_with("-existing") {
                setup_managed(root.path(), operation).unwrap();
            }
            if operation.contains("-rollover-tail") || operation.contains("-rebuild-tail") {
                setup_managed_tail(root.path(), operation).unwrap();
            }
            action(root.path(), operation).unwrap();
            verify(root.path(), operation, "success", "ordinary-first").unwrap();
            std::fs::write(root.path().join("source/checkpoint.selection"), b"corrupt").unwrap();
            assert!(verify(root.path(), operation, "error", "checkpoint-first").is_err());
        }
    }

    #[test]
    fn interrupted_initialization_fails_closed_without_rewriting_a_partial_log() {
        let root = tempfile::tempdir().unwrap();
        setup(root.path(), "v2").unwrap();
        let target = root.path().join("result");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("records.log"), b"partial").unwrap();
        verify(root.path(), "initialize", "error", "ordinary-first").unwrap();
        assert_eq!(
            std::fs::read(target.join("records.log")).unwrap(),
            b"partial"
        );
    }

    #[test]
    fn batch_verifier_rejects_partial_puts_or_delete() {
        let root = tempfile::tempdir().unwrap();
        setup(root.path(), "v2").unwrap();
        verify(root.path(), "batch", "error", "ordinary-first").unwrap();
        let source = Store::open(root.path().join("source")).unwrap();
        source.put(b"new", b"attempted").unwrap();
        drop(source);
        assert!(verify(root.path(), "batch", "error", "ordinary-first").is_err());
        let source = Store::open(root.path().join("source")).unwrap();
        source.put(b"new-peer", b"paired").unwrap();
        drop(source);
        assert!(verify(root.path(), "checkpoint-batch", "error", "checkpoint-first").is_err());
        let source = Store::open(root.path().join("source")).unwrap();
        source.delete(b"old").unwrap();
        drop(source);
        verify(root.path(), "batch", "success", "ordinary-first").unwrap();
        verify(
            root.path(),
            "checkpoint-batch",
            "success",
            "checkpoint-first",
        )
        .unwrap();
    }

    #[test]
    fn rejected_verifier_requires_corrupt_source_and_checks_both_paths() {
        use std::io::Write;

        for format in ["v2", "v3"] {
            if format == "v3" && !cfg!(target_os = "linux") {
                continue;
            }
            let root = tempfile::tempdir().unwrap();
            setup(root.path(), format).unwrap();
            assert!(verify_rejected(root.path(), "ordinary-first").is_err());
            let log = root.path().join("source/records.log");
            std::fs::OpenOptions::new()
                .append(true)
                .open(&log)
                .unwrap()
                .write_all(b"x")
                .unwrap();
            let evidence = std::fs::read(&log).unwrap();
            verify_rejected(root.path(), "checkpoint-first").unwrap();
            assert_eq!(std::fs::read(log).unwrap(), evidence);
        }
    }
}
