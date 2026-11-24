use database_engine::{
    BatchOperation, CheckpointRecoveryLimits, CheckpointStore, CheckpointStoreLimits,
    CheckpointView, CheckpointViewLimits, MAX_RECOVERED_VIEW_TAIL_BYTES,
    MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS, RecoveredCheckpointView,
    RecoveredViewLimits, RecoveryOpenFailure, Store,
};
use std::fs;
use std::io;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
    u64,
) {
    fixture_with_format(false)
}

fn fixture_with_format(
    force_v2: bool,
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
    u64,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    if force_v2 {
        fs::create_dir(&path).unwrap();
        fs::write(
            path.join("records.log"),
            include_bytes!("fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            path.join("commit.state"),
            include_bytes!("fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let length = fs::metadata(path.join("records.log")).unwrap().len();
    drop(store);
    (dir, path, checkpoint, digest, length)
}

fn assert_operation_limit<T>(result: io::Result<T>) {
    let error = result.err().expect("suffix operation limit must reject");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "checkpoint suffix operation limit exceeded"
    );
}

#[test]
fn recovery_scan_limit_counts_the_confirming_suffix_pass_only_for_pending_tails() {
    exercise_recovery_scan_limits(false);
}

#[test]
fn combined_checkpoint_recovery_scan_limit_counts_checkpoint_and_pending_confirmation() {
    exercise_recovery_scan_limits(true);
}

#[test]
fn readonly_combined_scan_ceiling_charges_only_the_viewed_log_range() {
    for force_v2 in [true, false] {
        let (_dir, path, checkpoint, digest, prefix) = fixture_with_format(force_v2);
        let store = Store::open(&path).unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"a",
                    value: b"new",
                },
                BatchOperation::Put {
                    key: b"b",
                    value: b"tail",
                },
            ])
            .unwrap();
        drop(store);

        let log_path = path.join("records.log");
        let log = fs::read(&log_path).unwrap();
        let witness_path = path.join("commit.state");
        let witness = fs::read(&witness_path).unwrap();
        let index = fs::read(&checkpoint).unwrap();
        let end = log.len() as u64;
        let historical_bytes = index.len() as u64 + prefix;
        let recovered_bytes = index.len() as u64 + end;
        assert!(historical_bytes < recovered_bytes);

        let historical = |max| {
            CheckpointView::open_with_total_recovery_scan_limit(
                &path,
                &checkpoint,
                &digest,
                CheckpointViewLimits::default(),
                Some(end),
                max,
            )
        };
        let recovered = |max, operations| {
            RecoveredCheckpointView::open_with_total_recovery_scan_limit(
                &path,
                &checkpoint,
                &digest,
                RecoveredViewLimits::default(),
                operations,
                max,
            )
        };
        // A bad trailer distinguishes admission from index authentication.
        let mut damaged = index.clone();
        *damaged.last_mut().unwrap() ^= 1;
        fs::write(&checkpoint, &damaged).unwrap();
        for error in [
            historical(historical_bytes - 1).err().unwrap(),
            recovered(recovered_bytes - 1, Some(2)).err().unwrap(),
        ] {
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                error.to_string(),
                "checkpoint total recovery scan byte limit exceeded"
            );
        }
        for error in [
            historical(historical_bytes).err().unwrap(),
            recovered(recovered_bytes, Some(2)).err().unwrap(),
        ] {
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(error.to_string(), "index checkpoint checksum mismatch");
        }
        assert_eq!(fs::read(&checkpoint).unwrap(), damaged);
        fs::write(&checkpoint, index).unwrap();

        let view = historical(historical_bytes).unwrap();
        assert_eq!(view.get(b"a").unwrap().as_deref(), Some(b"old".as_slice()));
        assert_eq!(view.get(b"b").unwrap(), None);
        let view = recovered(recovered_bytes, Some(2)).unwrap();
        assert_eq!(view.get(b"a").unwrap().as_deref(), Some(b"new".as_slice()));
        assert_eq!(view.get(b"b").unwrap().as_deref(), Some(b"tail".as_slice()));
        assert_operation_limit(recovered(recovered_bytes, Some(1)));
        drop(view);

        let mut damaged_tail = log.clone();
        *damaged_tail.last_mut().unwrap() ^= 1;
        fs::write(&log_path, &damaged_tail).unwrap();
        assert!(historical(historical_bytes).is_ok());
        assert_eq!(
            recovered(recovered_bytes, Some(2)).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(fs::read(&log_path).unwrap(), damaged_tail);
        fs::write(&log_path, &log).unwrap();

        let mut damaged_prefix = log.clone();
        damaged_prefix[prefix as usize - 1] ^= 1;
        fs::write(&log_path, &damaged_prefix).unwrap();
        for error in [
            historical(historical_bytes).err().unwrap(),
            recovered(recovered_bytes, Some(2)).err().unwrap(),
        ] {
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(error.to_string(), "checkpoint does not match log prefix");
        }
        assert_eq!(fs::read(&log_path).unwrap(), damaged_prefix);
        fs::write(&log_path, &log).unwrap();
        assert_eq!(fs::read(log_path).unwrap(), log);
        assert_eq!(fs::read(witness_path).unwrap(), witness);
    }
}

fn exercise_recovery_scan_limits(combined: bool) {
    for force_v2 in [true, false] {
        if !force_v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for managed in [false, true] {
            let (dir, path, checkpoint, digest, prefix) = fixture_with_format(force_v2);
            let selection = path.join("checkpoint.selection");
            let index_path = if managed {
                let index_path = dir.path().join("managed");
                CheckpointStore::rebuild_managed_selection(&path, &index_path).unwrap();
                index_path
            } else {
                checkpoint.clone()
            };
            let selected = fs::read(&selection).ok();
            assert_eq!(selected.is_some(), managed);
            let log_path = path.join("records.log");
            let anchor_path = path.join("commit.state");
            let file = fs::File::open(&log_path).unwrap();
            let metadata = fs::read(&log_path).unwrap().starts_with(b"EMDALOG3");
            let read_witness = || {
                if metadata {
                    let mut bytes = [0_u8; 36];
                    assert_eq!(
                        rustix::fs::fgetxattr(&file, "user.db.commit.v3", &mut bytes).unwrap(),
                        bytes.len()
                    );
                    bytes.to_vec()
                } else {
                    fs::read(&anchor_path).unwrap()
                }
            };
            let store = Store::open(&path).unwrap();
            store.put(b"early", b"acknowledged suffix").unwrap();
            let previous = read_witness();
            store
                .write_batch(&[
                    BatchOperation::Delete { key: b"a" },
                    BatchOperation::Put {
                        key: b"tail",
                        value: b"pending",
                    },
                ])
                .unwrap();
            drop(store);
            let committed = read_witness();
            if metadata {
                rustix::fs::fsetxattr(
                    &file,
                    "user.db.commit.v3",
                    &previous,
                    rustix::fs::XattrFlags::REPLACE,
                )
                .unwrap();
            } else {
                fs::write(&anchor_path, &previous).unwrap();
            }
            let anchor = fs::read(&anchor_path).unwrap();
            let complete = fs::read(&log_path).unwrap();
            let length = complete.len() as u64;
            let pending_limit = length + (length - prefix);
            let checkpoint_bytes = fs::metadata(&index_path).unwrap().len();
            let open_with_budgets = |max_log_scan_bytes, max_total_scan_bytes| {
                let limits = CheckpointStoreLimits::default();
                let recovery = CheckpointRecoveryLimits {
                    max_tail_operations: Some(3),
                    max_log_scan_bytes,
                };
                match (managed, max_total_scan_bytes) {
                    (true, Some(total)) => {
                        CheckpointStore::open_managed_with_total_recovery_scan_limit(
                            &path, limits, recovery, total,
                        )
                    }
                    (false, Some(total)) => CheckpointStore::open_with_total_recovery_scan_limit(
                        &path,
                        &checkpoint,
                        &digest,
                        limits,
                        recovery,
                        total,
                    ),
                    (true, None) => {
                        CheckpointStore::open_managed_with_recovery_limits(&path, limits, recovery)
                    }
                    (false, None) => CheckpointStore::open_with_recovery_limits(
                        &path,
                        &checkpoint,
                        &digest,
                        limits,
                        recovery,
                    ),
                }
            };
            let open = |max| {
                if combined {
                    open_with_budgets(None, Some(checkpoint_bytes + max))
                } else {
                    open_with_budgets(Some(max), None)
                }
            };
            let expected_error = if combined {
                "checkpoint total recovery scan byte limit exceeded"
            } else {
                "checkpoint recovery log scan byte limit exceeded"
            };
            let assert_budget = |max| {
                assert!(
                    matches!(open(max), Err(RecoveryOpenFailure::NotAdopted(error))
                    if error.kind() == io::ErrorKind::InvalidInput
                        && error.to_string() == expected_error)
                );
            };
            if combined {
                assert!(matches!(
                    open_with_budgets(Some(pending_limit - 1), Some(checkpoint_bytes + pending_limit)),
                    Err(RecoveryOpenFailure::NotAdopted(error))
                        if error.kind() == io::ErrorKind::InvalidInput
                            && error.to_string() == "checkpoint recovery log scan byte limit exceeded"
                ));
                assert!(matches!(
                    open_with_budgets(None, Some(0)),
                    Err(RecoveryOpenFailure::NotAdopted(error))
                        if error.kind() == io::ErrorKind::InvalidInput
                            && error.to_string() == expected_error
                ));
            }
            // The scan budget rejects from the header, before checking the
            // digest trailer. Admitted recovery must authenticate the index.
            let index = fs::read(&index_path).unwrap();
            let mut damaged_index = index.clone();
            *damaged_index.last_mut().unwrap() ^= 1;
            fs::write(&index_path, &damaged_index).unwrap();
            assert_budget(pending_limit - 1);
            assert!(matches!(
                open(pending_limit), Err(RecoveryOpenFailure::NotAdopted(error))
                    if error.kind() == io::ErrorKind::InvalidData
                        && error.to_string() == "index checkpoint checksum mismatch"
            ));
            assert_eq!(read_witness(), previous);
            assert_eq!(fs::read(&selection).ok(), selected);
            assert_eq!(fs::read(&index_path).unwrap(), damaged_index);
            fs::write(&index_path, &index).unwrap();

            let mut damaged = complete.clone();
            *damaged.last_mut().unwrap() ^= 1;
            fs::write(&log_path, &damaged).unwrap();
            assert_budget(pending_limit - 1);
            assert!(
                matches!(open(pending_limit), Err(RecoveryOpenFailure::NotAdopted(error))
                if error.kind() == io::ErrorKind::InvalidData)
            );
            assert_eq!(fs::read(&log_path).unwrap(), damaged);
            assert_eq!(fs::read(&anchor_path).unwrap(), anchor);
            assert_eq!(read_witness(), previous);
            assert_eq!(fs::read(&selection).ok(), selected);

            fs::write(&log_path, &complete).unwrap();
            assert_budget(pending_limit - 1);
            let store = open(pending_limit).unwrap();
            assert_eq!(store.get(b"a").unwrap(), None);
            assert_eq!(
                store.get(b"tail").unwrap().as_deref(),
                Some(b"pending".as_slice())
            );
            assert_eq!(store.suffix_operation_limit(), Some(3));
            assert_eq!(read_witness(), committed);
            drop(store);

            assert_budget(length - 1);
            let reopened = open(length).unwrap();
            assert_eq!(
                reopened.get(b"early").unwrap().as_deref(),
                Some(b"acknowledged suffix".as_slice())
            );
            assert_eq!(fs::read(&log_path).unwrap(), complete);
            assert_eq!(fs::read(&selection).ok(), selected);
            if metadata {
                assert_eq!(fs::read(&anchor_path).unwrap(), anchor);
            }
        }
    }
}

#[test]
fn suffix_operation_limit_counts_batch_children_overwrites_and_tombstones() {
    let (dir, path, checkpoint, digest, prefix) = fixture();
    let log = path.join("records.log");
    let limits = CheckpointStoreLimits {
        max_tail_keys: 1,
        ..Default::default()
    };
    let open = |limit| {
        CheckpointStore::open_with_suffix_operation_limit(
            &path,
            &checkpoint,
            &digest,
            limits,
            limit,
        )
    };
    let empty = open(0).unwrap();
    let initial = fs::read(&log).unwrap();
    assert_operation_limit(empty.put(b"a", b"zero"));
    assert_eq!(fs::read(&log).unwrap(), initial);
    drop(empty);
    let store = open(3).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"first",
            },
            BatchOperation::Delete { key: b"a" },
        ])
        .unwrap();
    let before = fs::read(&log).unwrap();
    assert_operation_limit(store.transaction(|tx| {
        tx.put(b"a", b"new")?;
        tx.put(b"b", b"new")
    }));
    assert_eq!(fs::read(&log).unwrap(), before);
    store.put(b"a", b"last").unwrap();
    assert_operation_limit(store.delete(b"a"));
    drop(store);

    let acknowledged = fs::read(&log).unwrap();
    let mut damaged = acknowledged.clone();
    damaged[prefix as usize - 5] ^= 1;
    fs::write(&log, &damaged).unwrap();
    assert_operation_limit(open(2));
    assert_operation_limit(RecoveredCheckpointView::open_with_suffix_operation_limit(
        &path,
        &checkpoint,
        &digest,
        RecoveredViewLimits::default(),
        2,
    ));
    for error in [
        CheckpointStore::open_with_options(
            &path,
            &checkpoint,
            &digest,
            CheckpointStoreLimits {
                max_tail_keys: 0,
                ..Default::default()
            },
        )
        .err()
        .unwrap(),
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                max_tail_key_bytes: 0,
                ..Default::default()
            },
        )
        .err()
        .unwrap(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("recovered view suffix key"));
    }
    let admitted_error = open(3).err().unwrap();
    assert_eq!(admitted_error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        admitted_error.to_string(),
        "checkpoint does not match log prefix"
    );
    assert_eq!(fs::read(&log).unwrap(), damaged);
    fs::write(&log, &acknowledged).unwrap();

    let view = RecoveredCheckpointView::open_with_suffix_operation_limit(
        &path,
        &checkpoint,
        &digest,
        RecoveredViewLimits::default(),
        3,
    )
    .unwrap();
    assert_eq!(view.get(b"a").unwrap().as_deref(), Some(b"last".as_slice()));
    drop(view);
    let store = open(3).unwrap();
    assert_operation_limit(store.put(b"a", b"full"));
    store.rollover_to(dir.path().join("next")).unwrap();
    assert_eq!(store.suffix_operation_limit(), Some(3));
    store.put(b"a", b"after rollover").unwrap();
}

#[test]
fn suffix_operation_rejection_leaves_complete_tail_unadopted() {
    let (_dir, path, checkpoint, digest, prefix) = fixture();
    let log_path = path.join("records.log");
    let anchor_path = path.join("commit.state");
    let file = fs::File::open(&log_path).unwrap();
    let metadata = fs::read(&log_path).unwrap().starts_with(b"EMDALOG3");
    let read_witness = || {
        if metadata {
            let mut bytes = [0_u8; 36];
            assert_eq!(
                rustix::fs::fgetxattr(&file, "user.db.commit.v3", &mut bytes).unwrap(),
                bytes.len()
            );
            bytes.to_vec()
        } else {
            fs::read(&anchor_path).unwrap()
        }
    };
    let previous = read_witness();
    let anchor = fs::read(&anchor_path).unwrap();
    let store = Store::open(&path).unwrap();
    store
        .write_batch(&[
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put {
                key: b"a",
                value: b"recovered",
            },
        ])
        .unwrap();
    drop(store);
    let complete = read_witness();
    if metadata {
        rustix::fs::fsetxattr(
            &file,
            "user.db.commit.v3",
            &previous,
            rustix::fs::XattrFlags::REPLACE,
        )
        .unwrap();
    } else {
        fs::write(&anchor_path, &previous).unwrap();
    }
    let before = fs::read(&log_path).unwrap();
    let open = |limit| {
        CheckpointStore::open_with_options_detailed(
            &path,
            &checkpoint,
            &digest,
            CheckpointStoreLimits::default(),
            Some(limit),
        )
    };
    let mut damaged = before.clone();
    damaged[prefix as usize - 5] ^= 1;
    fs::write(&log_path, &damaged).unwrap();
    assert!(
        matches!(open(1), Err(RecoveryOpenFailure::NotAdopted(error))
        if error.kind() == io::ErrorKind::InvalidInput
            && error.to_string() == "checkpoint suffix operation limit exceeded")
    );
    assert!(
        matches!(open(2), Err(RecoveryOpenFailure::NotAdopted(error))
        if error.kind() == io::ErrorKind::InvalidData
            && error.to_string() == "checkpoint does not match log prefix")
    );
    assert_eq!(fs::read(&log_path).unwrap(), damaged);
    assert_eq!(read_witness(), previous);
    if metadata {
        assert_eq!(fs::read(&anchor_path).unwrap(), anchor);
    }
    fs::write(&log_path, &before).unwrap();
    let store = open(2).unwrap();
    assert_eq!(read_witness(), complete);
    assert_eq!(
        store.get(b"a").unwrap().as_deref(),
        Some(b"recovered".as_slice())
    );
    assert_operation_limit(store.put(b"a", b"full"));
}

#[test]
fn busy_staging_cleanup_leaves_pending_tail_unadopted() {
    for force_v2 in [true, false] {
        let (dir, path, checkpoint, digest, _) = fixture_with_format(force_v2);
        let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
        store.rollover_managed(dir.path().join("managed")).unwrap();
        store.enable_automatic_checkpoints().unwrap();
        let log_path = path.join("records.log");
        let log_file = fs::File::open(&log_path).unwrap();
        let anchor_path = path.join("commit.state");
        let anchor = fs::read(&anchor_path).unwrap();
        let metadata = fs::read(&log_path).unwrap().starts_with(b"EMDALOG3");
        let read_witness = || {
            if metadata {
                let mut bytes = [0_u8; 36];
                let size =
                    rustix::fs::fgetxattr(&log_file, "user.db.commit.v3", &mut bytes).unwrap();
                assert_eq!(size, bytes.len());
                bytes.to_vec()
            } else {
                fs::read(&anchor_path).unwrap()
            }
        };
        let prior = read_witness();
        store.put(b"pending", b"complete").unwrap();
        let acknowledged = read_witness();
        assert_ne!(prior, acknowledged);
        drop(store);
        if metadata {
            rustix::fs::fsetxattr(
                &log_file,
                "user.db.commit.v3",
                &prior,
                rustix::fs::XattrFlags::REPLACE,
            )
            .unwrap();
        } else {
            fs::write(&anchor_path, &prior).unwrap();
        }
        let log = fs::read(&log_path).unwrap();
        let selection_path = path.join("checkpoint.selection");
        let selection = fs::read(&selection_path).unwrap();
        for parent in [&path, &path.join(".emda-auto-checkpoints-v1")] {
            let guard = fs::File::open(parent).unwrap();
            fs2::FileExt::try_lock_shared(&guard).unwrap();
            let error = CheckpointStore::open_managed(&path).err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(read_witness(), prior, "cleanup parent: {parent:?}");
            assert_eq!(fs::read(&log_path).unwrap(), log);
            assert_eq!(fs::read(&selection_path).unwrap(), selection);
            if metadata {
                assert_eq!(fs::read(&anchor_path).unwrap(), anchor);
            }
            fs2::FileExt::unlock(&guard).unwrap();
        }

        let reopened = CheckpointStore::open_managed(&path).unwrap();
        assert!(reopened.automatic_checkpoints_enabled().unwrap());
        assert_eq!(read_witness(), acknowledged);
        assert_eq!(fs::read(&log_path).unwrap(), log);
        assert_eq!(fs::read(&selection_path).unwrap(), selection);
        assert_eq!(
            reopened.get(b"pending").unwrap().as_deref(),
            Some(b"complete".as_slice())
        );
        reopened.put(b"after", b"works").unwrap();
    }
}

#[test]
fn suffix_operation_limit_survives_automatic_rollover_and_managed_reopen() {
    let (dir, path, checkpoint, digest, _) = fixture();
    let limits = CheckpointStoreLimits::default();
    let store =
        CheckpointStore::open_with_suffix_operation_limit(&path, &checkpoint, &digest, limits, 1)
            .unwrap();
    store.rollover_managed(dir.path().join("managed")).unwrap();
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"once").unwrap();
    let log = path.join("records.log");
    let selection = path.join("checkpoint.selection");
    let before = fs::read(&log).unwrap();
    let initial_selection = fs::read(&selection).unwrap();
    assert_operation_limit(store.write_batch(&[
        BatchOperation::Delete { key: b"a" },
        BatchOperation::Put {
            key: b"a",
            value: b"too many",
        },
    ]));
    assert_eq!(fs::read(&log).unwrap(), before);
    assert_eq!(fs::read(&selection).unwrap(), initial_selection);
    let snapshot = store.snapshot().unwrap();
    store.put(b"a", b"after").unwrap();
    assert_ne!(fs::read(&selection).unwrap(), initial_selection);
    assert_eq!(
        snapshot.get(b"a").unwrap().as_deref(),
        Some(b"once".as_slice())
    );
    drop(snapshot);
    drop(store);
    let store =
        CheckpointStore::open_managed_with_options_detailed(&path, limits, Some(1)).unwrap();
    assert_eq!(store.suffix_operation_limit(), Some(1));
    assert!(store.automatic_checkpoints_enabled().unwrap());
    assert_eq!(
        store.get(b"a").unwrap().as_deref(),
        Some(b"after".as_slice())
    );
    store.put(b"a", b"another rollover").unwrap();
}

#[test]
fn duplicate_batch_keys_fit_exact_limits_after_automatic_rollover() {
    let (dir, path, checkpoint, digest, _) = fixture();
    let limits = CheckpointStoreLimits {
        max_tail_keys: 2,
        max_tail_key_bytes: 4,
        ..Default::default()
    };
    let store =
        CheckpointStore::open_with_suffix_operation_limit(&path, &checkpoint, &digest, limits, 4)
            .unwrap();
    store.rollover_managed(dir.path().join("managed")).unwrap();
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"old", b"kept").unwrap();
    let log = path.join("records.log");
    let selection = path.join("checkpoint.selection");
    let before = fs::read(&log).unwrap();
    let initial_selection = fs::read(&selection).unwrap();
    let error = store
        .write_batch(&[
            BatchOperation::Delete { key: b"aaa" },
            BatchOperation::Delete { key: b"bb" },
        ])
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "checkpoint suffix key limit would be exceeded"
    );
    assert_eq!(fs::read(&log).unwrap(), before);
    assert_eq!(fs::read(&selection).unwrap(), initial_selection);

    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"aa",
                value: b"first",
            },
            BatchOperation::Delete { key: b"aa" },
            BatchOperation::Put {
                key: b"bb",
                value: b"second",
            },
            BatchOperation::Put {
                key: b"aa",
                value: b"last",
            },
        ])
        .unwrap();
    assert_ne!(fs::read(&selection).unwrap(), initial_selection);
    drop(store);

    let store =
        CheckpointStore::open_managed_with_suffix_operation_limit(&path, limits, 4).unwrap();
    assert_eq!(
        store.get(b"aa").unwrap().as_deref(),
        Some(b"last".as_slice())
    );
    assert_eq!(
        store.get(b"bb").unwrap().as_deref(),
        Some(b"second".as_slice())
    );
    assert_eq!(
        store.get(b"old").unwrap().as_deref(),
        Some(b"kept".as_slice())
    );
}

#[test]
fn concurrent_batches_cannot_oversubscribe_suffix_operations() {
    let (_dir, path, checkpoint, digest, _) = fixture();
    let open = || {
        CheckpointStore::open_with_suffix_operation_limit(
            &path,
            &checkpoint,
            &digest,
            CheckpointStoreLimits::default(),
            5,
        )
    };
    let store = open().unwrap();
    let start = Barrier::new(4);
    let accepted = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                start.wait();
                let result = store.write_batch(&[
                    BatchOperation::Delete { key: b"a" },
                    BatchOperation::Put {
                        key: b"a",
                        value: b"new",
                    },
                ]);
                if result.is_ok() {
                    accepted.fetch_add(1, Ordering::Relaxed);
                } else {
                    assert_operation_limit(result);
                }
            });
        }
    });
    assert_eq!(accepted.load(Ordering::Relaxed), 2);
    drop(store);
    let store = open().unwrap();
    store.put(b"a", b"last").unwrap();
    assert_operation_limit(store.delete(b"a"));
}

#[cfg(target_os = "linux")]
#[test]
fn suffix_operation_limit_survives_directory_handoff() {
    let (dir, path, checkpoint, digest, _) = fixture();
    let store = CheckpointStore::open_with_suffix_operation_limit(
        &path,
        &checkpoint,
        &digest,
        CheckpointStoreLimits::default(),
        1,
    )
    .unwrap();
    store.put(b"a", b"new").unwrap();
    let (next, _) = store
        .compact_and_replace(dir.path().join("previous"))
        .unwrap();
    assert_eq!(next.suffix_operation_limit(), Some(1));
    assert_eq!(next.get(b"a").unwrap().as_deref(), Some(b"new".as_slice()));
    next.put(b"a", b"after").unwrap();
    assert_operation_limit(next.delete(b"a"));
}

fn open(
    path: &std::path::Path,
    checkpoint: &std::path::Path,
    digest: &[u8; 32],
    limit: u64,
) -> io::Result<CheckpointStore> {
    CheckpointStore::open_with_resource_limits(
        path,
        checkpoint,
        digest,
        MAX_RECOVERED_VIEW_TAIL_BYTES,
        MAX_RECOVERED_VIEW_TAIL_KEYS,
        MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        Some(limit),
    )
}

#[test]
fn oversized_suffix_is_rejected_before_verifying_checkpoint_entries_or_log_prefix() {
    let (_dir, path, checkpoint, digest, prefix) = fixture();
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"new").unwrap();
    drop(store);
    let log = path.join("records.log");
    let original = fs::read(&log).unwrap();
    let read_witness = || {
        if original.starts_with(b"EMDALOG3") {
            let file = fs::File::open(&log).unwrap();
            let mut bytes = [0_u8; 64];
            let size = rustix::fs::fgetxattr(&file, "user.db.commit.v3", &mut bytes).unwrap();
            bytes[..size].to_vec()
        } else {
            fs::read(path.join("commit.state")).unwrap()
        }
    };
    let witness = read_witness();
    let tail_bytes = original.len() as u64 - prefix;
    let mut damaged = original.clone();
    // Damage the obsolete covered value while leaving the log header, witness,
    // suffix and independently trusted checkpoint intact.
    damaged[prefix as usize - 5] ^= 1;
    fs::write(&log, &damaged).unwrap();

    let read_only = |limit| {
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                max_tail_bytes: limit,
                ..Default::default()
            },
        )
    };
    let writable = |limit| {
        CheckpointStore::open_with_limits(
            &path,
            &checkpoint,
            &digest,
            limit,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        )
    };
    for error in [
        read_only(tail_bytes - 1).err().unwrap(),
        writable(tail_bytes - 1).err().unwrap(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("suffix byte limit exceeded"));
    }
    for error in [
        read_only(tail_bytes).err().unwrap(),
        writable(tail_bytes).err().unwrap(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("checkpoint does not match log prefix")
        );
    }
    assert_eq!(fs::read(&log).unwrap(), damaged);
    assert_eq!(read_witness(), witness);
    fs::write(&log, &original).unwrap();

    let original_checkpoint = fs::read(&checkpoint).unwrap();
    let mut damaged_checkpoint = original_checkpoint.clone();
    // Only the final checksum changes. The declared prefix in the header
    // can reject the over-limit open before inspecting any entries.
    *damaged_checkpoint.last_mut().unwrap() ^= 1;
    fs::write(&checkpoint, &damaged_checkpoint).unwrap();
    for error in [
        read_only(tail_bytes - 1).err().unwrap(),
        writable(tail_bytes - 1).err().unwrap(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("suffix byte limit exceeded"));
    }
    for error in [
        read_only(tail_bytes).err().unwrap(),
        writable(tail_bytes).err().unwrap(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("index checkpoint checksum mismatch")
        );
    }
    assert_eq!(fs::read(&checkpoint).unwrap(), damaged_checkpoint);
    assert_eq!(fs::read(&log).unwrap(), original);
    assert_eq!(read_witness(), witness);
    fs::write(&checkpoint, original_checkpoint).unwrap();
    assert_eq!(
        read_only(tail_bytes).unwrap().get(b"a").unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        writable(tail_bytes).unwrap().get(b"a").unwrap(),
        Some(b"new".to_vec())
    );
}

#[test]
fn whole_log_limit_survives_rollover_and_rejects_before_append() {
    let (dir, path, checkpoint, digest, initial) = fixture();
    let record_len = 21 + 1 + 3;
    let limit = initial + 2 * record_len;
    let store = open(&path, &checkpoint, &digest, limit).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"b", b"two").unwrap();
    let log = path.join("records.log");
    let before = fs::read(&log).unwrap();
    assert_eq!(before.len() as u64, limit);
    assert!(store.delete(b"a").is_err());
    assert_eq!(fs::read(&log).unwrap(), before);
    let next = dir.path().join("next");
    let next_digest = store.rollover_to(&next).unwrap();
    assert!(store.put(b"a", b"new").is_err());
    assert_eq!(fs::read(&log).unwrap(), before);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    drop(store);
    assert_eq!(
        open(&path, &next, &next_digest, limit - 1)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let reopened = open(&path, &next, &next_digest, limit).unwrap();
    assert_eq!(reopened.get(b"b").unwrap().as_deref(), Some(&b"two"[..]));
}

#[test]
fn concurrent_writers_cannot_oversubscribe_the_log_limit() {
    let (_dir, path, checkpoint, digest, initial) = fixture();
    let limit = initial + 8 * (21 + 1 + 3);
    let store = Arc::new(open(&path, &checkpoint, &digest, limit).unwrap());
    let start = Arc::new(Barrier::new(17));
    let accepted = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            let accepted = &accepted;
            scope.spawn(move || {
                start.wait();
                if store.put(b"a", b"new").is_ok() {
                    accepted.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        start.wait();
    });
    assert_eq!(accepted.load(Ordering::Relaxed), 8);
    assert_eq!(fs::metadata(path.join("records.log")).unwrap().len(), limit);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
    drop(store);
    assert_eq!(
        open(&path, &checkpoint, &digest, limit)
            .unwrap()
            .get(b"a")
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
}

#[test]
fn conditional_batch_mismatch_needs_no_log_space() {
    let (_dir, path, checkpoint, digest, initial) = fixture();
    let store = open(&path, &checkpoint, &digest, initial).unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    let operations = [BatchOperation::Put {
        key: b"a",
        value: b"new",
    }];
    assert!(
        !store
            .compare_exchange_batch(&[(b"a", Some(b"wrong"))], &operations)
            .unwrap()
    );
    assert!(
        store
            .compare_exchange_batch(&[(b"a", Some(b"old"))], &operations)
            .is_err()
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
}

#[test]
fn checkpoint_batch_writers_respect_exact_log_and_suffix_bounds() {
    let operations = [
        BatchOperation::Delete { key: b"a" },
        BatchOperation::Put {
            key: b"b",
            value: b"new",
        },
    ];
    // Outer record framing, one batch selector, count, and two DB01 records.
    let batch_bytes = 21 + 1 + 4 + (21 + 1) + (21 + 1 + 3);
    for write in ["blind", "conditional", "transaction"] {
        for (suffix_bytes, log_bytes, rejection) in [
            (
                batch_bytes,
                batch_bytes - 1,
                Some("configured log byte limit"),
            ),
            (
                batch_bytes - 1,
                batch_bytes,
                Some("checkpoint suffix byte limit"),
            ),
            (batch_bytes, batch_bytes, None),
        ] {
            let (_dir, path, checkpoint, digest, initial) = fixture();
            let open = || {
                CheckpointStore::open_with_resource_limits(
                    &path,
                    &checkpoint,
                    &digest,
                    suffix_bytes,
                    MAX_RECOVERED_VIEW_TAIL_KEYS,
                    MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                    Some(initial + log_bytes),
                )
                .unwrap()
            };
            let store = open();
            let log = path.join("records.log");
            let before = fs::read(&log).unwrap();
            let result = match write {
                "blind" => store.write_batch(&operations),
                "conditional" => store
                    .compare_exchange_batch(&[(b"a", Some(b"old"))], &operations)
                    .map(|matched| assert!(matched)),
                "transaction" => store.transaction(|tx| {
                    tx.delete(b"a")?;
                    tx.put(b"b", b"new")
                }),
                _ => unreachable!(),
            };
            if let Some(reason) = rejection {
                assert!(result.unwrap_err().to_string().contains(reason), "{write}");
                assert_eq!(fs::read(&log).unwrap(), before);
            } else {
                result.unwrap();
                assert_eq!(fs::metadata(&log).unwrap().len(), initial + batch_bytes);
            }
            drop(store);
            let reopened = open();
            assert_eq!(
                reopened.get(b"a").unwrap().as_deref(),
                rejection.map(|_| b"old".as_slice()),
            );
            assert_eq!(
                reopened.get(b"b").unwrap().as_deref(),
                rejection.is_none().then_some(b"new".as_slice()),
            );
        }
    }
}
