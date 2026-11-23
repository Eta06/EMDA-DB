//! Outcome classification and document/index recovery for failed appends.
use super::*;
use crate::document::detailed_fault_model::{
    OPERATIONS, RecoveryExpectation, apply, prepare, verify_recovery,
};
use crate::{DocumentDatabase, TransactionFailure};

#[test]
fn collection_archive_import_recovers_as_one_batch_under_v2_faults() {
    use crate::document::detailed_fault_model::{
        import_archive, import_archive_cuts, verify_archive_recovery,
    };
    let cuts = {
        let (_root, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        import_archive_cuts(&DocumentDatabase::with_checkpoint_store(&store), &path)
    };
    let faults = cuts
        .into_iter()
        .map(|(cut, expected)| (LogFault::Partial(cut), expected))
        .chain([(LogFault::SyncAfter, RecoveryExpectation::Committed(true))]);
    for (fault, expected) in faults {
        let (_root, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let before = store.log.len().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let result = import_archive(&DocumentDatabase::with_checkpoint_store(&store));
        assert!(
            matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
            "{result:?}"
        );
        if let LogFault::Partial(cut) = fault {
            assert_eq!(store.log.len().unwrap(), before + cut as u64);
        }
        drop(store);
        verify_archive_recovery(&path, &checkpoint, &digest, expected, false);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn collection_archive_import_recovers_as_one_batch_under_v3_witness_faults() {
    use crate::document::detailed_fault_model::{
        attach_v3_commit_fault, import_archive, v3_commit_faults, verify_archive_recovery,
    };
    v3_commit_faults(
        |path, checkpoint, digest, fault| {
            let mut store = retry_lock(|| CheckpointStore::open(path, checkpoint, digest)).unwrap();
            attach_v3_commit_fault(&mut store.log, path, fault);
            import_archive(&DocumentDatabase::with_checkpoint_store(&store))
        },
        |path, checkpoint, digest| {
            verify_archive_recovery(
                path,
                checkpoint,
                digest,
                RecoveryExpectation::Committed(true),
                false,
            );
        },
    );
}

#[test]
fn guarded_maintenance_restart_recovers_atomically_with_its_receipt() {
    use crate::document::detailed_fault_model::{
        guarded_restart, guarded_restart_torn_cuts, prepare_guarded_restart,
        verify_guarded_restart_recovery,
    };

    let key = crate::MaintenanceKey::generate().unwrap();
    let cuts = {
        let (_root, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        guarded_restart_torn_cuts(
            &DocumentDatabase::with_checkpoint_store(&store),
            &path,
            &key,
        )
    };
    let faults = [(LogFault::Partial(0), RecoveryExpectation::Committed(false))]
        .into_iter()
        .chain(cuts.map(|cut| (LogFault::Partial(cut), RecoveryExpectation::Torn)))
        .chain([(LogFault::SyncAfter, RecoveryExpectation::Committed(true))]);
    for (fault, expected) in faults {
        let (_root, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        prepare_guarded_restart(&DocumentDatabase::with_checkpoint_store(&store));
        let length_before = store.log.len().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let result = guarded_restart(&DocumentDatabase::with_checkpoint_store(&store), &key);
        assert!(
            matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
            "{result:?}"
        );
        if let LogFault::Partial(cut) = fault {
            assert_eq!(store.log.len().unwrap(), length_before + cut as u64);
        }
        drop(store);
        verify_guarded_restart_recovery(&path, &checkpoint, &digest, &key, expected, false);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn guarded_maintenance_restart_recovers_after_v3_witness_faults() {
    use crate::document::detailed_fault_model::{
        attach_v3_commit_fault, guarded_restart, guarded_restart_v3_faults, prepare_guarded_restart,
    };

    guarded_restart_v3_faults(false, |path, checkpoint, digest, key, fault| {
        let mut store = retry_lock(|| CheckpointStore::open(path, checkpoint, digest)).unwrap();
        prepare_guarded_restart(&DocumentDatabase::with_checkpoint_store(&store));
        attach_v3_commit_fault(&mut store.log, path, fault);
        guarded_restart(&DocumentDatabase::with_checkpoint_store(&store), key)
    });
}

#[test]
fn detailed_document_mutations_report_unknown_and_recover_atomic_indexes() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for operation in 0..OPERATIONS {
            for fault_kind in 0..5 {
                // V3 synchronization uses the separate witness handle.
                if !v2 && fault_kind >= 2 {
                    continue;
                }
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("store");
                if v2 {
                    fs::create_dir(&path).unwrap();
                    fs::write(
                        path.join("records.log"),
                        include_bytes!("../../../tests/fixtures/v2-empty-records.log"),
                    )
                    .unwrap();
                    fs::write(
                        path.join("commit.state"),
                        include_bytes!("../../../tests/fixtures/v2-empty-commit.state"),
                    )
                    .unwrap();
                }
                let checkpoint = root.path().join("checkpoint");
                let store = Store::open(&path).unwrap();
                let digest = store.checkpoint_to(&checkpoint).unwrap();
                drop(store);
                let mut store =
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
                prepare(&DocumentDatabase::with_checkpoint_store(&store));
                let file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(path.join("records.log"))
                    .unwrap();
                store.log.replace_io_for_test(Box::new(FaultLog {
                    file,
                    fault: match fault_kind {
                        0 => LogFault::Partial(0),
                        1 => LogFault::Partial(1),
                        2 => LogFault::SyncBefore,
                        3 => LogFault::SyncAfter,
                        _ => LogFault::SyncWouldBlock,
                    },
                }));
                let db = DocumentDatabase::with_checkpoint_store(&store);
                let result = apply(&db, operation);
                assert!(
                    matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
                    "v2={v2}, operation={operation}, fault={fault_kind}: {result:?}"
                );
                assert!(store.get(b"health-check").is_err());
                drop(store);
                let committed = match fault_kind {
                    0 => RecoveryExpectation::Committed(false),
                    1 => RecoveryExpectation::Torn,
                    _ => RecoveryExpectation::Committed(true),
                };
                verify_recovery(
                    &path,
                    &checkpoint,
                    &digest,
                    operation,
                    committed,
                    fault_kind % 2 == 0,
                );
            }
        }
    }
}

#[test]
fn detailed_unique_document_insert_and_replace_recover_at_every_v2_append_cut() {
    for operation in [0, 1] {
        let append_len = {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            fs::create_dir(&path).unwrap();
            fs::write(
                path.join("records.log"),
                include_bytes!("../../../tests/fixtures/v2-empty-records.log"),
            )
            .unwrap();
            fs::write(
                path.join("commit.state"),
                include_bytes!("../../../tests/fixtures/v2-empty-commit.state"),
            )
            .unwrap();
            let checkpoint = root.path().join("checkpoint");
            let store = Store::open(&path).unwrap();
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            prepare(&DocumentDatabase::with_checkpoint_store(&store));
            let before = fs::metadata(path.join("records.log")).unwrap().len();
            apply(&DocumentDatabase::with_checkpoint_store(&store), operation).unwrap();
            usize::try_from(fs::metadata(path.join("records.log")).unwrap().len() - before).unwrap()
        };
        assert!(
            append_len > 1,
            "operation={operation}, append_len={append_len}"
        );
        eprintln!("V2 detailed document operation {operation}: {append_len} append bytes");

        for cut in 0..=append_len {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            fs::create_dir(&path).unwrap();
            fs::write(
                path.join("records.log"),
                include_bytes!("../../../tests/fixtures/v2-empty-records.log"),
            )
            .unwrap();
            fs::write(
                path.join("commit.state"),
                include_bytes!("../../../tests/fixtures/v2-empty-commit.state"),
            )
            .unwrap();
            let checkpoint = root.path().join("checkpoint");
            let store = Store::open(&path).unwrap();
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let mut store =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            prepare(&DocumentDatabase::with_checkpoint_store(&store));
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(path.join("records.log"))
                .unwrap();
            store.log.replace_io_for_test(Box::new(FaultLog {
                file,
                fault: LogFault::Partial(cut),
            }));
            let result = apply(&DocumentDatabase::with_checkpoint_store(&store), operation);
            assert!(
                matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
                "operation={operation}, cut={cut}/{append_len}: {result:?}"
            );
            assert!(store.get(b"health-check").is_err());
            drop(store);

            let expected = if cut == 0 {
                RecoveryExpectation::Committed(false)
            } else if cut == append_len {
                RecoveryExpectation::Committed(true)
            } else {
                RecoveryExpectation::Torn
            };
            let recovered = verify_recovery(
                &path,
                &checkpoint,
                &digest,
                operation,
                expected,
                cut % 2 == 0,
            );
            assert_eq!(
                recovered,
                match expected {
                    RecoveryExpectation::Committed(value) => Some(value),
                    RecoveryExpectation::Torn => None,
                    #[cfg(target_os = "linux")]
                    RecoveryExpectation::Either => unreachable!(),
                },
                "operation={operation}, cut={cut}/{append_len}"
            );
        }
    }
}
