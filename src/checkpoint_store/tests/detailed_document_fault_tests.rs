//! Outcome classification and document/index recovery for failed appends.
use super::*;
use crate::document::detailed_fault_model::{
    OPERATIONS, RecoveryExpectation, apply, prepare, verify_recovery,
};
use crate::{DocumentDatabase, TransactionFailure};

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
