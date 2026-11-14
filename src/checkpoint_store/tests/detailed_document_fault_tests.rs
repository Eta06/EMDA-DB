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
