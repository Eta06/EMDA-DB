//! V3 witness errors for detailed document mutations.
use super::*;
use crate::document::detailed_fault_model::{
    OPERATIONS, RecoveryExpectation, apply, prepare, verify_recovery,
};
use crate::{DocumentDatabase, TransactionFailure};

#[test]
fn detailed_document_mutations_report_unknown_for_v3_witness_errors() {
    for operation in 0..OPERATIONS {
        for (index, fault) in [
            Fault::Read,
            Fault::WriteBefore,
            Fault::WriteAfter,
            Fault::SyncBefore,
            Fault::SyncTimeout,
            Fault::SyncAfter,
        ]
        .into_iter()
        .enumerate()
        {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            let checkpoint = root.path().join("checkpoint");
            let armed = Arc::new(AtomicBool::new(false));
            let (entered, _) = mpsc::channel();
            let (_, resume) = mpsc::channel();
            let store = store(&path, fault, Arc::clone(&armed), entered, resume);
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            let db = DocumentDatabase::new(&store);
            prepare(&db);
            armed.store(true, Ordering::Release);
            let result = apply(&db, operation);
            assert!(
                matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
                "operation={operation}, fault={index}: {result:?}"
            );
            assert!(store.get(b"health-check").is_err());
            drop(store);
            let observed = verify_recovery(
                &path,
                &checkpoint,
                &digest,
                operation,
                RecoveryExpectation::Either,
                index % 2 == 0,
            );
            eprintln!(
                "ordinary witness operation={operation} fault={index} committed={observed:?}"
            );
        }
    }
}
