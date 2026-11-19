//! Outcome classification and document/index recovery for failed appends.
use super::{
    Store,
    fault_tests::{Fault, open_versioned_fault_store},
};
use crate::document::detailed_fault_model::{
    OPERATIONS, RecoveryExpectation, apply, prepare, verify_recovery,
};
use crate::{DocumentDatabase, TransactionFailure, test_support::retry_lock};
use std::fs;
use std::sync::{Arc, Mutex};

fn v2_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join("records.log"),
        include_bytes!("../../tests/fixtures/v2-empty-records.log"),
    )
    .unwrap();
    fs::write(
        path.join("commit.state"),
        include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
    )
    .unwrap();
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    fs2::FileExt::unlock(&store.duplicate_store_lock().unwrap()).unwrap();
    drop(store);
    (root, path, checkpoint, digest)
}

#[test]
fn indexed_document_mutations_recover_at_every_v2_append_cut() {
    for operation in [0, 1] {
        let length = {
            let (_root, path, _, _) = v2_fixture();
            let store = Store::open(&path).unwrap();
            let db = DocumentDatabase::new(&store);
            prepare(&db);
            let before = fs::metadata(path.join("records.log")).unwrap().len();
            apply(&db, operation).unwrap();
            usize::try_from(fs::metadata(path.join("records.log")).unwrap().len() - before).unwrap()
        };
        assert!(length > 1);
        for cut in 0..=length {
            let (_root, path, checkpoint, digest) = v2_fixture();
            let armed = Arc::new(Mutex::new(None));
            let store =
                retry_lock(|| open_versioned_fault_store(&path, Arc::clone(&armed))).unwrap();
            let db = DocumentDatabase::new(&store);
            prepare(&db);
            *armed.lock().unwrap() = Some(Fault::PartialAppend(cut));
            let result = apply(&db, operation);
            assert!(
                matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
                "operation={operation}, cut={cut}/{length}: {result:?}"
            );
            drop(store);
            let expected = if cut == 0 {
                RecoveryExpectation::Committed(false)
            } else if cut == length {
                RecoveryExpectation::Committed(true)
            } else {
                RecoveryExpectation::Torn
            };
            verify_recovery(
                &path,
                &checkpoint,
                &digest,
                operation,
                expected,
                cut % 2 == 0,
            );
        }
    }
}

#[test]
fn detailed_document_mutations_report_unknown_and_recover_atomic_indexes() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for operation in 0..OPERATIONS {
            for fault_kind in 0..5 {
                // V3 uses the witness handle to synchronize the shared log;
                // injecting sync into this separate LogIo would not fire.
                if !v2 && fault_kind >= 2 {
                    continue;
                }
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("store");
                if v2 {
                    fs::create_dir(&path).unwrap();
                    fs::write(
                        path.join("records.log"),
                        include_bytes!("../../tests/fixtures/v2-empty-records.log"),
                    )
                    .unwrap();
                    fs::write(
                        path.join("commit.state"),
                        include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
                    )
                    .unwrap();
                }
                let checkpoint = root.path().join("checkpoint");
                let store = Store::open(&path).unwrap();
                let digest = store.checkpoint_to(&checkpoint).unwrap();
                drop(store);
                let armed = Arc::new(Mutex::new(None));
                let store =
                    retry_lock(|| open_versioned_fault_store(&path, Arc::clone(&armed))).unwrap();
                let db = DocumentDatabase::new(&store);
                prepare(&db);
                *armed.lock().unwrap() = Some(match fault_kind {
                    0 => Fault::PartialAppend(0),
                    1 => Fault::PartialAppend(1),
                    2 => Fault::SyncBefore,
                    3 => Fault::SyncAfter,
                    _ => Fault::SyncWouldBlock,
                });
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
fn schema_publication_and_partial_indexes_recover_together_after_append_failure() {
    use crate::store::compound_partial_index_fault_tests::{prepare, verify};
    use crate::{CollectionSchema, DocumentTransactionMode, FieldRule, FieldType};
    use serde_json::json;

    for committed in [false, true] {
        let (_root, path, _, _) = v2_fixture();
        let armed = Arc::new(Mutex::new(None));
        let store = open_versioned_fault_store(&path, Arc::clone(&armed)).unwrap();
        let db = DocumentDatabase::new(&store);
        prepare(&db);
        let schema = CollectionSchema {
            fields: vec![FieldRule {
                path: vec!["region".into()],
                field_type: FieldType::String,
                required: true,
                nullable: false,
            }],
        };
        db.start_schema_migration("c", schema.clone()).unwrap();
        let step = db.advance_schema_migration("c", 2).unwrap();
        assert_eq!((step.processed, step.complete), (2, false));
        *armed.lock().unwrap() = Some(if committed {
            Fault::SyncAfter
        } else {
            Fault::PartialAppend(0)
        });
        let result =
            db.transaction_detailed(DocumentTransactionMode::Conservative, None, None, |tx| {
                tx.replace(
                    "c",
                    "item",
                    &json!({"status":"open","region":"west","sku":2}),
                    false,
                )?;
                tx.put_raw(b"receipt", b"moved")?;
                let step = tx.advance_schema_migration("c", 1)?;
                assert_eq!((step.processed, step.complete), (0, true));
                Ok(())
            });
        assert!(
            matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
            "{result:?}"
        );
        assert!(store.get(b"health-check").is_err());
        fs2::FileExt::unlock(&store.duplicate_store_lock().unwrap()).unwrap();
        drop(store);

        let reopened = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&reopened);
        assert_eq!(verify(&db), committed);
        let migration = db.schema_migration("c").unwrap();
        if committed {
            assert_eq!(migration, None);
            assert_eq!(
                db.insert("c", "invalid", &json!({})).unwrap_err().kind(),
                std::io::ErrorKind::InvalidInput
            );
        } else {
            let migration = migration.unwrap();
            assert_eq!(migration.schema, schema);
            assert_eq!(migration.after.as_deref(), Some("other"));
        }
    }
}
