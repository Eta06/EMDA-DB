use super::*;
use crate::test_support::retry_lock;
use crate::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue};
use serde_json::json;

#[test]
fn incremental_collection_drop_remains_atomic_across_v3_metadata_faults() {
    use crate::store::collection_drop_fault_tests::{prepare, step, verify};
    for fault in [
        Fault::Read,
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncTimeout,
        Fault::SyncAfter,
        Fault::Panic,
    ] {
        let root = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered, _) = mpsc::channel();
        let (_, resume) = mpsc::channel();
        let store = store(root.path(), fault, Arc::clone(&armed), entered, resume);
        prepare(&DocumentDatabase::new(&store));
        armed.store(true, Ordering::Release);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            step(&DocumentDatabase::new(&store))
        }));
        assert!(!matches!(result, Ok(Ok(()))));
        drop(store);
        let reopened = retry_lock(|| Store::open(root.path())).unwrap();
        verify(&DocumentDatabase::new(&reopened));
    }
}

#[test]
fn collection_drop_marker_and_final_removal_remain_atomic_across_v3_metadata_faults() {
    for final_step in [false, true] {
        for fault in [
            Fault::Read,
            Fault::WriteBefore,
            Fault::WriteAfter,
            Fault::SyncBefore,
            Fault::SyncTimeout,
            Fault::SyncAfter,
            Fault::Panic,
        ] {
            let root = tempfile::tempdir().unwrap();
            let armed = Arc::new(AtomicBool::new(false));
            let (entered, _) = mpsc::channel();
            let (_, resume) = mpsc::channel();
            let store = store(root.path(), fault, Arc::clone(&armed), entered, resume);
            let db = DocumentDatabase::new(&store);
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.insert("c", "a", &json!({"x":1})).unwrap();
            if final_step {
                db.start_collection_drop("c").unwrap();
                assert_eq!(db.advance_collection_drop("c", 1).unwrap().removed, 1);
            }
            armed.store(true, Ordering::Release);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                db.transaction(|tx| {
                    if final_step {
                        assert!(tx.advance_collection_drop("c", 1)?.complete);
                    } else {
                        tx.start_collection_drop("c")?;
                    }
                    tx.put_raw(b"receipt", b"done")
                })
            }));
            assert!(!matches!(outcome, Ok(Ok(()))));
            drop(store);
            let reopened = retry_lock(|| Store::open(root.path())).unwrap();
            let db = DocumentDatabase::new(&reopened);
            let committed = db
                .transaction(|tx| tx.get_raw(b"receipt"))
                .unwrap()
                .is_some();
            if final_step && committed {
                assert_eq!(
                    db.get("c", "a").unwrap_err().kind(),
                    std::io::ErrorKind::NotFound
                );
            } else if final_step || committed {
                assert!(db.collection_drop_in_progress("c").unwrap());
                assert_eq!(
                    db.get("c", "a").unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
            } else {
                assert!(!db.collection_drop_in_progress("c").unwrap());
                assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":1})));
            }
        }
    }
}

#[test]
fn document_and_unique_index_remain_atomic_across_metadata_faults() {
    for fault in [
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncAfter,
        Fault::Panic,
    ] {
        let root = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered, _) = mpsc::channel();
        let (_, resume) = mpsc::channel();
        let store = store(root.path(), fault, Arc::clone(&armed), entered, resume);
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "i".into(),
                fields: vec![vec!["x".into()]],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
        db.insert("c", "id", &json!({"x":1})).unwrap();
        armed.store(true, Ordering::Release);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.transaction(|tx| {
                tx.replace("c", "id", &json!({"x":2}), false)?;
                tx.put_raw(b"receipt", b"new")
            })
        }));
        assert!(!matches!(outcome, Ok(Ok(()))));
        assert!(store.get(b"receipt").is_err());
        drop(store);
        let reopened = retry_lock(|| Store::open(root.path())).unwrap();
        let db = DocumentDatabase::new(&reopened);
        let value = db.get("c", "id").unwrap().unwrap()["x"].as_i64().unwrap();
        assert!(value == 1 || value == 2);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
            (value == 2).then(|| b"new".to_vec())
        );
        assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
        let page = db
            .query_index(
                "c",
                "i",
                &IndexQuery {
                    equal: vec![IndexValue::Integer(value)],
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(page.documents.len(), 1);
        assert_eq!(page.documents[0].id, "id");
    }
}

#[test]
fn array_index_and_raw_write_recover_together_after_v3_metadata_errors() {
    use crate::document::array_fault_model::{prepare, update, verify};
    for fault in [
        Fault::Read,
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncTimeout,
        Fault::SyncAfter,
        Fault::Panic,
    ] {
        let root = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered, _) = mpsc::channel();
        let (_, resume) = mpsc::channel();
        let store = store(root.path(), fault, Arc::clone(&armed), entered, resume);
        prepare(&DocumentDatabase::new(&store));
        armed.store(true, Ordering::Release);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            update(&DocumentDatabase::new(&store))
        }));
        assert!(!matches!(outcome, Ok(Ok(()))));
        assert!(store.get(b"receipt").is_err());
        drop(store);
        let reopened = retry_lock(|| Store::open(root.path())).unwrap();
        verify(&DocumentDatabase::new(&reopened));
    }
}
