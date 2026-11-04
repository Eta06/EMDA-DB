use super::*;
use crate::test_support::retry_lock;
use crate::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue};
use serde_json::json;

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
