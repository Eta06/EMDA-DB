use super::*;
use crate::test_support::retry_lock;
use crate::{CollectionSchema, DocumentDatabase, FieldRule, FieldType};
use serde_json::json;
fn schema() -> CollectionSchema {
    CollectionSchema {
        fields: vec![FieldRule {
            path: vec!["n".into()],
            field_type: FieldType::Integer,
            required: true,
            nullable: false,
        }],
    }
}
#[test]
fn schema_progress_recovers_atomically_after_metadata_failures() {
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
        db.insert("c", "a", &json!({"n":1})).unwrap();
        db.insert("c", "b", &json!({"n":2})).unwrap();
        db.start_schema_migration("c", schema()).unwrap();
        armed.store(true, Ordering::Release);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.advance_schema_migration("c", 1)
        }));
        assert!(!matches!(outcome, Ok(Ok(_))));
        assert!(store.get(b"base").is_err());
        drop(store);
        let reopened = retry_lock(|| Store::open(root.path())).unwrap();
        let db = DocumentDatabase::new(&reopened);
        assert!(db.schema_migration("c").unwrap().is_some());
        for _ in 0..5 {
            if db.advance_schema_migration("c", 1).unwrap().complete {
                break;
            }
        }
        assert!(db.schema_migration("c").unwrap().is_none());
        assert_eq!(
            db.insert("c", "invalid", &json!({})).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
