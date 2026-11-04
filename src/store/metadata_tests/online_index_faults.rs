use super::*;
use crate::test_support::retry_lock;
use crate::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexJobStatus, IndexQuery};
use serde_json::json;

#[test]
fn index_build_progress_and_entries_recover_together_after_metadata_failures() {
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
        db.start_index_build(
            "c",
            IndexDefinition {
                name: "idx".into(),
                fields: vec![vec!["n".into()]],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
        armed.store(true, Ordering::Release);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.advance_index_build("c", "idx", 1)
        }));
        assert!(!matches!(outcome, Ok(Ok(_))));
        assert!(store.get(b"base").is_err());
        drop(store);
        let reopened = retry_lock(|| Store::open(root.path())).unwrap();
        let db = DocumentDatabase::new(&reopened);
        assert!(matches!(
            &db.index_jobs("c").unwrap()[0],
            IndexJobStatus::Building {
                after: None | Some(_),
                ..
            }
        ));
        for _ in 0..5 {
            if db.advance_index_build("c", "idx", 1).unwrap().complete {
                break;
            }
        }
        assert!(db.index_jobs("c").unwrap().is_empty());
        assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);
        assert_eq!(
            db.query_index("c", "idx", &IndexQuery::default())
                .unwrap()
                .documents
                .len(),
            2
        );
    }
}
