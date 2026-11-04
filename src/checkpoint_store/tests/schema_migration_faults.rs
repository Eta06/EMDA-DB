use super::*;
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
fn failed_schema_migration_step_recovers_atomically_or_fails_closed() {
    for fault in [
        LogFault::Partial(0),
        LogFault::Partial(1),
        LogFault::Partial(8),
        LogFault::Partial(30),
        LogFault::SyncBefore,
        LogFault::SyncAfter,
    ] {
        let (_root, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let db = DocumentDatabase::with_checkpoint_store(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"n":1})).unwrap();
        db.insert("c", "b", &json!({"n":2})).unwrap();
        db.start_schema_migration("c", schema()).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        assert!(
            DocumentDatabase::with_checkpoint_store(&store)
                .advance_schema_migration("c", 1)
                .is_err()
        );
        assert!(store.get(b"base").is_err());
        drop(store);
        let before = fs::read(path.join("records.log")).unwrap();
        match retry_lock(|| Store::open(&path)) {
            Ok(reopened) => {
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
            Err(error) => {
                assert!(matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ));
                assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
            }
        }
    }
}
