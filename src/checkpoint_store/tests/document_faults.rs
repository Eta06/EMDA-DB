use super::*;
use crate::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue};
use serde_json::json;

#[test]
fn partial_and_failed_document_commits_recover_atomically_or_fail_closed() {
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
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let db = DocumentDatabase::with_checkpoint_store(&store);
        assert!(
            db.transaction(|tx| {
                tx.replace("c", "id", &json!({"x":2}), false)?;
                tx.put_raw(b"receipt", b"new")
            })
            .is_err()
        );
        assert!(store.get(b"base").is_err());
        drop(store);
        let bytes = fs::read(path.join("records.log")).unwrap();
        match retry_lock(|| Store::open(&path)) {
            Ok(reopened) => {
                let db = DocumentDatabase::new(&reopened);
                let value = db.get("c", "id").unwrap().unwrap()["x"].as_i64().unwrap();
                assert!(value == 1 || value == 2);
                assert_eq!(
                    db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
                    (value == 2).then(|| b"new".to_vec())
                );
                assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
                assert_eq!(
                    db.query_index(
                        "c",
                        "i",
                        &IndexQuery {
                            equal: vec![IndexValue::Integer(value)],
                            ..Default::default()
                        }
                    )
                    .unwrap()
                    .documents[0]
                        .id,
                    "id"
                );
            }
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )),
        }
        assert_eq!(fs::read(path.join("records.log")).unwrap(), bytes);
    }
}
