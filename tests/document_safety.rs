use database_engine::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, Store};
use serde_json::{Value, json};
use std::io;

fn index() -> IndexDefinition {
    IndexDefinition {
        name: "i".into(),
        fields: vec![vec!["x".into()]],
        unique: true,
        partial: None,
    }
}

#[test]
fn failed_unique_index_build_leaves_no_partial_metadata_or_entries() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    db.insert("c", "b", &json!({"x":1})).unwrap();
    assert_eq!(
        db.create_index("c", index()).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(db.indexes("c").unwrap().is_empty());
    db.delete("c", "b").unwrap();
    db.create_index("c", index()).unwrap();
    assert_eq!(
        db.query_index("c", "i", &IndexQuery::default())
            .unwrap()
            .documents
            .len(),
        1
    );
}

#[test]
fn corrupted_index_and_metadata_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    let rows = store.scan(None, None, 100).unwrap();
    let index_key = rows
        .iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01I"))
        .unwrap()
        .0
        .clone();
    store.put(&index_key, b"unexpected").unwrap();
    assert_eq!(
        db.query_index("c", "i", &IndexQuery::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let metadata_key = rows
        .iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01M"))
        .unwrap()
        .0
        .clone();
    store.put(&metadata_key, b"{broken").unwrap();
    assert_eq!(
        db.get("c", "a").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn document_bounds_and_unsupported_index_values_fail_before_commit() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    assert_eq!(
        db.insert("c", "a", &json!({"x":[1,2]})).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "a").unwrap(), None);
    let large = json!({"text":"x".repeat(1024*1024+1)});
    assert_eq!(
        db.insert("c", "large", &large).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let mut nested = Value::Null;
    for _ in 0..66 {
        nested = json!({"next":nested});
    }
    assert_eq!(
        db.insert("c", "deep", &nested).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "deep").unwrap(), None);
}

#[test]
fn verification_detects_missing_index_and_rebuild_restores_it_atomically() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    let report = db.verify_collection("c").unwrap();
    assert_eq!(
        (report.documents, report.indexes, report.index_keys),
        (1, 1, 2)
    );
    let key = store
        .scan(None, None, 100)
        .unwrap()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01I"))
        .unwrap()
        .0;
    store.delete(&key).unwrap();
    assert!(
        db.query_index("c", "i", &IndexQuery::default())
            .unwrap()
            .documents
            .is_empty()
    );
    assert_eq!(
        db.verify_collection("c").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    db.rebuild_index("c", "i").unwrap();
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
    assert_eq!(
        db.query_index("c", "i", &IndexQuery::default())
            .unwrap()
            .documents[0]
            .id,
        "a"
    );
}
