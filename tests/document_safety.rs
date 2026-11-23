mod support;

use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, Entry, FieldRule, FieldType,
    IndexDefinition, IndexQuery, Store,
};
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use support::retry_lock;

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

fn change_last_value_byte(path: &Path, original: u8) -> (File, u64) {
    let log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    // The last single-record value is immediately before its four-byte CRC.
    let offset = log.metadata().unwrap().len() - 5;
    let mut byte = [0];
    log.read_exact_at(&mut byte, offset).unwrap();
    assert_eq!(byte, [original]);
    log.write_all_at(&[original ^ 1], offset).unwrap();
    (log, offset)
}

fn maintenance_admission(
    db: &DocumentDatabase<'_>,
    source: &Path,
    put: impl Fn(&[u8], &[u8]),
    scan: impl Fn() -> Vec<Entry>,
) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    let overhead = br#"{"x":2,"body":""}"#.len();
    let maximum = json!({"x": 2, "body": "x".repeat(1024 * 1024 - overhead)});
    let encoded_maximum = serde_json::to_vec(&maximum).unwrap();
    assert_eq!(encoded_maximum.len(), 1024 * 1024);
    db.insert("c", "b", &maximum).unwrap();
    db.start_index_build("c", index()).unwrap();
    db.start_schema_migration(
        "c",
        CollectionSchema {
            fields: vec![FieldRule {
                path: vec!["x".into()],
                field_type: FieldType::Integer,
                required: true,
                nullable: false,
            }],
        },
    )
    .unwrap();
    let jobs = db.index_jobs("c").unwrap();
    let migration = db.schema_migration("c").unwrap();
    let document_key = b"\0emda-document\x01D\0\x01cb";
    let mut oversized = encoded_maximum.clone();
    oversized.push(b' ');
    put(document_key, &oversized);
    let (log, offset) = change_last_value_byte(source, b' ');
    let build_error = db.advance_index_build("c", "i", 2).unwrap_err();
    let schema_error = db.advance_schema_migration("c", 2).unwrap_err();
    log.write_all_at(b" ", offset).unwrap();
    for error in [build_error, schema_error] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "stored document exceeds size limit");
    }
    assert_eq!(db.index_jobs("c").unwrap(), jobs);
    assert_eq!(db.schema_migration("c").unwrap(), migration);
    assert!(
        !scan()
            .into_iter()
            .any(|(key, _)| key.starts_with(b"\0emda-document\x01I")
                || key.starts_with(b"\0emda-document\x01U"))
    );

    put(document_key, &encoded_maximum);
    assert_eq!(db.advance_index_build("c", "i", 2).unwrap().processed, 2);
    assert!(db.advance_index_build("c", "i", 2).unwrap().complete);
    assert_eq!(db.advance_schema_migration("c", 2).unwrap().processed, 2);
    assert!(db.advance_schema_migration("c", 2).unwrap().complete);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);

    let (key, correct) = scan()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01U"))
        .unwrap();
    put(&key, b"invalid-owner");
    let (log, offset) = change_last_value_byte(source, b'r');
    let error = db.verify_collection("c").unwrap_err();
    log.write_all_at(b"r", offset).unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "unexpected or incorrect index entry");

    // A same-length owner must still be read and checked.
    assert_eq!(correct.len(), 1);
    put(&key, b"z");
    let error = db.verify_collection("c").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "unexpected or incorrect index entry");
    put(&key, &correct);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);
}

#[test]
fn maintenance_admits_stored_document_and_index_sizes_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("ordinary");
    let ordinary = Store::open(&source).unwrap();
    maintenance_admission(
        &DocumentDatabase::new(&ordinary),
        &source,
        |key, value| ordinary.put(key, value).unwrap(),
        || ordinary.scan(None, None, 16).unwrap(),
    );
    let source = root.path().join("checkpoint-source");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&source).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    maintenance_admission(
        &DocumentDatabase::with_checkpoint_store(&store),
        &source,
        |key, value| store.put(key, value).unwrap(),
        || store.scan(None, None, 16).unwrap(),
    );
}
