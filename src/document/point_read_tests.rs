//! Atomic metadata/document observations for the optimized standalone read.
use super::*;
use crate::{BatchOperation, Store};
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;

fn metadata(field_type: FieldType) -> Vec<u8> {
    serde_json::to_vec(&transaction::Metadata {
        version: 1,
        schema: CollectionSchema {
            fields: vec![FieldRule {
                path: vec!["n".into()],
                field_type,
                required: true,
                nullable: false,
            }],
        },
        indexes: Vec::new(),
        jobs: Vec::new(),
        schema_job: None,
        dropping: false,
    })
    .unwrap()
}

#[test]
fn dropping_metadata_is_rejected_for_normal_reads_and_invalid_versions_fail_closed() {
    let mut value: serde_json::Value =
        serde_json::from_slice(&metadata(FieldType::Integer)).unwrap();
    for version in [1, 2] {
        value["version"] = json!(version);
        assert!(
            transaction::DocumentTransaction::decode_metadata(
                "c",
                Some(serde_json::to_vec(&value).unwrap())
            )
            .is_ok()
        );
    }
    value["version"] = json!(3);
    value["dropping"] = json!(true);
    assert_eq!(
        transaction::DocumentTransaction::decode_metadata(
            "c",
            Some(serde_json::to_vec(&value).unwrap())
        )
        .err()
        .unwrap()
        .kind(),
        std::io::ErrorKind::WouldBlock
    );
    value["version"] = json!(4);
    assert_eq!(
        transaction::DocumentTransaction::decode_metadata(
            "c",
            Some(serde_json::to_vec(&value).unwrap())
        )
        .err()
        .unwrap()
        .kind(),
        std::io::ErrorKind::WouldBlock
    );
    for (version, dropping) in [(3, false), (2, true), (4, false), (5, true)] {
        value["version"] = json!(version);
        value["dropping"] = json!(dropping);
        assert_eq!(
            transaction::DocumentTransaction::decode_metadata(
                "c",
                Some(serde_json::to_vec(&value).unwrap())
            )
            .err()
            .unwrap()
            .kind(),
            std::io::ErrorKind::InvalidData
        );
    }
    value["version"] = json!(3);
    value["dropping"] = json!(true);
    value["schema_job"] = json!({"schema": CollectionSchema::default(), "after": null});
    assert_eq!(
        transaction::DocumentTransaction::decode_metadata(
            "c",
            Some(serde_json::to_vec(&value).unwrap())
        )
        .err()
        .unwrap()
        .kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[test]
fn drop_cursor_rejects_forged_or_skipped_ranges_without_deleting_metadata() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({})).unwrap();
    db.insert("c", "b", &json!({})).unwrap();
    db.start_collection_drop("c").unwrap();
    let metadata_key = key::collection(b'M', "c").unwrap();
    let cursor_key = key::collection(b'P', "c").unwrap();
    let original = store.get(&metadata_key).unwrap().unwrap();
    for cursor in [vec![255], key::document("c", "z").unwrap()] {
        let mut metadata: Value = serde_json::from_slice(&original).unwrap();
        metadata["version"] = json!(4);
        let corrupt = serde_json::to_vec(&metadata).unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: &metadata_key,
                    value: &corrupt,
                },
                BatchOperation::Put {
                    key: &cursor_key,
                    value: &cursor,
                },
            ])
            .unwrap();
        assert_eq!(
            db.advance_collection_drop("c", 8).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            store.get(&metadata_key).unwrap().as_deref(),
            Some(corrupt.as_slice())
        );
        assert_eq!(
            store.get(&cursor_key).unwrap().as_deref(),
            Some(cursor.as_slice())
        );
        assert!(
            store
                .get(&key::document("c", "a").unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .get(&key::document("c", "b").unwrap())
                .unwrap()
                .is_some()
        );
    }
    // A cursor with the old marker, or a new marker without its cursor,
    // must also fail closed instead of accepting ambiguous progress.
    store.put(&metadata_key, &original).unwrap();
    assert_eq!(
        db.advance_collection_drop("c", 8).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    let mut marker: Value = serde_json::from_slice(&original).unwrap();
    marker["version"] = json!(4);
    let marker = serde_json::to_vec(&marker).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: &metadata_key,
                value: &marker,
            },
            BatchOperation::Delete { key: &cursor_key },
        ])
        .unwrap();
    assert_eq!(
        db.advance_collection_drop("c", 8).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    store
        .write_batch(&[
            BatchOperation::Put {
                key: &metadata_key,
                value: &original,
            },
            BatchOperation::Delete { key: &cursor_key },
        ])
        .unwrap();
    assert_eq!(db.advance_collection_drop("c", 8).unwrap().removed, 2);
    assert!(db.advance_collection_drop("c", 8).unwrap().complete);
}

#[test]
fn collection_creation_rejects_a_stray_drop_cursor() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let cursor_key = key::collection(b'P', "c").unwrap();
    store.put(&cursor_key, b"stray").unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(
        db.create_collection("c", CollectionSchema::default())
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(
        store.get(&cursor_key).unwrap().as_deref(),
        Some(&b"stray"[..])
    );
}

#[test]
fn checkpoint_drop_does_not_publish_completion_for_a_skipped_range() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({})).unwrap();
    db.start_collection_drop("c").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = crate::test_support::retry_lock(|| {
        crate::CheckpointStore::open(&path, &checkpoint, &digest)
    })
    .unwrap();
    let metadata_key = key::collection(b'M', "c").unwrap();
    let cursor_key = key::collection(b'P', "c").unwrap();
    let mut metadata: Value =
        serde_json::from_slice(&store.get(&metadata_key).unwrap().unwrap()).unwrap();
    metadata["version"] = json!(4);
    let metadata = serde_json::to_vec(&metadata).unwrap();
    let cursor = key::document("c", "z").unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: &metadata_key,
                value: &metadata,
            },
            BatchOperation::Put {
                key: &cursor_key,
                value: &cursor,
            },
        ])
        .unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    assert_eq!(
        db.advance_collection_drop("c", 8).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(
        store.get(&metadata_key).unwrap().as_deref(),
        Some(metadata.as_slice())
    );
    assert!(
        store
            .get(&key::document("c", "a").unwrap())
            .unwrap()
            .is_some()
    );
}

#[test]
fn checkpoint_cursor_starts_on_a_full_page_but_not_a_short_page() {
    for (limit, expected_version, expect_cursor) in [(2, 4, true), (3, 3, false)] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({})).unwrap();
        db.insert("c", "b", &json!({})).unwrap();
        db.start_collection_drop("c").unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);
        let store = crate::test_support::retry_lock(|| {
            crate::CheckpointStore::open(&path, &checkpoint, &digest)
        })
        .unwrap();
        let db = DocumentDatabase::with_checkpoint_store(&store);
        let step = db.advance_collection_drop("c", limit).unwrap();
        assert_eq!((step.removed, step.complete), (2, false));
        let metadata: Value = serde_json::from_slice(
            &store
                .get(&key::collection(b'M', "c").unwrap())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["version"], json!(expected_version));
        assert_eq!(
            store
                .get(&key::collection(b'P', "c").unwrap())
                .unwrap()
                .is_some(),
            expect_cursor
        );
        assert!(db.advance_collection_drop("c", limit).unwrap().complete);
    }
}

#[test]
fn checkpoint_drop_keeps_going_after_a_byte_limited_short_page() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    let payload = "x".repeat(1_000_000);
    for id in 0..34 {
        db.insert(
            "c",
            &format!("{id:02}"),
            &json!({"payload":payload.as_str()}),
        )
        .unwrap();
    }
    db.start_collection_drop("c").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = crate::test_support::retry_lock(|| {
        crate::CheckpointStore::open(&path, &checkpoint, &digest)
    })
    .unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    let first = db.advance_collection_drop("c", 64).unwrap();
    assert!(!first.complete && first.removed > 0 && first.removed < 34);
    let metadata: Value = serde_json::from_slice(
        &store
            .get(&key::collection(b'M', "c").unwrap())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(metadata["version"], json!(3));
    assert!(
        store
            .get(&key::collection(b'P', "c").unwrap())
            .unwrap()
            .is_none()
    );
    let mut removed = first.removed;
    loop {
        let step = db.advance_collection_drop("c", 64).unwrap();
        removed += step.removed;
        if step.complete {
            break;
        }
    }
    assert_eq!(removed, 34);
}

#[test]
fn previous_drop_marker_without_a_cursor_resumes_after_reopening() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({})).unwrap();
    db.insert("c", "b", &json!({})).unwrap();
    db.insert("c", "c", &json!({})).unwrap();
    db.insert("c", "d", &json!({})).unwrap();
    db.start_collection_drop("c").unwrap();
    assert_eq!(db.advance_collection_drop("c", 1).unwrap().removed, 1);
    let metadata_key = key::collection(b'M', "c").unwrap();
    let cursor_key = key::collection(b'P', "c").unwrap();
    let metadata: Value =
        serde_json::from_slice(&store.get(&metadata_key).unwrap().unwrap()).unwrap();
    assert_eq!(metadata["version"], json!(3));
    assert!(store.get(&cursor_key).unwrap().is_none());
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = crate::test_support::retry_lock(|| {
        crate::CheckpointStore::open(&path, &checkpoint, &digest)
    })
    .unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    assert_eq!(db.advance_collection_drop("c", 1).unwrap().removed, 1);
    let mut metadata: Value =
        serde_json::from_slice(&store.get(&metadata_key).unwrap().unwrap()).unwrap();
    assert_eq!(metadata["version"], json!(4));
    assert!(store.get(&cursor_key).unwrap().is_some());
    metadata["version"] = json!(3);
    let old_metadata = serde_json::to_vec(&metadata).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: &metadata_key,
                value: &old_metadata,
            },
            BatchOperation::Delete { key: &cursor_key },
        ])
        .unwrap();
    drop(store);
    let store = crate::test_support::retry_lock(|| {
        crate::CheckpointStore::open(&path, &checkpoint, &digest)
    })
    .unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    assert_eq!(db.advance_collection_drop("c", 1).unwrap().removed, 1);
    drop(store);
    let store = crate::test_support::retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(db.advance_collection_drop("c", 1).unwrap().removed, 1);
    assert!(db.advance_collection_drop("c", 1).unwrap().complete);
}

#[test]
fn standalone_get_never_mixes_schema_and_document_from_atomic_writes() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    let metadata_key = key::collection(b'M', "c").unwrap();
    let document_key = key::document("c", "a").unwrap();
    let integer_metadata = metadata(FieldType::Integer);
    let string_metadata = metadata(FieldType::String);
    let integer_document = br#"{"n":1}"#;
    let string_document = br#"{"n":"one"}"#;
    store
        .write_batch(&[
            BatchOperation::Put {
                key: &metadata_key,
                value: &integer_metadata,
            },
            BatchOperation::Put {
                key: &document_key,
                value: integer_document,
            },
        ])
        .unwrap();
    let start = Arc::new(Barrier::new(2));
    let writer_store = Arc::clone(&store);
    let writer_start = Arc::clone(&start);
    let writer = thread::spawn(move || {
        writer_start.wait();
        for index in 0..1000 {
            let (schema, document): (&[u8], &[u8]) = if index % 2 == 0 {
                (&string_metadata, string_document)
            } else {
                (&integer_metadata, integer_document)
            };
            writer_store
                .write_batch(&[
                    BatchOperation::Put {
                        key: &metadata_key,
                        value: schema,
                    },
                    BatchOperation::Put {
                        key: &document_key,
                        value: document,
                    },
                ])
                .unwrap();
            thread::yield_now();
        }
    });
    start.wait();
    let db = DocumentDatabase::new(&store);
    let mut successful = 0;
    for _ in 0..2000 {
        match db.get("c", "a") {
            Ok(Some(value)) => {
                assert!(value == json!({"n":1}) || value == json!({"n":"one"}));
                successful += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            outcome => panic!("unexpected standalone read: {outcome:?}"),
        }
        thread::yield_now();
    }
    writer.join().unwrap();
    assert!(successful > 0);
    assert_eq!(db.get("c", "a").unwrap(), Some(json!({"n":1})));
}
