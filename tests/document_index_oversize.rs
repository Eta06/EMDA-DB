//! Oversized corrupt documents must not turn an index page into an empty result.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    IndexDefinition, IndexQuery, Store,
};
use serde_json::json;
use std::io::ErrorKind;

mod support;
use support::retry_lock;

fn assert_corruption_is_reported(db: &DocumentDatabase<'_>) {
    let index_error = db
        .query_index("c", "x", &IndexQuery::default())
        .unwrap_err();
    assert_eq!(index_error.kind(), ErrorKind::InvalidData);
    let planned_error = db
        .query_planned(
            "c",
            &DocumentQuery {
                filter: DocumentFilter::Equal {
                    path: vec!["x".into()],
                    value: json!(1),
                },
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(planned_error.kind(), ErrorKind::InvalidData);
}

fn corrupt(store: &Store) {
    let db = DocumentDatabase::new(store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "x".into(),
            fields: vec![vec!["x".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    let document_key = store
        .scan(None, None, 32)
        .unwrap()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01D"))
        .unwrap()
        .0;
    // Unsupported direct writes to reserved keys emulate a valid-checksum
    // record containing a corrupt document exactly at the raw value limit.
    store
        .put(&document_key, &vec![b' '; 16 * 1024 * 1024])
        .unwrap();
}

#[test]
fn ordinary_index_rejects_oversized_document_instead_of_ending_pagination() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    corrupt(&store);
    assert_corruption_is_reported(&DocumentDatabase::new(&store));
}

#[test]
fn checkpoint_index_rejects_oversized_document_instead_of_ending_pagination() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&source).unwrap();
    corrupt(&store);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    assert_corruption_is_reported(&DocumentDatabase::with_checkpoint_store(&store));
}

#[test]
fn maximum_sized_valid_documents_paginate_without_loss() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "x".into(),
            fields: vec![vec!["x".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    let empty = json!({"x":0, "payload":""});
    let overhead = serde_json::to_vec(&empty).unwrap().len();
    let payload = "x".repeat(1024 * 1024 - overhead);
    for id in 0..17 {
        let document = json!({"x":id, "payload":payload});
        // Keep every encoded value exactly at the documented 1 MiB limit.
        // For two-digit numbers, shorten the string by one byte.
        let mut document = document;
        if id >= 10 {
            document["payload"] = json!(&payload[1..]);
        }
        assert_eq!(serde_json::to_vec(&document).unwrap().len(), 1024 * 1024);
        db.insert("c", &format!("{id:02}"), &document).unwrap();
    }
    let mut query = IndexQuery::default();
    let mut ids = Vec::new();
    let mut pages = 0;
    loop {
        let page = db.query_index("c", "x", &query).unwrap();
        if page.documents.is_empty() {
            break;
        }
        pages += 1;
        ids.extend(page.documents.into_iter().map(|row| row.id));
        query.after = page.next;
        assert!(query.after.is_some());
    }
    assert!(pages >= 2);
    assert_eq!(
        ids,
        (0..17).map(|id| format!("{id:02}")).collect::<Vec<_>>()
    );
}

#[test]
fn paginated_queries_report_later_oversized_entries() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "x".into(),
            fields: vec![vec!["x".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "a", &json!({"x":0})).unwrap();
    db.insert("c", "b", &json!({"x":1})).unwrap();
    let document_key = store
        .scan(None, None, 32)
        .unwrap()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01D") && key.ends_with(b"b"))
        .unwrap()
        .0;
    store
        .put(&document_key, &vec![b' '; 16 * 1024 * 1024])
        .unwrap();
    let first = db
        .query_index(
            "c",
            "x",
            &IndexQuery {
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(first.documents.len(), 1);
    assert_eq!(first.documents[0].id, "a");
    assert!(first.next.is_some());
    let next = IndexQuery {
        after: first.next,
        limit: 1,
        ..Default::default()
    };
    assert_eq!(
        db.query_index("c", "x", &next).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
    assert_eq!(
        db.query_index("c", "x", &IndexQuery::default())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}
