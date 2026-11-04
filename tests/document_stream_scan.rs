//! Streaming collection scans must preserve filtering and failure semantics.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentSort, Store,
};
use serde_json::json;
use std::io::ErrorKind;

mod support;
use support::retry_lock;

fn compare_results(db: &DocumentDatabase<'_>) {
    let result = db
        .query(
            "c",
            &DocumentQuery {
                filter: DocumentFilter::Equal {
                    path: vec!["group".into()],
                    value: json!("yes"),
                },
                max_scanned: 100,
                sort: Some(DocumentSort {
                    path: vec!["score".into()],
                    descending: true,
                }),
                projection: Some(vec![vec!["score".into()]]),
                offset: 1,
                limit: 7,
            },
        )
        .unwrap();
    assert_eq!(result.plan.scanned, 70);
    assert_eq!(result.plan.matched, 35);
    assert!(result.plan.sorted);
    assert_eq!(result.plan.returned, 7);
    assert_eq!(
        result
            .documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        ["id66", "id64", "id62", "id60", "id58", "id56", "id54"]
    );
    for (row, score) in result.documents.iter().zip([66, 64, 62, 60, 58, 56, 54]) {
        assert_eq!(row.document, json!({"score": score}));
    }
    let unsorted = db
        .query(
            "c",
            &DocumentQuery {
                filter: DocumentFilter::Equal {
                    path: vec!["group".into()],
                    value: json!("yes"),
                },
                max_scanned: 100,
                offset: 5,
                limit: 3,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(unsorted.plan.scanned, 70);
    assert_eq!(unsorted.plan.matched, 35);
    assert_eq!(unsorted.plan.returned, 3);
    assert!(!unsorted.plan.sorted);
    assert_eq!(
        unsorted
            .documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        ["id10", "id12", "id14"]
    );
}

#[test]
fn streaming_scan_preserves_results_across_pages_and_recovery() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    for id in 0..70 {
        db.insert(
            "c",
            &format!("id{id:02}"),
            &json!({"group": if id % 2 == 0 {"yes"} else {"no"}, "score":id}),
        )
        .unwrap();
    }
    compare_results(&db);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let checkpoint_store =
        retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    compare_results(&DocumentDatabase::with_checkpoint_store(&checkpoint_store));
    drop(checkpoint_store);
    let ordinary = retry_lock(|| Store::open(&path)).unwrap();
    compare_results(&DocumentDatabase::new(&ordinary));
}

fn assert_error_precedence(db: &DocumentDatabase<'_>) {
    let request = |max_scanned| DocumentQuery {
        max_scanned,
        sort: Some(DocumentSort {
            path: vec!["sort".into()],
            descending: false,
        }),
        ..Default::default()
    };
    // The first valid document has an unindexable sort value. A later scan
    // budget error and later corrupt document each take precedence, as before.
    let limit = db.query("c", &request(1)).unwrap_err();
    assert_eq!(limit.kind(), ErrorKind::InvalidInput);
    assert!(
        limit
            .to_string()
            .contains("document scan exceeds the operation limit")
    );
    let corruption = db.query("c", &request(10)).unwrap_err();
    assert_eq!(corruption.kind(), ErrorKind::InvalidData);
}

#[test]
fn scan_and_corruption_errors_precede_earlier_filter_or_sort_errors() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"sort": [1]})).unwrap();
    db.insert("c", "b", &json!({"sort": 2})).unwrap();
    let key = store
        .scan(None, None, 32)
        .unwrap()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01D") && key.ends_with(b"b"))
        .unwrap()
        .0;
    // An unsupported raw write emulates a checksummed but invalid document.
    store.put(&key, b"not JSON").unwrap();
    assert_error_precedence(&DocumentDatabase::new(&store));
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_error_precedence(&DocumentDatabase::with_checkpoint_store(&store));
}
