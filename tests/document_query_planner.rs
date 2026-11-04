//! Planned queries must preserve filtering and serializable index observations.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, IndexValue, PartialFilter, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::sync::Barrier;

mod support;
use support::retry_lock;

fn path(name: &str) -> Vec<String> {
    name.split('.').map(str::to_owned).collect()
}
fn equal(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(name),
        value,
    }
}
fn index(
    name: &str,
    fields: &[&str],
    unique: bool,
    partial: Option<PartialFilter>,
) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: fields.iter().map(|name| path(name)).collect(),
        unique,
        partial,
    }
}
fn compare(db: &DocumentDatabase<'_>, query: &DocumentQuery, access: DocumentQueryAccess) {
    let scan = db.query("c", query).unwrap();
    let planned = db.query_planned("c", query).unwrap();
    assert_eq!(planned.access, access);
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
    assert_eq!(planned.result.plan.returned, scan.plan.returned);
    assert_eq!(planned.result.plan.sorted, scan.plan.sorted);
}
fn exercise(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("email", &["email"], true, None))
        .unwrap();
    db.create_index("c", index("nonunique", &["score"], false, None))
        .unwrap();
    db.create_index("c", index("compound", &["group", "score"], true, None))
        .unwrap();
    db.create_index(
        "c",
        index(
            "partial",
            &["rank"],
            true,
            Some(PartialFilter::Equals {
                path: path("active"),
                value: IndexValue::Boolean(true),
            }),
        ),
    )
    .unwrap();
    db.insert("c", "a", &json!({"email":"a@example", "score":2, "group":"x", "rank":1, "active":true, "profile":{"n":"A"}})).unwrap();
    db.insert(
        "c",
        "b",
        &json!({"email":"b@example", "score":3, "group":"x", "rank":1, "active":false}),
    )
    .unwrap();
    db.insert("c", "n", &json!({"email":null, "score":1, "group":"x"}))
        .unwrap();
    db.insert("c", "m", &json!({"score":4, "group":"x"}))
        .unwrap();
    let indexed = DocumentQueryAccess::UniqueEqualityIndex {
        index: "email".into(),
    };
    for filter in [
        equal("email", json!("a@example")),
        equal("email", Value::Null),
        equal("email", json!("missing")),
        DocumentFilter::And(vec![
            DocumentFilter::Exists {
                path: path("score"),
                exists: true,
            },
            DocumentFilter::And(vec![
                equal("email", json!("a@example")),
                equal("score", json!(2)),
            ]),
        ]),
        DocumentFilter::And(vec![
            equal("email", json!("a@example")),
            equal("email", json!("b@example")),
        ]),
    ] {
        for offset in [0, 1] {
            let query = DocumentQuery {
                filter: filter.clone(),
                offset,
                limit: 2,
                max_scanned: 10,
                sort: Some(DocumentSort {
                    path: path("score"),
                    descending: true,
                }),
                projection: Some(vec![path("email"), path("profile.n")]),
            };
            compare(db, &query, indexed.clone());
        }
    }
    for filter in [
        DocumentFilter::All,
        DocumentFilter::Or(vec![
            equal("email", json!("a@example")),
            equal("email", json!("b@example")),
        ]),
        DocumentFilter::Not(Box::new(equal("email", json!("a@example")))),
        DocumentFilter::And(vec![
            DocumentFilter::Or(vec![
                equal("email", json!("a@example")),
                DocumentFilter::All,
            ]),
            DocumentFilter::Exists {
                path: path("score"),
                exists: true,
            },
        ]),
        equal("group", json!("x")),
        equal("rank", json!(1)),
        equal("email", json!([1])),
    ] {
        compare(
            db,
            &DocumentQuery {
                filter,
                max_scanned: 10,
                ..Default::default()
            },
            DocumentQueryAccess::CollectionScan,
        );
    }
    compare(
        db,
        &DocumentQuery {
            filter: equal("score", json!(2)),
            max_scanned: 10,
            ..Default::default()
        },
        DocumentQueryAccess::EqualityIndex {
            index: "nonunique".into(),
        },
    );
    let bad = DocumentQuery {
        filter: equal("email", json!("a@example")),
        max_scanned: 0,
        ..Default::default()
    };
    assert_eq!(
        db.query_planned("c", &bad).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let no_collection = db
        .query_planned("missing", &DocumentQuery::default())
        .unwrap_err();
    assert_eq!(no_collection.kind(), ErrorKind::NotFound);
}

#[test]
fn planner_matches_complete_scans_on_both_storage_backends_and_after_reopen() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            exercise(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            exercise(&DocumentDatabase::new(&store));
            drop(store);
        }
        let store = retry_lock(|| Store::open(&path)).unwrap();
        let db = DocumentDatabase::new(&store);
        compare(
            &db,
            &DocumentQuery {
                filter: equal("email", json!("a@example")),
                max_scanned: 10,
                ..Default::default()
            },
            DocumentQueryAccess::UniqueEqualityIndex {
                index: "email".into(),
            },
        );
        assert_eq!(db.verify_collection("c").unwrap().documents, 4);
    }
}

#[test]
fn unique_equality_query_scales_past_complete_scan_limit() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            large_collection(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            large_collection(&DocumentDatabase::new(&store));
        }
    }
}
fn large_collection(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("email", &["email"], true, None))
        .unwrap();
    for start in (0..1100).step_by(20) {
        db.transaction(|tx| {
            for i in start..start + 20 {
                tx.insert(
                    "c",
                    &format!("{i:04}"),
                    &json!({"email":format!("user{i:04}")}),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
    let query = DocumentQuery {
        filter: equal("email", json!("user0500")),
        max_scanned: 1,
        ..Default::default()
    };
    assert_eq!(
        db.query("c", &query).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let planned = db.query_planned("c", &query).unwrap();
    assert_eq!(
        planned.access,
        DocumentQueryAccess::UniqueEqualityIndex {
            index: "email".into()
        }
    );
    assert_eq!(planned.result.plan.scanned, 1);
    assert_eq!(planned.result.documents[0].id, "0500");
    let absent = DocumentQuery {
        filter: equal("email", json!("absent")),
        ..query
    };
    let result = db.query_planned("c", &absent).unwrap();
    assert!(result.result.documents.is_empty());
    assert_eq!(result.result.plan.scanned, 0);
}

fn phantom(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("email", &["email"], true, None))
        .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert("c", "new", &json!({"email":"reserved"})).unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let query = DocumentQuery {
                filter: equal("email", json!("reserved")),
                max_scanned: 1,
                ..Default::default()
            };
            let planned = tx.query_planned("c", &query)?;
            assert!(planned.result.documents.is_empty());
            entered.wait();
            changed.wait();
            tx.put_raw(b"phantom", b"must roll back")?;
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        writer.join().unwrap();
    });
    assert_eq!(db.transaction(|tx| tx.get_raw(b"phantom")).unwrap(), None);
}

#[test]
fn planned_empty_index_range_detects_concurrent_phantoms_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
            phantom(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            phantom(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn selected_index_corruption_fails_closed_when_the_entry_is_present() {
    for corrupt_unique in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index("email", &["email"], true, None))
            .unwrap();
        db.insert("c", "id", &json!({"email":"value"})).unwrap();
        let prefix = if corrupt_unique {
            b"\0emda-document\x01U"
        } else {
            b"\0emda-document\x01I"
        };
        let key = store
            .scan(None, None, 100)
            .unwrap()
            .into_iter()
            .find(|(key, _)| key.starts_with(prefix))
            .unwrap()
            .0;
        store.put(&key, b"invalid").unwrap();
        let query = DocumentQuery {
            filter: equal("email", json!("value")),
            ..Default::default()
        };
        assert_eq!(
            db.query_planned("c", &query).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}

#[test]
fn missing_index_entries_require_integrity_verification() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("email", &["email"], true, None))
        .unwrap();
    db.insert("c", "id", &json!({"email":"value"})).unwrap();
    let key = store
        .scan(None, None, 100)
        .unwrap()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01I"))
        .unwrap()
        .0;
    // Raw writes to the reserved namespace are unsupported. This test makes
    // the documented omission risk visible rather than claiming detection.
    store.delete(&key).unwrap();
    let query = DocumentQuery {
        filter: equal("email", json!("value")),
        ..Default::default()
    };
    assert_eq!(db.query("c", &query).unwrap().documents.len(), 1);
    assert!(
        db.query_planned("c", &query)
            .unwrap()
            .result
            .documents
            .is_empty()
    );
    assert_eq!(
        db.verify_collection("c").unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}

fn changed_index_definition_conflicts(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("email", &["email"], true, None))
        .unwrap();
    db.insert("c", "id", &json!({"email":"value"})).unwrap();
    let read = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            read.wait();
            db.drop_index("c", "email").unwrap();
            changed.wait();
        });
        let outcome = db.transaction_with_read_set(|tx| {
            let query = DocumentQuery {
                filter: equal("email", json!("value")),
                ..Default::default()
            };
            let result = tx.query_planned("c", &query)?;
            assert_eq!(result.result.documents.len(), 1);
            read.wait();
            changed.wait();
            tx.put_raw(b"changed-index", b"must roll back")?;
            Ok(())
        });
        assert_eq!(outcome.unwrap_err().kind(), ErrorKind::WouldBlock);
        writer.join().unwrap();
    });
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"changed-index")).unwrap(),
        None
    );
    assert_eq!(
        db.query_planned(
            "c",
            &DocumentQuery {
                filter: equal("email", json!("value")),
                ..Default::default()
            }
        )
        .unwrap()
        .access,
        DocumentQueryAccess::CollectionScan
    );
}

#[test]
fn planned_query_conflicts_when_its_index_is_dropped_concurrently() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            changed_index_definition_conflicts(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            changed_index_definition_conflicts(&DocumentDatabase::new(&store));
        }
    }
}
