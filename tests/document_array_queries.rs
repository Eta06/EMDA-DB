//! Array predicates preserve bounded query, index and transaction semantics.
use database_engine::{
    AggregateRequest, CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter,
    DocumentQuery, DocumentQueryAccess, IndexDefinition, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::sync::Barrier;

mod support;
use support::retry_lock;

fn path(value: &str) -> Vec<String> {
    value.split('.').map(str::to_owned).collect()
}
fn query(filter: DocumentFilter) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned: 32,
        ..Default::default()
    }
}
fn contains(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::ArrayContains {
        path: path(name),
        value,
    }
}
fn ids(db: &DocumentDatabase<'_>, filter: DocumentFilter) -> Vec<String> {
    let q = query(filter);
    let scan = db.query("c", &q).unwrap();
    let planned = db.query_planned("c", &q).unwrap();
    assert_eq!(planned.access, DocumentQueryAccess::CollectionScan);
    assert_eq!(scan.documents, planned.result.documents);
    assert_eq!(scan.plan.matched, planned.result.plan.matched);
    scan.documents.into_iter().map(|row| row.id).collect()
}

fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "handle".into(),
            fields: vec![path("handle")],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    for (id, value) in [
        (
            "a",
            json!({"handle":"a", "tags":[1,null,{"x":1},["deep"],1], "profile":{"tags":["x","y"]},"amount":4}),
        ),
        ("b", json!({"handle":"b", "tags":[2,{"x":1}],"amount":3})),
        ("e", json!({"handle":"e", "tags":[]})),
        ("f", json!({"handle":"f", "tags":[1.0]})),
        ("m", json!({"handle":"m"})),
        ("n", json!({"handle":"n", "tags":null})),
        ("s", json!({"handle":"s", "tags":"scalar"})),
    ] {
        db.insert("c", id, &value).unwrap();
    }
}

fn exercise(db: &DocumentDatabase<'_>) {
    use DocumentFilter::{ArrayContainsAll as All, ArrayContainsAny as Any, ArrayLength};
    assert_eq!(ids(db, contains("tags", json!(1))), ["a"]);
    assert_eq!(ids(db, contains("tags", json!(1.0))), ["f"]);
    assert_eq!(ids(db, contains("tags", Value::Null)), ["a"]);
    assert_eq!(ids(db, contains("tags", json!({"x":1}))), ["a", "b"]);
    assert_eq!(ids(db, contains("tags", json!(["deep"]))), ["a"]);
    assert_eq!(ids(db, contains("profile.tags", json!("x"))), ["a"]);
    assert_eq!(
        ids(
            db,
            Any {
                path: path("tags"),
                values: vec![json!(1), json!(2)]
            }
        ),
        ["a", "b"]
    );
    assert!(
        ids(
            db,
            Any {
                path: path("tags"),
                values: vec![]
            }
        )
        .is_empty()
    );
    assert_eq!(
        ids(
            db,
            All {
                path: path("tags"),
                values: vec![json!(1), json!(1), Value::Null]
            }
        ),
        ["a"]
    );
    assert_eq!(
        ids(
            db,
            All {
                path: path("tags"),
                values: vec![]
            }
        ),
        ["a", "b", "e", "f"]
    );
    assert_eq!(
        ids(
            db,
            ArrayLength {
                path: path("tags"),
                length: 0
            }
        ),
        ["e"]
    );
    assert_eq!(
        ids(
            db,
            ArrayLength {
                path: path("tags"),
                length: 2
            }
        ),
        ["b"]
    );
    assert_eq!(
        ids(
            db,
            DocumentFilter::Not(Box::new(contains("tags", Value::Null)))
        ),
        ["b", "e", "f", "m", "n", "s"]
    );

    let q = query(DocumentFilter::And(vec![
        DocumentFilter::Equal {
            path: path("handle"),
            value: json!("a"),
        },
        contains("tags", json!({"x":1})),
    ]));
    let planned = db.query_planned("c", &q).unwrap();
    assert_eq!(
        planned.access,
        DocumentQueryAccess::UniqueEqualityIndex {
            index: "handle".into()
        }
    );
    assert_eq!(
        planned.result.documents,
        db.query("c", &q).unwrap().documents
    );
    assert_eq!(planned.result.documents[0].id, "a");
    let rejected = query(DocumentFilter::And(vec![
        DocumentFilter::Equal {
            path: path("handle"),
            value: json!("a"),
        },
        contains("tags", json!("absent")),
    ]));
    let planned = db.query_planned("c", &rejected).unwrap();
    assert_eq!(
        planned.access,
        DocumentQueryAccess::UniqueEqualityIndex {
            index: "handle".into()
        }
    );
    assert!(planned.result.documents.is_empty());
    assert_eq!(
        planned.result.documents,
        db.query("c", &rejected).unwrap().documents
    );

    let aggregation = db
        .aggregate(
            "c",
            &AggregateRequest {
                filter: contains("tags", json!({"x":1})),
                group_by: None,
                sum: Some(path("amount")),
                max_scanned: 32,
            },
        )
        .unwrap();
    assert_eq!(
        (aggregation.groups[0].count, aggregation.groups[0].sum),
        (2, Some(7))
    );
    assert_eq!(db.verify_collection("c").unwrap().documents, 7);
}

#[test]
fn array_predicates_work_on_both_backends_and_after_recovery() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            prepare(&db);
            exercise(&db);
        } else {
            let db = DocumentDatabase::new(&store);
            prepare(&db);
            exercise(&db);
            drop(store);
        }
        let recovered = retry_lock(|| Store::open(&source)).unwrap();
        exercise(&DocumentDatabase::new(&recovered));
    }
}

fn limits(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    let mut array = vec![json!(0); 4_000];
    *array.last_mut().unwrap() = json!(1);
    for id in ["a", "b"] {
        db.insert("c", id, &json!({"tags": array})).unwrap();
    }
    let repeated =
        |count| DocumentFilter::And((0..count).map(|_| contains("tags", json!(1))).collect());
    assert_eq!(ids(db, repeated(125)), ["a", "b"]); // exactly one million comparisons
    let error = db.query("c", &query(repeated(126))).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("comparison limit"));
    let transaction_error = db
        .transaction(|tx| {
            tx.put_raw(b"array-query-rollback", b"must not commit")?;
            tx.query("c", &query(repeated(126)))?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(transaction_error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"array-query-rollback"))
            .unwrap(),
        None
    );
    let exact_values = DocumentFilter::ArrayContainsAny {
        path: path("absent"),
        values: vec![json!(0); 256],
    };
    assert!(
        db.query("c", &query(exact_values))
            .unwrap()
            .documents
            .is_empty()
    );
    let too_many = DocumentFilter::And(vec![
        DocumentFilter::ArrayContainsAny {
            path: path("absent"),
            values: vec![json!(0); 256],
        },
        contains("absent", json!(0)),
    ]);
    assert_eq!(
        db.query("c", &query(too_many)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let exact_byte_value = json!("x".repeat(1024 * 1024 - 2));
    let exact_bytes = DocumentFilter::ArrayContainsAny {
        path: path("absent"),
        values: vec![exact_byte_value.clone(); 4],
    };
    assert!(
        db.query("c", &query(exact_bytes))
            .unwrap()
            .documents
            .is_empty()
    );
    let too_large = DocumentFilter::ArrayContainsAny {
        path: path("absent"),
        values: vec![
            exact_byte_value.clone(),
            exact_byte_value.clone(),
            exact_byte_value.clone(),
            exact_byte_value,
            Value::Null,
        ],
    };
    assert_eq!(
        db.query("c", &query(too_large)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let invalid_path = DocumentFilter::ArrayLength {
        path: vec![],
        length: 0,
    };
    assert_eq!(
        db.query("c", &query(invalid_path)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        db.get("c", "a").unwrap().unwrap()["tags"]
            .as_array()
            .unwrap()
            .len(),
        4_000
    );
}

#[test]
fn array_predicate_limits_fail_closed_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            limits(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            limits(&DocumentDatabase::new(&store));
        }
    }
}

fn phantom(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert("c", "new", &json!({"tags":["match"]})).unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            assert!(
                tx.query("c", &query(contains("tags", json!("match"))))?
                    .documents
                    .is_empty()
            );
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
fn array_scan_detects_phantoms_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            phantom(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            phantom(&DocumentDatabase::new(&store));
        }
    }
}
