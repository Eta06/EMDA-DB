use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, FieldRule, FieldType, IndexDefinition,
    IndexValue, PartialFilter, Store,
};
use serde_json::json;
use std::io;
use std::sync::{Arc, Barrier};

fn path(value: &str) -> Vec<String> {
    vec![value.to_owned()]
}
fn schema() -> CollectionSchema {
    CollectionSchema {
        fields: vec![
            FieldRule {
                path: path("email"),
                field_type: FieldType::String,
                required: true,
                nullable: false,
            },
            FieldRule {
                path: path("age"),
                field_type: FieldType::Integer,
                required: false,
                nullable: true,
            },
        ],
    }
}
fn email_index() -> IndexDefinition {
    IndexDefinition {
        name: "email".into(),
        fields: vec![path("email")],
        unique: true,
        partial: None,
    }
}
fn exercise(db: &DocumentDatabase<'_>) {
    db.create_collection("users", schema()).unwrap();
    db.insert(
        "users",
        "1",
        &json!({"email":"one@example.test", "age":null, "profile":{"name":"one"}}),
    )
    .unwrap();
    assert_eq!(db.get("users", "1").unwrap().unwrap()["age"], json!(null));
    assert_eq!(db.get("users", "missing").unwrap(), None);
    assert_eq!(
        db.insert("users", "bad", &json!({"age":3}))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        db.insert("missing", "1", &json!({})).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    db.create_index("users", email_index()).unwrap();
    db.create_index(
        "users",
        IndexDefinition {
            name: "compound".into(),
            fields: vec![path("age"), path("email")],
            unique: false,
            partial: Some(PartialFilter::Equals {
                path: path("active"),
                value: IndexValue::Boolean(true),
            }),
        },
    )
    .unwrap();
    db.insert(
        "users",
        "2",
        &json!({"email":"two@example.test", "age":3,"active":true}),
    )
    .unwrap();
    assert_eq!(
        db.insert("users", "3", &json!({"email":"two@example.test"}))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    db.replace(
        "users",
        "2",
        &json!({"email":"changed@example.test","age":4,"active":true}),
        false,
    )
    .unwrap();
    db.insert("users", "3", &json!({"email":"two@example.test"}))
        .unwrap();
    let outcome = db.transaction(|tx| {
        tx.put_raw(b"audit", b"committed")?;
        tx.replace("users", "1", &json!({"email":"new@example.test"}), false)?;
        Ok(())
    });
    outcome.unwrap();
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"audit")).unwrap(),
        Some(b"committed".to_vec())
    );
    let result = db.transaction(|tx| {
        tx.put_raw(b"rollback", b"no")?;
        assert!(
            tx.insert("users", "4", &json!({"email":"two@example.test"}))
                .is_err()
        );
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);
    assert!(db.delete("users", "2").unwrap());
    assert!(!db.delete("users", "2").unwrap());
    db.insert("users", "5", &json!({"email":"changed@example.test"}))
        .unwrap();
    db.drop_index("users", "compound").unwrap();
    assert_eq!(db.indexes("users").unwrap(), vec![email_index()]);
}

#[test]
fn documents_and_indexes_work_on_both_backends_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let ordinary_path = dir.path().join("ordinary");
    let checkpoint_path = dir.path().join("checkpoint");
    let file = dir.path().join("index");
    {
        let store = Store::open(&ordinary_path).unwrap();
        exercise(&DocumentDatabase::new(&store));
    }
    {
        let store = Store::open(&ordinary_path).unwrap();
        let db = DocumentDatabase::new(&store);
        assert_eq!(
            db.get("users", "1").unwrap().unwrap()["email"],
            "new@example.test"
        );
        assert_eq!(
            db.insert("users", "6", &json!({"email":"changed@example.test"}))
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
    }
    let store = Store::open(&checkpoint_path).unwrap();
    let digest = store.checkpoint_to(&file).unwrap();
    drop(store);
    {
        let store = CheckpointStore::open(&checkpoint_path, &file, &digest).unwrap();
        exercise(&DocumentDatabase::with_checkpoint_store(&store));
    }
    let store = CheckpointStore::open(&checkpoint_path, &file, &digest).unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    assert_eq!(
        db.get("users", "1").unwrap().unwrap()["email"],
        "new@example.test"
    );
    assert_eq!(
        db.insert("users", "6", &json!({"email":"changed@example.test"}))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
}

#[test]
fn concurrent_unique_inserts_have_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let db = DocumentDatabase::new(&store);
    db.create_collection("users", schema()).unwrap();
    db.create_index("users", email_index()).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let mut threads = Vec::new();
    for number in 0..8 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let db = DocumentDatabase::new(&store);
            barrier.wait();
            loop {
                match db.insert(
                    "users",
                    &number.to_string(),
                    &json!({"email":"same@example.test"}),
                ) {
                    Ok(()) => break true,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => break false,
                    Err(error) => panic!("unexpected insert error: {error}"),
                }
            }
        }));
    }
    assert_eq!(
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(|won| *won)
            .count(),
        1
    );
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    let db = DocumentDatabase::new(&reopened);
    let found = (0..8)
        .filter(|n| db.get("users", &n.to_string()).unwrap().is_some())
        .count();
    assert_eq!(found, 1);
}

fn query_behavior(db: &DocumentDatabase<'_>) {
    use database_engine::{IndexBound, IndexQuery};
    db.create_collection("items", CollectionSchema::default())
        .unwrap();
    let values = [
        json!(null),
        json!(false),
        json!(true),
        json!(-8),
        json!(0),
        json!(3),
        json!(3),
        json!(""),
        json!("a\0b"),
        json!("é"),
    ];
    for (number, value) in values.iter().enumerate() {
        db.insert(
            "items",
            &format!("id-{number:02}"),
            &json!({"value":value, "active":number % 2 == 0, "group":"g"}),
        )
        .unwrap();
    }
    db.insert("items", "missing", &json!({"group":"g"}))
        .unwrap();
    db.create_index(
        "items",
        IndexDefinition {
            name: "value".into(),
            fields: vec![path("value")],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    db.create_index(
        "items",
        IndexDefinition {
            name: "compound".into(),
            fields: vec![path("group"), path("value")],
            unique: false,
            partial: Some(PartialFilter::Equals {
                path: path("active"),
                value: IndexValue::Boolean(true),
            }),
        },
    )
    .unwrap();
    let mut query = IndexQuery {
        limit: 3,
        ..IndexQuery::default()
    };
    let mut ids = Vec::new();
    loop {
        let page = db.query_index("items", "value", &query).unwrap();
        if page.documents.is_empty() {
            break;
        }
        ids.extend(page.documents.iter().map(|row| row.id.clone()));
        query.after = page.next;
    }
    assert_eq!(
        ids,
        (0..10).map(|n| format!("id-{n:02}")).collect::<Vec<_>>()
    );
    let range = IndexQuery {
        lower: Some(IndexBound {
            value: IndexValue::Integer(-8),
            inclusive: false,
        }),
        upper: Some(IndexBound {
            value: IndexValue::Integer(3),
            inclusive: true,
        }),
        ..IndexQuery::default()
    };
    let page = db.query_index("items", "value", &range).unwrap();
    assert_eq!(
        page.documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["id-04", "id-05", "id-06"]
    );
    let exact = IndexQuery {
        equal: vec![IndexValue::Integer(3)],
        ..IndexQuery::default()
    };
    let page = db.query_index("items", "value", &exact).unwrap();
    assert_eq!(
        page.documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["id-05", "id-06"]
    );
    let partial = IndexQuery {
        equal: vec![IndexValue::String("g".into())],
        lower: Some(IndexBound {
            value: IndexValue::Integer(0),
            inclusive: true,
        }),
        upper: Some(IndexBound {
            value: IndexValue::String("z".into()),
            inclusive: false,
        }),
        ..IndexQuery::default()
    };
    let page = db.query_index("items", "compound", &partial).unwrap();
    assert_eq!(
        page.documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["id-04", "id-06", "id-08"]
    );
    db.replace(
        "items",
        "id-04",
        &json!({"value":9,"active":false,"group":"g"}),
        false,
    )
    .unwrap();
    let page = db.query_index("items", "compound", &partial).unwrap();
    assert_eq!(
        page.documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["id-06", "id-08"]
    );
}

#[test]
fn indexed_queries_order_bound_and_paginate_on_both_backends() {
    let dir = tempfile::tempdir().unwrap();
    let ordinary = Store::open(dir.path().join("ordinary")).unwrap();
    query_behavior(&DocumentDatabase::new(&ordinary));
    let path = dir.path().join("checkpoint");
    let checkpoint = dir.path().join("base");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = CheckpointStore::open(&path, checkpoint, &digest).unwrap();
    query_behavior(&DocumentDatabase::with_checkpoint_store(&store));
}

#[test]
fn conditional_updates_are_atomic_and_preserve_missing_vs_null() {
    use database_engine::{DocumentUpdate, FieldCondition};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "id", &json!({"number":1,"explicit":null}))
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "number".into(),
            fields: vec![path("number")],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    let conditions = [
        FieldCondition {
            path: path("absent"),
            expected: None,
        },
        FieldCondition {
            path: path("explicit"),
            expected: Some(json!(null)),
        },
    ];
    let updates = [
        DocumentUpdate::Increment {
            path: path("number"),
            amount: 2,
        },
        DocumentUpdate::Set {
            path: vec!["nested".into(), "v".into()],
            value: json!("ok"),
        },
        DocumentUpdate::Unset {
            path: path("explicit"),
        },
    ];
    assert!(
        db.update_if("c", "id", &conditions, &updates, false)
            .unwrap()
    );
    assert_eq!(
        db.get("c", "id").unwrap().unwrap(),
        json!({"number":3,"nested":{"v":"ok"}})
    );
    assert!(
        !db.update_if("c", "id", &conditions, &updates, false)
            .unwrap()
    );
    let bad = [
        DocumentUpdate::Set {
            path: path("number"),
            value: json!(i64::MAX),
        },
        DocumentUpdate::Increment {
            path: path("number"),
            amount: 1,
        },
    ];
    assert_eq!(
        db.update_if("c", "id", &[], &bad, false)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "id").unwrap().unwrap()["number"], 3);
    assert!(
        db.update_if(
            "c",
            "new",
            &[FieldCondition {
                path: path("missing"),
                expected: None
            }],
            &[DocumentUpdate::Increment {
                path: path("number"),
                amount: 7
            }],
            true
        )
        .unwrap()
    );
    assert_eq!(db.get("c", "new").unwrap().unwrap(), json!({"number":7}));
    let query = database_engine::IndexQuery {
        equal: vec![IndexValue::Integer(3)],
        ..Default::default()
    };
    assert_eq!(
        db.query_index("c", "number", &query).unwrap().documents[0].id,
        "id"
    );
}

#[test]
fn bounded_scan_reports_work_and_applies_filter_sort_and_projection() {
    use database_engine::{Comparison, DocumentFilter, DocumentQuery, DocumentSort};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert(
        "c",
        "a",
        &json!({"score":2,"person":{"name":"Ada","secret":"x"}}),
    )
    .unwrap();
    db.insert(
        "c",
        "b",
        &json!({"score":3,"person":{"name":"Bea"},"flag":null}),
    )
    .unwrap();
    db.insert("c", "c", &json!({"score":3,"person":{"name":"Cal"}}))
        .unwrap();
    let query = DocumentQuery {
        filter: DocumentFilter::And(vec![
            DocumentFilter::Compare {
                path: path("score"),
                comparison: Comparison::GreaterOrEqual,
                value: IndexValue::Integer(2),
            },
            DocumentFilter::Not(Box::new(DocumentFilter::Exists {
                path: path("flag"),
                exists: true,
            })),
        ]),
        sort: Some(DocumentSort {
            path: path("score"),
            descending: true,
        }),
        projection: Some(vec![vec!["person".into(), "name".into()]]),
        offset: 0,
        limit: 10,
        max_scanned: 3,
    };
    let result = db.query("c", &query).unwrap();
    assert_eq!(
        result
            .documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["c", "a"]
    );
    assert_eq!(
        result.documents[0].document,
        json!({"person":{"name":"Cal"}})
    );
    assert_eq!(
        (
            result.plan.scanned,
            result.plan.matched,
            result.plan.returned,
            result.plan.sorted
        ),
        (3, 2, 2, true)
    );
    let mut too_small = query.clone();
    too_small.max_scanned = 2;
    assert_eq!(
        db.query("c", &too_small).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let mut overlapping = query;
    overlapping.projection = Some(vec![path("person"), vec!["person".into(), "name".into()]]);
    assert_eq!(
        db.query("c", &overlapping).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn concurrent_atomic_increments_survive_reopen() {
    use database_engine::DocumentUpdate;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "id", &json!({"count":0})).unwrap();
    let mut threads = Vec::new();
    for _ in 0..4 {
        let store = Arc::clone(&store);
        threads.push(std::thread::spawn(move || {
            let db = DocumentDatabase::new(&store);
            for _ in 0..20 {
                loop {
                    match db.update_if(
                        "c",
                        "id",
                        &[],
                        &[DocumentUpdate::Increment {
                            path: path("count"),
                            amount: 1,
                        }],
                        false,
                    ) {
                        Ok(true) => break,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                        result => panic!("unexpected update result: {result:?}"),
                    }
                }
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    drop(store);
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(
        DocumentDatabase::new(&store)
            .get("c", "id")
            .unwrap()
            .unwrap()["count"],
        80
    );
}

#[test]
fn bulk_schema_changes_and_collection_deletion_are_atomic() {
    use database_engine::{BulkOperation, BulkResult};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "unique".into(),
            fields: vec![path("x")],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    let first = json!({"x":1});
    let second = json!({"x":2});
    let duplicate = json!({"x":1});
    let bad = [
        BulkOperation::Insert {
            collection: "c",
            id: "a",
            document: &first,
        },
        BulkOperation::Insert {
            collection: "c",
            id: "b",
            document: &duplicate,
        },
    ];
    assert_eq!(
        db.bulk(&bad).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(db.get("c", "a").unwrap(), None);
    let good = [
        BulkOperation::Insert {
            collection: "c",
            id: "a",
            document: &first,
        },
        BulkOperation::Insert {
            collection: "c",
            id: "b",
            document: &second,
        },
        BulkOperation::Delete {
            collection: "c",
            id: "absent",
        },
    ];
    assert_eq!(
        db.bulk(&good).unwrap(),
        vec![
            BulkResult::Written,
            BulkResult::Written,
            BulkResult::Deleted(false)
        ]
    );
    let required = CollectionSchema {
        fields: vec![FieldRule {
            path: path("required"),
            field_type: FieldType::String,
            required: true,
            nullable: false,
        }],
    };
    assert_eq!(
        db.set_schema("c", required).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(first.clone()));
    db.drop_collection("c").unwrap();
    assert_eq!(
        db.get("c", "a").unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "unique".into(),
            fields: vec![path("x")],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "again", &first).unwrap();
}

#[test]
fn aggregate_and_lookup_use_bounded_consistent_document_reads() {
    use database_engine::{AggregateRequest, DocumentFilter, DocumentQuery};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("people", CollectionSchema::default())
        .unwrap();
    db.create_collection("orders", CollectionSchema::default())
        .unwrap();
    db.insert("people", "ada", &json!({"name":"Ada"})).unwrap();
    db.insert(
        "orders",
        "a",
        &json!({"person":"ada","group":"x","amount":4}),
    )
    .unwrap();
    db.insert(
        "orders",
        "b",
        &json!({"person":"missing","group":"x","amount":5}),
    )
    .unwrap();
    db.insert(
        "orders",
        "c",
        &json!({"person":null,"group":null,"amount":3}),
    )
    .unwrap();
    db.insert("orders", "d", &json!({"amount":null})).unwrap();
    let aggregate = db
        .aggregate(
            "orders",
            &AggregateRequest {
                filter: DocumentFilter::All,
                group_by: Some(path("group")),
                sum: Some(path("amount")),
                max_scanned: 4,
            },
        )
        .unwrap();
    assert_eq!(
        aggregate
            .groups
            .iter()
            .map(|g| (g.key.clone(), g.count, g.sum))
            .collect::<Vec<_>>(),
        vec![
            (None, 1, Some(0)),
            (Some(IndexValue::Null), 1, Some(3)),
            (Some(IndexValue::String("x".into())), 2, Some(9))
        ]
    );
    assert_eq!(aggregate.plan.scanned, 4);
    let result = db
        .lookup(
            "orders",
            &DocumentQuery {
                max_scanned: 4,
                ..Default::default()
            },
            &path("person"),
            "people",
        )
        .unwrap();
    assert_eq!(result.documents[0].related.as_ref().unwrap()["name"], "Ada");
    assert!(
        result.documents[1..]
            .iter()
            .all(|row| row.related.is_none())
    );
    assert_eq!(
        db.aggregate(
            "orders",
            &AggregateRequest {
                filter: DocumentFilter::All,
                group_by: None,
                sum: None,
                max_scanned: 3
            }
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn read_set_transactions_allow_disjoint_document_inserts_and_deadlines_do_not_submit() {
    use database_engine::{DocumentTransactionMode, TimedTransactionError};
    use std::time::{Duration, Instant};
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    DocumentDatabase::new(&store)
        .create_collection("c", CollectionSchema::default())
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut threads = Vec::new();
    for id in ["a", "b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            DocumentDatabase::new(&store).transaction_with_read_set(|tx| {
                tx.insert("c", id, &json!({"n":id}))?;
                barrier.wait();
                Ok(())
            })
        }));
    }
    for thread in threads {
        thread.join().unwrap().unwrap();
    }
    let db = DocumentDatabase::new(&store);
    let result = db.transaction_until(
        Instant::now() - Duration::from_millis(1),
        DocumentTransactionMode::ReadSet,
        |tx| tx.insert("c", "late", &json!({})),
    );
    assert!(matches!(
        result,
        Err(TimedTransactionError::DeadlineExceeded)
    ));
    assert_eq!(db.get("c", "late").unwrap(), None);
}
