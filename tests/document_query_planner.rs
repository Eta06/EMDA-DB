//! Planned queries must preserve filtering and serializable index observations.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentReadLimits, DocumentSnapshot, DocumentSort,
    DocumentTransactionMode, IndexBound, IndexDefinition, IndexValue, PartialFilter, Store,
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
    let explanation = db.explain_query("c", query).unwrap();
    assert_eq!(explanation.access, access);
    if matches!(
        access,
        DocumentQueryAccess::UniqueEqualityIndex { .. }
            | DocumentQueryAccess::UniqueCompoundEqualityIndex { .. }
            | DocumentQueryAccess::PartialUniqueEqualityIndex { .. }
    ) {
        assert_eq!(explanation.probes.len(), 1);
        assert!(explanation.probes[0].unique_equality);
    }
    assert_eq!(planned.access, access);
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
    assert_eq!(planned.result.plan.returned, scan.plan.returned);
    assert_eq!(planned.result.plan.sorted, scan.plan.sorted);
}

fn check_index_order_error_precedence(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("score", &["score"], false, None))
        .unwrap();
    db.insert("c", "z", &json!({"score":1, "bad":[], "tags":[1]}))
        .unwrap();
    db.insert(
        "c",
        "a",
        &json!({"score":2, "bad":"x".repeat(257), "tags":[1]}),
    )
    .unwrap();
    let query = DocumentQuery {
        filter: DocumentFilter::In {
            path: path("score"),
            values: vec![json!(1), json!(2)],
        },
        sort: Some(DocumentSort {
            path: path("bad"),
            descending: false,
        }),
        offset: 2,
        limit: 1,
        max_scanned: 2,
        ..Default::default()
    };
    let scan_error = db.query("c", &query).unwrap_err();
    assert_eq!(scan_error.to_string(), "indexed string exceeds 256 bytes");
    let planned_error = db.query_planned("c", &query).unwrap_err();
    assert_eq!(planned_error.kind(), scan_error.kind());
    assert_eq!(planned_error.to_string(), scan_error.to_string());
    let array_query = DocumentQuery {
        filter: DocumentFilter::And(vec![
            query.filter.clone(),
            DocumentFilter::ArrayContains {
                path: path("tags"),
                value: json!(1),
            },
        ]),
        ..query.clone()
    };
    assert_eq!(
        db.query_planned("c", &array_query).unwrap_err().to_string(),
        scan_error.to_string()
    );
    let limit_error = db
        .query_planned(
            "c",
            &DocumentQuery {
                max_scanned: 1,
                ..query
            },
        )
        .unwrap_err();
    assert_eq!(
        limit_error.to_string(),
        "document scan exceeds the operation limit"
    );
}

#[test]
fn planned_queries_report_errors_in_document_order_after_probe_admission() {
    for checkpoint in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let store = Store::open(&path).unwrap();
        if checkpoint {
            let base = directory.path().join("checkpoint");
            let digest = store.checkpoint_to(&base).unwrap();
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
            check_index_order_error_precedence(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            check_index_order_error_precedence(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn explain_planned_query_uses_metadata_without_consuming_candidates() {
    for checkpoint in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index("group_score", &["group", "score"], false, None))
            .unwrap();
        for (id, score) in [("a", 2), ("b", 3)] {
            db.insert("c", id, &json!({"group":"x", "score":score}))
                .unwrap();
        }
        if checkpoint {
            let base = directory.path().join("checkpoint");
            let digest = store.checkpoint_to(&base).unwrap();
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
            let snapshot = store.snapshot().unwrap();
            check_explanation(
                &DocumentDatabase::with_checkpoint_store(&store),
                &DocumentSnapshot::with_checkpoint_snapshot(&snapshot),
            );
        } else {
            let snapshot = store.snapshot(16, 4096).unwrap();
            check_explanation(&db, &DocumentSnapshot::new(&snapshot));
        }
    }
}

fn check_explanation(db: &DocumentDatabase<'_>, snapshot: &DocumentSnapshot<'_>) {
    use database_engine::Comparison;

    let query = DocumentQuery {
        filter: DocumentFilter::And(vec![
            equal("group", json!("x")),
            DocumentFilter::Compare {
                path: path("score"),
                comparison: Comparison::Greater,
                value: IndexValue::Integer(1),
            },
            DocumentFilter::Compare {
                path: path("score"),
                comparison: Comparison::LessOrEqual,
                value: IndexValue::Integer(3),
            },
        ]),
        max_scanned: 1,
        ..Default::default()
    };
    let expected = db.explain_query("c", &query).unwrap();
    assert_eq!(
        expected.access,
        DocumentQueryAccess::CompoundRangeIndex {
            index: "group_score".into(),
            equality_fields: 1,
        }
    );
    let [probe] = expected.probes.as_slice() else {
        panic!("expected one compound range");
    };
    assert_eq!(probe.index, "group_score");
    assert_eq!(probe.equal, [IndexValue::String("x".into())]);
    assert_eq!(
        probe.lower,
        Some(IndexBound {
            value: IndexValue::Integer(1),
            inclusive: false,
        })
    );
    assert_eq!(
        probe.upper,
        Some(IndexBound {
            value: IndexValue::Integer(3),
            inclusive: true,
        })
    );
    assert!(!probe.unique_equality);
    assert_eq!(
        db.query_planned("c", &query).unwrap_err().to_string(),
        "document scan exceeds the operation limit"
    );

    let admitted = DocumentQuery {
        max_scanned: 2,
        ..query.clone()
    };
    assert_eq!(
        db.query_planned("c", &admitted).unwrap().access,
        expected.access
    );
    assert_eq!(
        db.transaction_with_read_limits(
            DocumentReadLimits {
                max_read_units: 1,
                max_read_bytes: usize::MAX
            },
            DocumentTransactionMode::ReadSet,
            None,
            None,
            |tx| tx.explain_query("c", &query),
        )
        .unwrap(),
        expected
    );

    let exact_tuple = |score| {
        DocumentFilter::And(vec![
            equal("group", json!("x")),
            equal("score", json!(score)),
        ])
    };
    let disjunction = DocumentQuery {
        filter: DocumentFilter::Or(vec![exact_tuple(3), exact_tuple(2), exact_tuple(3)]),
        max_scanned: 2,
        ..Default::default()
    };
    let union = db.explain_query("c", &disjunction).unwrap();
    assert_eq!(
        union.access,
        DocumentQueryAccess::DisjunctionIndex {
            indexes: vec!["group_score".into()],
            probes: 2,
        }
    );
    assert_eq!(union.probes.len(), 2);
    for (probe, score) in union.probes.iter().zip([3, 2]) {
        assert_eq!(probe.index, "group_score");
        assert_eq!(
            probe.equal,
            [IndexValue::String("x".into()), IndexValue::Integer(score)]
        );
        assert!(probe.lower.is_none() && probe.upper.is_none());
    }
    assert_eq!(
        db.query_planned("c", &disjunction).unwrap().access,
        union.access
    );

    let invalid = DocumentQuery {
        max_scanned: 0,
        ..query.clone()
    };
    let invalid_error = db.explain_query("c", &invalid).unwrap_err();
    assert_eq!(invalid_error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        invalid_error.to_string(),
        db.query_planned("c", &invalid).unwrap_err().to_string()
    );
    assert_eq!(
        db.explain_query("missing", &query).unwrap_err().kind(),
        ErrorKind::NotFound
    );

    db.transaction(|tx| {
        tx.drop_index("c", "group_score")?;
        let staged = tx.explain_query("c", &admitted)?;
        assert_eq!(staged.access, DocumentQueryAccess::CollectionScan);
        assert!(staged.probes.is_empty());
        assert_eq!(tx.query_planned("c", &admitted)?.access, staged.access);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        db.explain_query("c", &query).unwrap().access,
        DocumentQueryAccess::CollectionScan
    );
    assert_eq!(snapshot.explain_query("c", &query).unwrap(), expected);
    assert_eq!(
        snapshot.query_planned("c", &admitted).unwrap().access,
        expected.access
    );
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
    compare(
        db,
        &DocumentQuery {
            filter: DocumentFilter::Or(vec![
                equal("email", json!("a@example")),
                equal("email", json!("b@example")),
            ]),
            max_scanned: 10,
            ..Default::default()
        },
        DocumentQueryAccess::DisjunctionIndex {
            indexes: vec!["email".into()],
            probes: 2,
        },
    );
    for filter in [
        DocumentFilter::All,
        DocumentFilter::Not(Box::new(equal("email", json!("a@example")))),
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
            filter: DocumentFilter::And(vec![
                DocumentFilter::Or(vec![
                    equal("email", json!("a@example")),
                    DocumentFilter::All,
                ]),
                DocumentFilter::Exists {
                    path: path("score"),
                    exists: true,
                },
            ]),
            max_scanned: 10,
            ..Default::default()
        },
        DocumentQueryAccess::PresenceIndex {
            index: "nonunique".into(),
            partial: false,
        },
    );
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
