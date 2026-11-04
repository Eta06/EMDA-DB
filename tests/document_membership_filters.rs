//! Membership filters preserve exact JSON equality, limits and transaction isolation.
use database_engine::{
    AggregateRequest, CheckpointStore, CollectionSchema, Comparison, DocumentDatabase,
    DocumentFilter, DocumentQuery, DocumentQueryAccess, DocumentSort, IndexDefinition, IndexValue,
    PartialFilter, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::sync::Barrier;

mod support;
use support::retry_lock;

fn path(name: &str) -> Vec<String> {
    name.split('.').map(str::to_owned).collect()
}
fn in_values(name: &str, values: Vec<Value>) -> DocumentFilter {
    DocumentFilter::In {
        path: path(name),
        values,
    }
}
fn not_in(name: &str, values: Vec<Value>) -> DocumentFilter {
    DocumentFilter::NotIn {
        path: path(name),
        values,
    }
}
fn eq(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(name),
        value,
    }
}
fn query(filter: DocumentFilter) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned: 32,
        ..Default::default()
    }
}
fn ids(db: &DocumentDatabase<'_>, filter: DocumentFilter) -> Vec<String> {
    db.query("c", &query(filter))
        .unwrap()
        .documents
        .into_iter()
        .map(|row| row.id)
        .collect()
}
fn on_backends(exercise: impl Fn(&DocumentDatabase<'_>)) {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = Store::open(&source).unwrap();
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(initial);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            exercise(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            exercise(&DocumentDatabase::new(&initial));
        }
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
        fields: fields.iter().map(|field| path(field)).collect(),
        unique,
        partial,
    }
}
fn compare(db: &DocumentDatabase<'_>, request: &DocumentQuery, expected: DocumentQueryAccess) {
    let scan = db.query("c", request).unwrap();
    let planned = db.query_planned("c", request).unwrap();
    assert_eq!(planned.access, expected);
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
    assert_eq!(planned.result.plan.returned, scan.plan.returned);
}

fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("handle", &["handle"], true, None))
        .unwrap();
    for (id, doc) in [
        ("a", json!({"handle":"ha"})),
        ("b", json!({"handle":"hb","v":null,"profile":{"code":null}})),
        ("c", json!({"handle":"hc","v":1,"profile":{"code":"x"}})),
        ("d", json!({"handle":"hd","v":1.0,"profile":null})),
        ("e", json!({"handle":"he","v":"one"})),
        ("f", json!({"handle":"hf","v":[1,2]})),
        ("g", json!({"handle":"hg","v":{"x":1}})),
        ("h", json!({"handle":"hh","v":false})),
    ] {
        db.insert("c", id, &doc).unwrap();
    }
}
fn exercise(db: &DocumentDatabase<'_>) {
    let values = vec![Value::Null, json!(1), json!([1, 2]), json!({"x":1})];
    assert_eq!(
        ids(db, in_values("v", values.clone())),
        ["b", "c", "f", "g"]
    );
    assert_eq!(ids(db, not_in("v", values.clone())), ["a", "d", "e", "h"]);
    assert_eq!(ids(db, in_values("v", vec![json!(1)])), ["c"]);
    assert_eq!(ids(db, in_values("v", vec![json!(1.0)])), ["d"]);
    assert!(ids(db, in_values("v", vec![])).is_empty());
    assert_eq!(
        ids(db, not_in("v", vec![])),
        ["a", "b", "c", "d", "e", "f", "g", "h"]
    );
    assert_eq!(ids(db, in_values("profile.code", vec![Value::Null])), ["b"]);
    assert_eq!(
        ids(db, not_in("profile.code", vec![Value::Null])),
        ["a", "c", "d", "e", "f", "g", "h"]
    );
    let singleton = query(in_values("handle", vec![json!("hc")]));
    compare(
        db,
        &singleton,
        DocumentQueryAccess::UniqueEqualityIndex {
            index: "handle".into(),
        },
    );
    compare(
        db,
        &query(in_values("handle", vec![json!("hc"), json!("hd")])),
        DocumentQueryAccess::MembershipIndex {
            index: "handle".into(),
            probes: 2,
        },
    );
    compare(
        db,
        &query(in_values("handle", vec![json!({"not":"indexable"})])),
        DocumentQueryAccess::CollectionScan,
    );
    compare(
        db,
        &query(not_in("handle", vec![json!("hc")])),
        DocumentQueryAccess::CollectionScan,
    );
    let aggregation = db
        .aggregate(
            "c",
            &AggregateRequest {
                filter: in_values("v", values),
                group_by: None,
                sum: None,
                max_scanned: 32,
            },
        )
        .unwrap();
    assert_eq!(aggregation.groups[0].count, 4);
    let sorted = DocumentQuery {
        filter: in_values("v", vec![Value::Null, json!(1)]),
        sort: Some(DocumentSort {
            path: path("v"),
            descending: false,
        }),
        projection: Some(vec![path("handle")]),
        offset: 1,
        limit: 1,
        max_scanned: 32,
    };
    let result = db.query("c", &sorted).unwrap();
    assert_eq!(
        (
            result.plan.scanned,
            result.plan.matched,
            result.plan.returned
        ),
        (8, 2, 1)
    );
    assert_eq!(result.documents[0].id, "c");
    assert_eq!(result.documents[0].document, json!({"handle":"hc"}));
}

#[test]
fn indexed_null_is_distinct_from_a_missing_field() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index("v", &["v"], false, None))
            .unwrap();
        for (id, doc) in [
            ("a", json!({})),
            ("b", json!({"v":null})),
            ("c", json!({"v":1})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let positive = query(in_values("v", vec![Value::Null]));
        compare(
            db,
            &positive,
            DocumentQueryAccess::EqualityIndex { index: "v".into() },
        );
        assert_eq!(ids(db, positive.filter), ["b"]);
        let negative = query(not_in("v", vec![Value::Null]));
        compare(db, &negative, DocumentQueryAccess::CollectionScan);
        assert_eq!(ids(db, negative.filter), ["a", "c"]);
    });
}

#[test]
fn membership_filters_see_staged_writes_within_a_transaction() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index("v", &["v"], false, None))
            .unwrap();
        db.insert("c", "old", &json!({"v":1})).unwrap();
        let request = query(in_values("v", vec![json!(1)]));
        db.transaction_with_read_set(|tx| {
            assert!(tx.delete("c", "old")?);
            tx.insert("c", "new", &json!({"v":1}))?;
            let scan = tx.query("c", &request)?;
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(
                planned.access,
                DocumentQueryAccess::EqualityIndex { index: "v".into() }
            );
            assert_eq!(planned.result.documents, scan.documents);
            assert_eq!(planned.result.documents[0].id, "new");
            Ok(())
        })
        .unwrap();
        assert_eq!(ids(db, request.filter), ["new"]);
    });
}

#[test]
fn membership_uses_exact_values_and_explicit_missing_null_semantics_after_recovery() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = Store::open(&source).unwrap();
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(initial);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            prepare(&db);
            exercise(&db);
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            exercise(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            let db = DocumentDatabase::new(&initial);
            prepare(&db);
            exercise(&db);
            drop(initial);
            let store = retry_lock(|| Store::open(&source)).unwrap();
            exercise(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn partial_presence_and_compound_suffix_plans_never_omit_missing_values() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "score",
                &["score"],
                false,
                Some(PartialFilter::Exists {
                    path: path("active"),
                    exists: true,
                }),
            ),
        )
        .unwrap();
        for (id, doc) in [
            ("a", json!({"score":1,"active":true})),
            ("b", json!({"score":1,"active":false})),
            ("c", json!({"score":1})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let qualifying = query(DocumentFilter::And(vec![
            eq("score", json!(1)),
            in_values("active", vec![json!(true), json!(false)]),
        ]));
        compare(
            db,
            &qualifying,
            DocumentQueryAccess::PartialEqualityIndex {
                index: "score".into(),
                fields: 1,
            },
        );
        let includes_missing = query(DocumentFilter::And(vec![
            eq("score", json!(1)),
            not_in("active", vec![json!(false)]),
        ]));
        compare(db, &includes_missing, DocumentQueryAccess::CollectionScan);
        assert_eq!(ids(db, includes_missing.filter), ["a", "c"]);
    });
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "score",
                &["score"],
                false,
                Some(PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                }),
            ),
        )
        .unwrap();
        for (id, doc) in [
            ("a", json!({"score":1,"active":true})),
            ("b", json!({"score":1,"active":false})),
            ("c", json!({"score":1})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let singleton = query(DocumentFilter::And(vec![
            eq("score", json!(1)),
            in_values("active", vec![json!(true)]),
        ]));
        compare(
            db,
            &singleton,
            DocumentQueryAccess::PartialEqualityIndex {
                index: "score".into(),
                fields: 1,
            },
        );
        let multiple = query(DocumentFilter::And(vec![
            eq("score", json!(1)),
            in_values("active", vec![json!(true), json!(false)]),
        ]));
        compare(db, &multiple, DocumentQueryAccess::CollectionScan);
    });
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index("triple", &["tenant", "score", "suffix"], false, None),
        )
        .unwrap();
        for (id, doc) in [
            ("a", json!({"tenant":"t","score":1,"suffix":null})),
            ("b", json!({"tenant":"t","score":2,"suffix":true})),
            ("c", json!({"tenant":"t","score":3})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let base = || {
            vec![
                eq("tenant", json!("t")),
                DocumentFilter::Compare {
                    path: path("score"),
                    comparison: Comparison::GreaterOrEqual,
                    value: IndexValue::Integer(1),
                },
            ]
        };
        let mut positive = base();
        positive.push(in_values("suffix", vec![Value::Null, json!(true)]));
        compare(
            db,
            &query(DocumentFilter::And(positive)),
            DocumentQueryAccess::CompoundRangeIndex {
                index: "triple".into(),
                equality_fields: 1,
            },
        );
        let mut negative = base();
        negative.push(not_in("suffix", vec![json!(false)]));
        let request = query(DocumentFilter::And(negative));
        compare(db, &request, DocumentQueryAccess::CollectionScan);
        assert_eq!(ids(db, request.filter), ["a", "b", "c"]);
    });
}

#[test]
fn membership_and_array_predicates_share_input_and_work_budgets() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        let mut values = vec![json!(0); 4_000];
        *values.last_mut().unwrap() = json!(1);
        for id in ["a", "b"] {
            db.insert("c", id, &json!({"n":0,"tags":values})).unwrap();
        }
        let filters = |count| {
            let mut filters = (0..count)
                .map(|_| DocumentFilter::ArrayContains {
                    path: path("tags"),
                    value: json!(1),
                })
                .collect::<Vec<_>>();
            filters.push(in_values("n", vec![json!(0)]));
            DocumentFilter::And(filters)
        };
        assert_eq!(ids(db, filters(124)), ["a", "b"]);
        let error = db.query("c", &query(filters(125))).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("comparison limit"));
        let error = db
            .transaction(|tx| {
                tx.put_raw(b"membership-rollback", b"bad")?;
                tx.query("c", &query(filters(125)))?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"membership-rollback"))
                .unwrap(),
            None
        );
        assert!(ids(db, in_values("missing", vec![json!(0); 256])).is_empty());
        let too_many = DocumentFilter::And(vec![
            in_values("missing", vec![json!(0); 256]),
            DocumentFilter::ArrayContains {
                path: path("tags"),
                value: json!(0),
            },
        ]);
        assert_eq!(
            db.query("c", &query(too_many)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            db.query("c", &query(not_in("missing", vec![json!(0); 257])))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        let large = json!("x".repeat(1024 * 1024 - 2));
        assert!(ids(db, in_values("missing", vec![large.clone(); 4])).is_empty());
        let oversized = in_values(
            "missing",
            vec![
                large.clone(),
                large.clone(),
                large.clone(),
                large,
                Value::Null,
            ],
        );
        assert_eq!(
            db.query("c", &query(oversized)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        let invalid = DocumentFilter::In {
            path: vec![],
            values: vec![],
        };
        assert_eq!(
            db.query("c", &query(invalid)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

fn phantom(db: &DocumentDatabase<'_>, inside: bool) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("score", &["score"], false, None))
        .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert("c", "new", &json!({"score":if inside {5} else {6}}))
                .unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let planned = tx.query_planned("c", &query(in_values("score", vec![json!(5)])))?;
            assert_eq!(
                planned.access,
                DocumentQueryAccess::EqualityIndex {
                    index: "score".into()
                }
            );
            assert!(planned.result.documents.is_empty());
            entered.wait();
            changed.wait();
            tx.put_raw(b"membership-phantom", b"committed")?;
            Ok(())
        });
        if inside {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        } else {
            result.unwrap();
        }
        writer.join().unwrap();
    });
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"membership-phantom"))
            .unwrap(),
        if inside {
            None
        } else {
            Some(b"committed".to_vec())
        }
    );
}
#[test]
fn singleton_membership_detects_index_phantoms_and_allows_unrelated_inserts() {
    for inside in [false, true] {
        on_backends(|db| phantom(db, inside));
    }
}
