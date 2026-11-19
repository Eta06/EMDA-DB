use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, IndexValue, PartialFilter, Store,
    is_transaction_conflict,
};
use serde_json::{Value, json};
use std::{io::ErrorKind, sync::mpsc, time::Duration};

fn path(name: &str) -> Vec<String> {
    vec![name.into()]
}

fn eq(field: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(field),
        value,
    }
}

fn in_range(field: &str, length: usize) -> DocumentFilter {
    DocumentFilter::In {
        path: path(field),
        values: (0..length).map(|value| json!(value)).collect(),
    }
}

fn index(name: &str, fields: &[&str]) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: fields.iter().map(|field| path(field)).collect(),
        unique: false,
        partial: None,
    }
}

fn on_backends(exercise: impl Fn(&DocumentDatabase<'_>)) {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let checkpoint = tmp.path().join("checkpoint");
    let store = Store::open(&source).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let checkpoint_store = CheckpointStore::open(&source, &checkpoint, &digest).unwrap();
    exercise(&DocumentDatabase::with_checkpoint_store(&checkpoint_store));
    let ordinary = Store::open(tmp.path().join("ordinary")).unwrap();
    exercise(&DocumentDatabase::new(&ordinary));
}

fn request(filter: DocumentFilter, max_scanned: usize) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned,
        ..Default::default()
    }
}

fn access(names: &[&str], probes: usize) -> DocumentQueryAccess {
    DocumentQueryAccess::DisjunctionIndex {
        indexes: names.iter().map(|name| name.to_string()).collect(),
        probes,
    }
}

#[test]
fn disjunction_merges_partial_equality_and_range_with_one_budget() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "items",
            IndexDefinition {
                unique: true,
                partial: Some(PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                }),
                ..index("code", &["code"])
            },
        )
        .unwrap();
        db.create_index("items", index("score", &["score"]))
            .unwrap();
        for (id, doc) in [
            (
                "a",
                json!({"code":"target", "active":false, "score":1, "visible":true}),
            ),
            (
                "b",
                json!({"code":"target", "active":true, "score":2, "visible":true}),
            ),
            ("c", json!({"code":"other", "score":5, "visible":true})),
            ("d", json!({"score":2, "visible":false})),
            ("e", json!({"score":0, "visible":true})),
        ] {
            db.insert("items", id, &doc).unwrap();
        }
        let active_code =
            DocumentFilter::And(vec![eq("active", json!(true)), eq("code", json!("target"))]);
        let score_range = DocumentFilter::And(vec![
            DocumentFilter::Compare {
                path: path("score"),
                comparison: Comparison::GreaterOrEqual,
                value: IndexValue::Integer(2),
            },
            DocumentFilter::Compare {
                path: path("score"),
                comparison: Comparison::LessOrEqual,
                value: IndexValue::Integer(5),
            },
        ]);
        let filter = DocumentFilter::And(vec![
            eq("visible", json!(true)),
            DocumentFilter::Or(vec![active_code, score_range]),
        ]);
        let mut query = request(filter, 4);
        query.sort = Some(DocumentSort {
            path: path("score"),
            descending: true,
        });
        query.projection = Some(vec![path("score")]);
        query.offset = 1;
        query.limit = 1;
        let planned = db.query_planned("items", &query).unwrap();
        assert_eq!(planned.access, access(&["code", "score"], 2));
        assert_eq!(planned.result.plan.scanned, 4);
        assert_eq!(planned.result.plan.matched, 2);
        assert_eq!(planned.result.documents[0].id, "b");
        assert_eq!(planned.result.documents[0].document, json!({"score":2}));
        let mut scan = query.clone();
        scan.max_scanned = 5;
        assert_eq!(
            planned.result.documents,
            db.query("items", &scan).unwrap().documents
        );
        let mut over = query.clone();
        over.max_scanned = 3;
        assert_eq!(
            db.query_planned("items", &over).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        db.transaction_with_read_set(|tx| {
            tx.delete("items", "c")?;
            tx.replace(
                "items",
                "b",
                &json!({"code":"target","active":true,"score":0,"visible":true}),
                false,
            )?;
            tx.insert("items", "new", &json!({"score":4,"visible":true}))?;
            let staged = tx.query_planned("items", &query)?;
            assert_eq!(staged.result.plan.scanned, 3);
            assert_eq!(staged.result.documents, tx.query("items", &scan)?.documents);
            assert_eq!(staged.result.documents[0].id, "b");
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn disjunction_branches_use_shared_guards_and_compound_fields() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        for definition in [
            index("code", &["tenant", "code"]),
            index("score", &["tenant", "score", "rank"]),
        ] {
            db.create_index(
                "items",
                IndexDefinition {
                    partial: Some(PartialFilter::Equals {
                        path: path("published"),
                        value: IndexValue::Boolean(true),
                    }),
                    ..definition
                },
            )
            .unwrap();
        }
        for (id, doc) in [
            (
                "code",
                json!({"tenant":1,"published":true,"code":"hit","rank":1}),
            ),
            (
                "range",
                json!({"tenant":1,"published":true,"score":5,"rank":null}),
            ),
            (
                "wrong-tenant",
                json!({"tenant":2,"published":true,"code":"hit","rank":1}),
            ),
            (
                "not-published",
                json!({"tenant":1,"published":false,"score":5,"rank":1}),
            ),
            (
                "missing-rank",
                json!({"tenant":1,"published":true,"score":5}),
            ),
        ] {
            db.insert("items", id, &doc).unwrap();
        }
        let shared = [
            eq("tenant", json!(1)),
            DocumentFilter::Exists {
                path: path("rank"),
                exists: true,
            },
        ];
        let guard = eq("published", json!(true));
        let code = eq("code", json!("hit"));
        let score = DocumentFilter::Compare {
            path: path("score"),
            comparison: Comparison::GreaterOrEqual,
            value: IndexValue::Integer(5),
        };
        let filter = |alternatives| {
            DocumentFilter::And(vec![
                shared[0].clone(),
                DocumentFilter::And(vec![shared[1].clone(), alternatives]),
            ])
        };
        let query = request(
            filter(DocumentFilter::And(vec![
                guard.clone(),
                DocumentFilter::Or(vec![
                    code.clone(),
                    DocumentFilter::Or(vec![score.clone(), eq("code", json!("empty"))]),
                ]),
            ])),
            2,
        );
        let mut scan = query.clone();
        scan.max_scanned = 5;
        let planned = db.query_planned("items", &query).unwrap();
        assert_eq!(planned.access, access(&["code", "score"], 3));
        assert_eq!(planned.result.plan.scanned, 2);
        assert_eq!(
            planned.result.documents,
            db.query("items", &scan).unwrap().documents
        );
        assert_eq!(
            planned
                .result
                .documents
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["code", "range"]
        );

        let query = request(
            filter(DocumentFilter::Or(vec![
                DocumentFilter::And(vec![guard, code]),
                score,
            ])),
            5,
        );
        let planned = db.query_planned("items", &query).unwrap();
        assert_eq!(planned.access, DocumentQueryAccess::CollectionScan);
        assert_eq!(
            planned.result.documents,
            db.query("items", &query).unwrap().documents
        );
        assert!(
            planned
                .result
                .documents
                .iter()
                .any(|row| row.id == "not-published")
        );
    });
}

#[test]
fn disjunction_falls_back_for_unindexed_branches_and_shared_probe_overflow() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index("items", index("compound", &["a", "b"]))
            .unwrap();
        db.create_index("items", index("tag", &["tag"])).unwrap();
        db.insert("items", "a", &json!({"a":0, "b":0})).unwrap();
        db.insert("items", "b", &json!({"tag":"lone"})).unwrap();
        db.insert("items", "c", &json!({"other":[1]})).unwrap();
        for (filter, expected) in [
            (
                DocumentFilter::Or(vec![
                    DocumentFilter::And(vec![in_range("a", 15), in_range("b", 17)]),
                    eq("tag", json!("lone")),
                ]),
                access(&["compound", "tag"], 256),
            ),
            (
                DocumentFilter::Or(vec![
                    DocumentFilter::And(vec![in_range("a", 16), in_range("b", 16)]),
                    eq("tag", json!("lone")),
                ]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                DocumentFilter::Or(vec![eq("tag", json!("lone")), eq("other", json!([1]))]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                DocumentFilter::Or(vec![]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                DocumentFilter::Not(Box::new(DocumentFilter::Or(vec![
                    eq("tag", json!("lone")),
                    eq("tag", json!("another")),
                ]))),
                DocumentQueryAccess::CollectionScan,
            ),
        ] {
            let query = request(filter, 4);
            let planned = db.query_planned("items", &query).unwrap();
            assert_eq!(planned.access, expected);
            assert_eq!(
                planned.result.documents,
                db.query("items", &query).unwrap().documents
            );
        }
    });
}

#[test]
fn repeated_disjunct_hits_share_the_document_byte_budget() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index("items", index("tag", &["tag"])).unwrap();
        db.insert(
            "items",
            "a",
            &json!({"tag": 1, "payload": "x".repeat(1_000_000)}),
        )
        .unwrap();
        let query = |probes| {
            let filter = DocumentFilter::Or(vec![eq("tag", json!(1)); probes]);
            let mut query = request(filter, 34);
            query.projection = Some(vec![path("tag")]);
            query
        };
        let within = db.query_planned("items", &query(33)).unwrap();
        assert_eq!(within.access, access(&["tag"], 33));
        assert_eq!(within.result.plan.scanned, 33);
        assert_eq!(within.result.plan.matched, 1);
        assert_eq!(within.result.documents[0].document, json!({"tag": 1}));
        assert_eq!(
            db.query_planned("items", &query(34)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn empty_disjunction_branches_detect_matching_phantoms() {
    for (guarded, matching) in [(false, false), (false, true), (true, false), (true, true)] {
        on_backends(|db| {
            db.create_collection("items", CollectionSchema::default())
                .unwrap();
            for name in ["left", "right"] {
                db.create_index(
                    "items",
                    IndexDefinition {
                        partial: guarded.then(|| PartialFilter::Equals {
                            path: path("active"),
                            value: IndexValue::Boolean(true),
                        }),
                        ..index(name, &[name])
                    },
                )
                .unwrap();
            }
            let alternatives = DocumentFilter::Or(vec![
                eq("left", json!(1)),
                DocumentFilter::Or(vec![eq("right", json!(2)), eq("right", json!(3))]),
            ]);
            let filter = if guarded {
                DocumentFilter::And(vec![eq("active", json!(true)), alternatives])
            } else {
                alternatives
            };
            let query = request(filter, 4);
            let (read, observed) = mpsc::channel();
            let (commit, written) = mpsc::channel();
            std::thread::scope(|scope| {
                let writer = scope.spawn(move || {
                    observed.recv_timeout(Duration::from_secs(5)).unwrap();
                    db.insert(
                        "items",
                        "new",
                        &json!({"right": if guarded || matching { 3 } else { 4 }, "active": matching}),
                    )
                    .unwrap();
                    commit.send(()).unwrap();
                });
                let reader = db.transaction_with_read_set(|tx| {
                    let plan = tx.query_planned("items", &query)?;
                    assert_eq!(plan.access, access(&["left", "right"], 3));
                    assert!(plan.result.documents.is_empty());
                    read.send(()).unwrap();
                    written.recv_timeout(Duration::from_secs(5)).unwrap();
                    tx.put_raw(b"receipt", b"committed")
                });
                writer.join().unwrap();
                if matching {
                    assert!(is_transaction_conflict(&reader.unwrap_err()));
                } else {
                    reader.unwrap();
                }
            });
            assert_eq!(
                db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
                (!matching).then(|| b"committed".to_vec())
            );
            let final_result = db.query_planned("items", &query).unwrap();
            assert_eq!(final_result.result.documents.len(), usize::from(matching));
            assert_eq!(
                final_result.result.documents,
                db.query("items", &query).unwrap().documents
            );
        });
    }
}
