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

fn exists(field: &[&str], present: bool) -> DocumentFilter {
    DocumentFilter::Exists {
        path: field.iter().map(|component| (*component).into()).collect(),
        exists: present,
    }
}

fn negated(filter: DocumentFilter) -> DocumentFilter {
    DocumentFilter::Not(Box::new(filter))
}

fn negate_disjunctions(filter: DocumentFilter) -> DocumentFilter {
    match filter {
        DocumentFilter::And(children) => {
            DocumentFilter::And(children.into_iter().map(negate_disjunctions).collect())
        }
        DocumentFilter::Or(children) => negated(DocumentFilter::And(
            children
                .into_iter()
                .map(negate_disjunctions)
                .map(negated)
                .collect(),
        )),
        DocumentFilter::Not(child) => negated(negate_disjunctions(*child)),
        other => other,
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
fn full_index_scan_requires_explicit_presence_and_the_partial_guard() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "items",
            IndexDefinition {
                name: "covered".into(),
                fields: vec![vec!["meta".into(), "score".into()], path("rank")],
                unique: true,
                partial: Some(PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                }),
            },
        )
        .unwrap();
        for (id, document) in [
            ("a", json!({"meta":{"score":7},"rank":true,"active":true})),
            (
                "b",
                json!({"meta":{"score":null},"rank":false,"active":true}),
            ),
            ("c", json!({"meta":{"score":7},"active":true})),
            ("d", json!({"meta":{},"rank":true,"active":true})),
            ("e", json!({"meta":{"score":7},"rank":true,"active":false})),
        ] {
            db.insert("items", id, &document).unwrap();
        }
        let score = exists(&["meta", "score"], true);
        let rank = negated(exists(&["rank"], false));
        let active = eq("active", json!(true));
        let filter = DocumentFilter::And(vec![
            negate_disjunctions(DocumentFilter::Or(vec![
                score.clone(),
                DocumentFilter::And(vec![score.clone(), active.clone()]),
            ])),
            rank.clone(),
            active.clone(),
        ]);
        let query = request(filter.clone(), 2);
        let planned = db.query_planned("items", &query).unwrap();
        assert_eq!(
            planned.access,
            DocumentQueryAccess::PresenceIndex {
                index: "covered".into(),
                partial: true,
            }
        );
        assert_eq!(planned.result.plan.scanned, 2);
        assert_eq!(planned.result.documents[0].id, "a");
        assert_eq!(planned.result.documents[1].id, "b");
        assert_eq!(
            planned.result.documents,
            db.query("items", &request(filter.clone(), 5))
                .unwrap()
                .documents
        );
        assert_eq!(
            db.query_planned("items", &request(filter, 1))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        for uncovered in [
            DocumentFilter::And(vec![score.clone(), rank]),
            DocumentFilter::And(vec![score, active]),
        ] {
            let query = request(uncovered, 5);
            let planned = db.query_planned("items", &query).unwrap();
            assert_eq!(planned.access, DocumentQueryAccess::CollectionScan);
            assert_eq!(planned.result.documents.len(), 3);
            assert_eq!(
                planned.result.documents,
                db.query("items", &query).unwrap().documents
            );
        }
    });
}

#[test]
fn empty_presence_scan_tracks_concurrent_index_entries() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index("items", index("flag", &["flag"])).unwrap();
        db.insert("items", "old", &json!({})).unwrap();
        let query = request(exists(&["flag"], true), 1);
        let (read, observed) = mpsc::channel();
        let (commit, written) = mpsc::channel();
        std::thread::scope(|scope| {
            let writer = scope.spawn(move || {
                observed.recv_timeout(Duration::from_secs(5)).unwrap();
                db.replace("items", "old", &json!({"flag":null}), false)
                    .unwrap();
                commit.send(()).unwrap();
            });
            let reader = db.transaction_with_read_set(|tx| {
                let planned = tx.query_planned("items", &query)?;
                assert_eq!(
                    planned.access,
                    DocumentQueryAccess::PresenceIndex {
                        index: "flag".into(),
                        partial: false
                    }
                );
                assert!(planned.result.documents.is_empty());
                read.send(()).unwrap();
                written.recv_timeout(Duration::from_secs(5)).unwrap();
                tx.put_raw(b"presence-read", b"stale")
            });
            writer.join().unwrap();
            assert!(is_transaction_conflict(&reader.unwrap_err()));
        });
        let receipt = db.transaction(|tx| tx.get_raw(b"presence-read")).unwrap();
        assert_eq!(receipt, None);
        let planned = db.query_planned("items", &query).unwrap();
        assert_eq!(
            planned.result.documents,
            db.query("items", &query).unwrap().documents
        );
        assert_eq!(planned.result.documents.len(), 1);
        assert_eq!(planned.result.documents[0].document, json!({"flag":null}));
    });
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
                    DocumentFilter::And(vec![eq("a", json!(0)), eq("b", json!(0))]),
                ]),
                access(&["compound"], 256),
            ),
            (
                DocumentFilter::Or(vec![
                    DocumentFilter::And(vec![in_range("a", 16), in_range("b", 16)]),
                    eq("tag", json!("lone")),
                ]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                DocumentFilter::And(vec![
                    DocumentFilter::Or(vec![
                        DocumentFilter::And(vec![in_range("a", 16), in_range("b", 16)]),
                        eq("tag", json!("lone")),
                    ]),
                    DocumentFilter::Or(vec![eq("tag", json!("lone")), eq("tag", json!("other"))]),
                ]),
                access(&["tag"], 2),
            ),
            (
                DocumentFilter::Or(vec![eq("tag", json!("lone")), eq("other", json!([1]))]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                negated(DocumentFilter::And(vec![
                    eq("tag", json!("lone")),
                    negated(eq("a", json!(0))),
                ])),
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
            for filter in [filter.clone(), negate_disjunctions(filter)] {
                let query = request(filter, 4);
                let planned = db.query_planned("items", &query).unwrap();
                assert_eq!(planned.access, expected);
                assert_eq!(
                    planned.result.documents,
                    db.query("items", &query).unwrap().documents
                );
            }
        }
    });
}

#[test]
fn independent_literal_disjunctions_complete_a_compound_range() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "items",
            IndexDefinition {
                partial: Some(PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                }),
                ..index("compound", &["tenant", "code", "rank"])
            },
        )
        .unwrap();
        for (id, row) in [
            (
                "first",
                json!({"tenant":"t","code":null,"rank":5,"active":true}),
            ),
            (
                "other-type",
                json!({"tenant":"u","code":true,"rank":6,"active":true}),
            ),
            (
                "too-small",
                json!({"tenant":"t","code":null,"rank":4,"active":true}),
            ),
            (
                "integer",
                json!({"tenant":"u","code":1,"rank":7,"active":true}),
            ),
            (
                "other-tenant",
                json!({"tenant":"v","code":null,"rank":7,"active":true}),
            ),
            (
                "inactive",
                json!({"tenant":"t","code":null,"rank":5,"active":false}),
            ),
            ("missing", json!({"tenant":"t","rank":5,"active":true})),
        ] {
            db.insert("items", id, &row).unwrap();
        }
        let tenants = DocumentFilter::Or(vec![
            eq("tenant", json!("t")),
            DocumentFilter::Or(vec![eq("tenant", json!("u")), eq("tenant", json!("t"))]),
        ]);
        let codes = DocumentFilter::Or(vec![
            eq("code", Value::Null),
            DocumentFilter::In {
                path: path("code"),
                values: vec![json!(true), Value::Null],
            },
        ]);
        let query = |codes, max_scanned| {
            request(
                DocumentFilter::And(vec![
                    tenants.clone(),
                    DocumentFilter::And(vec![eq("active", json!(true)), codes]),
                    DocumentFilter::Compare {
                        path: path("rank"),
                        comparison: Comparison::GreaterOrEqual,
                        value: IndexValue::Integer(5),
                    },
                ]),
                max_scanned,
            )
        };
        for use_negation in [false, true] {
            let mut tight = query(codes.clone(), 2);
            if use_negation {
                tight.filter = negate_disjunctions(tight.filter);
            }
            let planned = db.query_planned("items", &tight).unwrap();
            assert_eq!(
                planned.access,
                DocumentQueryAccess::PartialCompoundMembershipRangeIndex {
                    index: "compound".into(),
                    prefix_fields: 2,
                    probes: 4,
                },
            );
            assert_eq!(planned.result.plan.scanned, 2);
            assert_eq!(
                planned
                    .result
                    .documents
                    .iter()
                    .map(|row| row.id.as_str())
                    .collect::<Vec<_>>(),
                ["first", "other-type"],
            );
            assert_eq!(
                planned.result.documents,
                db.query("items", &query(codes.clone(), 7))
                    .unwrap()
                    .documents,
            );
        }

        for (unsupported, expected) in [
            (eq("code", json!(1.0)), access(&["compound"], 2)),
            (eq("rank", json!(7)), DocumentQueryAccess::CollectionScan),
        ] {
            let query = query(
                DocumentFilter::Or(vec![eq("code", Value::Null), unsupported]),
                7,
            );
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
fn independent_literal_disjunctions_share_the_compound_probe_limit() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index("items", index("compound", &["a", "b"]))
            .unwrap();
        db.insert("items", "inside", &json!({"a":15,"b":15}))
            .unwrap();
        db.insert("items", "outside", &json!({"a":16,"b":16}))
            .unwrap();
        let choices = |field, count| {
            DocumentFilter::Or((0..count).map(|value| eq(field, json!(value))).collect())
        };
        for (count, expected) in [
            (
                16,
                DocumentQueryAccess::CompoundMembershipIndex {
                    index: "compound".into(),
                    fields: 2,
                    probes: 256,
                },
            ),
            (17, access(&["compound"], 16)),
        ] {
            let query = request(
                DocumentFilter::And(vec![choices("a", 16), choices("b", count)]),
                2,
            );
            let planned = db.query_planned("items", &query).unwrap();
            assert_eq!(planned.access, expected);
            assert_eq!(
                planned.result.documents,
                db.query("items", &query).unwrap().documents
            );
            assert_eq!(planned.result.documents.len(), 1);
        }
    });
}

#[test]
fn independent_literal_disjunctions_track_empty_compound_probes() {
    for matching in [false, true] {
        on_backends(|db| {
            db.create_collection("items", CollectionSchema::default())
                .unwrap();
            db.create_index("items", index("compound", &["a", "b"]))
                .unwrap();
            let choices =
                |field| DocumentFilter::Or(vec![eq(field, json!(1)), eq(field, json!(2))]);
            let query = request(DocumentFilter::And(vec![choices("a"), choices("b")]), 1);
            let (read, observed) = mpsc::channel();
            let (committed, written) = mpsc::channel();
            std::thread::scope(|scope| {
                let writer = scope.spawn(move || {
                    observed.recv_timeout(Duration::from_secs(5)).unwrap();
                    db.insert("items", "new", &json!({"a":2,"b":if matching {2} else {3}}))
                        .unwrap();
                    committed.send(()).unwrap();
                });
                let result = db.transaction_with_read_set(|tx| {
                    let plan = tx.query_planned("items", &query)?;
                    assert_eq!(
                        plan.access,
                        DocumentQueryAccess::CompoundMembershipIndex {
                            index: "compound".into(),
                            fields: 2,
                            probes: 4
                        }
                    );
                    assert!(plan.result.documents.is_empty());
                    read.send(()).unwrap();
                    written.recv_timeout(Duration::from_secs(5)).unwrap();
                    tx.put_raw(b"receipt", b"committed")
                });
                writer.join().unwrap();
                if matching {
                    assert!(is_transaction_conflict(&result.unwrap_err()));
                } else {
                    result.unwrap();
                }
            });
            assert_eq!(
                db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
                (!matching).then(|| b"committed".to_vec()),
            );
        });
    }
}

#[test]
fn overlapping_disjunct_ranges_share_the_document_byte_budget() {
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
            let filter = DocumentFilter::Or(
                (0..probes)
                    .map(|offset| DocumentFilter::Compare {
                        path: path("tag"),
                        comparison: Comparison::GreaterOrEqual,
                        value: IndexValue::Integer(1 - offset),
                    })
                    .collect(),
            );
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
fn overlapping_membership_deduplicates_probes_and_keeps_residual_predicates() {
    on_backends(|db| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index("items", index("tag", &["tag"])).unwrap();
        for (id, tag, visible) in [
            ("a", 1, true),
            ("b", 2, false),
            ("c", 3, true),
            ("d", 4, false),
        ] {
            db.insert("items", id, &json!({"tag": tag, "visible": visible}))
                .unwrap();
        }
        let branch = |values, visible| {
            DocumentFilter::And(vec![
                DocumentFilter::In {
                    path: path("tag"),
                    values,
                },
                eq("visible", json!(visible)),
            ])
        };
        let mut query = request(
            DocumentFilter::Or(vec![
                branch(vec![json!(1), json!(2)], true),
                branch(vec![json!(2), json!(3)], false),
            ]),
            3,
        );
        let planned = db.query_planned("items", &query).unwrap();
        assert_eq!(planned.access, access(&["tag"], 3));
        assert_eq!(planned.result.plan.scanned, 3);
        query.max_scanned = 4;
        assert_eq!(
            planned.result.documents,
            db.query("items", &query).unwrap().documents
        );
        assert_eq!(
            planned
                .result
                .documents
                .iter()
                .map(|doc| doc.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
    });
}

#[test]
fn empty_disjunction_branches_detect_matching_phantoms() {
    for (use_negation, guarded, matching) in [
        (false, false, false),
        (false, false, true),
        (false, true, false),
        (false, true, true),
        (true, false, false),
        (true, false, true),
        (true, true, false),
        (true, true, true),
    ] {
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
                DocumentFilter::Or(vec![
                    eq("right", json!(2)),
                    eq("right", json!(3)),
                    eq("right", json!(3)),
                ]),
            ]);
            let filter = if guarded {
                DocumentFilter::And(vec![eq("active", json!(true)), alternatives])
            } else {
                alternatives
            };
            let filter = if use_negation {
                negate_disjunctions(filter)
            } else {
                filter
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

#[test]
fn guarded_index_union_detects_a_membership_swap_after_reopen() {
    fn assert_rows(db: &DocumentDatabase<'_>, query: &DocumentQuery, wanted_id: &str) {
        let planned = db.query_planned("items", query).unwrap();
        assert_eq!(planned.access, access(&["left", "right"], 2));
        // The same document occurs in both probes and must be returned once.
        assert_eq!(planned.result.plan.scanned, 2);
        assert_eq!(
            planned.result.documents,
            db.query("items", query).unwrap().documents
        );
        assert_eq!(planned.result.documents.len(), 1);
        assert_eq!(planned.result.documents[0].id, wanted_id);
    }

    let query = request(
        DocumentFilter::And(vec![
            DocumentFilter::Not(Box::new(DocumentFilter::Or(vec![
                DocumentFilter::Exists {
                    path: path("visible"),
                    exists: false,
                },
                eq("blocked", json!(true)),
            ]))),
            DocumentFilter::Or(vec![eq("left", json!(1)), eq("right", json!(2))]),
        ]),
        2,
    );
    let present = json!({"left": 1, "right": 2, "visible": null});
    let absent = json!({"left": 1, "right": 2});
    let seed = |db: &DocumentDatabase<'_>| {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        for name in ["left", "right"] {
            db.create_index(
                "items",
                IndexDefinition {
                    partial: Some(PartialFilter::Exists {
                        path: path("visible"),
                        exists: true,
                    }),
                    ..index(name, &[name])
                },
            )
            .unwrap();
        }
        db.insert("items", "a", &present).unwrap();
        db.insert("items", "b", &absent).unwrap();
    };
    let race = |db: &DocumentDatabase<'_>| {
        assert_rows(db, &query, "a");
        let (present, absent) = (&present, &absent);
        let (read, observed) = mpsc::channel();
        let (committed, written) = mpsc::channel();
        std::thread::scope(|scope| {
            let writer = scope.spawn(move || {
                observed.recv_timeout(Duration::from_secs(5)).unwrap();
                db.transaction(|tx| {
                    tx.replace("items", "a", absent, false)?;
                    tx.replace("items", "b", present, false)?;
                    tx.put_raw(b"swap", b"committed")
                })
                .unwrap();
                committed.send(()).unwrap();
            });
            let reader = db.transaction_with_read_set(|tx| {
                let before = tx.query_planned("items", &query)?;
                assert_eq!(before.access, access(&["left", "right"], 2));
                assert_eq!(before.result.documents.len(), 1);
                assert_eq!(before.result.documents[0].id, "a");
                tx.put_raw(b"reader", b"must roll back")?;
                read.send(()).unwrap();
                written.recv_timeout(Duration::from_secs(5)).unwrap();
                assert_eq!(tx.query_planned("items", &query)?, before);
                Ok(())
            });
            writer.join().unwrap();
            assert!(is_transaction_conflict(&reader.unwrap_err()));
        });
    };
    let verify_reopen = |db: &DocumentDatabase<'_>| {
        assert_rows(db, &query, "b");
        assert_eq!(db.get("items", "a").unwrap().as_ref(), Some(&absent));
        assert_eq!(db.get("items", "b").unwrap().as_ref(), Some(&present));
        db.transaction(|tx| {
            assert_eq!(
                tx.get_raw(b"swap")?.as_deref(),
                Some(b"committed".as_slice())
            );
            assert_eq!(tx.get_raw(b"reader")?, None);
            Ok(())
        })
        .unwrap();
    };

    let root = tempfile::tempdir().unwrap();
    for (name, checkpoint_writer) in [("ordinary", false), ("checkpoint", true)] {
        let source = root.path().join(name);
        let checkpoint = root.path().join(format!("{name}.checkpoint"));
        let initial = Store::open(&source).unwrap();
        seed(&DocumentDatabase::new(&initial));
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_writer {
            drop(initial);
            let writer = CheckpointStore::open(&source, &checkpoint, &digest).unwrap();
            race(&DocumentDatabase::with_checkpoint_store(&writer));
        } else {
            race(&DocumentDatabase::new(&initial));
            drop(initial);
        }
        let view = CheckpointStore::open(&source, &checkpoint, &digest).unwrap();
        verify_reopen(&DocumentDatabase::with_checkpoint_store(&view));
        drop(view);
        let store = Store::open(&source).unwrap();
        verify_reopen(&DocumentDatabase::new(&store));
    }
}
