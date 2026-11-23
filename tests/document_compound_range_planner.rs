//! Compound ranges must not omit documents missing an indexed suffix field.
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexBound, IndexDefinition, IndexQuery, IndexValue,
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
fn eq(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(name),
        value,
    }
}
fn exists(name: &str, present: bool) -> DocumentFilter {
    DocumentFilter::Exists {
        path: path(name),
        exists: present,
    }
}
fn membership(name: &str, values: Vec<Value>) -> DocumentFilter {
    DocumentFilter::In {
        path: path(name),
        values,
    }
}
fn cmp(name: &str, comparison: Comparison, value: IndexValue) -> DocumentFilter {
    DocumentFilter::Compare {
        path: path(name),
        comparison,
        value,
    }
}
fn and(filters: Vec<DocumentFilter>) -> DocumentFilter {
    DocumentFilter::And(filters)
}

fn closed_bounds(name: &str, value: IndexValue) -> DocumentFilter {
    and(vec![
        cmp(name, Comparison::GreaterOrEqual, value.clone()),
        cmp(name, Comparison::LessOrEqual, value),
    ])
}
fn query(filter: DocumentFilter, max_scanned: usize) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned,
        ..Default::default()
    }
}
fn index(fields: &[&str], partial: Option<PartialFilter>) -> IndexDefinition {
    IndexDefinition {
        name: "compound".into(),
        fields: fields.iter().map(|field| path(field)).collect(),
        unique: false,
        partial,
    }
}
fn access(equal: usize, partial: bool) -> DocumentQueryAccess {
    if partial {
        DocumentQueryAccess::PartialCompoundRangeIndex {
            index: "compound".into(),
            equality_fields: equal,
        }
    } else {
        DocumentQueryAccess::CompoundRangeIndex {
            index: "compound".into(),
            equality_fields: equal,
        }
    }
}

fn membership_range_access(
    prefix_fields: usize,
    probes: usize,
    partial: bool,
) -> DocumentQueryAccess {
    if partial {
        DocumentQueryAccess::PartialCompoundMembershipRangeIndex {
            index: "compound".into(),
            prefix_fields,
            probes,
        }
    } else {
        DocumentQueryAccess::CompoundMembershipRangeIndex {
            index: "compound".into(),
            prefix_fields,
            probes,
        }
    }
}
fn compare(db: &DocumentDatabase<'_>, request: &DocumentQuery, expected: DocumentQueryAccess) {
    let scan = db.query("c", request).unwrap();
    let plan = db.query_planned("c", request).unwrap();
    assert_eq!(plan.access, expected);
    assert_eq!(plan.result.documents, scan.documents);
    assert_eq!(plan.result.plan.matched, scan.plan.matched);
    assert_eq!(plan.result.plan.returned, scan.plan.returned);
    assert_eq!(plan.result.plan.sorted, scan.plan.sorted);
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

#[test]
fn bounded_membership_replaces_compound_ranges_only_with_complete_coverage() {
    for trailing in [false, true] {
        on_backends(|db| {
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            let fields: &[&str] = if trailing {
                &["tenant", "score", "region"]
            } else {
                &["tenant", "score"]
            };
            db.create_index(
                "c",
                index(
                    fields,
                    Some(PartialFilter::Equals {
                        path: path("active"),
                        value: IndexValue::Boolean(true),
                    }),
                ),
            )
            .unwrap();
            for (id, score, active) in [
                ("first", 3, true),
                ("last", 5, true),
                ("distraction", 4, true),
                ("inactive", 3, false),
            ] {
                db.insert(
                    "c",
                    id,
                    &json!({"tenant":"t","score":score,"region":"west","active":active}),
                )
                .unwrap();
            }
            let mut filters = vec![
                eq("tenant", json!("t")),
                eq("active", json!(true)),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(2)),
                cmp("score", Comparison::LessOrEqual, IndexValue::Integer(5)),
            ];
            let with_members = |mut filters: Vec<_>, values: &[i64]| {
                filters.push(membership(
                    "score",
                    values.iter().map(|v| json!(v)).collect(),
                ));
                and(filters)
            };
            if trailing {
                db.insert(
                    "c",
                    "missing",
                    &json!({"tenant":"t","score":3,"active":true}),
                )
                .unwrap();
                compare(
                    db,
                    &query(with_members(filters.clone(), &[1, 3, 5, 6]), 8),
                    DocumentQueryAccess::CollectionScan,
                );
                filters.push(exists("region", true));
            }
            let expected_access = if trailing {
                DocumentQueryAccess::CompoundPrefixIndex {
                    index: "compound".into(),
                    prefix_fields: 2,
                    probes: 2,
                    partial: true,
                }
            } else {
                DocumentQueryAccess::PartialCompoundMembershipIndex {
                    index: "compound".into(),
                    fields: 2,
                    probes: 2,
                }
            };
            let grouped_access = DocumentQueryAccess::IntegerMembershipIndex {
                index: "compound".into(),
                prefix_fields: 1,
                probes: 1,
                partial: true,
            };
            let mut grouped = query(with_members(filters.clone(), &[1, 3, 4, 5, 6]), 8);
            compare(db, &grouped, grouped_access.clone());
            grouped.max_scanned = 3;
            let planned = db.query_planned("c", &grouped).unwrap();
            assert_eq!(planned.access, grouped_access);
            assert_eq!(planned.result.plan.scanned, 3);
            let mut request = query(with_members(filters, &[1, 3, 5, 6]), 8);
            compare(db, &request, expected_access.clone());
            let expected = db.query("c", &request).unwrap().documents;
            request.max_scanned = 2;
            let planned = db.query_planned("c", &request).unwrap();
            assert_eq!(planned.access, expected_access);
            assert_eq!(planned.result.documents, expected);
            assert_eq!(planned.result.plan.scanned, 2);

            db.transaction_with_read_set(|tx| {
                assert!(tx.delete("c", "first")?);
                tx.replace(
                    "c",
                    "distraction",
                    &json!({"tenant":"t","score":5,"region":"east","active":true}),
                    false,
                )?;
                let planned = tx.query_planned("c", &request)?;
                assert_eq!(planned.access, expected_access);
                assert_eq!(
                    planned
                        .result
                        .documents
                        .iter()
                        .map(|r| r.id.as_str())
                        .collect::<Vec<_>>(),
                    ["distraction", "last"]
                );
                Ok(())
            })
            .unwrap();
            request.max_scanned = 8;
            compare(db, &request, expected_access);
        });
    }
}

#[test]
fn encoded_compound_bounds_beyond_the_storage_key_limit_match_the_scan() {
    on_backends(|db| {
        let tenant = "\0".repeat(256);
        let bound = IndexValue::String("\0".repeat(256));
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score"], None))
            .unwrap();
        for (id, owner, score) in [
            ("short", tenant.as_str(), "".to_owned()),
            ("zeros", tenant.as_str(), "\0".repeat(200)),
            ("greater", tenant.as_str(), "a".to_owned()),
            ("outside", "other", "".to_owned()),
        ] {
            db.insert("c", id, &json!({"tenant":owner,"score":score}))
                .unwrap();
        }

        for comparison in [
            Comparison::Less,
            Comparison::LessOrEqual,
            Comparison::Greater,
            Comparison::GreaterOrEqual,
        ] {
            compare(
                db,
                &query(
                    and(vec![
                        eq("tenant", json!(tenant)),
                        cmp("score", comparison, bound.clone()),
                    ]),
                    4,
                ),
                access(1, false),
            );
        }
        compare(
            db,
            &query(
                and(vec![
                    eq("tenant", json!(tenant)),
                    closed_bounds("score", bound.clone()),
                ]),
                4,
            ),
            DocumentQueryAccess::CompoundEqualityIndex {
                index: "compound".into(),
                fields: 2,
            },
        );
        compare(
            db,
            &query(
                and(vec![
                    eq("tenant", json!(tenant)),
                    membership("score", vec![json!("a"), json!("\0".repeat(256))]),
                ]),
                4,
            ),
            DocumentQueryAccess::CompoundMembershipIndex {
                index: "compound".into(),
                fields: 2,
                probes: 2,
            },
        );

        let mut query = IndexQuery {
            equal: vec![IndexValue::String(tenant)],
            upper: Some(IndexBound {
                value: bound,
                inclusive: false,
            }),
            limit: 1,
            ..Default::default()
        };
        for expected in ["short", "zeros"] {
            let page = db.query_index("c", "compound", &query).unwrap();
            assert_eq!(page.documents.len(), 1);
            assert_eq!(page.documents[0].id, expected);
            query.after = page.next;
        }
        assert!(
            db.query_index("c", "compound", &query)
                .unwrap()
                .documents
                .is_empty()
        );
    });
}

#[test]
fn prefix_range_matches_scan_and_requires_every_omitted_field_to_be_present() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score", "flag"], None))
            .unwrap();
        for (id, document) in [
            ("a", json!({"tenant":"t","score":1,"flag":null})),
            ("b", json!({"tenant":"t","score":3,"flag":true})),
            ("c", json!({"tenant":"t","score":4})),
            ("d", json!({"tenant":"t","score":7,"flag":false})),
            ("e", json!({"tenant":"u","score":2,"flag":true})),
            ("f", json!({"tenant":"t","flag":true})),
            ("g", json!({"score":2,"flag":true})),
        ] {
            db.insert("c", id, &document).unwrap();
        }
        let bounds = || {
            vec![
                eq("tenant", json!("t")),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(1)),
                cmp("score", Comparison::LessOrEqual, IndexValue::Integer(4)),
            ]
        };
        let mut required = bounds();
        required.push(exists("flag", true));
        let request = DocumentQuery {
            filter: and(required),
            max_scanned: 20,
            sort: Some(DocumentSort {
                path: path("score"),
                descending: true,
            }),
            projection: Some(vec![path("tenant"), path("score")]),
            offset: 1,
            limit: 2,
        };
        compare(db, &request, access(1, false));
        let rows = db.query_planned("c", &request).unwrap().result.documents;
        assert_eq!(
            rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["a"]
        );
        let mut equal_suffix = bounds();
        equal_suffix.push(eq("flag", Value::Null));
        compare(db, &query(and(equal_suffix), 20), access(1, false));
        let mut compared_suffix = bounds();
        compared_suffix.push(cmp("flag", Comparison::GreaterOrEqual, IndexValue::Null));
        compare(db, &query(and(compared_suffix), 20), access(1, false));
        let nested = query(
            and(vec![
                eq("tenant", json!("t")),
                and(vec![
                    cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(1)),
                    exists("flag", true),
                ]),
            ]),
            20,
        );
        compare(db, &nested, access(1, false));
        let missing_proof = query(and(bounds()), 20);
        compare(db, &missing_proof, DocumentQueryAccess::CollectionScan);
        assert!(
            db.query_planned("c", &missing_proof)
                .unwrap()
                .result
                .documents
                .iter()
                .any(|row| row.id == "c")
        );
        let mut optional = bounds();
        optional.push(DocumentFilter::Or(vec![
            exists("flag", true),
            DocumentFilter::All,
        ]));
        compare(
            db,
            &query(and(optional), 20),
            DocumentQueryAccess::CollectionScan,
        );
        let mut negated = bounds();
        negated.push(DocumentFilter::Not(Box::new(exists("flag", false))));
        compare(db, &query(and(negated), 20), access(1, false));
        let mut negated_equality = bounds();
        negated_equality.push(DocumentFilter::Not(Box::new(eq("flag", json!(true)))));
        compare(
            db,
            &query(and(negated_equality), 20),
            DocumentQueryAccess::CollectionScan,
        );
        compare(
            db,
            &query(
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(1)),
                20,
            ),
            DocumentQueryAccess::CollectionScan,
        );
        let first_range = query(
            and(vec![
                cmp(
                    "tenant",
                    Comparison::GreaterOrEqual,
                    IndexValue::String("t".into()),
                ),
                exists("score", true),
                exists("flag", true),
            ]),
            20,
        );
        compare(db, &first_range, access(0, false));
    });
}

#[test]
fn unique_compound_index_can_serve_a_multi_value_range() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        let mut definition = index(&["tenant", "score"], None);
        definition.unique = true;
        db.create_index("c", definition).unwrap();
        for (id, score) in [("a", 2), ("b", 3), ("c", 4)] {
            db.insert("c", id, &json!({"tenant":"t", "score":score}))
                .unwrap();
        }
        let request = query(
            and(vec![
                eq("tenant", json!("t")),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(2)),
            ]),
            10,
        );
        compare(db, &request, access(1, false));
        assert_eq!(
            db.query_planned("c", &request).unwrap().result.plan.matched,
            3
        );
    });
}

#[test]
fn existing_single_field_range_takes_precedence_over_compound_ranges() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score"], None))
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "score".into(),
                fields: vec![path("score")],
                unique: false,
                partial: None,
            },
        )
        .unwrap();
        db.insert("c", "a", &json!({"tenant":"t", "score":2}))
            .unwrap();
        let request = query(
            and(vec![
                eq("tenant", json!("t")),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(1)),
            ]),
            10,
        );
        compare(
            db,
            &request,
            DocumentQueryAccess::RangeIndex {
                index: "score".into(),
            },
        );
    });
}

#[test]
fn partial_compound_range_requires_its_guard_and_suffix_presence() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                &["tenant", "score", "flag"],
                Some(PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                }),
            ),
        )
        .unwrap();
        for (id, doc) in [
            (
                "a",
                json!({"tenant":"t","score":3,"flag":null,"active":true}),
            ),
            (
                "b",
                json!({"tenant":"t","score":3,"flag":true,"active":false}),
            ),
            ("c", json!({"tenant":"t","score":3,"active":true})),
            (
                "d",
                json!({"tenant":"u","score":3,"flag":true,"active":true}),
            ),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let base = || {
            vec![
                eq("tenant", json!("t")),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
                exists("flag", true),
            ]
        };
        let mut qualified = base();
        qualified.push(eq("active", json!(true)));
        compare(db, &query(and(qualified), 10), access(1, true));
        compare(
            db,
            &query(and(base()), 10),
            DocumentQueryAccess::CollectionScan,
        );
        let mut mere_presence = base();
        mere_presence.push(exists("active", true));
        compare(
            db,
            &query(and(mere_presence), 10),
            DocumentQueryAccess::CollectionScan,
        );
        let without_suffix = query(
            and(vec![
                eq("tenant", json!("t")),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
                eq("active", json!(true)),
            ]),
            10,
        );
        compare(db, &without_suffix, DocumentQueryAccess::CollectionScan);
        assert!(
            db.query_planned("c", &without_suffix)
                .unwrap()
                .result
                .documents
                .iter()
                .any(|row| row.id == "c")
        );
    });
}

fn prepare_large(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index(&["tenant", "score"], None))
        .unwrap();
    for start in (0..300).step_by(20) {
        db.transaction(|tx| {
            for n in start..start + 20 {
                tx.insert(
                    "c",
                    &format!("id{n:03}"),
                    &json!({"tenant":"t","score":(n * 47) % 300}),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
}
fn large_request(max_scanned: usize) -> DocumentQuery {
    DocumentQuery {
        filter: and(vec![
            eq("tenant", json!("t")),
            cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
            cmp("score", Comparison::LessOrEqual, IndexValue::Integer(299)),
        ]),
        max_scanned,
        offset: 260,
        limit: 20,
        ..Default::default()
    }
}
#[test]
fn compound_range_reads_all_pages_in_id_order_and_obeys_candidate_limits() {
    on_backends(|db| {
        prepare_large(db);
        compare(db, &large_request(300), access(1, false));
        let plan = db.query_planned("c", &large_request(300)).unwrap();
        assert_eq!(plan.result.plan.scanned, 300);
        assert_eq!(plan.result.plan.matched, 300);
        assert_eq!(
            plan.result
                .documents
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            (260..280).map(|n| format!("id{n:03}")).collect::<Vec<_>>()
        );
        assert_eq!(
            db.query_planned("c", &large_request(299))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        let error = db
            .transaction(|tx| {
                tx.put_raw(b"range-rollback", b"bad")?;
                tx.query_planned("c", &large_request(299))?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"range-rollback")).unwrap(),
            None
        );
    });
}

fn phantom(db: &DocumentDatabase<'_>, mode: u8) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index(&["tenant", "score", "flag"], None))
        .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            let document = match mode {
                0 => json!({"tenant":"t","score":25,"flag":true}),
                1 => json!({"tenant":"u","score":25,"flag":true}),
                2 => json!({"tenant":"t","score":100,"flag":true}),
                _ => json!({"tenant":"t","score":25}),
            };
            db.insert("c", "new", &document).unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let request = query(
                and(vec![
                    closed_bounds("tenant", IndexValue::String("t".into())),
                    cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(20)),
                    cmp("score", Comparison::LessOrEqual, IndexValue::Integer(30)),
                    exists("flag", true),
                ]),
                10,
            );
            let plan = tx.query_planned("c", &request)?;
            assert_eq!(plan.access, access(1, false));
            assert!(plan.result.documents.is_empty());
            entered.wait();
            changed.wait();
            tx.put_raw(b"compound-range-phantom", b"committed")?;
            Ok(())
        });
        if mode == 0 {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        } else {
            result.unwrap();
        }
        writer.join().unwrap();
    });
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"compound-range-phantom"))
            .unwrap(),
        if mode == 0 {
            None
        } else {
            Some(b"committed".to_vec())
        }
    );
}

#[test]
fn closed_bounds_enable_later_compound_fields() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                unique: true,
                ..index(&["tenant", "score"], None)
            },
        )
        .unwrap();
        for (id, tenant, score) in [("a", "t", 1), ("b", "t", 2), ("c", "t", 3), ("d", "u", 3)] {
            db.insert("c", id, &json!({"tenant":tenant, "score":score}))
                .unwrap();
        }
        let tenant = closed_bounds("tenant", IndexValue::String("t".into()));
        let range = and(vec![
            tenant.clone(),
            cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(3)),
        ]);
        let narrowed = db.query_planned("c", &query(range.clone(), 1)).unwrap();
        assert_eq!(narrowed.access, access(1, false));
        assert_eq!(narrowed.result.plan.scanned, 1);
        assert_eq!(narrowed.result.documents[0].id, "c");
        compare(db, &query(range, 4), access(1, false));

        let exact = and(vec![
            tenant.clone(),
            closed_bounds("score", IndexValue::Integer(3)),
        ]);
        compare(
            db,
            &query(exact, 4),
            DocumentQueryAccess::UniqueCompoundEqualityIndex {
                index: "compound".into(),
                fields: 2,
            },
        );
        let union = and(vec![
            tenant.clone(),
            membership("score", vec![json!(2), json!(3)]),
        ]);
        compare(
            db,
            &query(union, 4),
            DocumentQueryAccess::CompoundMembershipIndex {
                index: "compound".into(),
                fields: 2,
                probes: 2,
            },
        );
        compare(
            db,
            &query(and(vec![tenant, exists("score", true)]), 4),
            prefix_access(1, 1, false),
        );
    });
}
#[test]
fn compound_range_detects_matching_phantoms_and_allows_nonmatching_inserts() {
    for mode in 0..4 {
        on_backends(|db| phantom(db, mode));
    }
}

#[test]
fn compound_range_observes_staged_changes() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score"], None))
            .unwrap();
        db.insert("c", "a", &json!({"tenant":"t","score":3}))
            .unwrap();
        let request = query(
            and(vec![
                eq("tenant", json!("t")),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(1)),
            ]),
            10,
        );
        db.transaction_with_read_set(|tx| {
            assert!(tx.delete("c", "a")?);
            tx.insert("c", "new", &json!({"tenant":"t","score":4}))?;
            let scan = tx.query("c", &request)?;
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(planned.access, access(1, false));
            assert_eq!(planned.result.documents, scan.documents);
            Ok(())
        })
        .unwrap();
        compare(db, &request, access(1, false));
    });
}

#[test]
fn compound_range_uses_scalar_order_and_intersected_exclusive_bounds() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score"], None))
            .unwrap();
        for (id, score) in [
            ("a", Value::Null),
            ("b", json!(false)),
            ("c", json!(true)),
            ("d", json!(-1)),
            ("e", json!(0)),
            ("f", json!(5)),
            ("g", json!("a")),
            ("h", json!("a\u{0}z")),
            ("i", json!("z")),
        ] {
            db.insert("c", id, &json!({"tenant":"t","score":score}))
                .unwrap();
        }
        for bound in [
            IndexValue::Null,
            IndexValue::Boolean(false),
            IndexValue::Integer(0),
            IndexValue::Integer(5),
            IndexValue::String("a\0z".into()),
        ] {
            compare(
                db,
                &query(
                    and(vec![
                        eq("tenant", json!("t")),
                        closed_bounds("score", bound.clone()),
                    ]),
                    20,
                ),
                DocumentQueryAccess::CompoundEqualityIndex {
                    index: "compound".into(),
                    fields: 2,
                },
            );
            for comparison in [
                Comparison::Less,
                Comparison::LessOrEqual,
                Comparison::Greater,
                Comparison::GreaterOrEqual,
            ] {
                compare(
                    db,
                    &query(
                        and(vec![
                            eq("tenant", json!("t")),
                            cmp("score", comparison, bound.clone()),
                        ]),
                        20,
                    ),
                    access(1, false),
                );
            }
        }
        for (lower, upper) in [
            (Comparison::Greater, Comparison::LessOrEqual),
            (Comparison::GreaterOrEqual, Comparison::Less),
            (Comparison::Greater, Comparison::Less),
        ] {
            let request = query(
                and(vec![
                    eq("tenant", json!("t")),
                    cmp("score", lower, IndexValue::Integer(5)),
                    cmp("score", upper, IndexValue::Integer(5)),
                ]),
                20,
            );
            compare(db, &request, access(1, false));
            assert!(
                db.query_planned("c", &request)
                    .unwrap()
                    .result
                    .documents
                    .is_empty()
            );
        }
    });
}

#[test]
fn four_field_range_requires_equal_prefix_and_present_suffix() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "kind", "score", "tail"], None))
            .unwrap();
        for (id, document) in [
            ("a", json!({"tenant":"t","kind":"k","score":4,"tail":null})),
            ("b", json!({"tenant":"t","kind":"k","score":4})),
            (
                "c",
                json!({"tenant":"t","kind":"other","score":4,"tail":true}),
            ),
        ] {
            db.insert("c", id, &document).unwrap();
        }
        let base = || {
            vec![
                eq("tenant", json!("t")),
                eq("kind", json!("k")),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(1)),
            ]
        };
        let mut full = base();
        full.push(exists("tail", true));
        compare(db, &query(and(full), 10), access(2, false));
        let missing = query(and(base()), 10);
        compare(db, &missing, DocumentQueryAccess::CollectionScan);
        assert!(
            db.query_planned("c", &missing)
                .unwrap()
                .result
                .documents
                .iter()
                .any(|row| row.id == "b")
        );
    });
}

#[test]
fn compound_range_detects_a_multi_page_phantom() {
    on_backends(|db| {
        prepare_large(db);
        let entered = Barrier::new(2);
        let changed = Barrier::new(2);
        std::thread::scope(|scope| {
            let writer = scope.spawn(|| {
                entered.wait();
                db.insert("c", "new", &json!({"tenant":"t","score":150}))
                    .unwrap();
                changed.wait();
            });
            let result = db.transaction_with_read_set(|tx| {
                let plan = tx.query_planned("c", &large_request(350))?;
                assert_eq!(plan.access, access(1, false));
                assert_eq!(plan.result.plan.scanned, 300);
                entered.wait();
                changed.wait();
                tx.put_raw(b"compound-range-paged-phantom", b"bad")?;
                Ok(())
            });
            let error = result.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::WouldBlock);
            assert!(database_engine::is_transaction_conflict(&error));
            writer.join().unwrap();
        });
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"compound-range-paged-phantom"))
                .unwrap(),
            None
        );
    });
}

#[test]
fn compound_range_enforces_original_document_byte_budget_and_rolls_back() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score"], None))
            .unwrap();
        for id in 0..31 {
            let overhead = serde_json::to_vec(&json!({"tenant":"t","score":id,"payload":""}))
                .unwrap()
                .len();
            let doc = json!({"tenant":"t","score":id,"payload":"x".repeat(1024 * 1024 - overhead)});
            assert_eq!(serde_json::to_vec(&doc).unwrap().len(), 1024 * 1024);
            db.insert("c", &format!("id{id:02}"), &doc).unwrap();
        }
        let request = DocumentQuery {
            filter: and(vec![
                eq("tenant", json!("t")),
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
            ]),
            max_scanned: 32,
            limit: 1,
            ..Default::default()
        };
        assert_eq!(
            db.query_planned("c", &request).unwrap().result.plan.scanned,
            31
        );
        let overhead = serde_json::to_vec(&json!({"tenant":"t","score":31,"payload":""}))
            .unwrap()
            .len();
        db.insert(
            "c",
            "id31",
            &json!({"tenant":"t","score":31,"payload":"x".repeat(1024 * 1024 - overhead)}),
        )
        .unwrap();
        assert_eq!(
            db.query("c", &request).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            db.query_planned("c", &request).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        let error = db
            .transaction(|tx| {
                tx.put_raw(b"compound-range-byte-rollback", b"bad")?;
                tx.query_planned("c", &request)?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"compound-range-byte-rollback"))
                .unwrap(),
            None
        );
    });
}

fn check_corruption(db: &DocumentDatabase<'_>) {
    let request = query(
        and(vec![
            eq("tenant", json!("t")),
            cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
        ]),
        10,
    );
    assert_eq!(
        db.query_planned("c", &request).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}
#[test]
fn compound_range_rejects_encountered_corrupt_index_entries() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score"], None))
            .unwrap();
        db.insert("c", "a", &json!({"tenant":"t","score":1}))
            .unwrap();
        let index_key = store
            .scan(None, None, 32)
            .unwrap()
            .into_iter()
            .find(|(key, _)| key.starts_with(b"\0emda-document\x01I"))
            .unwrap()
            .0;
        store.put(&index_key, b"corrupt").unwrap();
        if checkpoint_mode {
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            check_corruption(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            check_corruption(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn compound_ranges_work_after_full_log_recovery() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        let check = |db: &DocumentDatabase<'_>| {
            compare(
                db,
                &query(
                    and(vec![
                        eq("tenant", json!("t")),
                        cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(1)),
                    ]),
                    10,
                ),
                access(1, false),
            )
        };
        if checkpoint_mode {
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(&["tenant", "score"], None))
                .unwrap();
            db.insert("c", "a", &json!({"tenant":"t","score":3}))
                .unwrap();
            check(&db);
            drop(store);
        } else {
            let db = DocumentDatabase::new(&store);
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(&["tenant", "score"], None))
                .unwrap();
            db.insert("c", "a", &json!({"tenant":"t","score":3}))
                .unwrap();
            check(&db);
            drop(store);
        }
        let ordinary = retry_lock(|| Store::open(&source)).unwrap();
        check(&DocumentDatabase::new(&ordinary));
    }
}

fn or_presence_request(proof: DocumentFilter) -> DocumentQuery {
    query(
        and(vec![
            eq("tenant", json!("t")),
            cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(1)),
            proof,
        ]),
        20,
    )
}

fn or_presence_proof() -> DocumentFilter {
    DocumentFilter::Or(vec![
        eq("flag", json!(null)),
        and(vec![exists("flag", true), eq("kind", json!("yes"))]),
        DocumentFilter::Or(vec![eq("flag", json!(true)), eq("flag", json!(false))]),
    ])
}

#[test]
fn or_presence_proves_compound_suffix_and_mixed_branches_fall_back() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score", "flag"], None))
            .unwrap();
        for (id, doc) in [
            ("a", json!({"tenant":"t", "score":2, "flag":null})),
            (
                "b",
                json!({"tenant":"t", "score":3, "flag":true, "kind":"yes"}),
            ),
            ("c", json!({"tenant":"t", "score":4, "kind":"yes"})),
            ("d", json!({"tenant":"t", "score":5, "flag":false})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let request = or_presence_request(or_presence_proof());
        compare(db, &request, access(1, false));
        let ids: Vec<_> = db
            .query_planned("c", &request)
            .unwrap()
            .result
            .documents
            .into_iter()
            .map(|row| row.id)
            .collect();
        assert_eq!(ids, ["a", "b", "d"]);
        for proof in [
            DocumentFilter::Or(vec![exists("flag", true), eq("kind", json!("yes"))]),
            DocumentFilter::Or(vec![exists("flag", true), exists("flag", false)]),
            DocumentFilter::Or(vec![]),
        ] {
            compare(
                db,
                &or_presence_request(proof),
                DocumentQueryAccess::CollectionScan,
            );
        }
        db.transaction_with_read_set(|tx| {
            assert!(tx.delete("c", "a")?);
            tx.replace(
                "c",
                "c",
                &json!({"tenant":"t", "score":4, "flag":true}),
                false,
            )?;
            tx.replace("c", "b", &json!({"tenant":"t", "score":3}), false)?;
            let scan = tx.query("c", &request)?;
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(planned.access, access(1, false));
            assert_eq!(planned.result.documents, scan.documents);
            assert_eq!(
                planned
                    .result
                    .documents
                    .iter()
                    .map(|r| r.id.as_str())
                    .collect::<Vec<_>>(),
                ["c", "d"]
            );
            Ok(())
        })
        .unwrap();
        compare(db, &request, access(1, false));
    });
}

#[test]
fn or_presence_compound_range_conflicts_on_concurrent_phantom() {
    use std::sync::mpsc;
    use std::time::Duration;
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["tenant", "score", "flag"], None))
            .unwrap();
        let request = or_presence_request(or_presence_proof());
        compare(db, &request, access(1, false));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let writer = scope.spawn(move || {
                ready_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("reader did not reach query");
                let result = db.insert(
                    "c",
                    "phantom",
                    &json!({"tenant":"t", "score":2, "flag":true}),
                );
                done_tx.send(result).unwrap();
            });
            let result = db.transaction_with_read_set(|tx| {
                let planned = tx.query_planned("c", &request)?;
                assert_eq!(planned.access, access(1, false));
                assert!(planned.result.documents.is_empty());
                ready_tx.send(()).unwrap();
                done_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("writer timed out")?;
                tx.put_raw(b"or-presence-conflict-marker", b"must roll back")?;
                Ok(())
            });
            let error = result.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::WouldBlock);
            assert!(database_engine::is_transaction_conflict(&error));
            writer.join().unwrap();
        });
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"or-presence-conflict-marker"))
                .unwrap(),
            None
        );
        compare(db, &request, access(1, false));
        assert_eq!(db.query("c", &request).unwrap().documents.len(), 1);
    });
}

#[test]
fn membership_prefix_ranges_preserve_partial_and_suffix_requirements() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                unique: true,
                ..index(
                    &["region", "tenant", "score", "flag"],
                    Some(PartialFilter::Equals {
                        path: path("active"),
                        value: IndexValue::Boolean(true),
                    }),
                )
            },
        )
        .unwrap();
        for (id, document) in [
            (
                "a",
                json!({"region":1,"tenant":"t","score":2,"flag":null,"active":true}),
            ),
            (
                "b",
                json!({"region":1,"tenant":"t","score":3,"flag":true,"active":true}),
            ),
            (
                "c",
                json!({"region":1,"tenant":null,"score":3,"flag":true,"active":true}),
            ),
            (
                "d",
                json!({"region":1,"tenant":"t","score":3,"active":true}),
            ),
            (
                "e",
                json!({"region":1,"tenant":"t","score":3,"flag":true,"active":false}),
            ),
            (
                "f",
                json!({"region":2,"tenant":"t","score":3,"flag":true,"active":true}),
            ),
            (
                "g",
                json!({"region":1,"tenant":"t","score":4,"flag":true,"active":true}),
            ),
        ] {
            db.insert("c", id, &document).unwrap();
        }
        let predicates = vec![
            eq("region", json!(1)),
            membership("tenant", vec![Value::Null, json!("t"), json!("t")]),
            cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(2)),
            cmp("score", Comparison::Less, IndexValue::Integer(4)),
            exists("flag", true),
            eq("active", json!(true)),
        ];
        let full_scan = query(and(predicates.clone()), 7);
        let request = query(and(predicates.clone()), 3);
        let planned = db.query_planned("c", &request).unwrap();
        assert_eq!(planned.access, membership_range_access(2, 2, true));
        assert_eq!(planned.result.plan.scanned, 3);
        assert_eq!(
            planned.result.documents,
            db.query("c", &full_scan).unwrap().documents
        );
        assert_eq!(
            planned
                .result
                .documents
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );

        for missing in [4, 5] {
            let mut unsafe_to_plan = predicates.clone();
            unsafe_to_plan.remove(missing);
            compare(
                db,
                &query(and(unsafe_to_plan), 7),
                DocumentQueryAccess::CollectionScan,
            );
        }
        let mut mixed = predicates.clone();
        mixed[1] = membership("tenant", vec![json!("t"), json!(["array"])]);
        compare(db, &query(and(mixed), 7), prefix_access(1, 1, true));
        let mut impossible = predicates.clone();
        impossible[3] = cmp("score", Comparison::Less, IndexValue::Integer(2));
        let impossible = query(and(impossible), 7);
        compare(db, &impossible, membership_range_access(2, 2, true));
        assert!(
            db.query_planned("c", &impossible)
                .unwrap()
                .result
                .documents
                .is_empty()
        );

        let over = query(and(predicates), 2);
        assert_eq!(
            db.query_planned("c", &over).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        db.transaction_with_read_set(|tx| {
            assert!(tx.delete("c", "a")?);
            tx.replace(
                "c",
                "d",
                &json!({"region":1,"tenant":"t","score":2,"flag":false,"active":true}),
                false,
            )?;
            let staged = tx.query_planned("c", &request)?;
            assert_eq!(staged.access, membership_range_access(2, 2, true));
            assert_eq!(
                staged.result.documents,
                tx.query("c", &full_scan)?.documents
            );
            assert_eq!(
                staged
                    .result
                    .documents
                    .iter()
                    .map(|row| row.id.as_str())
                    .collect::<Vec<_>>(),
                ["b", "c", "d"]
            );
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn membership_range_probe_products_share_the_disjunction_limit() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["a", "b", "score"], None))
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "tag".into(),
                ..index(&["tag"], None)
            },
        )
        .unwrap();
        db.insert("c", "product", &json!({"a":0,"b":0,"score":3}))
            .unwrap();
        db.insert("c", "separate", &json!({"tag":"other"})).unwrap();
        let range = |a, b| {
            and(vec![
                membership("a", (0..a).map(|n| json!(n)).collect()),
                membership("b", (0..b).map(|n| json!(n)).collect()),
                cmp("score", Comparison::Greater, IndexValue::Integer(2)),
            ])
        };
        compare(
            db,
            &query(range(16, 16), 4),
            membership_range_access(2, 256, false),
        );
        compare(
            db,
            &query(range(16, 17), 4),
            DocumentQueryAccess::CollectionScan,
        );
        let disjunction = |a, b| DocumentFilter::Or(vec![range(a, b), eq("tag", json!("other"))]);
        compare(
            db,
            &query(disjunction(15, 17), 4),
            DocumentQueryAccess::DisjunctionIndex {
                indexes: vec!["compound".into(), "tag".into()],
                probes: 256,
            },
        );
        compare(
            db,
            &query(disjunction(16, 16), 4),
            DocumentQueryAccess::CollectionScan,
        );
    });
}

#[test]
fn empty_membership_prefix_ranges_detect_matching_phantoms() {
    use std::sync::mpsc;
    use std::time::Duration;

    for (tenant, score, conflict) in [("z", 3, true), ("z", 4, false), ("m", 3, false)] {
        on_backends(|db| {
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(&["tenant", "score"], None))
                .unwrap();
            let request = query(
                and(vec![
                    membership("tenant", vec![json!("a"), json!("z")]),
                    cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(3)),
                    cmp("score", Comparison::Less, IndexValue::Integer(4)),
                ]),
                4,
            );
            let (observed, read) = mpsc::channel();
            let (committed, written) = mpsc::channel();
            let result = std::thread::scope(|scope| {
                let writer = scope.spawn(move || {
                    read.recv_timeout(Duration::from_secs(10)).unwrap();
                    committed
                        .send(db.insert("c", "new", &json!({"tenant":tenant,"score":score})))
                        .unwrap();
                });
                let result = db.transaction_with_read_set(|tx| {
                    let plan = tx.query_planned("c", &request)?;
                    assert_eq!(plan.access, membership_range_access(1, 2, false));
                    assert!(plan.result.documents.is_empty());
                    observed.send(()).unwrap();
                    written.recv_timeout(Duration::from_secs(10)).unwrap()?;
                    tx.put_raw(b"membership-range-phantom", b"committed")
                });
                drop(observed);
                writer.join().unwrap();
                result
            });
            if conflict {
                assert!(database_engine::is_transaction_conflict(
                    &result.unwrap_err()
                ));
            } else {
                result.unwrap();
            }
            assert_eq!(
                db.transaction(|tx| tx.get_raw(b"membership-range-phantom"))
                    .unwrap(),
                (!conflict).then(|| b"committed".to_vec())
            );
            compare(db, &request, membership_range_access(1, 2, false));
        });
    }
}

fn prefix_access(prefix_fields: usize, probes: usize, partial: bool) -> DocumentQueryAccess {
    DocumentQueryAccess::CompoundPrefixIndex {
        index: "compound".into(),
        prefix_fields,
        probes,
        partial,
    }
}

#[test]
fn compound_prefix_requires_trailing_presence_and_scans_unique_tuples() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                unique: true,
                ..index(
                    &["tenant", "rank", "flag"],
                    Some(PartialFilter::Exists {
                        path: path("active"),
                        exists: true,
                    }),
                )
            },
        )
        .unwrap();
        for (id, doc) in [
            (
                "a",
                json!({"tenant":"t","rank":1,"flag":null,"active":true}),
            ),
            (
                "b",
                json!({"tenant":"t","rank":2,"flag":null,"active":false}),
            ),
            (
                "c",
                json!({"tenant":"u","rank":3,"flag":true,"active":true}),
            ),
            ("d", json!({"tenant":"t","rank":4,"active":true})),
            ("e", json!({"tenant":"t","rank":5,"flag":true})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let predicates = vec![
            eq("tenant", json!("t")),
            exists("rank", true),
            exists("flag", true),
            exists("active", true),
        ];
        compare(
            db,
            &query(and(predicates.clone()), 10),
            prefix_access(1, 1, true),
        );
        let bounded = query(and(predicates.clone()), 2);
        let plan = db.query_planned("c", &bounded).unwrap();
        assert_eq!(plan.access, prefix_access(1, 1, true));
        assert_eq!(plan.result.plan.scanned, 2);
        assert!(db.query("c", &bounded).is_err());
        assert_eq!(
            db.query_planned("c", &query(and(predicates.clone()), 1))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput,
        );
        for remove in 1..predicates.len() {
            let mut incomplete = predicates.clone();
            incomplete.remove(remove);
            compare(
                db,
                &query(and(incomplete), 10),
                DocumentQueryAccess::CollectionScan,
            );
        }
        let mut union = predicates;
        union[0] = membership("tenant", vec![json!("u"), json!("t"), json!("t")]);
        let request = query(and(union), 10);
        compare(db, &request, prefix_access(1, 2, true));
        db.transaction_with_read_set(|tx| {
            assert!(tx.delete("c", "a")?);
            tx.replace(
                "c",
                "d",
                &json!({"tenant":"u","rank":4,"flag":false,"active":true}),
                false,
            )?;
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(planned.result.documents, tx.query("c", &request)?.documents);
            assert_eq!(
                planned
                    .result
                    .documents
                    .iter()
                    .map(|row| row.id.as_str())
                    .collect::<Vec<_>>(),
                ["b", "c", "d"]
            );
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn compound_prefix_probe_limits_and_existing_disjunction_preference() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(&["a", "b", "score"], None))
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "tag".into(),
                ..index(&["tag"], None)
            },
        )
        .unwrap();
        db.insert("c", "one", &json!({"a":0,"b":0,"score":1}))
            .unwrap();
        db.insert("c", "two", &json!({"a":0,"b":0,"score":2}))
            .unwrap();
        db.insert("c", "other", &json!({"tag":"other"})).unwrap();
        let product = |a, b| {
            and(vec![
                membership("a", (0..a).map(|n| json!(n)).collect()),
                membership("b", (0..b).map(|n| json!(n)).collect()),
                exists("score", true),
            ])
        };
        compare(db, &query(product(16, 16), 4), prefix_access(2, 256, false));
        compare(
            db,
            &query(product(16, 17), 4),
            DocumentQueryAccess::CollectionScan,
        );
        let union = |a, b| {
            query(
                DocumentFilter::Or(vec![product(a, b), eq("tag", json!("other"))]),
                4,
            )
        };
        compare(
            db,
            &union(15, 17),
            DocumentQueryAccess::DisjunctionIndex {
                indexes: vec!["compound".into(), "tag".into()],
                probes: 256,
            },
        );
        compare(db, &union(16, 16), DocumentQueryAccess::CollectionScan);
        let existing_disjunction = query(
            and(vec![
                eq("a", json!(0)),
                eq("b", json!(0)),
                exists("score", true),
                DocumentFilter::Or(vec![eq("score", json!(1)), eq("score", json!(4))]),
            ]),
            1,
        );
        let plan = db.query_planned("c", &existing_disjunction).unwrap();
        assert_eq!(
            plan.access,
            DocumentQueryAccess::DisjunctionIndex {
                indexes: vec!["compound".into()],
                probes: 2,
            }
        );
        assert_eq!(plan.result.documents[0].id, "one");
    });
}

#[test]
fn empty_compound_prefix_detects_a_concurrent_indexed_insert() {
    use std::{sync::mpsc, time::Duration};
    for (doc, conflict) in [
        (json!({"tenant":"z","flag":null}), true),
        (json!({"tenant":"z"}), false),
        (json!({"tenant":"q","flag":true}), false),
    ] {
        on_backends(|db| {
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(&["tenant", "flag"], None))
                .unwrap();
            let request = query(
                and(vec![
                    membership("tenant", vec![json!("a"), json!("z")]),
                    exists("flag", true),
                ]),
                4,
            );
            let (read, observed) = mpsc::channel();
            let (committed, written) = mpsc::channel();
            let doc = &doc;
            let result = std::thread::scope(|scope| {
                let writer = scope.spawn(move || {
                    observed.recv_timeout(Duration::from_secs(10)).unwrap();
                    committed.send(db.insert("c", "new", doc)).unwrap();
                });
                let result = db.transaction_with_read_set(|tx| {
                    let plan = tx.query_planned("c", &request)?;
                    assert_eq!(plan.access, prefix_access(1, 2, false));
                    assert!(plan.result.documents.is_empty());
                    read.send(()).unwrap();
                    written.recv_timeout(Duration::from_secs(10)).unwrap()?;
                    tx.put_raw(b"prefix-phantom", b"committed")
                });
                drop(read);
                writer.join().unwrap();
                result
            });
            if conflict {
                assert!(database_engine::is_transaction_conflict(
                    &result.unwrap_err()
                ));
            } else {
                result.unwrap();
            }
            assert_eq!(
                db.transaction(|tx| tx.get_raw(b"prefix-phantom")).unwrap(),
                (!conflict).then(|| b"committed".to_vec())
            );
            compare(db, &request, prefix_access(1, 2, false));
        });
    }
}
