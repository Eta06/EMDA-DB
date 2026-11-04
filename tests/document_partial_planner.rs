//! A partial index is safe only when the whole query implies its predicate.
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, IndexDefinition, IndexValue, PartialFilter, Store,
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
fn query(filter: DocumentFilter, max_scanned: usize) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned,
        ..Default::default()
    }
}
fn index(name: &str, fields: &[&str], unique: bool, partial: PartialFilter) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: fields.iter().map(|field| path(field)).collect(),
        unique,
        partial: Some(partial),
    }
}
fn partial_eq(name: &str, fields: usize) -> DocumentQueryAccess {
    DocumentQueryAccess::PartialEqualityIndex {
        index: name.into(),
        fields,
    }
}
fn compare(db: &DocumentDatabase<'_>, request: &DocumentQuery, access: DocumentQueryAccess) {
    let scan = db.query("c", request).unwrap();
    let planned = db.query_planned("c", request).unwrap();
    assert_eq!(planned.access, access);
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
    assert_eq!(planned.result.plan.returned, scan.plan.returned);
    assert_eq!(planned.result.plan.sorted, scan.plan.sorted);
}
fn on_backends(exercise: impl Fn(&DocumentDatabase<'_>)) {
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
            exercise(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            exercise(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn present_partial_condition_requires_a_proof_and_counts_null_as_present() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "score_present",
                &["score"],
                false,
                PartialFilter::Exists {
                    path: path("profile.tag"),
                    exists: true,
                },
            ),
        )
        .unwrap();
        for (id, doc) in [
            ("a", json!({"score":1,"profile":{"tag":null}})),
            ("b", json!({"score":1})),
            ("c", json!({"score":1,"profile":{"tag":"x"}})),
            ("d", json!({"score":2,"profile":{"tag":7}})),
            ("e", json!({"score":1,"profile":null})),
            ("f", json!({"score":1,"profile":{"tag":[1,2]}})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let cases = [
            (eq("score", json!(1)), DocumentQueryAccess::CollectionScan),
            (
                and(vec![eq("score", json!(1)), exists("profile.tag", true)]),
                partial_eq("score_present", 1),
            ),
            (
                and(vec![eq("score", json!(1)), eq("profile.tag", Value::Null)]),
                partial_eq("score_present", 1),
            ),
            (
                and(vec![
                    eq("score", json!(1)),
                    cmp("profile.tag", Comparison::Greater, IndexValue::Null),
                ]),
                partial_eq("score_present", 1),
            ),
            (
                and(vec![
                    eq("score", json!(1)),
                    DocumentFilter::ArrayContains {
                        path: path("profile.tag"),
                        value: json!(2),
                    },
                ]),
                partial_eq("score_present", 1),
            ),
            (
                and(vec![
                    eq("score", json!(1)),
                    DocumentFilter::ArrayContainsAny {
                        path: path("profile.tag"),
                        values: vec![json!(2)],
                    },
                ]),
                partial_eq("score_present", 1),
            ),
            (
                and(vec![
                    eq("score", json!(1)),
                    DocumentFilter::ArrayContainsAll {
                        path: path("profile.tag"),
                        values: vec![],
                    },
                ]),
                partial_eq("score_present", 1),
            ),
            (
                and(vec![
                    eq("score", json!(1)),
                    DocumentFilter::ArrayLength {
                        path: path("profile.tag"),
                        length: 2,
                    },
                ]),
                partial_eq("score_present", 1),
            ),
            (
                and(vec![
                    eq("score", json!(1)),
                    DocumentFilter::Not(Box::new(exists("profile.tag", false))),
                ]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                and(vec![
                    eq("score", json!(1)),
                    DocumentFilter::Or(vec![exists("profile.tag", true), DocumentFilter::All]),
                ]),
                DocumentQueryAccess::CollectionScan,
            ),
        ];
        for (filter, access) in cases {
            compare(db, &query(filter, 20), access);
        }
        let unguarded = db
            .query_planned("c", &query(eq("score", json!(1)), 20))
            .unwrap();
        assert_eq!(
            unguarded
                .result
                .documents
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c", "e", "f"]
        );
        let null_guard = db
            .query_planned(
                "c",
                &query(
                    and(vec![eq("score", json!(1)), eq("profile.tag", Value::Null)]),
                    20,
                ),
            )
            .unwrap();
        assert_eq!(null_guard.result.documents[0].id, "a");
    });
}

#[test]
fn absent_partial_condition_is_not_inferred_from_nonrequired_predicates() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "score_absent",
                &["score"],
                false,
                PartialFilter::Exists {
                    path: path("profile.tag"),
                    exists: false,
                },
            ),
        )
        .unwrap();
        for (id, doc) in [
            ("a", json!({"score":1})),
            ("b", json!({"score":1,"profile":{"tag":null}})),
            ("c", json!({"score":1,"profile":null})),
            ("d", json!({"score":2})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        for (filter, access) in [
            (
                and(vec![eq("score", json!(1)), exists("profile.tag", false)]),
                partial_eq("score_absent", 1),
            ),
            (eq("score", json!(1)), DocumentQueryAccess::CollectionScan),
            (
                and(vec![eq("score", json!(1)), exists("profile.tag", true)]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                and(vec![
                    eq("score", json!(1)),
                    DocumentFilter::Not(Box::new(exists("profile.tag", true))),
                ]),
                DocumentQueryAccess::CollectionScan,
            ),
        ] {
            compare(db, &query(filter, 10), access);
        }
        let selected = db
            .query_planned(
                "c",
                &query(
                    and(vec![eq("score", json!(1)), exists("profile.tag", false)]),
                    10,
                ),
            )
            .unwrap();
        assert_eq!(
            selected
                .result
                .documents
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "c"]
        );
    });
}

#[test]
fn exact_partial_equality_is_required_for_unique_and_range_plans() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        let active = || PartialFilter::Equals {
            path: path("active"),
            value: IndexValue::Boolean(true),
        };
        db.create_index("c", index("email_active", &["email"], true, active()))
            .unwrap();
        db.create_index("c", index("score_active", &["score"], false, active()))
            .unwrap();
        for (id, doc) in [
            ("a", json!({"email":"e","active":true,"score":5})),
            ("b", json!({"email":"e","active":false,"score":5})),
            ("c", json!({"email":"z","active":true,"score":7})),
            ("d", json!({"email":"e","score":5})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        for (filter, access) in [
            (eq("email", json!("e")), DocumentQueryAccess::CollectionScan),
            (
                and(vec![eq("email", json!("e")), eq("active", json!(true))]),
                DocumentQueryAccess::PartialUniqueEqualityIndex {
                    index: "email_active".into(),
                    fields: 1,
                },
            ),
            (
                and(vec![eq("email", json!("e")), exists("active", true)]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                and(vec![eq("email", json!("e")), eq("active", json!(false))]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                and(vec![
                    eq("email", json!("e")),
                    DocumentFilter::Or(vec![eq("active", json!(true)), DocumentFilter::All]),
                ]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                and(vec![
                    cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(5)),
                    eq("active", json!(true)),
                ]),
                DocumentQueryAccess::PartialRangeIndex {
                    index: "score_active".into(),
                },
            ),
            (
                cmp("score", Comparison::GreaterOrEqual, IndexValue::Integer(5)),
                DocumentQueryAccess::CollectionScan,
            ),
        ] {
            compare(db, &query(filter, 10), access);
        }
    });
}

#[test]
fn partial_compound_tuples_still_require_every_field_and_the_partial_condition() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "pair",
                &["tenant", "category"],
                false,
                PartialFilter::Exists {
                    path: path("flag"),
                    exists: true,
                },
            ),
        )
        .unwrap();
        for (id, doc) in [
            ("a", json!({"tenant":"t","category":"red","flag":null})),
            ("b", json!({"tenant":"t","category":"red"})),
            ("c", json!({"tenant":"t","flag":true})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let full = and(vec![
            eq("tenant", json!("t")),
            eq("category", json!("red")),
            exists("flag", true),
        ]);
        compare(db, &query(full, 10), partial_eq("pair", 2));
        let missing_guard = and(vec![eq("tenant", json!("t")), eq("category", json!("red"))]);
        compare(
            db,
            &query(missing_guard, 10),
            DocumentQueryAccess::CollectionScan,
        );
        let missing_field = and(vec![eq("tenant", json!("t")), exists("flag", true)]);
        compare(
            db,
            &query(missing_field, 10),
            DocumentQueryAccess::CollectionScan,
        );
    });
}

fn phantom(db: &DocumentDatabase<'_>, inside: bool) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        index(
            "group_active",
            &["group"],
            false,
            PartialFilter::Equals {
                path: path("active"),
                value: IndexValue::Boolean(true),
            },
        ),
    )
    .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert("c", "new", &json!({"group":"g","active":inside}))
                .unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let request = query(
                and(vec![eq("group", json!("g")), eq("active", json!(true))]),
                10,
            );
            let result = tx.query_planned("c", &request)?;
            assert_eq!(result.access, partial_eq("group_active", 1));
            assert!(result.result.documents.is_empty());
            entered.wait();
            changed.wait();
            tx.put_raw(b"partial-phantom", b"committed")?;
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
        db.transaction(|tx| tx.get_raw(b"partial-phantom")).unwrap(),
        if inside {
            None
        } else {
            Some(b"committed".to_vec())
        }
    );
}
#[test]
fn partial_index_detects_phantoms_but_allows_nonmatching_inserts() {
    for inside in [false, true] {
        on_backends(|db| phantom(db, inside));
    }
}

#[test]
fn partial_index_detects_phantoms_after_multiple_pages() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "group_active",
                &["group"],
                false,
                PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                },
            ),
        )
        .unwrap();
        for id in 0..260 {
            db.insert(
                "c",
                &format!("id{id:03}"),
                &json!({"group":"g","active":true}),
            )
            .unwrap();
        }
        let entered = Barrier::new(2);
        let changed = Barrier::new(2);
        std::thread::scope(|scope| {
            let writer = scope.spawn(|| {
                entered.wait();
                db.insert("c", "new", &json!({"group":"g","active":true}))
                    .unwrap();
                changed.wait();
            });
            let result = db.transaction_with_read_set(|tx| {
                let planned = tx.query_planned(
                    "c",
                    &query(
                        and(vec![eq("group", json!("g")), eq("active", json!(true))]),
                        300,
                    ),
                )?;
                assert_eq!(planned.access, partial_eq("group_active", 1));
                assert_eq!(planned.result.plan.matched, 260);
                entered.wait();
                changed.wait();
                tx.put_raw(b"partial-multipage-phantom", b"committed")?;
                Ok(())
            });
            assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
            writer.join().unwrap();
        });
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"partial-multipage-phantom"))
                .unwrap(),
            None
        );
    });
}

#[test]
fn partial_unique_compound_index_requires_full_tuple_and_its_guard() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "unique_pair",
                &["tenant", "category"],
                true,
                PartialFilter::Exists {
                    path: path("flag"),
                    exists: true,
                },
            ),
        )
        .unwrap();
        db.insert(
            "c",
            "a",
            &json!({"tenant":"t","category":"red","flag":null}),
        )
        .unwrap();
        db.insert("c", "b", &json!({"tenant":"t","category":"red"}))
            .unwrap();
        let complete = query(
            and(vec![
                eq("tenant", json!("t")),
                eq("category", json!("red")),
                exists("flag", true),
            ]),
            10,
        );
        compare(
            db,
            &complete,
            DocumentQueryAccess::PartialUniqueEqualityIndex {
                index: "unique_pair".into(),
                fields: 2,
            },
        );
        let no_guard = query(
            and(vec![eq("tenant", json!("t")), eq("category", json!("red"))]),
            10,
        );
        compare(db, &no_guard, DocumentQueryAccess::CollectionScan);
        assert_eq!(
            db.query_planned("c", &no_guard)
                .unwrap()
                .result
                .documents
                .len(),
            2
        );
        let no_field = query(
            and(vec![eq("tenant", json!("t")), exists("flag", true)]),
            10,
        );
        compare(db, &no_field, DocumentQueryAccess::CollectionScan);
    });
}

fn large(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        index(
            "group_active",
            &["group"],
            false,
            PartialFilter::Equals {
                path: path("active"),
                value: IndexValue::Boolean(true),
            },
        ),
    )
    .unwrap();
    for start in (0..360).step_by(20) {
        db.transaction(|tx| {
            for id in start..start + 20 {
                tx.insert(
                    "c",
                    &format!("{id:04}"),
                    &json!({"group":"g","active":id<260}),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
    let base = DocumentQuery {
        filter: and(vec![eq("group", json!("g")), eq("active", json!(true))]),
        max_scanned: 400,
        offset: 240,
        limit: 20,
        ..Default::default()
    };
    compare(db, &base, partial_eq("group_active", 1));
    let exact = DocumentQuery {
        max_scanned: 260,
        ..base.clone()
    };
    let result = db.query_planned("c", &exact).unwrap();
    assert_eq!(result.result.plan.scanned, 260);
    assert_eq!(
        result
            .result
            .documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        (240..260).map(|id| format!("{id:04}")).collect::<Vec<_>>()
    );
    assert_eq!(
        db.query("c", &exact).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let too_small = DocumentQuery {
        max_scanned: 259,
        ..base
    };
    assert_eq!(
        db.query_planned("c", &too_small).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}
#[test]
fn partial_index_reads_complete_multi_page_results_and_enforces_candidate_limit() {
    on_backends(large);
}

fn byte_limits(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        index(
            "group_active",
            &["group"],
            false,
            PartialFilter::Equals {
                path: path("active"),
                value: IndexValue::Boolean(true),
            },
        ),
    )
    .unwrap();
    let overhead = serde_json::to_vec(&json!({"group":"g","active":true,"payload":""}))
        .unwrap()
        .len();
    let doc = json!({"group":"g","active":true,"payload":"x".repeat(1024*1024-overhead)});
    assert_eq!(serde_json::to_vec(&doc).unwrap().len(), 1024 * 1024);
    for id in 0..31 {
        db.insert("c", &format!("{id:02}"), &doc).unwrap();
    }
    let request = DocumentQuery {
        filter: and(vec![eq("group", json!("g")), eq("active", json!(true))]),
        max_scanned: 32,
        limit: 1,
        ..Default::default()
    };
    assert_eq!(
        db.query_planned("c", &request).unwrap().result.plan.scanned,
        31
    );
    db.insert("c", "31", &doc).unwrap();
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
            tx.put_raw(b"partial-budget-rollback", b"must not commit")?;
            tx.query_planned("c", &request)?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"partial-budget-rollback"))
            .unwrap(),
        None
    );
}
#[test]
fn partial_index_enforces_document_byte_budget_and_rolls_back() {
    on_backends(byte_limits);
}

fn eligibility_update(db: &DocumentDatabase<'_>, enters: bool) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        index(
            "group_active",
            &["group"],
            false,
            PartialFilter::Equals {
                path: path("active"),
                value: IndexValue::Boolean(true),
            },
        ),
    )
    .unwrap();
    db.insert("c", "item", &json!({"group":"g","active":!enters}))
        .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.replace("c", "item", &json!({"group":"g","active":enters}), false)
                .unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let request = query(
                and(vec![eq("group", json!("g")), eq("active", json!(true))]),
                10,
            );
            let result = tx.query_planned("c", &request)?;
            assert_eq!(result.access, partial_eq("group_active", 1));
            assert_eq!(result.result.documents.len(), usize::from(!enters));
            entered.wait();
            changed.wait();
            tx.put_raw(b"partial-eligibility", b"must not commit")?;
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        writer.join().unwrap();
    });
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"partial-eligibility"))
            .unwrap(),
        None
    );
}
#[test]
fn partial_index_detects_documents_entering_and_leaving_its_predicate() {
    for enters in [false, true] {
        on_backends(|db| eligibility_update(db, enters));
    }
}

fn check_corruption(db: &DocumentDatabase<'_>) {
    let request = query(
        and(vec![eq("group", json!("g")), eq("active", json!(true))]),
        10,
    );
    assert_eq!(
        db.query_planned("c", &request).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}
#[test]
fn partial_plan_rejects_encountered_corrupt_index_entries() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "group_active",
                &["group"],
                false,
                PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                },
            ),
        )
        .unwrap();
        db.insert("c", "a", &json!({"group":"g","active":true}))
            .unwrap();
        let entry = store
            .scan(None, None, 32)
            .unwrap()
            .into_iter()
            .find(|(key, _)| key.starts_with(b"\0emda-document\x01I"))
            .unwrap()
            .0;
        store.put(&entry, b"corrupt").unwrap();
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
fn partial_value_proof_distinguishes_integer_float_null_and_missing() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "group_one",
                &["group"],
                false,
                PartialFilter::Equals {
                    path: path("flag"),
                    value: IndexValue::Integer(1),
                },
            ),
        )
        .unwrap();
        for (id, doc) in [
            ("a", json!({"group":"g","flag":1})),
            ("b", json!({"group":"g","flag":1.0})),
            ("c", json!({"group":"g","flag":null})),
            ("d", json!({"group":"g"})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let integer = query(and(vec![eq("group", json!("g")), eq("flag", json!(1))]), 10);
        compare(db, &integer, partial_eq("group_one", 1));
        let float = query(
            and(vec![eq("group", json!("g")), eq("flag", json!(1.0))]),
            10,
        );
        compare(db, &float, DocumentQueryAccess::CollectionScan);
        assert_eq!(
            db.query_planned("c", &float).unwrap().result.documents[0].id,
            "b"
        );
        let only_group = query(eq("group", json!("g")), 10);
        compare(db, &only_group, DocumentQueryAccess::CollectionScan);
        assert_eq!(
            db.query_planned("c", &only_group)
                .unwrap()
                .result
                .documents
                .len(),
            4
        );
    });
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "group_null",
                &["group"],
                false,
                PartialFilter::Equals {
                    path: path("flag"),
                    value: IndexValue::Null,
                },
            ),
        )
        .unwrap();
        db.insert("c", "null", &json!({"group":"g","flag":null}))
            .unwrap();
        db.insert("c", "missing", &json!({"group":"g"})).unwrap();
        let explicit = query(
            and(vec![eq("group", json!("g")), eq("flag", Value::Null)]),
            10,
        );
        compare(db, &explicit, partial_eq("group_null", 1));
        let missing = query(
            and(vec![eq("group", json!("g")), exists("flag", false)]),
            10,
        );
        compare(db, &missing, DocumentQueryAccess::CollectionScan);
        assert_eq!(
            db.query_planned("c", &missing).unwrap().result.documents[0].id,
            "missing"
        );
    });
}

#[test]
fn partial_queries_see_staged_documents_entering_and_leaving_the_index() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                "group_active",
                &["group"],
                false,
                PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                },
            ),
        )
        .unwrap();
        db.insert("c", "a", &json!({"group":"g","active":false}))
            .unwrap();
        let request = query(
            and(vec![eq("group", json!("g")), eq("active", json!(true))]),
            10,
        );
        db.transaction_with_read_set(|tx| {
            tx.replace("c", "a", &json!({"group":"g","active":true}), false)?;
            tx.insert("c", "b", &json!({"group":"g","active":true}))?;
            let scan = tx.query("c", &request)?;
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(planned.access, partial_eq("group_active", 1));
            assert_eq!(planned.result.documents, scan.documents);
            assert_eq!(planned.result.documents.len(), 2);
            tx.replace("c", "a", &json!({"group":"g","active":false}), false)?;
            assert!(tx.delete("c", "b")?);
            let scan = tx.query("c", &request)?;
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(planned.result.documents, scan.documents);
            assert!(planned.result.documents.is_empty());
            Ok(())
        })
        .unwrap();
        compare(db, &request, partial_eq("group_active", 1));
    });
}

#[test]
fn partial_plans_work_after_full_log_recovery() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        let prepare = |db: &DocumentDatabase<'_>| {
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index(
                "c",
                index(
                    "group_active",
                    &["group"],
                    false,
                    PartialFilter::Equals {
                        path: path("active"),
                        value: IndexValue::Boolean(true),
                    },
                ),
            )
            .unwrap();
            db.insert("c", "a", &json!({"group":"g","active":true}))
                .unwrap();
            db.insert("c", "b", &json!({"group":"g","active":false}))
                .unwrap();
        };
        if checkpoint_mode {
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            prepare(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            prepare(&DocumentDatabase::new(&store));
            drop(store);
        }
        let store = retry_lock(|| Store::open(&source)).unwrap();
        let db = DocumentDatabase::new(&store);
        let request = query(
            and(vec![eq("group", json!("g")), eq("active", json!(true))]),
            10,
        );
        compare(&db, &request, partial_eq("group_active", 1));
    }
}
