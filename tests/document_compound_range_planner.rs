//! Compound ranges must not omit documents missing an indexed suffix field.
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
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
        compare(
            db,
            &query(and(negated), 20),
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
                    eq("tenant", json!("t")),
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
            assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
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
