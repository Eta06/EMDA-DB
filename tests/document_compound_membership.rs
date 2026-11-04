//! Compound membership plans must be equivalent to a bounded complete scan.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, IndexValue, PartialFilter, Store,
};
use serde_json::{Value, json};
use std::{io::ErrorKind, sync::Barrier};

mod support;
use support::retry_lock;

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
fn path(name: &str) -> Vec<String> {
    vec![name.into()]
}
fn eq(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(name),
        value,
    }
}
fn inside(name: &str, values: Vec<Value>) -> DocumentFilter {
    DocumentFilter::In {
        path: path(name),
        values,
    }
}
fn and(filters: Vec<DocumentFilter>) -> DocumentFilter {
    DocumentFilter::And(filters)
}
fn query(filter: DocumentFilter) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned: 1024,
        ..Default::default()
    }
}
fn index(unique: bool, partial: Option<PartialFilter>) -> IndexDefinition {
    IndexDefinition {
        name: "pair".into(),
        fields: vec![path("a"), path("b")],
        unique,
        partial,
    }
}
fn access(probes: usize, partial: bool) -> DocumentQueryAccess {
    if partial {
        DocumentQueryAccess::PartialCompoundMembershipIndex {
            index: "pair".into(),
            fields: 2,
            probes,
        }
    } else {
        DocumentQueryAccess::CompoundMembershipIndex {
            index: "pair".into(),
            fields: 2,
            probes,
        }
    }
}
fn compare(db: &DocumentDatabase<'_>, request: &DocumentQuery, expected: DocumentQueryAccess) {
    let scan = db.query("c", request).unwrap();
    let planned = db.query_planned("c", request).unwrap();
    assert_eq!(planned.access, expected);
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
    assert_eq!(planned.result.plan.returned, scan.plan.returned);
    assert_eq!(planned.result.plan.sorted, scan.plan.sorted);
}

#[test]
fn compound_unions_match_scans_for_unique_and_nonunique_indexes() {
    for unique in [false, true] {
        on_backends(|db| {
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(unique, None)).unwrap();
            for (id, document) in [
                ("a", json!({"a": 1, "b": "x", "rank": 5})),
                ("b", json!({"a": 2, "b": "x", "rank": 4})),
                ("c", json!({"a": null, "b": "y", "rank": 3})),
                ("d", json!({"a": 1, "b": "y", "rank": 2})),
                ("e", json!({"a": 1, "rank": 1})),
                ("f", json!({"b": "x", "rank": 0})),
            ] {
                db.insert("c", id, &document).unwrap();
            }
            let mut request = query(and(vec![
                inside("a", vec![json!(2), Value::Null, json!(1), json!(1)]),
                inside("b", vec![json!("y"), json!("x"), json!("y")]),
            ]));
            request.sort = Some(DocumentSort {
                path: path("rank"),
                descending: false,
            });
            request.offset = 1;
            request.limit = 2;
            request.projection = Some(vec![path("a"), path("rank")]);
            compare(db, &request, access(6, false));
            compare(
                db,
                &query(and(vec![
                    eq("a", json!(1)),
                    inside("b", vec![json!("x"), json!("y")]),
                ])),
                access(2, false),
            );
            compare(
                db,
                &query(and(vec![
                    inside("a", vec![json!(1), json!(1)]),
                    eq("b", json!("x")),
                ])),
                access(1, false),
            );
            let mut limited = query(and(vec![
                inside("a", vec![json!(1), json!(2)]),
                eq("b", json!("x")),
            ]));
            limited.max_scanned = 1;
            assert_eq!(
                db.query_planned("c", &limited).unwrap_err().kind(),
                ErrorKind::InvalidInput
            );
        });
    }
}

#[test]
fn incomplete_unindexable_and_boolean_alternatives_safely_fall_back() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(false, None)).unwrap();
        db.insert("c", "one", &json!({"a": 1})).unwrap();
        db.insert("c", "two", &json!({"a": 2, "b": "x"})).unwrap();
        for filter in [
            inside("a", vec![json!(1), json!(2)]),
            and(vec![
                inside("a", vec![json!(1), json!([1])]),
                eq("b", json!("x")),
            ]),
            and(vec![
                DocumentFilter::Or(vec![inside("a", vec![json!(1), json!(2)])]),
                eq("b", json!("x")),
            ]),
            and(vec![
                DocumentFilter::Not(Box::new(inside("a", vec![json!(1), json!(2)]))),
                eq("b", json!("x")),
            ]),
        ] {
            compare(db, &query(filter), DocumentQueryAccess::CollectionScan);
        }
    });
}

#[test]
fn compound_probe_product_is_bounded_before_allocating() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(false, None)).unwrap();
        db.insert("c", "one", &json!({"a": 1, "b": 1})).unwrap();
        let values = |count| (0..count).map(|n| json!(n)).collect();
        let exact = query(and(vec![inside("a", values(16)), inside("b", values(16))]));
        compare(db, &exact, access(256, false));
        let above = query(and(vec![inside("a", values(17)), inside("b", values(16))]));
        compare(db, &above, DocumentQueryAccess::CollectionScan);
    });
}

#[test]
fn partial_guard_must_be_proved_for_every_result() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                false,
                Some(PartialFilter::Equals {
                    path: path("active"),
                    value: IndexValue::Boolean(true),
                }),
            ),
        )
        .unwrap();
        for (id, document) in [
            ("a", json!({"a": 1, "b": "x", "active": true})),
            ("b", json!({"a": 2, "b": "x", "active": false})),
            ("c", json!({"a": 2, "b": "x"})),
        ] {
            db.insert("c", id, &document).unwrap();
        }
        let filters = vec![inside("a", vec![json!(1), json!(2)]), eq("b", json!("x"))];
        compare(
            db,
            &query(and(filters.clone())),
            DocumentQueryAccess::CollectionScan,
        );
        let mut guarded = filters;
        guarded.push(eq("active", json!(true)));
        compare(db, &query(and(guarded)), access(2, true));
    });
}

#[test]
fn each_compound_probe_tracks_phantoms_and_transactional_staging() {
    for (a, b, conflicts) in [
        (1, "x", true),
        (2, "y", true),
        (3, "x", false),
        (1, "z", false),
    ] {
        on_backends(|db| {
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(false, None)).unwrap();
            let request = query(and(vec![
                inside("a", vec![json!(1), json!(2)]),
                inside("b", vec![json!("x"), json!("y")]),
            ]));
            db.transaction_with_read_set(|tx| {
                tx.insert("c", "staged", &json!({"a": 1, "b": "y"}))?;
                let planned = tx.query_planned("c", &request)?;
                assert_eq!(planned.access, access(4, false));
                assert_eq!(planned.result.documents[0].id, "staged");
                assert!(tx.delete("c", "staged")?);
                Ok(())
            })
            .unwrap();
            let entered = Barrier::new(2);
            let changed = Barrier::new(2);
            std::thread::scope(|scope| {
                let writer = scope.spawn(|| {
                    entered.wait();
                    db.insert("c", "concurrent", &json!({"a":a,"b":b})).unwrap();
                    changed.wait();
                });
                let result = db.transaction_with_read_set(|tx| {
                    let planned = tx.query_planned("c", &request)?;
                    assert_eq!(planned.access, access(4, false));
                    assert!(planned.result.documents.is_empty());
                    entered.wait();
                    changed.wait();
                    tx.put_raw(b"compound-union-commit", b"yes")?;
                    Ok(())
                });
                if conflicts {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                } else {
                    result.unwrap();
                }
                writer.join().unwrap();
            });
            assert_eq!(
                db.transaction(|tx| tx.get_raw(b"compound-union-commit"))
                    .unwrap(),
                (!conflicts).then(|| b"yes".to_vec())
            );
        });
    }
}

#[test]
fn compound_union_enforces_one_candidate_budget_across_pages_and_probes() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(false, None)).unwrap();
        for start in (0..260).step_by(20) {
            db.transaction(|tx| {
                for id in start..start + 20 {
                    tx.insert("c", &format!("{id:04}"), &json!({"a": 1, "b": "x"}))?;
                }
                Ok(())
            })
            .unwrap();
        }
        for id in 260..263 {
            db.insert("c", &format!("{id:04}"), &json!({"a": 2, "b": "x"}))
                .unwrap();
        }
        let mut request = query(and(vec![
            inside("a", vec![json!(1), json!(2)]),
            eq("b", json!("x")),
        ]));
        request.max_scanned = 263;
        request.offset = 253;
        request.limit = 10;
        compare(db, &request, access(2, false));
        request.max_scanned = 262;
        assert_eq!(
            db.query_planned("c", &request).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn compound_union_enforces_shared_document_bytes_and_rolls_back() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(false, None)).unwrap();
        let payload = "x".repeat(1_000_000);
        for id in 0..34 {
            db.insert(
                "c",
                &format!("{id:02}"),
                &json!({"a": id % 2, "b": "x", "payload": payload}),
            )
            .unwrap();
        }
        let request = query(and(vec![
            inside("a", vec![json!(0), json!(1)]),
            eq("b", json!("x")),
        ]));
        let error = db
            .transaction(|tx| {
                tx.put_raw(b"compound-union-byte-rollback", b"must not commit")?;
                tx.query_planned("c", &request)?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"compound-union-byte-rollback"))
                .unwrap(),
            None
        );
    });
}

#[test]
fn compound_union_detects_corruption_on_a_later_probe() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(false, None)).unwrap();
        db.insert("c", "a", &json!({"a": 1, "b": "x"})).unwrap();
        db.insert("c", "z", &json!({"a": 2, "b": "x"})).unwrap();
        let entry = store
            .scan(None, None, 32)
            .unwrap()
            .into_iter()
            .find(|(key, _)| key.starts_with(b"\0emda-document\x01I") && key.ends_with(b"z"))
            .unwrap()
            .0;
        store.put(&entry, b"corrupt").unwrap();
        let check = |db: &DocumentDatabase<'_>| {
            let request = query(and(vec![
                inside("a", vec![json!(1), json!(2)]),
                eq("b", json!("x")),
            ]));
            assert_eq!(db.query("c", &request).unwrap().documents.len(), 2);
            assert_eq!(
                db.query_planned("c", &request).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
        };
        if checkpoint_mode {
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            check(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            check(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn compound_union_survives_checkpoint_and_full_log_recovery() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&source).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index(false, None)).unwrap();
    db.insert("c", "a", &json!({"a": 1, "b": "x"})).unwrap();
    db.insert("c", "b", &json!({"a": 2, "b": "x"})).unwrap();
    let request = query(and(vec![
        inside("a", vec![json!(1), json!(2)]),
        eq("b", json!("x")),
    ]));
    compare(&db, &request, access(2, false));
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let checkpoint_store =
        retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    compare(
        &DocumentDatabase::with_checkpoint_store(&checkpoint_store),
        &request,
        access(2, false),
    );
    drop(checkpoint_store);
    let recovered = retry_lock(|| Store::open(&source)).unwrap();
    compare(
        &DocumentDatabase::new(&recovered),
        &request,
        access(2, false),
    );
}
