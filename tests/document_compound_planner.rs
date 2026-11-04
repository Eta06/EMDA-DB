//! Compound plans must not omit documents missing unconstrained tuple fields.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::sync::Barrier;

mod support;
use support::retry_lock;

fn path(name: &str) -> Vec<String> {
    name.split('.').map(str::to_owned).collect()
}
fn index(name: &str, fields: &[&str], unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: fields.iter().map(|field| path(field)).collect(),
        unique,
        partial: None,
    }
}
fn equal(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(name),
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
fn compound(name: &str, fields: usize) -> DocumentQueryAccess {
    DocumentQueryAccess::CompoundEqualityIndex {
        index: name.into(),
        fields,
    }
}
fn unique(name: &str, fields: usize) -> DocumentQueryAccess {
    DocumentQueryAccess::UniqueCompoundEqualityIndex {
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
fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        index("tenant_category", &["tenant", "category"], false),
    )
    .unwrap();
    db.create_index("c", index("tenant_email", &["tenant", "email"], true))
        .unwrap();
    for (id, doc) in [
        (
            "a",
            json!({"tenant":"t", "category":"red", "email":"a", "n":3}),
        ),
        (
            "b",
            json!({"tenant":"t", "category":"red", "email":"b", "n":2}),
        ),
        (
            "c",
            json!({"tenant":"t", "category":null, "email":"c", "n":1}),
        ),
        ("d", json!({"tenant":"t", "email":"d"})),
        ("e", json!({"category":"red", "email":"e"})),
        ("f", json!({"tenant":null, "category":"red", "email":"f"})),
        (
            "g",
            json!({"tenant":"t", "category":"a\u{0}z", "email":"g"}),
        ),
    ] {
        db.insert("c", id, &doc).unwrap();
    }
}

#[test]
fn complete_compound_equalities_match_scans_and_incomplete_tuples_fall_back() {
    on_backends(|db| {
        prepare(db);
        let cases = [
            (
                and(vec![
                    equal("tenant", json!("t")),
                    equal("category", json!("red")),
                ]),
                compound("tenant_category", 2),
            ),
            (
                and(vec![
                    equal("category", Value::Null),
                    equal("tenant", json!("t")),
                ]),
                compound("tenant_category", 2),
            ),
            (
                and(vec![
                    equal("tenant", Value::Null),
                    equal("category", json!("red")),
                ]),
                compound("tenant_category", 2),
            ),
            (
                and(vec![
                    equal("tenant", json!("t")),
                    equal("category", json!("a\u{0}z")),
                ]),
                compound("tenant_category", 2),
            ),
            (
                and(vec![
                    equal("tenant", json!("t")),
                    equal("email", json!("a")),
                    equal("category", json!("red")),
                ]),
                unique("tenant_email", 2),
            ),
            (
                and(vec![
                    equal("tenant", json!("t")),
                    equal("category", json!("red")),
                    equal("category", json!("blue")),
                ]),
                compound("tenant_category", 2),
            ),
            (
                equal("tenant", json!("t")),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                and(vec![
                    equal("tenant", json!("t")),
                    DocumentFilter::Or(vec![equal("category", json!("red")), DocumentFilter::All]),
                ]),
                DocumentQueryAccess::CollectionScan,
            ),
            (
                DocumentFilter::Not(Box::new(and(vec![
                    equal("tenant", json!("t")),
                    equal("category", json!("red")),
                ]))),
                DocumentQueryAccess::CollectionScan,
            ),
        ];
        for (filter, expected) in cases {
            let base = query(filter, 16);
            compare(db, &base, expected.clone());
            let decorated = DocumentQuery {
                sort: Some(DocumentSort {
                    path: path("email"),
                    descending: true,
                }),
                projection: Some(vec![path("tenant"), path("category")]),
                offset: 1,
                limit: 2,
                ..base
            };
            compare(db, &decorated, expected);
        }
        let only_tenant = db
            .query_planned("c", &query(equal("tenant", json!("t")), 16))
            .unwrap();
        assert!(
            only_tenant.result.documents.iter().any(|row| row.id == "d"),
            "the missing category must not disappear"
        );
        let exact = query(
            and(vec![
                equal("tenant", json!("t")),
                equal("category", json!("red")),
            ]),
            16,
        );
        db.transaction_with_read_set(|tx| {
            assert!(tx.delete("c", "a")?);
            tx.replace(
                "c",
                "b",
                &json!({"tenant":"t", "category":"other", "email":"b"}),
                false,
            )?;
            tx.insert(
                "c",
                "new",
                &json!({"tenant":"t", "category":"red", "email":"new"}),
            )?;
            let scan = tx.query("c", &exact)?;
            let planned = tx.query_planned("c", &exact)?;
            assert_eq!(planned.access, compound("tenant_category", 2));
            assert_eq!(planned.result.documents, scan.documents);
            Ok(())
        })
        .unwrap();
        compare(db, &exact, compound("tenant_category", 2));
    });
}

#[test]
fn four_field_tuple_requires_every_indexed_field() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index("four", &["a", "b", "c", "d"], true))
            .unwrap();
        db.insert("c", "full", &json!({"a":1,"b":true,"c":null,"d":"z"}))
            .unwrap();
        db.insert("c", "partial", &json!({"a":1,"b":true,"c":null}))
            .unwrap();
        let complete = query(
            and(vec![
                equal("a", json!(1)),
                equal("b", json!(true)),
                equal("c", Value::Null),
                equal("d", json!("z")),
            ]),
            10,
        );
        compare(db, &complete, unique("four", 4));
        let incomplete = query(
            and(vec![
                equal("a", json!(1)),
                equal("b", json!(true)),
                equal("c", Value::Null),
            ]),
            10,
        );
        compare(db, &incomplete, DocumentQueryAccess::CollectionScan);
        let ids = db
            .query_planned("c", &incomplete)
            .unwrap()
            .result
            .documents
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, ["full", "partial"]);
    });
}

fn large(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("pair", &["tenant", "category"], false))
        .unwrap();
    for start in (0..360).step_by(20) {
        db.transaction(|tx| {
            for i in start..start + 20 {
                let category = if i < 300 { "match" } else { "other" };
                tx.insert(
                    "c",
                    &format!("{i:04}"),
                    &json!({"tenant":"t", "category":category, "n":i}),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
    let base = DocumentQuery {
        filter: and(vec![
            equal("tenant", json!("t")),
            equal("category", json!("match")),
        ]),
        max_scanned: 400,
        offset: 260,
        limit: 20,
        ..Default::default()
    };
    compare(db, &base, compound("pair", 2));
    let exact = DocumentQuery {
        max_scanned: 300,
        ..base.clone()
    };
    let result = db.query_planned("c", &exact).unwrap();
    assert_eq!(result.result.plan.scanned, 300);
    assert_eq!(
        result
            .result
            .documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        (260..280).map(|i| format!("{i:04}")).collect::<Vec<_>>()
    );
    let too_small = DocumentQuery {
        max_scanned: 299,
        ..base
    };
    assert_eq!(
        db.query_planned("c", &too_small).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}
#[test]
fn compound_equality_reads_all_pages_and_enforces_candidate_limits() {
    on_backends(large);
}

fn phantom(db: &DocumentDatabase<'_>, inside: bool) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("pair", &["tenant", "category"], false))
        .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert(
                "c",
                "new",
                &json!({"tenant":"t", "category":if inside {"match"} else {"other"}}),
            )
            .unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let request = query(
                and(vec![
                    equal("tenant", json!("t")),
                    equal("category", json!("match")),
                ]),
                10,
            );
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(planned.access, compound("pair", 2));
            assert!(planned.result.documents.is_empty());
            entered.wait();
            changed.wait();
            tx.put_raw(b"compound-phantom", b"committed")?;
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
        db.transaction(|tx| tx.get_raw(b"compound-phantom"))
            .unwrap(),
        if inside {
            None
        } else {
            Some(b"committed".to_vec())
        }
    );
}
#[test]
fn compound_ranges_detect_phantoms_and_allow_unrelated_tuples() {
    for inside in [false, true] {
        on_backends(|db| phantom(db, inside));
    }
}

fn byte_limits(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("pair", &["tenant", "category"], false))
        .unwrap();
    let overhead = serde_json::to_vec(&json!({"tenant":"t", "category":"match", "payload":""}))
        .unwrap()
        .len();
    let doc =
        json!({"tenant":"t", "category":"match", "payload":"x".repeat(1024 * 1024 - overhead)});
    assert_eq!(serde_json::to_vec(&doc).unwrap().len(), 1024 * 1024);
    for id in 0..31 {
        db.insert("c", &format!("{id:02}"), &doc).unwrap();
    }
    let request = DocumentQuery {
        filter: and(vec![
            equal("tenant", json!("t")),
            equal("category", json!("match")),
        ]),
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
            tx.put_raw(b"compound-budget-rollback", b"must not commit")?;
            tx.query_planned("c", &request)?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"compound-budget-rollback"))
            .unwrap(),
        None
    );
}
#[test]
fn compound_plans_enforce_document_byte_budget_and_rollback() {
    on_backends(byte_limits);
}

fn paged_phantom(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("pair", &["tenant", "category"], false))
        .unwrap();
    for start in (0..260).step_by(20) {
        db.transaction(|tx| {
            for id in start..start + 20 {
                tx.insert(
                    "c",
                    &format!("{id:04}"),
                    &json!({"tenant":"t", "category":"match"}),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert("c", "new", &json!({"tenant":"t", "category":"match"}))
                .unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let request = query(
                and(vec![
                    equal("tenant", json!("t")),
                    equal("category", json!("match")),
                ]),
                300,
            );
            let result = tx.query_planned("c", &request)?;
            assert_eq!(result.access, compound("pair", 2));
            assert_eq!(result.result.plan.scanned, 260);
            entered.wait();
            changed.wait();
            tx.put_raw(b"compound-page-phantom", b"must not commit")?;
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        writer.join().unwrap();
    });
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"compound-page-phantom"))
            .unwrap(),
        None
    );
}
#[test]
fn multi_page_compound_plan_detects_concurrent_phantoms() {
    on_backends(paged_phantom);
}

fn check_corruption(db: &DocumentDatabase<'_>) {
    let request = query(
        and(vec![
            equal("tenant", json!("t")),
            equal("category", json!("match")),
        ]),
        10,
    );
    assert_eq!(
        db.query_planned("c", &request).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}
#[test]
fn compound_plan_rejects_encountered_corrupt_index_entries() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index("pair", &["tenant", "category"], false))
            .unwrap();
        db.insert("c", "a", &json!({"tenant":"t", "category":"match"}))
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
fn compound_plans_work_after_full_log_recovery() {
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
            prepare(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            prepare(&DocumentDatabase::new(&store));
            drop(store);
        }
        let store = retry_lock(|| Store::open(&source)).unwrap();
        let db = DocumentDatabase::new(&store);
        let request = query(
            and(vec![
                equal("tenant", json!("t")),
                equal("category", json!("red")),
            ]),
            16,
        );
        compare(&db, &request, compound("tenant_category", 2));
    }
}
