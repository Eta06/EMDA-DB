//! Planned scalar ranges must match complete scans and preserve serializability.
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, IndexValue, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::sync::Barrier;

mod support;
use support::retry_lock;

fn path(name: &str) -> Vec<String> {
    name.split('.').map(str::to_owned).collect()
}
fn index(name: &str, unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: vec![path(name)],
        unique,
        partial: None,
    }
}
fn range(name: &str, comparison: Comparison, value: IndexValue) -> DocumentFilter {
    DocumentFilter::Compare {
        path: path(name),
        comparison,
        value,
    }
}
fn equality(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(name),
        value,
    }
}
fn query(filter: DocumentFilter, max_scanned: usize) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned,
        ..Default::default()
    }
}
fn access(name: &str) -> DocumentQueryAccess {
    DocumentQueryAccess::RangeIndex { index: name.into() }
}
fn compare(db: &DocumentDatabase<'_>, query: &DocumentQuery, expected: DocumentQueryAccess) {
    let scan = db.query("c", query).unwrap();
    let planned = db.query_planned("c", query).unwrap();
    assert_eq!(planned.access, expected);
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
fn prepare_mixed(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("score", false)).unwrap();
    db.create_index("c", index("handle", true)).unwrap();
    for (id, score) in [
        ("a", Some(json!(5))),
        ("b", Some(json!(-1))),
        ("c", Some(json!("z"))),
        ("d", Some(Value::Null)),
        ("e", Some(json!(false))),
        ("f", Some(json!(5))),
        ("g", None),
        ("h", Some(json!("a"))),
        ("i", Some(json!(true))),
        ("j", Some(json!(0))),
        ("k", Some(json!("a\u{0}z"))),
    ] {
        let mut doc = json!({"handle":id, "tags":[id, "shared"]});
        if let Some(score) = score {
            doc["score"] = score;
        }
        db.insert("c", id, &doc).unwrap();
    }
}

#[test]
fn scalar_ranges_match_scans_with_mixed_types_bounds_sort_and_projection() {
    on_backends(|db| {
        prepare_mixed(db);
        for value in [
            IndexValue::Null,
            IndexValue::Boolean(false),
            IndexValue::Boolean(true),
            IndexValue::Integer(-1),
            IndexValue::Integer(0),
            IndexValue::Integer(5),
            IndexValue::String("a".into()),
            IndexValue::String("a\0z".into()),
            IndexValue::String("z".into()),
        ] {
            for comparison in [
                Comparison::Less,
                Comparison::LessOrEqual,
                Comparison::Greater,
                Comparison::GreaterOrEqual,
            ] {
                let base = query(range("score", comparison, value.clone()), 20);
                compare(db, &base, access("score"));
                let decorated = DocumentQuery {
                    filter: DocumentFilter::And(vec![
                        base.filter.clone(),
                        DocumentFilter::ArrayContains {
                            path: path("tags"),
                            value: json!("shared"),
                        },
                    ]),
                    sort: Some(DocumentSort {
                        path: path("handle"),
                        descending: true,
                    }),
                    projection: Some(vec![path("handle"), path("score")]),
                    offset: 1,
                    limit: 3,
                    ..base
                };
                compare(db, &decorated, access("score"));
            }
        }
        let between = query(
            DocumentFilter::And(vec![
                range("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
                range("score", Comparison::LessOrEqual, IndexValue::Integer(5)),
            ]),
            20,
        );
        let ids = db
            .query_planned("c", &between)
            .unwrap()
            .result
            .documents
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, ["a", "f", "j"]);
        compare(db, &between, access("score"));
        for (lower, upper) in [
            (Comparison::Greater, Comparison::LessOrEqual),
            (Comparison::GreaterOrEqual, Comparison::Less),
            (Comparison::Greater, Comparison::Less),
        ] {
            let empty = query(
                DocumentFilter::And(vec![
                    range("score", lower, IndexValue::Integer(5)),
                    range("score", upper, IndexValue::Integer(5)),
                ]),
                20,
            );
            compare(db, &empty, access("score"));
            assert!(
                db.query_planned("c", &empty)
                    .unwrap()
                    .result
                    .documents
                    .is_empty()
            );
        }
        let conflicting = query(
            DocumentFilter::And(vec![
                range("score", Comparison::GreaterOrEqual, IndexValue::Integer(5)),
                range("score", Comparison::LessOrEqual, IndexValue::Integer(-1)),
            ]),
            20,
        );
        compare(db, &conflicting, access("score"));
        let unique = query(
            DocumentFilter::And(vec![
                range("score", Comparison::Greater, IndexValue::Integer(0)),
                equality("handle", json!("a")),
            ]),
            20,
        );
        compare(
            db,
            &unique,
            DocumentQueryAccess::UniqueEqualityIndex {
                index: "handle".into(),
            },
        );
        let or_query = query(
            DocumentFilter::Or(vec![
                range("score", Comparison::Less, IndexValue::Integer(0)),
                range("score", Comparison::Greater, IndexValue::Integer(5)),
            ]),
            20,
        );
        compare(db, &or_query, DocumentQueryAccess::CollectionScan);
        let not_query = query(
            DocumentFilter::Not(Box::new(range(
                "score",
                Comparison::Greater,
                IndexValue::Integer(0),
            ))),
            20,
        );
        compare(db, &not_query, DocumentQueryAccess::CollectionScan);
    });
}

fn prepare_large(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("score", true)).unwrap();
    for start in (0..360).step_by(20) {
        db.transaction(|tx| {
            for i in start..start + 20 {
                tx.insert("c", &format!("{i:04}"), &json!({"score":(i * 37) % 360}))?;
            }
            Ok(())
        })
        .unwrap();
    }
}
fn large_query(max_scanned: usize) -> DocumentQuery {
    DocumentQuery {
        filter: DocumentFilter::And(vec![
            range("score", Comparison::GreaterOrEqual, IndexValue::Integer(30)),
            range("score", Comparison::LessOrEqual, IndexValue::Integer(329)),
        ]),
        max_scanned,
        offset: 260,
        limit: 20,
        ..Default::default()
    }
}
#[test]
fn unique_index_range_reads_all_pages_in_id_order_and_obeys_candidate_limits() {
    on_backends(|db| {
        prepare_large(db);
        compare(db, &large_query(400), access("score"));
        let planned = db.query_planned("c", &large_query(300)).unwrap();
        assert_eq!(planned.result.plan.scanned, 300);
        assert_eq!(planned.result.plan.matched, 300);
        assert_eq!(planned.result.documents.len(), 20);
        assert_eq!(
            db.query_planned("c", &large_query(299)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            db.query("c", &large_query(300)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

fn phantom(db: &DocumentDatabase<'_>, inside: bool) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("score", false)).unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert("c", "new", &json!({"score":if inside { 25 } else { 100 }}))
                .unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let request = query(
                DocumentFilter::And(vec![
                    range("score", Comparison::GreaterOrEqual, IndexValue::Integer(20)),
                    range("score", Comparison::LessOrEqual, IndexValue::Integer(30)),
                ]),
                10,
            );
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(planned.access, access("score"));
            assert!(planned.result.documents.is_empty());
            entered.wait();
            changed.wait();
            tx.put_raw(b"range-result", b"committed")?;
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
        db.transaction(|tx| tx.get_raw(b"range-result")).unwrap(),
        if inside {
            None
        } else {
            Some(b"committed".to_vec())
        }
    );
}
#[test]
fn planned_range_detects_phantoms_but_allows_unrelated_inserts() {
    for inside in [false, true] {
        on_backends(|db| phantom(db, inside));
    }
}

fn byte_limits(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("score", true)).unwrap();
    for id in 0..31 {
        let empty = json!({"score":id, "payload":""});
        let overhead = serde_json::to_vec(&empty).unwrap().len();
        let doc = json!({"score":id, "payload":"x".repeat(1024 * 1024 - overhead)});
        assert_eq!(serde_json::to_vec(&doc).unwrap().len(), 1024 * 1024);
        db.insert("c", &format!("{id:02}"), &doc).unwrap();
    }
    let request = DocumentQuery {
        filter: range("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
        max_scanned: 32,
        limit: 1,
        ..Default::default()
    };
    assert_eq!(
        db.query_planned("c", &request).unwrap().result.plan.scanned,
        31
    );
    let empty = json!({"score":31, "payload":""});
    let overhead = serde_json::to_vec(&empty).unwrap().len();
    db.insert(
        "c",
        "31",
        &json!({"score":31, "payload":"x".repeat(1024 * 1024 - overhead)}),
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
            tx.put_raw(b"range-budget-rollback", b"must not commit")?;
            tx.query_planned("c", &request)?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"range-budget-rollback"))
            .unwrap(),
        None
    );
}
#[test]
fn planned_range_enforces_original_document_byte_budget_and_rolls_back() {
    on_backends(byte_limits);
}

#[test]
fn range_query_reads_staged_changes_in_the_same_transaction() {
    on_backends(|db| {
        prepare_mixed(db);
        let request = query(
            range("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
            20,
        );
        db.transaction_with_read_set(|tx| {
            assert!(tx.delete("c", "a")?);
            tx.replace("c", "b", &json!({"handle":"b", "score":7}), false)?;
            tx.insert("c", "new", &json!({"handle":"new", "score":3}))?;
            let scan = tx.query("c", &request)?;
            let planned = tx.query_planned("c", &request)?;
            assert_eq!(planned.access, access("score"));
            assert_eq!(planned.result.documents, scan.documents);
            assert_eq!(planned.result.plan.matched, scan.plan.matched);
            Ok(())
        })
        .unwrap();
        compare(db, &request, access("score"));
    });
}

fn paged_phantom(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("score", false)).unwrap();
    for start in (0..260).step_by(20) {
        db.transaction(|tx| {
            for i in start..start + 20 {
                tx.insert("c", &format!("{i:04}"), &json!({"score":i}))?;
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
            db.insert("c", "new", &json!({"score":100})).unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let request = query(
                DocumentFilter::And(vec![
                    range("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
                    range("score", Comparison::LessOrEqual, IndexValue::Integer(259)),
                ]),
                300,
            );
            let result = tx.query_planned("c", &request)?;
            assert_eq!(result.access, access("score"));
            assert_eq!(result.result.plan.scanned, 260);
            entered.wait();
            changed.wait();
            tx.put_raw(b"range-paged-phantom", b"must not commit")?;
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        writer.join().unwrap();
    });
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"range-paged-phantom"))
            .unwrap(),
        None
    );
}
#[test]
fn planned_multi_page_range_detects_concurrent_phantoms() {
    on_backends(paged_phantom);
}

fn check_corruption(db: &DocumentDatabase<'_>) {
    let request = query(
        range("score", Comparison::GreaterOrEqual, IndexValue::Integer(0)),
        10,
    );
    assert_eq!(
        db.query_planned("c", &request).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}
#[test]
fn planned_range_fails_on_encountered_corrupt_index_entries() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index("score", false)).unwrap();
        db.insert("c", "a", &json!({"score":1})).unwrap();
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
