use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, IndexValue, PartialFilter, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::sync::Barrier;

mod support;
use support::retry_lock;

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

fn in_values(values: Vec<Value>) -> DocumentFilter {
    DocumentFilter::In {
        path: vec!["v".into()],
        values,
    }
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
        name: "v".into(),
        fields: vec![vec!["v".into()]],
        unique,
        partial,
    }
}
fn equal_scan(db: &DocumentDatabase<'_>, request: &DocumentQuery, access: DocumentQueryAccess) {
    let scan = db.query("c", request).unwrap();
    let planned = db.query_planned("c", request).unwrap();
    assert_eq!(planned.access, access);
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
}

#[test]
fn unique_union_deduplicates_values_and_restores_id_order() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(true, None)).unwrap();
        for (id, value) in [
            ("a", json!(2)),
            ("b", Value::Null),
            ("c", json!(false)),
            ("d", json!(1)),
            ("e", json!("x")),
        ] {
            db.insert("c", id, &json!({"v":value})).unwrap();
        }
        db.insert("c", "missing", &json!({})).unwrap();
        let request = DocumentQuery {
            filter: in_values(vec![json!(1), Value::Null, json!(2), json!(1), json!(999)]),
            offset: 1,
            limit: 2,
            projection: Some(vec![vec!["v".into()]]),
            ..query(DocumentFilter::All)
        };
        equal_scan(
            db,
            &request,
            DocumentQueryAccess::MembershipIndex {
                index: "v".into(),
                probes: 4,
            },
        );
        let ids = db
            .query_planned("c", &request)
            .unwrap()
            .result
            .documents
            .into_iter()
            .map(|r| r.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, ["b", "d"]);
        equal_scan(
            db,
            &query(in_values(vec![json!(1), json!(1)])),
            DocumentQueryAccess::MembershipIndex {
                index: "v".into(),
                probes: 1,
            },
        );
        for filter in [
            in_values(vec![json!(1), json!([1])]),
            DocumentFilter::Or(vec![in_values(vec![json!(1), json!(2)])]),
            DocumentFilter::Not(Box::new(in_values(vec![json!(1), json!(2)]))),
        ] {
            equal_scan(db, &query(filter), DocumentQueryAccess::CollectionScan);
        }
    });
}

#[test]
fn nonunique_union_enforces_one_combined_candidate_limit_across_pages() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(false, None)).unwrap();
        for n in 0..530 {
            db.insert("c", &format!("{n:04}"), &json!({"v":n % 2, "rank":530-n}))
                .unwrap();
        }
        let request = DocumentQuery {
            filter: in_values(vec![json!(1), json!(0), json!(1)]),
            sort: Some(DocumentSort {
                path: vec!["rank".into()],
                descending: false,
            }),
            offset: 11,
            limit: 25,
            max_scanned: 530,
            ..Default::default()
        };
        equal_scan(
            db,
            &request,
            DocumentQueryAccess::MembershipIndex {
                index: "v".into(),
                probes: 2,
            },
        );
        let limited = DocumentQuery {
            max_scanned: 529,
            ..request
        };
        assert_eq!(
            db.query_planned("c", &limited).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn union_uses_partial_index_only_when_its_guard_is_proved() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            index(
                false,
                Some(PartialFilter::Equals {
                    path: vec!["active".into()],
                    value: IndexValue::Boolean(true),
                }),
            ),
        )
        .unwrap();
        for (id, doc) in [
            ("a", json!({"v":1,"active":true})),
            ("b", json!({"v":2,"active":true})),
            ("c", json!({"v":1,"active":false})),
            ("d", json!({"v":2})),
        ] {
            db.insert("c", id, &doc).unwrap();
        }
        let values = in_values(vec![json!(1), json!(2)]);
        let guarded = query(DocumentFilter::And(vec![
            values.clone(),
            DocumentFilter::Equal {
                path: vec!["active".into()],
                value: json!(true),
            },
        ]));
        equal_scan(
            db,
            &guarded,
            DocumentQueryAccess::PartialMembershipIndex {
                index: "v".into(),
                probes: 2,
            },
        );
        equal_scan(db, &query(values), DocumentQueryAccess::CollectionScan);
    });
}

#[test]
fn union_sees_staged_changes_and_detects_phantoms_for_each_probe() {
    for inserted in [5, 7, 6] {
        on_backends(|db| {
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(false, None)).unwrap();
            db.transaction_with_read_set(|tx| {
                tx.insert("c", "staged", &json!({"v":5}))?;
                let request = query(in_values(vec![json!(5), json!(7)]));
                let planned = tx.query_planned("c", &request)?;
                assert_eq!(planned.result.documents.len(), 1);
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
                    db.insert("c", "new", &json!({"v":inserted})).unwrap();
                    changed.wait();
                });
                let result = db.transaction_with_read_set(|tx| {
                    let result =
                        tx.query_planned("c", &query(in_values(vec![json!(5), json!(7)])))?;
                    assert_eq!(
                        result.access,
                        DocumentQueryAccess::MembershipIndex {
                            index: "v".into(),
                            probes: 2
                        }
                    );
                    assert!(result.result.documents.is_empty());
                    entered.wait();
                    changed.wait();
                    tx.put_raw(b"union-check", b"yes")?;
                    Ok(())
                });
                if inserted == 6 {
                    result.unwrap();
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
                writer.join().unwrap();
            });
            assert_eq!(
                db.transaction(|tx| tx.get_raw(b"union-check")).unwrap(),
                (inserted == 6).then(|| b"yes".to_vec())
            );
        });
    }
}

#[test]
fn union_enforces_total_document_byte_budget_across_distinct_probes() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(false, None)).unwrap();
        let payload = "x".repeat(1_000_000);
        for n in 0..34 {
            db.insert(
                "c",
                &format!("{n:02}"),
                &json!({"v":n % 2, "payload":payload}),
            )
            .unwrap();
        }
        let request = query(in_values(vec![json!(0), json!(1)]));
        assert_eq!(
            db.query_planned("c", &request).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn union_rejects_corrupt_entries_encountered_on_a_later_probe() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index("c", index(false, None)).unwrap();
        db.insert("c", "a", &json!({"v":1})).unwrap();
        db.insert("c", "b", &json!({"v":2})).unwrap();
        let key = store
            .scan(None, None, 32)
            .unwrap()
            .into_iter()
            .find(|(key, _)| key.starts_with(b"\0emda-document\x01I") && key.ends_with(b"b"))
            .unwrap()
            .0;
        store.put(&key, b"corrupt").unwrap();
        let check = |db: &DocumentDatabase<'_>| {
            let request = query(in_values(vec![json!(1), json!(2)]));
            assert_eq!(db.query("c", &request).unwrap().documents.len(), 2);
            assert_eq!(
                db.query_planned("c", &request).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
        };
        if checkpoint_mode {
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            check(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            check(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn union_results_survive_checkpoint_and_full_log_recovery() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        let check = |db: &DocumentDatabase<'_>| {
            equal_scan(
                db,
                &query(in_values(vec![json!(1), json!(2)])),
                DocumentQueryAccess::MembershipIndex {
                    index: "v".into(),
                    probes: 2,
                },
            );
        };
        if checkpoint_mode {
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(false, None)).unwrap();
            db.insert("c", "a", &json!({"v":1})).unwrap();
            db.insert("c", "b", &json!({"v":2})).unwrap();
            check(&db);
            drop(store);
        } else {
            let db = DocumentDatabase::new(&store);
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", index(false, None)).unwrap();
            db.insert("c", "a", &json!({"v":1})).unwrap();
            db.insert("c", "b", &json!({"v":2})).unwrap();
            check(&db);
            drop(store);
        }
        let store = retry_lock(|| Store::open(&path)).unwrap();
        check(&DocumentDatabase::new(&store));
    }
}
