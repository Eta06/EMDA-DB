//! Bounded one-to-many relationships through a named non-unique index, on
//! ordinary and checkpoint stores.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentQuery, DocumentQueryAccess,
    DocumentSnapshot, IndexDefinition, IndexValue, LookupManyLimits, LookupManyResult,
    PartialFilter, Store,
};
use serde_json::json;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Barrier;

mod support;
use support::retry_lock;

fn path(name: &str) -> Vec<String> {
    vec![name.into()]
}

fn index(name: &str, fields: &[&str], partial: Option<PartialFilter>) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: fields.iter().map(|field| path(field)).collect(),
        unique: false,
        partial,
    }
}

fn wide() -> LookupManyLimits {
    LookupManyLimits::new(1024, 4096, 16 * 1024 * 1024)
}

fn seed(db: &DocumentDatabase<'_>) {
    db.create_collection("orders", CollectionSchema::default())
        .unwrap();
    db.create_collection("items", CollectionSchema::default())
        .unwrap();
    db.create_index("items", index("by_order", &["order"], None))
        .unwrap();
    db.create_index("items", index("by_order_kind", &["order", "kind"], None))
        .unwrap();
    db.create_index(
        "items",
        index(
            "active_by_order",
            &["order"],
            Some(PartialFilter::Equals {
                path: path("active"),
                value: IndexValue::Boolean(true),
            }),
        ),
    )
    .unwrap();
    db.transaction(|tx| {
        for (id, order) in [
            ("s1", json!({"ref": "o1", "kind": "book"})),
            ("s2", json!({"ref": "o2", "kind": "pen"})),
            ("s3", json!({"kind": "book"})),
            ("s4", json!({"ref": null})),
            ("s5", json!({"ref": 1, "kind": "book"})),
        ] {
            tx.insert("orders", id, &order)?;
        }
        for (id, item) in [
            ("i3", json!({"order": "o1", "kind": "book", "active": true})),
            ("i1", json!({"order": "o1", "kind": "pen", "active": false})),
            ("i2", json!({"order": "o1", "kind": "book"})),
            ("i9", json!({"order": null, "kind": "x"})),
            ("i5", json!({"order": "1", "kind": "book", "active": true})),
        ] {
            tx.insert("items", id, &item)?;
        }
        Ok(())
    })
    .unwrap();
}

// Source ID -> related foreign IDs.
fn shape(result: &LookupManyResult) -> Vec<(String, Vec<String>)> {
    result
        .documents
        .iter()
        .map(|row| {
            (
                row.source.id.clone(),
                row.related.iter().map(|entry| entry.id.clone()).collect(),
            )
        })
        .collect()
}

fn expected(rows: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
    rows.iter()
        .map(|(source, related)| {
            (
                source.to_string(),
                related.iter().map(|id| id.to_string()).collect(),
            )
        })
        .collect()
}

fn by_order() -> Vec<(String, Vec<String>)> {
    expected(&[
        ("s1", &["i1", "i2", "i3"]),
        ("s2", &[]),
        ("s3", &[]),
        ("s4", &["i9"]),
        ("s5", &[]),
    ])
}

fn single() -> Vec<Vec<String>> {
    vec![path("ref")]
}

fn lookup(db: &DocumentDatabase<'_>, index: &str, local: &[Vec<String>]) -> LookupManyResult {
    db.lookup_many(
        "orders",
        &DocumentQuery::default(),
        local,
        ("items", index),
        wide(),
    )
    .unwrap()
}

fn on_backends(exercise: impl Fn(&DocumentDatabase<'_>, &Path, bool)) {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let store = Store::open(&source).unwrap();
        seed(&DocumentDatabase::new(&store));
        if checkpoint_mode {
            let checkpoint = root.path().join("checkpoint");
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            store.rollover_managed(root.path().join("managed")).unwrap();
            let committed = {
                let db = DocumentDatabase::with_checkpoint_store(&store);
                exercise(&db, &source, true);
                shape(&lookup(&db, "by_order", &single()))
            };
            {
                let snapshot = store.snapshot().unwrap();
                let documents = DocumentSnapshot::with_checkpoint_snapshot(&snapshot);
                let result = documents
                    .lookup_many(
                        "orders",
                        &DocumentQuery::default(),
                        &single(),
                        ("items", "by_order"),
                        wide(),
                    )
                    .unwrap();
                assert_eq!(shape(&result), committed);
            }
            drop(store);
            let store = retry_lock(|| CheckpointStore::open_managed(&source)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            assert_eq!(shape(&lookup(&db, "by_order", &single())), committed);
        } else {
            let committed = {
                let db = DocumentDatabase::new(&store);
                exercise(&db, &source, false);
                shape(&lookup(&db, "by_order", &single()))
            };
            {
                let snapshot = store.snapshot(1024, 1 << 20).unwrap();
                let result = DocumentSnapshot::new(&snapshot)
                    .lookup_many(
                        "orders",
                        &DocumentQuery::default(),
                        &single(),
                        ("items", "by_order"),
                        wide(),
                    )
                    .unwrap();
                assert_eq!(shape(&result), committed);
            }
            drop(store);
            let store = retry_lock(|| Store::open(&source)).unwrap();
            let db = DocumentDatabase::new(&store);
            assert_eq!(shape(&lookup(&db, "by_order", &single())), committed);
        }
    }
}

#[test]
fn many_zero_missing_null_typed_compound_and_partial_relationships() {
    on_backends(|db, _, _| {
        let result = lookup(db, "by_order", &single());
        // Several matches in foreign ID order, none, a missing field, an
        // indexed null, and an integer that never matches the string "1".
        assert_eq!(shape(&result), by_order());
        assert_eq!((result.related, result.index_entries_read), (4, 4));
        assert_eq!(result.plan.matched, 5);
        let first = &result.documents[0].related[0];
        assert_eq!(
            first.document,
            json!({"order": "o1", "kind": "pen", "active": false})
        );
        // Response bytes are each related ID plus its stored document.
        let bytes: usize = result
            .documents
            .iter()
            .flat_map(|row| &row.related)
            .map(|entry| entry.id.len() + serde_json::to_vec(&entry.document).unwrap().len())
            .sum();
        assert_eq!(result.response_bytes, bytes);
        // A compound tuple must be complete; a missing component matches nothing.
        let compound = lookup(db, "by_order_kind", &[path("ref"), path("kind")]);
        assert_eq!(
            shape(&compound),
            expected(&[
                ("s1", &["i2", "i3"]),
                ("s2", &[]),
                ("s3", &[]),
                ("s4", &[]),
                ("s5", &[]),
            ])
        );
        // A partial index relates only the documents it covers.
        let partial = lookup(db, "active_by_order", &single());
        assert_eq!(
            shape(&partial),
            expected(&[
                ("s1", &["i3"]),
                ("s2", &[]),
                ("s3", &[]),
                ("s4", &[]),
                ("s5", &[])
            ])
        );
        // Planned selection of the sources returns the same rows.
        let planned = db
            .lookup_many_planned(
                "orders",
                &DocumentQuery::default(),
                &single(),
                ("items", "by_order"),
                wide(),
            )
            .unwrap();
        assert_eq!(planned.access, DocumentQueryAccess::CollectionScan);
        assert_eq!(shape(&planned.result), by_order());
    });
}

#[test]
fn limits_are_exact_and_invalid_input_is_refused_before_reading() {
    on_backends(|db, _, _| {
        let bytes = lookup(db, "by_order", &single()).response_bytes;
        let run = |limits| {
            db.lookup_many(
                "orders",
                &DocumentQuery::default(),
                &single(),
                ("items", "by_order"),
                limits,
            )
        };
        // s1 relates three documents; four in total; `bytes` in total.
        for exact in [
            LookupManyLimits::new(3, 4096, 1 << 24),
            LookupManyLimits::new(1024, 4, 1 << 24),
            LookupManyLimits::new(1024, 4096, bytes),
        ] {
            assert_eq!(shape(&run(exact).unwrap()), by_order(), "{exact:?}");
        }
        for over in [
            LookupManyLimits::new(2, 4096, 1 << 24),
            LookupManyLimits::new(1024, 3, 1 << 24),
            LookupManyLimits::new(1024, 4096, bytes - 1),
            LookupManyLimits::new(0, 4096, 1 << 24),
            LookupManyLimits::new(1025, 4096, 1 << 24),
            LookupManyLimits::new(1024, 0, 1 << 24),
            LookupManyLimits::new(1024, 4097, 1 << 24),
            LookupManyLimits::new(1024, 4096, 0),
            LookupManyLimits::new(1024, 4096, (1 << 24) + 1),
        ] {
            let error = run(over).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidInput, "{over:?}: {error}");
        }
        let invalid = |result: std::io::Result<LookupManyResult>| {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
        };
        // A projected source, a unique or unknown index, a wrong path count
        // and a non-scalar local value.
        invalid(db.lookup_many(
            "orders",
            &DocumentQuery {
                projection: Some(vec![path("ref")]),
                ..Default::default()
            },
            &single(),
            ("items", "by_order"),
            wide(),
        ));
        invalid(db.lookup_many(
            "orders",
            &DocumentQuery::default(),
            &[path("ref")],
            ("items", "by_order_kind"),
            wide(),
        ));
        db.insert("orders", "s6", &json!({"ref": ["o1"]})).unwrap();
        invalid(db.lookup_many(
            "orders",
            &DocumentQuery::default(),
            &single(),
            ("items", "by_order"),
            wide(),
        ));
        db.delete("orders", "s6").unwrap();
        assert!(
            db.lookup_many(
                "orders",
                &DocumentQuery::default(),
                &single(),
                ("items", "missing"),
                wide(),
            )
            .is_err()
        );
    });
}

#[test]
fn unique_indexes_stay_on_the_one_to_one_api() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("store")).unwrap();
    let db = DocumentDatabase::new(&store);
    seed(&db);
    db.create_index(
        "items",
        IndexDefinition {
            name: "unique_order".into(),
            fields: vec![path("order"), path("active")],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    let error = db
        .lookup_many(
            "orders",
            &DocumentQuery::default(),
            &[path("ref"), path("kind")],
            ("items", "unique_order"),
            wide(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    // The existing one-to-one lookup is unchanged.
    let one = db
        .lookup_unique_index(
            "orders",
            &DocumentQuery::default(),
            &[path("ref"), path("kind")],
            "items",
            "unique_order",
        )
        .unwrap();
    let related = one
        .documents
        .iter()
        .map(|row| row.related.clone())
        .collect::<Vec<_>>();
    assert_eq!(related.len(), 5);
    assert!(related.iter().all(Option::is_none));
}

#[test]
fn staged_edits_are_visible_inside_and_snapshots_keep_their_view() {
    on_backends(|db, _, checkpoint| {
        // Inside one transaction the lookup reads its own staged edits.
        db.transaction(|tx| {
            tx.insert("items", "i7", &json!({"order": "o2", "kind": "pen"}))?;
            tx.delete("items", "i1")?;
            let result = tx.lookup_many(
                "orders",
                &DocumentQuery::default(),
                &single(),
                ("items", "by_order"),
                wide(),
            )?;
            assert_eq!(
                shape(&result),
                expected(&[
                    ("s1", &["i2", "i3"]),
                    ("s2", &["i7"]),
                    ("s3", &[]),
                    ("s4", &["i9"]),
                    ("s5", &[]),
                ])
            );
            Ok(())
        })
        .unwrap();
        // An aborted transaction's staged edits never become visible.
        let aborted = db.transaction(|tx| -> std::io::Result<()> {
            tx.insert("items", "i8", &json!({"order": "o2"}))?;
            Err(std::io::Error::other("abort"))
        });
        assert!(aborted.is_err());
        let committed = expected(&[
            ("s1", &["i2", "i3"]),
            ("s2", &["i7"]),
            ("s3", &[]),
            ("s4", &["i9"]),
            ("s5", &[]),
        ]);
        assert_eq!(shape(&lookup(db, "by_order", &single())), committed);
        let _ = checkpoint;
        // Restore the seed for the shared reopen and snapshot checks.
        db.delete("items", "i7").unwrap();
        db.insert(
            "items",
            "i1",
            &json!({"order": "o1", "kind": "pen", "active": false}),
        )
        .unwrap();
    });
}

#[test]
fn a_snapshot_keeps_its_relationships_while_the_store_changes() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("store")).unwrap();
    let db = DocumentDatabase::new(&store);
    seed(&db);
    let snapshot = store.snapshot(1024, 1 << 20).unwrap();
    db.insert("items", "i0", &json!({"order": "o1"})).unwrap();
    let captured = DocumentSnapshot::new(&snapshot)
        .lookup_many(
            "orders",
            &DocumentQuery::default(),
            &single(),
            ("items", "by_order"),
            wide(),
        )
        .unwrap();
    assert_eq!(shape(&captured), by_order());
    let live = lookup(&db, "by_order", &single());
    assert_eq!(live.documents[0].related[0].id, "i0");
}

#[test]
fn a_concurrent_matching_foreign_insert_conflicts_under_read_set_validation() {
    on_backends(|db, _, _| {
        // A disjoint foreign insert is the control: it must not conflict.
        for (planned, matching) in [(false, true), (true, true), (false, false), (true, false)] {
            let phantom = format!("phantom-{planned}-{matching}");
            let decided = format!("decided-{planned}-{matching}");
            let order = if matching { "o2" } else { "o9" };
            let ready = Barrier::new(2);
            let changed = Barrier::new(2);
            let seen = std::cell::Cell::new(None);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    ready.wait();
                    // A new related document for o2, which had none.
                    let inserted = db.insert("items", &phantom, &json!({"order": order}));
                    changed.wait();
                    inserted.unwrap();
                });
                let result = db.transaction_with_read_set(|tx| {
                    let related = if planned {
                        tx.lookup_many_planned(
                            "orders",
                            &DocumentQuery::default(),
                            &single(),
                            ("items", "by_order"),
                            wide(),
                        )?
                        .result
                    } else {
                        tx.lookup_many(
                            "orders",
                            &DocumentQuery::default(),
                            &single(),
                            ("items", "by_order"),
                            wide(),
                        )?
                    };
                    // Checked after the barriers, so a failure cannot hang.
                    seen.set(Some(related.documents[1].related.len()));
                    ready.wait();
                    changed.wait();
                    // Record the decision, so the transaction must validate.
                    tx.insert("orders", &decided, &json!({}))?;
                    Ok(())
                });
                if matching {
                    assert_eq!(
                        result.unwrap_err().kind(),
                        ErrorKind::WouldBlock,
                        "planned={planned}"
                    );
                } else {
                    result.unwrap();
                }
            });
            assert_eq!(db.get("orders", &decided).unwrap().is_some(), !matching);
            assert_eq!(seen.get(), Some(0), "planned={planned}");
            // Remove both writes so later rounds and the reopen check see
            // the seed.
            assert!(db.delete("items", &phantom).unwrap());
            db.delete("orders", &decided).unwrap();
        }
    });
}
