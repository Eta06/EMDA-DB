//! Atomic filter-driven updates and deletes on both writable backends.
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentUpdate, FieldRule, FieldType, IndexDefinition, IndexValue, MatchOptions, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Barrier;

mod support;
use support::retry_lock;

const DOCUMENTS: i64 = 10;

fn index(name: &str, unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: vec![vec![name.into()]],
        unique,
        partial: None,
    }
}

fn path(name: &str) -> Vec<String> {
    vec![name.into()]
}

fn ends_with(suffix: &str) -> DocumentFilter {
    DocumentFilter::EndsWith {
        path: path("name"),
        suffix: suffix.into(),
    }
}

fn at_least(value: i64) -> DocumentFilter {
    DocumentFilter::Compare {
        path: path("score"),
        comparison: Comparison::GreaterOrEqual,
        value: IndexValue::Integer(value),
    }
}

fn seed(db: &DocumentDatabase<'_>) {
    let schema = CollectionSchema {
        fields: vec![FieldRule {
            path: path("score"),
            field_type: FieldType::Integer,
            required: true,
            nullable: false,
        }],
    };
    db.create_collection("c", schema).unwrap();
    db.create_index("c", index("handle", true)).unwrap();
    db.create_index("c", index("score", false)).unwrap();
    db.transaction(|tx| {
        for value in 0..DOCUMENTS {
            // Even IDs end in "-even", odd ones in "-odd".
            let parity = if value % 2 == 0 { "even" } else { "odd" };
            tx.insert(
                "c",
                &format!("doc-{value:02}"),
                &json!({"name": format!("n{value}-{parity}"), "handle": format!("h{value}"), "score": value}),
            )?;
        }
        Ok(())
    })
    .unwrap();
}

fn all(db: &DocumentDatabase<'_>) -> Vec<(String, Value)> {
    let query = DocumentQuery {
        max_scanned: 64,
        ..Default::default()
    };
    db.query("c", &query)
        .unwrap()
        .documents
        .into_iter()
        .map(|entry| (entry.id, entry.document))
        .collect()
}

fn on_backends(exercise: impl Fn(&DocumentDatabase<'_>, &Path)) {
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
            let db = DocumentDatabase::with_checkpoint_store(&store);
            exercise(&db, &source);
            let committed = all(&db);
            drop(db);
            drop(store);
            // The committed result survives reopening.
            let store = retry_lock(|| CheckpointStore::open_managed(&source)).unwrap();
            assert_eq!(
                all(&DocumentDatabase::with_checkpoint_store(&store)),
                committed
            );
        } else {
            let db = DocumentDatabase::new(&store);
            exercise(&db, &source);
            let committed = all(&db);
            drop(db);
            drop(store);
            let store = retry_lock(|| Store::open(&source)).unwrap();
            assert_eq!(all(&DocumentDatabase::new(&store)), committed);
        }
    }
}

#[test]
fn updates_and_deletes_every_match_atomically() {
    on_backends(|db, _| {
        let mark = [DocumentUpdate::Set {
            path: path("marked"),
            value: json!(true),
        }];
        let options = MatchOptions::new(DOCUMENTS as usize, 5);
        assert_eq!(
            db.update_matching("c", &ends_with("-odd"), &mark, options)
                .unwrap(),
            5
        );
        for (id, doc) in all(db) {
            let odd = doc["name"].as_str().unwrap().ends_with("-odd");
            assert_eq!(doc.get("marked") == Some(&json!(true)), odd, "{id}");
        }
        // Planned selection through the score range finds the same set.
        let planned = MatchOptions::new(DOCUMENTS as usize, 4).planned();
        assert_eq!(db.delete_matching("c", &at_least(6), planned).unwrap(), 4);
        assert_eq!(all(db).len(), 6);
        // No matches commit nothing and report zero.
        assert_eq!(
            db.delete_matching("c", &ends_with("-none"), options)
                .unwrap(),
            0
        );
        // Delete every remaining odd document.
        assert_eq!(
            db.delete_matching("c", &ends_with("-odd"), options)
                .unwrap(),
            3
        );
        assert_eq!(all(db).len(), 3);
    });
}

#[test]
fn limits_are_exact_and_reject_without_partial_changes() {
    on_backends(|db, _| {
        let before = all(db);
        let set = [DocumentUpdate::Set {
            path: path("x"),
            value: json!(1),
        }];
        // Five odd matches: four is one too few.
        for options in [
            MatchOptions::new(DOCUMENTS as usize, 4),
            MatchOptions::new(DOCUMENTS as usize, 4).planned(),
            MatchOptions::new(DOCUMENTS as usize - 1, 5),
            MatchOptions::new(DOCUMENTS as usize, 0),
            MatchOptions::new(DOCUMENTS as usize, 129),
        ] {
            let error = db
                .update_matching("c", &ends_with("-odd"), &set, options)
                .unwrap_err();
            assert_eq!(
                error.kind(),
                ErrorKind::InvalidInput,
                "{options:?}: {error}"
            );
            assert_eq!(all(db), before, "{options:?}");
        }
        // Invalid update inputs are rejected even with no matches.
        let empty = db
            .update_matching("c", &ends_with("-none"), &[], MatchOptions::new(16, 1))
            .unwrap_err();
        assert_eq!(empty.kind(), ErrorKind::InvalidInput);
        let long = db
            .delete_matching("c", &ends_with(&"x".repeat(257)), MatchOptions::new(16, 1))
            .unwrap_err();
        assert_eq!(long.kind(), ErrorKind::InvalidInput);
        // Exact limits succeed.
        db.update_matching(
            "c",
            &ends_with("-odd"),
            &set,
            MatchOptions::new(DOCUMENTS as usize, 5),
        )
        .unwrap();
        // Planned selection over a range with more matches than allowed.
        let error = db
            .delete_matching("c", &at_least(0), MatchOptions::new(64, 9).planned())
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(all(db).len(), DOCUMENTS as usize);
    });
}

#[test]
fn a_failing_document_rolls_back_earlier_staged_writes_and_indexes() {
    on_backends(|db, _| {
        let before = all(db);
        // Every match gets the same unique handle: the second write fails.
        let duplicate = [DocumentUpdate::Set {
            path: path("handle"),
            value: json!("taken"),
        }];
        let options = MatchOptions::new(DOCUMENTS as usize, 5);
        assert!(
            db.update_matching("c", &ends_with("-odd"), &duplicate, options)
                .is_err()
        );
        // A schema violation fails the same way.
        let wrong_type = [DocumentUpdate::Set {
            path: path("score"),
            value: json!("not an integer"),
        }];
        assert!(
            db.update_matching("c", &ends_with("-odd"), &wrong_type, options)
                .is_err()
        );
        assert_eq!(all(db), before);
        // The unique index still maps every original handle and not the new one.
        let by_handle = |handle: &str| {
            db.query_planned(
                "c",
                &DocumentQuery {
                    filter: DocumentFilter::Equal {
                        path: path("handle"),
                        value: json!(handle),
                    },
                    max_scanned: 4,
                    ..Default::default()
                },
            )
            .unwrap()
            .result
            .documents
            .len()
        };
        assert_eq!(by_handle("h1"), 1);
        assert_eq!(by_handle("taken"), 0);
        // A later valid call in a new transaction still works.
        db.delete_matching("c", &ends_with("-odd"), options)
            .unwrap();
    });
}

#[test]
fn a_concurrent_matching_insert_conflicts_under_read_set_validation() {
    on_backends(|db, _| {
        for planned in [false, true] {
            let ready = Barrier::new(2);
            let changed = Barrier::new(2);
            let options = if planned {
                MatchOptions::new(DOCUMENTS as usize, 8).planned()
            } else {
                MatchOptions::new(DOCUMENTS as usize + 2, 8)
            };
            let before = all(db);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    ready.wait();
                    // A phantom that matches the filter, inserted mid-transaction.
                    let inserted = db.insert(
                        "c",
                        &format!("phantom-{planned}"),
                        &json!({"name": "p-odd", "handle": format!("p{planned}"), "score": 7}),
                    );
                    changed.wait();
                    inserted.unwrap();
                });
                let result = db.transaction_with_read_set(|tx| {
                    let filter = if planned {
                        at_least(5)
                    } else {
                        ends_with("-odd")
                    };
                    let count = tx.delete_matching("c", &filter, options);
                    ready.wait();
                    changed.wait();
                    count
                });
                assert_eq!(
                    result.unwrap_err().kind(),
                    ErrorKind::WouldBlock,
                    "planned={planned}"
                );
            });
            // Only the concurrent insert committed.
            assert_eq!(all(db).len(), before.len() + 1, "planned={planned}");
        }
    });
}
