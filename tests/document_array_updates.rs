//! Array updates must commit atomically with schema and index maintenance.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, FieldCondition, FieldRule,
    FieldType, IndexDefinition, IndexQuery, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::sync::Barrier;
use std::time::{Duration, Instant};

mod support;
use support::retry_lock;

fn path(value: &str) -> Vec<String> {
    value.split('.').map(str::to_owned).collect()
}

fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection(
        "c",
        CollectionSchema {
            fields: vec![FieldRule {
                path: path("items"),
                field_type: FieldType::Array,
                required: false,
                nullable: false,
            }],
        },
    )
    .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "handle".into(),
            fields: vec![path("handle")],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "tag".into(),
            fields: vec![path("tag")],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "a", &json!({"handle":"a", "scratch":null}))
        .unwrap();
}

fn exercise(db: &DocumentDatabase<'_>) {
    use DocumentUpdate::{
        ArrayAddUnique as Add, ArrayPop as Pop, ArrayPush as Push, ArrayRemove as Remove,
    };
    let items = path("items");
    let nested = path("profile.tags");
    assert!(
        db.update_if(
            "c",
            "a",
            &[],
            &[
                Push {
                    path: items.clone(),
                    value: json!(1)
                },
                Push {
                    path: items.clone(),
                    value: json!(1)
                },
                Add {
                    path: items.clone(),
                    value: json!(1)
                },
                Push {
                    path: items.clone(),
                    value: json!({"x":1})
                },
                Add {
                    path: items.clone(),
                    value: json!({"x":1})
                },
                Push {
                    path: items.clone(),
                    value: Value::Null
                },
                Remove {
                    path: items.clone(),
                    value: json!(1)
                },
                Pop {
                    path: items.clone(),
                    first: false
                },
                Push {
                    path: nested.clone(),
                    value: json!("first")
                },
                Push {
                    path: nested.clone(),
                    value: json!("last")
                },
                Pop {
                    path: nested,
                    first: true
                },
                Remove {
                    path: path("missing.field"),
                    value: json!(0)
                },
                Pop {
                    path: path("other.field"),
                    first: false
                },
            ],
            false
        )
        .unwrap()
    );
    let current = db.get("c", "a").unwrap().unwrap();
    assert_eq!(
        current,
        json!({"handle":"a", "scratch":null, "items":[{"x":1}], "profile":{"tags":["last"]}})
    );
    assert!(
        !db.update_if(
            "c",
            "a",
            &[FieldCondition {
                path: items.clone(),
                expected: None
            }],
            &[Push {
                path: items.clone(),
                value: json!("not written")
            }],
            false
        )
        .unwrap()
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(current.clone()));
    for update in [
        Push {
            path: path("handle"),
            value: json!(1),
        },
        Add {
            path: path("scratch"),
            value: json!(1),
        },
        Remove {
            path: path("handle"),
            value: json!(1),
        },
        Pop {
            path: path("scratch"),
            first: true,
        },
        Push {
            path: path("handle.nested"),
            value: json!(1),
        },
        Push {
            path: path("tag"),
            value: json!(1),
        },
    ] {
        let result = db.transaction(|tx| {
            tx.put_raw(b"rollback", b"must not appear")?;
            assert_eq!(
                tx.update_if("c", "a", &[], &[update], false)
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidInput
            );
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);
        assert_eq!(db.get("c", "a").unwrap(), Some(current.clone()));
        assert_eq!(
            db.query_index("c", "tag", &IndexQuery::default())
                .unwrap()
                .documents
                .len(),
            0
        );
        assert_eq!(db.verify_collection("c").unwrap().documents, 1);
    }
    assert!(
        db.update_if(
            "c",
            "new",
            &[],
            &[Push {
                path: items,
                value: json!("new")
            }],
            true
        )
        .unwrap()
    );
    assert_eq!(db.get("c", "new").unwrap(), Some(json!({"items":["new"]})));
}

#[test]
fn array_operations_recover_and_preserve_schema_indexes_and_atomicity() {
    for checkpoint_mode in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let checkpoint = dir.path().join("checkpoint");
        let ordinary = retry_lock(|| Store::open(&path)).unwrap();
        let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(ordinary);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            prepare(&db);
            exercise(&db);
        } else {
            let db = DocumentDatabase::new(&ordinary);
            prepare(&db);
            exercise(&db);
            drop(ordinary);
        }
        let recovered = retry_lock(|| Store::open(&path)).unwrap();
        let db = DocumentDatabase::new(&recovered);
        assert_eq!(
            db.get("c", "a").unwrap().unwrap()["items"],
            json!([{"x":1}])
        );
        assert_eq!(
            db.get("c", "new").unwrap().unwrap()["items"],
            json!(["new"])
        );
        assert_eq!(db.verify_collection("c").unwrap().documents, 2);
        assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);
    }
}

#[test]
fn excessive_array_input_and_work_fail_without_committing_any_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    prepare(&db);
    let original = db.get("c", "a").unwrap().unwrap();
    // Four encoded strings exactly fill the aggregate 4 MiB input budget.
    let boundary = Value::String("x".repeat(1024 * 1024 - 2));
    let accepted = (0..4)
        .map(|_| DocumentUpdate::ArrayRemove {
            path: path("missing"),
            value: boundary.clone(),
        })
        .collect::<Vec<_>>();
    assert!(db.update_if("c", "a", &[], &accepted, false).unwrap());
    assert_eq!(db.get("c", "a").unwrap(), Some(original.clone()));
    let large = Value::String("x".repeat(1024 * 1024));
    let pushes = (0..4)
        .map(|_| DocumentUpdate::ArrayPush {
            path: path("items"),
            value: large.clone(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        db.update_if("c", "a", &[], &pushes, false)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(original));
    db.replace(
        "c",
        "a",
        &json!({"handle":"a", "items":vec![1; 10_000]}),
        false,
    )
    .unwrap();
    let original = db.get("c", "a").unwrap().unwrap();
    let remove = DocumentUpdate::ArrayRemove {
        path: path("items"),
        value: json!(2),
    };
    assert!(
        db.update_if("c", "a", &[], &vec![remove.clone(); 100], false)
            .unwrap()
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(original.clone()));
    let mut updates = vec![DocumentUpdate::Set {
        path: path("would_write"),
        value: json!(true),
    }];
    updates.extend(std::iter::repeat_n(remove, 101));
    assert_eq!(
        db.update_if("c", "a", &[], &updates, false)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(original));
    assert_eq!(db.verify_collection("c").unwrap().documents, 1);
}

#[test]
fn concurrent_unique_array_additions_are_serializable() {
    for checkpoint_mode in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let checkpoint = dir.path().join("checkpoint");
        let ordinary = Store::open(&path).unwrap();
        let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(ordinary);
            let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
            concurrent(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            concurrent(&DocumentDatabase::new(&ordinary));
            drop(ordinary);
        }
        let reopened = retry_lock(|| Store::open(&path)).unwrap();
        let db = DocumentDatabase::new(&reopened);
        assert_eq!(
            db.get("c", "a").unwrap().unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            8
        );
        assert_eq!(db.verify_collection("c").unwrap().documents, 1);
    }
}

fn concurrent(db: &DocumentDatabase<'_>) {
    prepare(db);
    let barrier = Barrier::new(9);
    std::thread::scope(|scope| {
        for number in 0..8 {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                let deadline = Instant::now() + Duration::from_secs(15);
                for _ in 0..2 {
                    loop {
                        match db.update_if(
                            "c",
                            "a",
                            &[],
                            &[DocumentUpdate::ArrayAddUnique {
                                path: path("items"),
                                value: json!(number),
                            }],
                            false,
                        ) {
                            Ok(true) => break,
                            Err(error)
                                if error.kind() == ErrorKind::WouldBlock
                                    && Instant::now() < deadline =>
                            {
                                std::thread::yield_now()
                            }
                            result => panic!("unexpected update outcome: {result:?}"),
                        }
                    }
                }
            });
        }
        barrier.wait();
    });
    let document = db.get("c", "a").unwrap().unwrap();
    let mut items = document["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect::<Vec<_>>();
    items.sort_unstable();
    assert_eq!(items, (0..8).collect::<Vec<_>>());
}
