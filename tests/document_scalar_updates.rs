use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, FieldCondition, FieldRule,
    FieldType, IndexDefinition, IndexQuery, IndexValue, Store,
};
use serde_json::json;
use std::{io::ErrorKind, path::Path};

mod support;
use support::retry_lock;

fn path(value: &str) -> Vec<String> {
    value.split('.').map(str::to_owned).collect()
}
enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn db(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }
}
fn open(source: &Path, checkpoint: &Path, digest: &[u8; 32], checkpoint_mode: bool) -> Backend {
    if checkpoint_mode {
        Backend::Checkpoint(
            retry_lock(|| CheckpointStore::open(source, checkpoint, digest)).unwrap(),
        )
    } else {
        Backend::Ordinary(retry_lock(|| Store::open(source)).unwrap())
    }
}
fn both_backends(action: impl Fn(&DocumentDatabase<'_>)) {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);
        let store = open(&source, &checkpoint, &digest, checkpoint_mode);
        action(&store.db());
    }
}

#[test]
fn ordered_scalar_and_rename_updates_distinguish_missing_null_and_types() {
    use DocumentUpdate::{Max, Min, Multiply, Rename, SetIfMissing};
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert(
            "c",
            "a",
            &json!({"score":10,"profile":{"age":5,"nothing":null},"source":{"x":1},"dest":"old"}),
        )
        .unwrap();
        let updates = [
            SetIfMissing {
                path: path("profile.nothing"),
                value: json!(99),
            },
            SetIfMissing {
                path: path("profile.city"),
                value: json!("Ankara"),
            },
            Multiply {
                path: path("score"),
                factor: 3,
            },
            Multiply {
                path: path("profile.missing"),
                factor: -7,
            },
            Min {
                path: path("score"),
                value: IndexValue::Integer(20),
            },
            Max {
                path: path("score"),
                value: IndexValue::Integer(25),
            },
            Rename {
                from: path("profile.city"),
                to: path("profile.town"),
            },
            Rename {
                from: path("source"),
                to: path("dest"),
            },
            Rename {
                from: path("absent"),
                to: path("ignored.child"),
            },
        ];
        assert!(db.update_if("c", "a", &[], &updates, false).unwrap());
        assert_eq!(
            db.get("c", "a").unwrap(),
            Some(
                json!({"score":25,"profile":{"age":5,"nothing":null,"town":"Ankara","missing":0},"dest":{"x":1}})
            )
        );
        let mixed = [
            Max {
                path: path("score"),
                value: IndexValue::String("z".into()),
            },
            Min {
                path: path("score"),
                value: IndexValue::Null,
            },
            Max {
                path: path("score"),
                value: IndexValue::Boolean(false),
            },
        ];
        assert!(db.update_if("c", "a", &[], &mixed, false).unwrap());
        assert_eq!(db.get("c", "a").unwrap().unwrap()["score"], json!(false));
        assert!(
            !db.update_if(
                "c",
                "a",
                &[FieldCondition {
                    path: path("score"),
                    expected: Some(json!(true))
                }],
                &updates,
                false
            )
            .unwrap()
        );
        assert!(
            db.update_if(
                "c",
                "created",
                &[],
                &[
                    SetIfMissing {
                        path: path("v"),
                        value: json!(3)
                    },
                    Multiply {
                        path: path("v"),
                        factor: -2
                    }
                ],
                true
            )
            .unwrap()
        );
        assert_eq!(db.get("c", "created").unwrap(), Some(json!({"v":-6})));
        assert!(!db.update_if("c", "missing", &[], &updates, false).unwrap());
    });
}

#[test]
fn scalar_failures_are_atomic_and_preserve_indexes_and_raw_writes() {
    use DocumentUpdate::{Max, Multiply, Rename, Set, SetIfMissing};
    both_backends(|db| {
        db.create_collection(
            "c",
            CollectionSchema {
                fields: vec![FieldRule {
                    path: path("required"),
                    field_type: FieldType::Integer,
                    required: true,
                    nullable: false,
                }],
            },
        )
        .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "alias".into(),
                fields: vec![path("alias")],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
        db.insert(
            "c",
            "a",
            &json!({"required":1,"alias":"a","next":"b","num":i64::MIN,"nested":{"x":1}}),
        )
        .unwrap();
        db.insert("c", "b", &json!({"required":2,"alias":"b"}))
            .unwrap();
        let original = db.get("c", "a").unwrap();
        let bad = [
            vec![Multiply {
                path: path("num"),
                factor: -1,
            }],
            vec![
                Set {
                    path: path("num"),
                    value: json!(null),
                },
                Multiply {
                    path: path("num"),
                    factor: 2,
                },
            ],
            vec![
                Set {
                    path: path("num"),
                    value: json!(1.0),
                },
                Multiply {
                    path: path("num"),
                    factor: 2,
                },
            ],
            vec![
                Set {
                    path: path("num"),
                    value: json!(u64::MAX),
                },
                Multiply {
                    path: path("num"),
                    factor: 2,
                },
            ],
            vec![Max {
                path: path("nested"),
                value: IndexValue::Integer(2),
            }],
            vec![Max {
                path: path("num"),
                value: IndexValue::String("x".repeat(257)),
            }],
            vec![Rename {
                from: path("nested"),
                to: path("nested.x"),
            }],
            vec![Rename {
                from: path("nested.x"),
                to: path("nested"),
            }],
            vec![Rename {
                from: path("nested.x"),
                to: path("nested.x"),
            }],
            vec![Rename {
                from: vec![],
                to: path("destination"),
            }],
            vec![Rename {
                from: path("nested.x"),
                to: vec![],
            }],
            vec![Rename {
                from: path("nested.x"),
                to: path("num.child"),
            }],
            vec![Rename {
                from: path("required"),
                to: path("away"),
            }],
            vec![Rename {
                from: path("next"),
                to: path("alias"),
            }],
        ];
        for updates in bad {
            let result = db.transaction(|tx| {
                tx.put_raw(b"must-rollback", b"no")?;
                tx.update_if("c", "a", &[], &updates, false)?;
                Ok(())
            });
            assert!(result.is_err(), "{updates:?}");
            assert_eq!(db.get("c", "a").unwrap(), original);
            assert_eq!(
                db.transaction(|tx| tx.get_raw(b"must-rollback")).unwrap(),
                None
            );
        }
        assert_eq!(db.verify_collection("c").unwrap().documents, 2);
        assert_eq!(
            db.query_index(
                "c",
                "alias",
                &IndexQuery {
                    equal: vec![IndexValue::String("a".into())],
                    ..Default::default()
                }
            )
            .unwrap()
            .documents[0]
                .id,
            "a"
        );
        let invalid = SetIfMissing {
            path: vec![],
            value: json!(1),
        };
        assert_eq!(
            db.update_if("c", "a", &[], &[invalid], false)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn rename_updates_unique_index_atomically_and_persists_across_recovery() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = Store::open(&source).unwrap();
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        drop(initial);
        let store = open(&source, &checkpoint, &digest, checkpoint_mode);
        let db = store.db();
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "alias".into(),
                fields: vec![path("alias")],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
        db.insert("c", "a", &json!({"alias":"a"})).unwrap();
        db.update_if(
            "c",
            "a",
            &[],
            &[DocumentUpdate::Rename {
                from: path("alias"),
                to: path("name"),
            }],
            false,
        )
        .unwrap();
        db.insert("c", "b", &json!({"alias":"a"})).unwrap();
        assert_eq!(db.verify_collection("c").unwrap().documents, 2);
        drop(store);
        let recovered = open(&source, &checkpoint, &digest, checkpoint_mode);
        let db = recovered.db();
        assert_eq!(db.get("c", "a").unwrap(), Some(json!({"name":"a"})));
        assert_eq!(
            db.query_index(
                "c",
                "alias",
                &IndexQuery {
                    equal: vec![IndexValue::String("a".into())],
                    ..Default::default()
                }
            )
            .unwrap()
            .documents[0]
                .id,
            "b"
        );
        assert_eq!(db.verify_collection("c").unwrap().documents, 2);
    }
}

#[test]
fn concurrent_scalar_updates_retry_conflicts_without_lost_writes() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "num".into(),
                fields: vec![path("num")],
                unique: false,
                partial: None,
            },
        )
        .unwrap();
        db.insert("c", "a", &json!({"num":1})).unwrap();
        let start = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..2 {
                let start = &start;
                workers.push(scope.spawn(move || {
                    start.wait();
                    for _ in 0..20 {
                        let mut attempts = 0;
                        loop {
                            attempts += 1;
                            assert!(attempts <= 1000, "too many transaction conflicts");
                            match db.update_if(
                                "c",
                                "a",
                                &[],
                                &[DocumentUpdate::Multiply {
                                    path: path("num"),
                                    factor: -1,
                                }],
                                false,
                            ) {
                                Ok(true) => break,
                                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                                    std::thread::yield_now()
                                }
                                other => panic!("unexpected update result: {other:?}"),
                            }
                        }
                    }
                }));
            }
            start.wait();
            for worker in workers {
                worker.join().unwrap();
            }
        });
        assert_eq!(db.get("c", "a").unwrap(), Some(json!({"num":1})));
        assert_eq!(
            db.query_index(
                "c",
                "num",
                &IndexQuery {
                    equal: vec![IndexValue::Integer(1)],
                    ..Default::default()
                }
            )
            .unwrap()
            .documents
            .len(),
            1
        );
        assert_eq!(db.verify_collection("c").unwrap().documents, 1);
    });
}
