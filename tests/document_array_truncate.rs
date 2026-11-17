//! Array truncation keeps a prefix and commits with schema and index maintenance.
use database_engine::{
    CheckpointStore, CollectionIntegrity, CollectionSchema, DocumentDatabase, DocumentUpdate,
    FieldCondition, FieldRule, FieldType, IndexDefinition, IndexQuery, IndexValue,
    MAX_DOCUMENT_UPDATE_INPUT_BYTES, PartialFilter, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;

mod support;
use support::retry_lock;

// The private array budget admits one million examined entries. One hundred
// scans of 10,000 entries fill it. The same number of scans of 9,999 entries
// leaves room for exactly 100 discarded elements.
const ARRAY_WORK_LIMIT: usize = 1_000_000;
const FULL_SCANS: usize = 100;
const FULL_ARRAY: usize = 10_000;
const COUNTED_ARRAY: usize = 9_999;
const REMAINDER: usize = ARRAY_WORK_LIMIT - FULL_SCANS * COUNTED_ARRAY;

fn path(value: &str) -> Vec<String> {
    value.split('.').map(str::to_owned).collect()
}

fn truncate(field: &str, max_len: usize) -> DocumentUpdate {
    DocumentUpdate::ArrayTruncate {
        path: path(field),
        max_len,
    }
}

fn push(field: &str, value: Value) -> DocumentUpdate {
    DocumentUpdate::ArrayPush {
        path: path(field),
        value,
    }
}

fn set(field: &str, value: Value) -> DocumentUpdate {
    DocumentUpdate::Set {
        path: path(field),
        value,
    }
}

fn unset(field: &str) -> DocumentUpdate {
    DocumentUpdate::Unset { path: path(field) }
}

fn removes(count: usize) -> Vec<DocumentUpdate> {
    vec![
        DocumentUpdate::ArrayRemove {
            path: path("items"),
            value: json!(2),
        };
        count
    ]
}

fn missing_removes(count: usize, value: &Value) -> Vec<DocumentUpdate> {
    std::iter::repeat_n(
        DocumentUpdate::ArrayRemove {
            path: path("missing"),
            value: value.clone(),
        },
        count,
    )
    .collect()
}

fn text(value: &str) -> IndexValue {
    IndexValue::String(value.to_owned())
}

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn index(name: &str, field: &str, unique: bool, partial: Option<PartialFilter>) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: vec![path(field)],
        unique,
        partial,
    }
}

fn original_a() -> Value {
    json!({
        "handle": "a",
        "tag": "old",
        "score": 1,
        "items": [1, {"x": 1}, 1, null, "tail"],
        "profile": {"tags": ["keep", "drop"]},
        "note": null,
        "flag": true
    })
}

fn prefix_a() -> Value {
    json!({
        "handle": "a",
        "tag": "old",
        "score": 1,
        "items": [1, {"x": 1}, "after"],
        "profile": {"tags": ["keep"]},
        "note": null,
        "flag": true
    })
}

fn final_a() -> Value {
    json!({
        "handle": "a",
        "tag": "new",
        "score": 1,
        "items": [],
        "profile": {"tags": ["keep"]},
        "note": null,
        "flag": true
    })
}

fn original_b() -> Value {
    json!({"handle": "b", "tag": "old", "score": 2, "items": [9]})
}

fn final_b() -> Value {
    json!({"handle": "b", "tag": "old", "score": 2, "items": []})
}

fn final_c() -> Value {
    json!({"score": 4, "handle": "c"})
}

fn with_ones(count: usize) -> Value {
    json!({"handle": "a", "tag": "old", "score": 1, "items": vec![1; count]})
}

fn short_items() -> Value {
    json!({"handle": "a", "tag": "old", "score": 1, "items": [1, 2, 3, 4]})
}

fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection(
        "c",
        CollectionSchema {
            fields: vec![
                FieldRule {
                    path: path("items"),
                    field_type: FieldType::Array,
                    required: false,
                    nullable: false,
                },
                FieldRule {
                    path: path("score"),
                    field_type: FieldType::Integer,
                    required: true,
                    nullable: false,
                },
            ],
        },
    )
    .unwrap();
    db.create_index("c", index("handle", "handle", true, None))
        .unwrap();
    db.create_index("c", index("tag", "tag", false, None))
        .unwrap();
    db.create_index(
        "c",
        index(
            "with_items",
            "tag",
            false,
            Some(PartialFilter::Exists {
                path: path("items"),
                exists: true,
            }),
        ),
    )
    .unwrap();
    db.create_index(
        "c",
        index(
            "without_items",
            "handle",
            false,
            Some(PartialFilter::Exists {
                path: path("items"),
                exists: false,
            }),
        ),
    )
    .unwrap();
    db.insert("c", "a", &original_a()).unwrap();
    db.insert("c", "b", &original_b()).unwrap();
}

fn ids(db: &DocumentDatabase<'_>, index_name: &str, equal: &[IndexValue]) -> Vec<String> {
    db.query_index(
        "c",
        index_name,
        &IndexQuery {
            equal: equal.to_vec(),
            ..Default::default()
        },
    )
    .unwrap()
    .documents
    .into_iter()
    .map(|entry| entry.id)
    .collect()
}

fn assert_pair(db: &DocumentDatabase<'_>) {
    assert_eq!(ids(db, "handle", &[text("a")]), names(&["a"]));
    assert_eq!(ids(db, "handle", &[text("b")]), names(&["b"]));
    assert_eq!(ids(db, "tag", &[text("old")]), names(&["a", "b"]));
    assert_eq!(ids(db, "tag", &[text("new")]), names(&[]));
    assert_eq!(ids(db, "with_items", &[]), names(&["a", "b"]));
    assert_eq!(ids(db, "without_items", &[]), names(&[]));
    assert_eq!(
        db.verify_collection("c").unwrap(),
        CollectionIntegrity {
            documents: 2,
            indexes: 4,
            index_keys: 8,
        }
    );
}

fn assert_final(db: &DocumentDatabase<'_>) {
    assert_eq!(db.get("c", "a").unwrap(), Some(final_a()));
    assert_eq!(db.get("c", "b").unwrap(), Some(final_b()));
    assert_eq!(db.get("c", "c").unwrap(), Some(final_c()));
    assert_eq!(ids(db, "handle", &[text("a")]), names(&["a"]));
    assert_eq!(ids(db, "handle", &[text("b")]), names(&["b"]));
    assert_eq!(ids(db, "handle", &[text("c")]), names(&["c"]));
    assert_eq!(ids(db, "tag", &[text("old")]), names(&["b"]));
    assert_eq!(ids(db, "tag", &[text("new")]), names(&["a"]));
    assert_eq!(ids(db, "with_items", &[]), names(&["a", "b"]));
    assert_eq!(ids(db, "without_items", &[]), names(&["c"]));
    assert_eq!(
        db.verify_collection("c").unwrap(),
        CollectionIntegrity {
            documents: 3,
            indexes: 4,
            index_keys: 11,
        }
    );
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
        Some(b"truncated".to_vec())
    );
    assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);
}

fn with_both_backends(check: impl Fn(&DocumentDatabase<'_>)) {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = retry_lock(|| Store::open(&source)).unwrap();
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(initial);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            check(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            check(&DocumentDatabase::new(&initial));
        }
    }
}

fn exercise(db: &DocumentDatabase<'_>) {
    prepare(db);
    assert_pair(db);
    assert!(
        db.update_if(
            "c",
            "a",
            &[],
            &[
                truncate("items", 2),
                truncate("items", usize::MAX),
                truncate("profile.tags", 1),
                truncate("missing.field", 0),
                truncate("profile.missing", 3),
                push("items", json!("after")),
                truncate("items", 3),
                push("items", json!("late")),
                truncate("items", 3),
            ],
            false,
        )
        .unwrap()
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(prefix_a()));
    assert_pair(db);
    assert!(
        !db.update_if(
            "c",
            "a",
            &[
                FieldCondition {
                    path: path("score"),
                    expected: Some(json!(1)),
                },
                FieldCondition {
                    path: path("tag"),
                    expected: Some(json!("other")),
                },
            ],
            &[truncate("items", 0), set("tag", json!("changed"))],
            false,
        )
        .unwrap()
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(prefix_a()));
    assert!(
        !db.update_if("c", "c", &[], &[truncate("items", 0)], false)
            .unwrap()
    );
    assert_eq!(db.get("c", "c").unwrap(), None);
    assert!(
        db.update_if(
            "c",
            "a",
            &[
                FieldCondition {
                    path: path("score"),
                    expected: Some(json!(1)),
                },
                FieldCondition {
                    path: path("tag"),
                    expected: Some(json!("old")),
                },
            ],
            &[truncate("items", 1), set("tag", json!("new"))],
            false,
        )
        .unwrap()
    );
    assert_eq!(
        db.get("c", "a").unwrap(),
        Some(json!({
            "handle": "a",
            "tag": "new",
            "score": 1,
            "items": [1],
            "profile": {"tags": ["keep"]},
            "note": null,
            "flag": true
        }))
    );
    assert_eq!(ids(db, "tag", &[text("new")]), names(&["a"]));
    assert_eq!(ids(db, "tag", &[text("old")]), names(&["b"]));
    db.transaction(|tx| {
        tx.put_raw(b"receipt", b"truncated")?;
        assert!(tx.update_if("c", "a", &[], &[truncate("items", 0)], false)?);
        Ok(())
    })
    .unwrap();
    assert_eq!(db.get("c", "a").unwrap().unwrap()["items"], json!([]));
    assert_eq!(ids(db, "with_items", &[]), names(&["a", "b"]));
    assert_eq!(ids(db, "without_items", &[]), names(&[]));
    assert!(
        db.update_if("c", "b", &[], &[truncate("items", 0)], false)
            .unwrap()
    );
    assert!(
        db.update_if("c", "b", &[], &[truncate("items", 0)], false)
            .unwrap()
    );
    assert_eq!(db.get("c", "b").unwrap(), Some(final_b()));
    assert!(
        db.update_if(
            "c",
            "c",
            &[],
            &[
                set("score", json!(4)),
                set("handle", json!("c")),
                truncate("items", 2),
                truncate("profile.tags", 1),
            ],
            true,
        )
        .unwrap()
    );
    assert_final(db);
}

#[test]
fn truncate_keeps_prefix_and_updates_indexes_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = retry_lock(|| Store::open(&source)).unwrap();
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(initial);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            exercise(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            exercise(&DocumentDatabase::new(&initial));
            drop(initial);
        }
        let recovered = retry_lock(|| Store::open(&source)).unwrap();
        assert_final(&DocumentDatabase::new(&recovered));
    }
}

fn reject(
    db: &DocumentDatabase<'_>,
    id: &str,
    conditions: &[FieldCondition],
    updates: &[DocumentUpdate],
    upsert: bool,
    kind: ErrorKind,
    message: &str,
) {
    let before_a = db.get("c", "a").unwrap();
    let before_b = db.get("c", "b").unwrap();
    let before_target = db.get("c", id).unwrap();
    let failed = db.transaction(|tx| {
        tx.put_raw(b"rollback", b"no")?;
        let error = match tx.update_if("c", id, conditions, updates, upsert) {
            Ok(committed) => panic!("committed update {committed}: {message}"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), kind, "{message}");
        assert_eq!(error.to_string(), message);
        Ok(())
    });
    assert!(failed.is_err(), "{message}");
    assert_eq!(db.get("c", "a").unwrap(), before_a, "{message}");
    assert_eq!(db.get("c", "b").unwrap(), before_b, "{message}");
    assert_eq!(db.get("c", id).unwrap(), before_target, "{message}");
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(),
        None,
        "{message}"
    );
    assert_pair(db);
}

#[test]
fn rejected_truncates_roll_back_schema_indexes_and_raw_writes() {
    with_both_backends(|db| {
        prepare(db);
        assert_pair(db);
        for field in ["handle", "note", "score", "flag", "profile"] {
            reject(
                db,
                "a",
                &[],
                &[truncate(field, 0)],
                false,
                ErrorKind::InvalidInput,
                "array update requires an array",
            );
        }
        reject(
            db,
            "a",
            &[],
            &[truncate("score", usize::MAX)],
            false,
            ErrorKind::InvalidInput,
            "array update requires an array",
        );
        for field in [
            "items.0",
            "note.child",
            "score.child",
            "flag.child",
            "profile.tags.extra",
        ] {
            reject(
                db,
                "a",
                &[],
                &[truncate(field, 0)],
                false,
                ErrorKind::InvalidInput,
                "update path traverses a non-object",
            );
        }
        reject(
            db,
            "a",
            &[],
            &[truncate("items", 1), truncate("score", 0)],
            false,
            ErrorKind::InvalidInput,
            "array update requires an array",
        );
        reject(
            db,
            "a",
            &[],
            &[DocumentUpdate::ArrayTruncate {
                path: vec![],
                max_len: 1,
            }],
            false,
            ErrorKind::InvalidInput,
            "field path must have between 1 and 32 components",
        );
        reject(
            db,
            "a",
            &[],
            &[DocumentUpdate::ArrayTruncate {
                path: vec!["p".into(); 33],
                max_len: 0,
            }],
            false,
            ErrorKind::InvalidInput,
            "field path must have between 1 and 32 components",
        );
        reject(
            db,
            "a",
            &[],
            &[DocumentUpdate::ArrayTruncate {
                path: vec!["x".repeat(129)],
                max_len: usize::MAX,
            }],
            false,
            ErrorKind::InvalidInput,
            "name must contain between 1 and 128 UTF-8 bytes",
        );
        reject(
            db,
            "a",
            &[],
            &[
                truncate("items", 1),
                DocumentUpdate::ArrayTruncate {
                    path: vec![],
                    max_len: 0,
                },
            ],
            false,
            ErrorKind::InvalidInput,
            "field path must have between 1 and 32 components",
        );
        reject(
            db,
            "a",
            &[FieldCondition {
                path: path("score"),
                expected: Some(json!(0)),
            }],
            &[DocumentUpdate::ArrayTruncate {
                path: vec![],
                max_len: 0,
            }],
            false,
            ErrorKind::InvalidInput,
            "field path must have between 1 and 32 components",
        );
        reject(
            db,
            "a",
            &[],
            &[truncate("items", 0), unset("score")],
            false,
            ErrorKind::InvalidInput,
            "required field is missing",
        );
        reject(
            db,
            "a",
            &[],
            &[truncate("items", 1), set("items", Value::Null)],
            false,
            ErrorKind::InvalidInput,
            "field does not permit null",
        );
        reject(
            db,
            "a",
            &[],
            &[truncate("items", 1), set("score", json!("x"))],
            false,
            ErrorKind::InvalidInput,
            "field does not match its schema type",
        );
        reject(
            db,
            "a",
            &[],
            &[truncate("items", 0), set("handle", json!("b"))],
            false,
            ErrorKind::AlreadyExists,
            "unique index constraint violated",
        );
        reject(
            db,
            "a",
            &[],
            &[
                truncate("items", 0),
                unset("items"),
                set("handle", json!("b")),
            ],
            false,
            ErrorKind::AlreadyExists,
            "unique index constraint violated",
        );
        reject(
            db,
            "new",
            &[],
            &[truncate("items", 0)],
            true,
            ErrorKind::InvalidInput,
            "required field is missing",
        );
        assert_eq!(db.get("c", "a").unwrap(), Some(original_a()));
        assert_eq!(db.get("c", "b").unwrap(), Some(original_b()));
        assert_eq!(db.get("c", "new").unwrap(), None);
    });
}

fn overrun(drop_count: usize) -> Vec<DocumentUpdate> {
    let mut updates = vec![set("tag", json!("new"))];
    updates.extend(removes(FULL_SCANS));
    updates.push(truncate("items", COUNTED_ARRAY - drop_count));
    updates
}

fn reject_work(db: &DocumentDatabase<'_>, updates: &[DocumentUpdate]) {
    let saved = db.get("c", "a").unwrap();
    let failed = db.transaction(|tx| {
        tx.put_raw(b"rollback", b"no")?;
        let error = tx
            .update_if("c", "a", &[], updates, false)
            .expect_err("work limit");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "array update work limit exceeded");
        Ok(())
    });
    assert!(failed.is_err());
    assert_eq!(db.get("c", "a").unwrap(), saved);
    assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);
    assert_pair(db);
}

#[test]
fn truncate_enforces_input_work_and_operator_limits() {
    assert_eq!(FULL_SCANS * FULL_ARRAY, ARRAY_WORK_LIMIT);
    assert_eq!(REMAINDER, 100);
    with_both_backends(|db| {
        prepare(db);
        let too_many = std::iter::repeat_n(truncate("items", usize::MAX), 129).collect::<Vec<_>>();
        reject(
            db,
            "a",
            &[],
            &too_many,
            false,
            ErrorKind::InvalidInput,
            "update requires 1 to 128 operators and at most 64 conditions",
        );
        let mut no_op = removes(FULL_SCANS);
        no_op.extend(std::iter::repeat_n(truncate("items", usize::MAX), 28));
        assert_eq!(no_op.len(), 128);
        assert!(!db.update_if("c", "absent", &[], &no_op, false).unwrap());
        assert_eq!(db.get("c", "absent").unwrap(), None);
        assert!(
            !db.update_if("c", "absent", &[], &overrun(REMAINDER + 1), false)
                .unwrap()
        );

        db.replace("c", "a", &with_ones(FULL_ARRAY), false).unwrap();
        assert!(db.update_if("c", "a", &[], &no_op, false).unwrap());
        assert_eq!(db.get("c", "a").unwrap(), Some(with_ones(FULL_ARRAY)));
        assert_pair(db);

        let mut clear = vec![truncate("items", 0)];
        clear.extend(removes(127));
        assert_eq!(clear.len(), 128);
        assert!(db.update_if("c", "a", &[], &clear, false).unwrap());
        assert_eq!(
            db.get("c", "a").unwrap(),
            Some(json!({"handle": "a", "tag": "old", "score": 1, "items": []}))
        );
        assert_eq!(ids(db, "with_items", &[]), names(&["a", "b"]));
        assert_eq!(ids(db, "without_items", &[]), names(&[]));
        assert_pair(db);

        db.replace("c", "a", &with_ones(COUNTED_ARRAY), false)
            .unwrap();
        let failing = overrun(REMAINDER + 1);
        assert!(
            !db.update_if(
                "c",
                "a",
                &[FieldCondition {
                    path: path("score"),
                    expected: Some(json!(0)),
                }],
                &failing,
                false,
            )
            .unwrap()
        );
        assert_eq!(db.get("c", "a").unwrap(), Some(with_ones(COUNTED_ARRAY)));
        reject_work(db, &failing);
        let mut exact_drop = removes(FULL_SCANS);
        exact_drop.push(truncate("items", COUNTED_ARRAY - REMAINDER));
        assert!(db.update_if("c", "a", &[], &exact_drop, false).unwrap());
        assert_eq!(
            db.get("c", "a").unwrap(),
            Some(with_ones(COUNTED_ARRAY - REMAINDER))
        );
        assert_pair(db);

        let share = MAX_DOCUMENT_UPDATE_INPUT_BYTES / 4;
        let boundary = Value::String("x".repeat(share - 2));
        let large = Value::String("x".repeat(share));
        let mut exact = vec![truncate("items", 1)];
        exact.extend(missing_removes(4, &boundary));
        let mut over = vec![truncate("items", 1), set("tag", json!("new"))];
        over.extend(missing_removes(4, &large));
        let over_error = db.update_if("c", "absent", &[], &over, false).unwrap_err();
        assert_eq!(over_error.kind(), ErrorKind::InvalidInput);
        assert_eq!(over_error.to_string(), "update input exceeds 4 MiB");
        assert_eq!(db.get("c", "absent").unwrap(), None);
        assert!(!db.update_if("c", "absent", &[], &exact, false).unwrap());
        assert_eq!(db.get("c", "absent").unwrap(), None);

        db.replace("c", "a", &short_items(), false).unwrap();
        assert!(db.update_if("c", "a", &[], &exact, false).unwrap());
        assert_eq!(
            db.get("c", "a").unwrap(),
            Some(json!({"handle": "a", "tag": "old", "score": 1, "items": [1]}))
        );
        assert_pair(db);
        db.replace("c", "a", &short_items(), false).unwrap();
        let failed = db.transaction(|tx| {
            tx.put_raw(b"rollback", b"no")?;
            let error = tx
                .update_if("c", "a", &[], &over, false)
                .expect_err("input limit");
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert_eq!(error.to_string(), "update input exceeds 4 MiB");
            Ok(())
        });
        assert!(failed.is_err());
        assert_eq!(db.get("c", "a").unwrap(), Some(short_items()));
        assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);
        assert_pair(db);
    });
}

#[test]
fn reverse_is_ordered_bounded_and_atomic_on_both_backends() {
    with_both_backends(|db| {
        prepare(db);
        let reverse = |field: &str| DocumentUpdate::ArrayReverse { path: path(field) };
        db.update_if(
            "c",
            "a",
            &[],
            &[
                reverse("items"),
                push("items", json!("end")),
                reverse("profile.tags"),
                reverse("absent.child"),
            ],
            false,
        )
        .unwrap();
        let mut expected = original_a();
        expected["items"] = json!(["tail", null, 1, {"x": 1}, 1, "end"]);
        expected["profile"]["tags"] = json!(["drop", "keep"]);
        assert_eq!(db.get("c", "a").unwrap(), Some(expected.clone()));
        assert_pair(db);
        let result = db.transaction(|tx| {
            tx.put_raw(b"reverse-rollback", b"value")?;
            tx.update_if("c", "a", &[], &[reverse("items"), reverse("note")], false)?;
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(db.get("c", "a").unwrap(), Some(expected));
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"reverse-rollback"))
                .unwrap(),
            None
        );
        assert_pair(db);
        db.update_if(
            "c",
            "a",
            &[],
            &[set("items", json!(vec![0; 10_000]))],
            false,
        )
        .unwrap();
        let before = db.get("c", "a").unwrap();
        let updates = vec![reverse("items"); 101];
        assert!(db.update_if("c", "a", &[], &updates, false).is_err());
        assert_eq!(db.get("c", "a").unwrap(), before);
    });
}

#[test]
fn reverse_rejects_invalid_updates_atomically_on_both_backends() {
    with_both_backends(|db| {
        prepare(db);
        let reverse = |field: &str| DocumentUpdate::ArrayReverse { path: path(field) };
        let invalid_paths = [
            vec![],
            vec!["p".into(); 33],
            vec!["x".repeat(129)],
            vec!["".into()],
        ];
        for path in invalid_paths {
            let message = if path.len() == 33 || path.is_empty() {
                "field path must have between 1 and 32 components"
            } else {
                "name must contain between 1 and 128 UTF-8 bytes"
            };
            reject(
                db,
                "a",
                &[],
                &[reverse("items"), DocumentUpdate::ArrayReverse { path }],
                false,
                ErrorKind::InvalidInput,
                message,
            );
        }
        for field in ["note.child", "score.child", "items.0"] {
            reject(
                db,
                "a",
                &[],
                &[reverse("items"), reverse(field)],
                false,
                ErrorKind::InvalidInput,
                "update path traverses a non-object",
            );
        }
        reject(
            db,
            "a",
            &[],
            &[reverse("items"), reverse("note")],
            false,
            ErrorKind::InvalidInput,
            "array update requires an array",
        );
        reject(
            db,
            "a",
            &[],
            &[reverse("items"), unset("score")],
            false,
            ErrorKind::InvalidInput,
            "required field is missing",
        );
        reject(
            db,
            "a",
            &[],
            &[reverse("items"), unset("items"), set("handle", json!("b"))],
            false,
            ErrorKind::AlreadyExists,
            "unique index constraint violated",
        );
    });
}

#[test]
fn reverse_shares_exact_work_boundary_and_rolls_back_one_over() {
    with_both_backends(|db| {
        prepare(db);
        let mut original = with_ones(COUNTED_ARRAY);
        original["items"][0] = json!(7);
        db.replace("c", "a", &original, false).unwrap();
        let reverse = DocumentUpdate::ArrayReverse {
            path: path("items"),
        };
        // 99 removals and one reverse examine 999,900 entries; truncation
        // accounts for the remaining 100 (or 101 for the rejection).
        let mut prefix = vec![set("tag", json!("new"))];
        prefix.extend(removes(99));
        prefix.push(reverse);
        let mut over = prefix.clone();
        over.push(truncate("items", COUNTED_ARRAY - REMAINDER - 1));
        reject_work(db, &over);
        let mut exact = prefix;
        exact.push(truncate("items", COUNTED_ARRAY - REMAINDER));
        assert!(db.update_if("c", "a", &[], &exact, false).unwrap());
        let mut expected = original;
        expected["tag"] = json!("new");
        expected["items"] = json!(vec![1; COUNTED_ARRAY - REMAINDER]);
        assert_eq!(db.get("c", "a").unwrap(), Some(expected));
        assert_eq!(ids(db, "tag", &[text("new")]), names(&["a"]));
        assert_eq!(ids(db, "with_items", &[]), names(&["a", "b"]));
        assert_eq!(db.verify_collection("c").unwrap().documents, 2);
    });
}

#[test]
fn keep_last_is_ordered_bounded_and_atomic_on_both_backends() {
    with_both_backends(|db| {
        prepare(db);
        let keep = |field: &str, max_len| DocumentUpdate::ArrayKeepLast {
            path: path(field),
            max_len,
        };
        db.update_if(
            "c",
            "a",
            &[],
            &[
                keep("items", 2),
                push("items", json!("new")),
                keep("absent.child", 0),
            ],
            false,
        )
        .unwrap();
        let mut expected = original_a();
        expected["items"] = json!([null, "tail", "new"]);
        assert_eq!(db.get("c", "a").unwrap(), Some(expected.clone()));
        assert_pair(db);
        assert!(
            db.transaction(|tx| {
                tx.put_raw(b"keep-rollback", b"value")?;
                tx.update_if("c", "a", &[], &[keep("items", 0), keep("note", 1)], false)?;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(db.get("c", "a").unwrap(), Some(expected));
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"keep-rollback")).unwrap(),
            None
        );
        assert_pair(db);
        db.update_if(
            "c",
            "a",
            &[],
            &[set("items", json!(vec![0; 20_000]))],
            false,
        )
        .unwrap();
        let before_large = db.get("c", "a").unwrap();
        let mut updates = Vec::new();
        for _ in 0..51 {
            updates.push(keep("items", 19_999));
            updates.push(push("items", json!(0)));
        }
        db.update_if("c", "a", &[], &updates[..100], false)
            .unwrap();
        assert_eq!(db.get("c", "a").unwrap(), before_large);
        assert!(db.update_if("c", "a", &[], &updates, false).is_err());
        assert_eq!(db.get("c", "a").unwrap(), before_large);
        db.update_if(
            "c",
            "a",
            &[],
            &[keep("items", usize::MAX), keep("items", 0)],
            false,
        )
        .unwrap();
        assert_eq!(db.get("c", "a").unwrap().unwrap()["items"], json!([]));
    });
}
