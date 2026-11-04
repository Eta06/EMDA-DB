use database_engine::{
    AggregateStatsRequest, CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter,
    IndexValue, Store,
};
use serde_json::json;
use std::{io::ErrorKind, sync::Barrier};

mod support;
use support::retry_lock;

fn path(value: &str) -> Vec<String> {
    value.split('.').map(str::to_owned).collect()
}

fn request() -> AggregateStatsRequest {
    AggregateStatsRequest {
        filter: DocumentFilter::All,
        group_by: Some(path("group")),
        fields: vec![path("n"), path("nested.m")],
        max_scanned: 32,
    }
}

fn both_backends(action: impl Fn(&DocumentDatabase<'_>)) {
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
            action(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            action(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn statistics_group_exactly_and_keep_missing_null_and_empty_distinct() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        let empty = db
            .aggregate_stats(
                "c",
                &AggregateStatsRequest {
                    group_by: None,
                    ..request()
                },
            )
            .unwrap();
        assert_eq!(empty.groups.len(), 1);
        assert_eq!(empty.groups[0].count, 0);
        assert_eq!(
            (
                empty.groups[0].fields[0].count,
                empty.groups[0].fields[0].sum,
                empty.groups[0].fields[0].mean()
            ),
            (0, 0, None)
        );
        assert!(
            db.aggregate_stats("c", &request())
                .unwrap()
                .groups
                .is_empty()
        );
        for (id, value) in [
            ("a", json!({"group":"x", "n":i64::MAX, "nested":{"m":-8}})),
            ("b", json!({"group":"x", "n":i64::MAX, "nested":{"m":2}})),
            ("c", json!({"group":null, "n":-5, "nested":{"m":null}})),
            ("d", json!({"n":null})),
        ] {
            db.insert("c", id, &value).unwrap();
        }
        let result = db.aggregate_stats("c", &request()).unwrap();
        assert_eq!(result.fields, request().fields);
        assert_eq!(
            (
                result.plan.scanned,
                result.plan.matched,
                result.plan.returned
            ),
            (4, 4, 4)
        );
        assert_eq!(
            result
                .groups
                .iter()
                .map(|g| (g.key.clone(), g.count))
                .collect::<Vec<_>>(),
            vec![
                (None, 1),
                (Some(IndexValue::Null), 1),
                (Some(IndexValue::String("x".into())), 2)
            ]
        );
        let group = &result.groups[2];
        assert_eq!(
            (
                group.fields[0].count,
                group.fields[0].sum,
                group.fields[0].min,
                group.fields[0].max
            ),
            (2, 2 * i128::from(i64::MAX), Some(i64::MAX), Some(i64::MAX))
        );
        assert_eq!(group.fields[0].mean(), Some(i64::MAX as f64));
        assert_eq!(
            (
                group.fields[1].count,
                group.fields[1].sum,
                group.fields[1].min,
                group.fields[1].max,
                group.fields[1].mean()
            ),
            (2, -6, Some(-8), Some(2), Some(-3.0))
        );
        assert_eq!(result.groups[1].fields[1].mean(), None);
        let filtered = db
            .aggregate_stats(
                "c",
                &AggregateStatsRequest {
                    filter: DocumentFilter::Equal {
                        path: path("group"),
                        value: json!("x"),
                    },
                    group_by: None,
                    ..request()
                },
            )
            .unwrap();
        assert_eq!(
            (filtered.groups[0].count, filtered.groups[0].fields[1].sum),
            (2, -6)
        );
    });
}

#[test]
fn statistics_validate_work_bounds_types_and_rollback() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "one", &json!({"n":1})).unwrap();
        for bad in [
            AggregateStatsRequest {
                fields: vec![],
                ..request()
            },
            AggregateStatsRequest {
                fields: vec![path("n"); 17],
                ..request()
            },
            AggregateStatsRequest {
                fields: vec![path("n"), path("n")],
                ..request()
            },
            AggregateStatsRequest {
                fields: vec![vec![]],
                ..request()
            },
            AggregateStatsRequest {
                group_by: Some(vec![]),
                ..request()
            },
            AggregateStatsRequest {
                max_scanned: 0,
                ..request()
            },
            AggregateStatsRequest {
                max_scanned: 257,
                ..request()
            },
        ] {
            assert_eq!(
                db.aggregate_stats("c", &bad).unwrap_err().kind(),
                ErrorKind::InvalidInput
            );
        }
        db.insert("c", "two", &json!({"n":1.0})).unwrap();
        let error = db
            .transaction(|tx| {
                tx.put_raw(b"stats-rollback", b"no")?;
                tx.aggregate_stats("c", &request())?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"stats-rollback")).unwrap(),
            None
        );
        assert_eq!(
            db.aggregate_stats(
                "c",
                &AggregateStatsRequest {
                    max_scanned: 1,
                    ..request()
                }
            )
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidInput
        );
        db.delete("c", "two").unwrap();
        db.insert("c", "bad-group", &json!({"group": [], "n": 1}))
            .unwrap();
        assert_eq!(
            db.aggregate_stats("c", &request()).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        db.delete("c", "bad-group").unwrap();
        db.insert("c", "bad-number", &json!({"n":u64::MAX}))
            .unwrap();
        assert_eq!(
            db.aggregate_stats("c", &request()).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn statistics_accept_all_fields_and_documents_at_the_limits() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        let fields = (0..16).map(|n| path(&format!("f{n}"))).collect::<Vec<_>>();
        for n in 0..256 {
            let document = (0..16)
                .map(|field| (format!("f{field}"), json!(n)))
                .collect::<serde_json::Map<String, serde_json::Value>>();
            db.insert("c", &format!("{n:03}"), &document.into())
                .unwrap();
        }
        let request = AggregateStatsRequest {
            filter: DocumentFilter::All,
            group_by: None,
            fields,
            max_scanned: 256,
        };
        let result = db.aggregate_stats("c", &request).unwrap();
        assert_eq!(result.fields, request.fields);
        assert_eq!(
            (
                result.plan.scanned,
                result.groups[0].count,
                result.groups[0].fields.len()
            ),
            (256, 256, 16)
        );
        for stats in &result.groups[0].fields {
            assert_eq!(
                (stats.count, stats.sum, stats.min, stats.max, stats.mean()),
                (256, 32640, Some(0), Some(255), Some(127.5))
            );
        }
        db.insert("c", "overflow", &json!({"f0":1})).unwrap();
        assert_eq!(
            db.aggregate_stats("c", &request).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn statistics_order_typed_groups_and_sum_negative_extremes() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        for (id, value) in [
            ("a", json!({"n": 0})),
            ("b", json!({"group": null, "n": 0})),
            ("c", json!({"group": false, "n": 0})),
            ("d", json!({"group": true, "n": 0})),
            ("e", json!({"group": -2, "n": 0})),
            ("f", json!({"group": 0, "n": 0})),
            ("g", json!({"group": "", "n": 0})),
            ("h", json!({"group": "z", "n": i64::MIN})),
            ("i", json!({"group": "z", "n": i64::MIN})),
        ] {
            db.insert("c", id, &value).unwrap();
        }
        let groups = db.aggregate_stats("c", &request()).unwrap().groups;
        assert_eq!(
            groups
                .iter()
                .map(|group| group.key.clone())
                .collect::<Vec<_>>(),
            vec![
                None,
                Some(IndexValue::Null),
                Some(IndexValue::Boolean(false)),
                Some(IndexValue::Boolean(true)),
                Some(IndexValue::Integer(-2)),
                Some(IndexValue::Integer(0)),
                Some(IndexValue::String("".into())),
                Some(IndexValue::String("z".into()))
            ]
        );
        assert_eq!(
            (
                groups[7].fields[0].count,
                groups[7].fields[0].sum,
                groups[7].fields[0].min,
                groups[7].fields[0].max
            ),
            (2, 2 * i128::from(i64::MIN), Some(i64::MIN), Some(i64::MIN))
        );
    });
}

#[test]
fn statistics_include_staged_writes_and_conflict_on_matching_phantoms() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.transaction(|tx| {
            tx.insert("c", "staged", &json!({"n":9}))?;
            assert_eq!(
                tx.aggregate_stats(
                    "c",
                    &AggregateStatsRequest {
                        group_by: None,
                        ..request()
                    }
                )?
                .groups[0]
                    .fields[0]
                    .sum,
                9
            );
            Ok(())
        })
        .unwrap();
        let entered = Barrier::new(2);
        let changed = Barrier::new(2);
        std::thread::scope(|scope| {
            let writer = scope.spawn(|| {
                entered.wait();
                db.insert("c", "new", &json!({"n":7})).unwrap();
                changed.wait();
            });
            let result = db.transaction_with_read_set(|tx| {
                assert_eq!(
                    tx.aggregate_stats(
                        "c",
                        &AggregateStatsRequest {
                            group_by: None,
                            ..request()
                        }
                    )?
                    .groups[0]
                        .count,
                    1
                );
                entered.wait();
                changed.wait();
                tx.put_raw(b"stats-phantom", b"no")?;
                Ok(())
            });
            assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
            writer.join().unwrap();
        });
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"stats-phantom")).unwrap(),
            None
        );
    });
}

#[test]
fn statistics_survive_full_log_and_checkpoint_backed_recovery() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "one", &json!({"group":"a","n":4})).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        db.insert("c", "two", &json!({"group":"a","n":9})).unwrap();
        let expected = db.aggregate_stats("c", &request()).unwrap();
        drop(store);
        if checkpoint_mode {
            let reopened =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            assert_eq!(
                DocumentDatabase::with_checkpoint_store(&reopened)
                    .aggregate_stats("c", &request())
                    .unwrap(),
                expected
            );
        } else {
            let reopened = retry_lock(|| Store::open(&source)).unwrap();
            assert_eq!(
                DocumentDatabase::new(&reopened)
                    .aggregate_stats("c", &request())
                    .unwrap(),
                expected
            );
        }
    }
}
