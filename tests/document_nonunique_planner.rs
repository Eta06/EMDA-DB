//! Nonunique index planning must preserve complete, serializable query results.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, Store,
};
use serde_json::{Value, json};
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Barrier;

mod support;
use support::retry_lock;

fn path(name: &str) -> Vec<String> {
    name.split('.').map(str::to_owned).collect()
}
fn equal(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(name),
        value,
    }
}
fn group_access() -> DocumentQueryAccess {
    DocumentQueryAccess::EqualityIndex {
        index: "group".into(),
    }
}
fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    for (name, unique) in [("group", false), ("handle", true)] {
        db.create_index(
            "c",
            IndexDefinition {
                name: name.into(),
                fields: vec![path(name)],
                unique,
                partial: None,
            },
        )
        .unwrap();
    }
    for start in (0..360).step_by(20) {
        db.transaction(|tx| {
            for i in start..start + 20 {
                let group = if i < 300 {
                    json!("common")
                } else if i < 330 {
                    Value::Null
                } else {
                    json!("rare")
                };
                tx.insert(
                    "c",
                    &format!("{i:04}"),
                    &json!({
                        "handle":format!("user{i:04}"), "group":group,
                        "score":i % 7, "tags":[i % 3, "shared"]
                    }),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
    db.insert("c", "missing", &json!({"handle":"missing"}))
        .unwrap();
}
fn compare(db: &DocumentDatabase<'_>, query: &DocumentQuery, access: DocumentQueryAccess) {
    let scanned = db.query("c", query).unwrap();
    let planned = db.query_planned("c", query).unwrap();
    assert_eq!(planned.access, access);
    assert_eq!(planned.result.documents, scanned.documents);
    assert_eq!(planned.result.plan.matched, scanned.plan.matched);
    assert_eq!(planned.result.plan.sorted, scanned.plan.sorted);
    assert_eq!(planned.result.plan.returned, scanned.plan.returned);
}
fn exercise(db: &DocumentDatabase<'_>) {
    let base = DocumentQuery {
        filter: equal("group", json!("common")),
        max_scanned: 400,
        ..Default::default()
    };
    for query in [
        base.clone(),
        DocumentQuery {
            offset: 260,
            limit: 20,
            ..base.clone()
        },
        DocumentQuery {
            filter: DocumentFilter::And(vec![
                equal("group", json!("common")),
                DocumentFilter::ArrayContains {
                    path: path("tags"),
                    value: json!(2),
                },
            ]),
            sort: Some(DocumentSort {
                path: path("score"),
                descending: true,
            }),
            projection: Some(vec![path("handle"), path("tags")]),
            offset: 5,
            limit: 12,
            ..base.clone()
        },
        DocumentQuery {
            filter: equal("group", Value::Null),
            ..base.clone()
        },
        DocumentQuery {
            filter: equal("group", json!("absent")),
            ..base.clone()
        },
        DocumentQuery {
            filter: DocumentFilter::And(vec![
                equal("group", json!("rare")),
                equal("group", json!("common")),
            ]),
            ..base.clone()
        },
    ] {
        compare(db, &query, group_access());
    }
    let page = db
        .query_planned(
            "c",
            &DocumentQuery {
                offset: 260,
                limit: 20,
                ..base.clone()
            },
        )
        .unwrap();
    assert_eq!(page.result.plan.scanned, 300);
    assert_eq!(
        page.result
            .documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        (260..280).map(|i| format!("{i:04}")).collect::<Vec<_>>()
    );
    let limit = DocumentQuery {
        max_scanned: 299,
        ..base.clone()
    };
    assert_eq!(
        db.query_planned("c", &limit).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let exact = DocumentQuery {
        max_scanned: 300,
        ..base.clone()
    };
    assert_eq!(
        db.query_planned("c", &exact).unwrap().result.plan.scanned,
        300
    );
    let small = DocumentQuery {
        filter: equal("group", json!("rare")),
        max_scanned: 30,
        ..base.clone()
    };
    assert_eq!(
        db.query("c", &small).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        db.query_planned("c", &small).unwrap().result.plan.scanned,
        30
    );
    let unique = DocumentQuery {
        filter: DocumentFilter::And(vec![
            equal("group", json!("common")),
            equal("handle", json!("user0001")),
        ]),
        ..base.clone()
    };
    compare(
        db,
        &unique,
        DocumentQueryAccess::UniqueEqualityIndex {
            index: "handle".into(),
        },
    );
    let disjunction = DocumentQuery {
        filter: DocumentFilter::Or(vec![
            equal("group", json!("common")),
            equal("group", json!("rare")),
        ]),
        ..base
    };
    compare(
        db,
        &disjunction,
        DocumentQueryAccess::DisjunctionIndex {
            indexes: vec!["group".into()],
            probes: 2,
        },
    );
}

#[test]
fn nonunique_index_matches_scans_on_both_backends_and_after_recovery() {
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
            let db = DocumentDatabase::with_checkpoint_store(&store);
            prepare(&db);
            exercise(&db);
        } else {
            let db = DocumentDatabase::new(&store);
            prepare(&db);
            exercise(&db);
            drop(store);
        }
        let recovered = retry_lock(|| Store::open(&source)).unwrap();
        exercise(&DocumentDatabase::new(&recovered));
    }
}

fn phantom(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "group".into(),
            fields: vec![path("group")],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert("c", "new", &json!({"group":"target"})).unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let planned = tx.query_planned(
                "c",
                &DocumentQuery {
                    filter: equal("group", json!("target")),
                    ..Default::default()
                },
            )?;
            assert_eq!(planned.access, group_access());
            assert!(planned.result.documents.is_empty());
            entered.wait();
            changed.wait();
            tx.put_raw(b"phantom", b"must roll back")?;
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        writer.join().unwrap();
    });
    assert_eq!(db.transaction(|tx| tx.get_raw(b"phantom")).unwrap(), None);
}

#[test]
fn nonunique_index_empty_range_detects_phantoms_on_both_backends() {
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
            phantom(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            phantom(&DocumentDatabase::new(&store));
        }
    }
}

fn byte_limits(db: &DocumentDatabase<'_>, source: &Path, overwrite: impl FnOnce(&[u8], &[u8])) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "group".into(),
            fields: vec![path("group")],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    let overhead = serde_json::to_vec(&json!({"group":"same", "payload":""}))
        .unwrap()
        .len();
    let last_key = b"\0emda-document\x01D\0\x01c32";
    let payload = "x".repeat(1024 * 1024 - overhead - last_key.len());
    let document = json!({"group":"same", "payload":payload});
    assert_eq!(
        serde_json::to_vec(&document).unwrap().len() + last_key.len(),
        1024 * 1024
    );
    for id in 0..31 {
        db.insert("c", &format!("{id:02}"), &document).unwrap();
    }
    let query = DocumentQuery {
        filter: equal("group", json!("same")),
        max_scanned: 33,
        limit: 1,
        ..Default::default()
    };
    assert_eq!(db.query("c", &query).unwrap().plan.scanned, 31);
    let planned = db.query_planned("c", &query).unwrap();
    assert_eq!(planned.access, group_access());
    assert_eq!(planned.result.plan.scanned, 31);
    assert_eq!(planned.result.documents.len(), 1);
    db.insert("c", "31", &document).unwrap();
    assert_eq!(db.query("c", &query).unwrap().plan.scanned, 32);
    assert_eq!(
        db.query_planned("c", &query).unwrap().result.plan.scanned,
        32
    );

    db.insert("c", "32", &json!({"group":"same"})).unwrap();
    // Overwrite just the payload. The planned query must reject this extra
    // candidate by original size before parsing the malformed JSON.
    overwrite(last_key, b"!");
    assert_eq!(
        db.query("c", &query).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        db.query_planned("c", &query).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    // Damage the single-byte payload in the last DB01 record without
    // changing its length. An attempted value read would now fail its CRC.
    let log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(source.join("records.log"))
        .unwrap();
    let byte_offset = log.metadata().unwrap().len() - 5;
    let mut original = [0];
    log.read_exact_at(&mut original, byte_offset).unwrap();
    assert_eq!(&original, b"!");
    log.write_all_at(b"?", byte_offset).unwrap();
    let error = db
        .transaction(|tx| {
            tx.put_raw(b"byte-budget-rollback", b"must not commit")?;
            tx.query_planned("c", &query)?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"byte-budget-rollback"))
            .unwrap(),
        None
    );
    assert!(db.delete("c", "00").unwrap());
    let error = db.query_planned("c", &query).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("stored value checksum mismatch"));
}

#[test]
fn nonunique_index_preserves_full_scan_byte_budget_on_both_backends() {
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
            byte_limits(
                &DocumentDatabase::with_checkpoint_store(&store),
                &source,
                |key, value| store.put(key, value).unwrap(),
            );
        } else {
            byte_limits(&DocumentDatabase::new(&store), &source, |key, value| {
                store.put(key, value).unwrap()
            });
        }
    }
}

fn paged_phantom(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "group".into(),
            fields: vec![path("group")],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    for start in (0..260).step_by(20) {
        db.transaction(|tx| {
            for i in start..start + 20 {
                tx.insert("c", &format!("{i:04}"), &json!({"group":"same"}))?;
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
            db.insert("c", "new", &json!({"group":"same"})).unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            let planned = tx.query_planned(
                "c",
                &DocumentQuery {
                    filter: equal("group", json!("same")),
                    max_scanned: 300,
                    ..Default::default()
                },
            )?;
            assert_eq!(planned.access, group_access());
            assert_eq!(planned.result.plan.scanned, 260);
            entered.wait();
            changed.wait();
            tx.put_raw(b"paged-phantom", b"must roll back")?;
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        writer.join().unwrap();
    });
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"paged-phantom")).unwrap(),
        None
    );
}

#[test]
fn nonunique_multi_page_query_detects_phantoms_on_both_backends() {
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
            paged_phantom(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            paged_phantom(&DocumentDatabase::new(&store));
        }
    }
}

fn check_bad_index(db: &DocumentDatabase<'_>) {
    let query = DocumentQuery {
        filter: equal("group", json!("same")),
        ..Default::default()
    };
    assert_eq!(
        db.query_planned("c", &query).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn nonunique_planner_rejects_corrupt_index_entries_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "group".into(),
                fields: vec![path("group")],
                unique: false,
                partial: None,
            },
        )
        .unwrap();
        db.insert("c", "a", &json!({"group":"same"})).unwrap();
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
            check_bad_index(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            check_bad_index(&DocumentDatabase::new(&store));
        }
    }
}
