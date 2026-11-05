mod support;
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, IndexDefinition, Store,
};
#[cfg(target_os = "linux")]
use database_engine::{OnlineCompactionOptions, OnlineStore};
use serde_json::json;
use std::{
    io,
    sync::{Barrier, mpsc},
    thread,
    time::{Duration, Instant},
};
use support::retry_lock;

fn index() -> IndexDefinition {
    IndexDefinition {
        name: "unique".into(),
        fields: vec![vec!["n".into()]],
        unique: true,
        partial: None,
    }
}

fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    db.create_collection("neighbor", CollectionSchema::default())
        .unwrap();
    db.insert("neighbor", "id", &json!({"safe":true})).unwrap();
    for start in (0..270).step_by(30) {
        db.transaction(|tx| {
            for n in start..start + 30 {
                tx.insert("c", &n.to_string(), &json!({"n":n}))?;
            }
            Ok(())
        })
        .unwrap();
    }
    assert!(!db.collection_drop_in_progress("c").unwrap());
    assert!(db.drop_collection("c").is_err()); // 810 document/index keys exceed the atomic cap.
    assert_eq!(db.get("c", "0").unwrap(), Some(json!({"n":0})));
}

fn start_and_check(db: &DocumentDatabase<'_>) {
    db.start_collection_drop("c").unwrap();
    assert!(db.collection_drop_in_progress("c").unwrap());
    assert_eq!(
        db.start_collection_drop("c").unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        db.get("c", "0").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        db.insert("c", "new", &json!({"n":900})).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        db.indexes("c").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        db.drop_collection("c").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        db.advance_collection_drop("c", 0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        db.advance_collection_drop("c", 129).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let cancelled = db.transaction::<()>(|tx| {
        assert_eq!(tx.advance_collection_drop("c", 3)?.removed, 3);
        tx.put_raw(b"atomic", b"must-not-commit")?;
        Err(io::Error::other("abort"))
    });
    assert!(cancelled.is_err());
    assert_eq!(db.transaction(|tx| tx.get_raw(b"atomic")).unwrap(), None);
    assert_eq!(db.advance_collection_drop("c", 3).unwrap().removed, 3);
    assert_eq!(
        db.get("neighbor", "id").unwrap(),
        Some(json!({"safe":true}))
    );
}

fn finish_and_check(db: &DocumentDatabase<'_>) {
    let mut removed = 3;
    loop {
        let step = db.advance_collection_drop("c", 37).unwrap();
        removed += step.removed;
        if step.complete {
            assert_eq!(step.removed, 0);
            break;
        }
        assert!((1..=37).contains(&step.removed));
    }
    assert_eq!(removed, 810);
    assert_eq!(
        db.get("c", "0").unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        db.collection_drop_in_progress("c").unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    db.insert("c", "again", &json!({"n":0})).unwrap();
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
    assert_eq!(
        db.get("neighbor", "id").unwrap(),
        Some(json!({"safe":true}))
    );
}

#[test]
fn ordinary_drop_resumes_across_reopen_and_does_not_touch_neighbors() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    prepare(&db);
    start_and_check(&db);
    drop(store);
    let store = retry_lock(|| Store::open(root.path())).unwrap();
    let db = DocumentDatabase::new(&store);
    assert!(db.collection_drop_in_progress("c").unwrap());
    finish_and_check(&db);
}

#[test]
fn checkpoint_drop_resumes_across_reopen_and_full_recovery() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    prepare(&DocumentDatabase::new(&store));
    let digest = store.checkpoint_to(&base).unwrap();
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
    start_and_check(&DocumentDatabase::with_checkpoint_store(&store));
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
    finish_and_check(&DocumentDatabase::with_checkpoint_store(&store));
    drop(store);
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(db.get("c", "again").unwrap(), Some(json!({"n":0})));
    assert_eq!(
        db.get("neighbor", "id").unwrap(),
        Some(json!({"safe":true}))
    );
}

#[test]
fn ordinary_drop_can_continue_from_a_new_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    prepare(&db);
    start_and_check(&db);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    finish_and_check(&DocumentDatabase::with_checkpoint_store(&store));
    drop(store);
    let store = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        DocumentDatabase::new(&store).get("c", "again").unwrap(),
        Some(json!({"n":0}))
    );
}

#[test]
fn backup_restore_resumes_the_captured_drop_even_if_source_progresses() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source");
        let backup = root.path().join("backup");
        let restored = root.path().join("restored");
        let initial = Store::open(&path).unwrap();
        prepare(&DocumentDatabase::new(&initial));
        if checkpoint_mode {
            let checkpoint = root.path().join("checkpoint");
            let digest = initial.checkpoint_to(&checkpoint).unwrap();
            drop(initial);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            start_and_check(&db);
            store.backup(&backup).unwrap();
            assert_eq!(db.advance_collection_drop("c", 37).unwrap().removed, 37);
        } else {
            let db = DocumentDatabase::new(&initial);
            start_and_check(&db);
            initial.backup(&backup).unwrap();
            assert_eq!(db.advance_collection_drop("c", 37).unwrap().removed, 37);
        }
        Store::restore(&backup, &restored).unwrap();
        let store = retry_lock(|| Store::open(&restored)).unwrap();
        let db = DocumentDatabase::new(&store);
        assert!(db.collection_drop_in_progress("c").unwrap());
        finish_and_check(&db);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn compaction_handoffs_preserve_an_in_progress_drop() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("ordinary");
    let previous = root.path().join("ordinary-previous");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    prepare(&db);
    start_and_check(&db);
    let store = store.compact_and_replace(&path, &previous).unwrap();
    finish_and_check(&DocumentDatabase::new(&store));

    let path = root.path().join("checkpoint-store");
    let checkpoint = root.path().join("base");
    let previous = root.path().join("checkpoint-previous");
    let initial = Store::open(&path).unwrap();
    prepare(&DocumentDatabase::new(&initial));
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    start_and_check(&DocumentDatabase::with_checkpoint_store(&store));
    let (store, _) = store.compact_and_replace(&previous).unwrap();
    finish_and_check(&DocumentDatabase::with_checkpoint_store(&store));
}

#[cfg(target_os = "linux")]
#[test]
fn online_handoff_preserves_a_checkpoint_origin_drop_cursor() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let previous = root.path().join("previous");
    let initial = Store::open(&path).unwrap();
    prepare(&DocumentDatabase::new(&initial));
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    start_and_check(&DocumentDatabase::with_checkpoint_store(&store));
    drop(store);
    let online = retry_lock(|| OnlineStore::open(&path)).unwrap();
    online
        .compact_and_replace(&previous, &OnlineCompactionOptions::default())
        .unwrap();
    online
        .with_store(|store| {
            finish_and_check(&DocumentDatabase::new(store));
            Ok(())
        })
        .unwrap();
}

fn race_old_transaction(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "old", &json!({})).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    thread::scope(|scope| {
        let writer = scope.spawn(move || {
            db.transaction_with_read_set(|tx| {
                assert!(tx.get("c", "old")?.is_some());
                ready_tx.send(()).unwrap();
                go_rx.recv().unwrap();
                tx.insert("c", "late", &json!({}))
            })
        });
        ready_rx.recv().unwrap();
        db.start_collection_drop("c").unwrap();
        go_tx.send(()).unwrap();
        assert!(writer.join().unwrap().is_err());
    });
    let mut removed = 0;
    loop {
        let step = db.advance_collection_drop("c", 2).unwrap();
        removed += step.removed;
        if step.complete {
            break;
        }
    }
    assert_eq!(removed, 1);
}

fn concurrent_drop_workers(db: &DocumentDatabase<'_>) {
    prepare(db);
    start_and_check(db);
    let barrier = Barrier::new(4);
    let results = thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut removed = 0;
                    barrier.wait();
                    let deadline = Instant::now() + Duration::from_secs(30);
                    loop {
                        assert!(
                            Instant::now() < deadline,
                            "workers did not complete the collection drop"
                        );
                        match db.advance_collection_drop("c", 7) {
                            Ok(step) => {
                                removed += step.removed;
                                if step.complete {
                                    return (removed, true);
                                }
                            }
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                thread::yield_now()
                            }
                            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                                return (removed, false);
                            }
                            Err(error) => panic!("unexpected collection-drop error: {error}"),
                        }
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().map(|result| result.0).sum::<usize>(), 807);
    assert_eq!(results.iter().filter(|result| result.1).count(), 1);
    assert_eq!(
        db.get("c", "0").unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        db.get("neighbor", "id").unwrap(),
        Some(json!({"safe":true}))
    );
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    db.insert("c", "new", &json!({"n":0})).unwrap();
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
}

#[test]
fn concurrent_drop_workers_commit_each_key_once_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("ordinary");
    let store = Store::open(&path).unwrap();
    concurrent_drop_workers(&DocumentDatabase::new(&store));
    drop(store);
    let reopened = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        DocumentDatabase::new(&reopened).get("c", "new").unwrap(),
        Some(json!({"n":0}))
    );
    let path = root.path().join("checkpoint-store");
    let base = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&base).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
    concurrent_drop_workers(&DocumentDatabase::with_checkpoint_store(&store));
    drop(store);
    let reopened = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        DocumentDatabase::new(&reopened).get("c", "new").unwrap(),
        Some(json!({"n":0}))
    );
}

#[test]
fn deletion_marker_conflicts_with_inflight_read_set_writers_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let ordinary = Store::open(root.path().join("ordinary")).unwrap();
    race_old_transaction(&DocumentDatabase::new(&ordinary));
    let path = root.path().join("checkpoint-store");
    let base = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&base).unwrap();
    drop(initial);
    let checkpoint = retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
    race_old_transaction(&DocumentDatabase::with_checkpoint_store(&checkpoint));
}

#[test]
fn pending_schema_and_index_jobs_must_finish_before_deletion() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.start_schema_migration("c", CollectionSchema::default())
        .unwrap();
    assert_eq!(
        db.start_collection_drop("c").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    db.cancel_schema_migration("c").unwrap();
    db.start_index_build("c", index()).unwrap();
    assert_eq!(
        db.start_collection_drop("c").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    db.advance_index_build("c", "unique", 10).unwrap();
    db.start_collection_drop("c").unwrap();
}
