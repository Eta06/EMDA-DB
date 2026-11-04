mod support;
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, IndexDefinition, IndexDropPhase,
    IndexJobStatus, IndexQuery, IndexValue, PartialFilter, Store,
};
use serde_json::json;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use support::retry_lock;

fn definition(unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: "idx".into(),
        fields: vec![vec!["n".into()]],
        unique,
        partial: None,
    }
}
fn complete_build(db: &DocumentDatabase<'_>, limit: usize) {
    for _ in 0..200 {
        if db.advance_index_build("c", "idx", limit).unwrap().complete {
            return;
        }
    }
    panic!("index build did not finish");
}
fn complete_drop(db: &DocumentDatabase<'_>, limit: usize) {
    for _ in 0..200 {
        if db.advance_index_drop("c", "idx", limit).unwrap().complete {
            return;
        }
    }
    panic!("index drop did not finish");
}
fn build_with_interleaved_writes(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    for (id, n) in [("a", 1), ("b", 2), ("c", 3)] {
        db.insert("c", id, &json!({"n":n})).unwrap();
    }
    db.start_index_build("c", definition(true)).unwrap();
    assert!(matches!(
        &db.index_jobs("c").unwrap()[0],
        IndexJobStatus::Building { after: None, .. }
    ));
    assert_eq!(
        db.query_index("c", "idx", &IndexQuery::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    let first = db.advance_index_build("c", "idx", 1).unwrap();
    assert_eq!((first.processed, first.complete), (1, false));
    db.replace("c", "a", &json!({"n":4}), false).unwrap();
    db.insert("c", "0", &json!({"n":5})).unwrap();
    assert!(db.delete("c", "b").unwrap());
    db.replace("c", "c", &json!({"n":6}), false).unwrap();
    complete_build(db, 1);
    assert!(db.index_jobs("c").unwrap().is_empty());
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 6);
    let page = db.query_index("c", "idx", &IndexQuery::default()).unwrap();
    assert_eq!(
        page.documents
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "0", "c"]
    );
    assert_eq!(
        db.insert("c", "duplicate", &json!({"n":5}))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
}

#[test]
fn incremental_build_tracks_interleaved_changes_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let ordinary = Store::open(root.path().join("ordinary")).unwrap();
    build_with_interleaved_writes(&DocumentDatabase::new(&ordinary));
    let path = root.path().join("checkpoint");
    let checkpoint = root.path().join("base");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    build_with_interleaved_writes(&DocumentDatabase::with_checkpoint_store(&store));
}

#[test]
fn duplicate_build_can_be_repaired_or_cancelled_without_exposing_partial_index() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"n":1})).unwrap();
    db.insert("c", "b", &json!({"n":1})).unwrap();
    db.start_index_build("c", definition(true)).unwrap();
    db.advance_index_build("c", "idx", 1).unwrap();
    assert_eq!(
        db.advance_index_build("c", "idx", 1).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(
        matches!(&db.index_jobs("c").unwrap()[0],IndexJobStatus::Building{after:Some(id),..} if id=="a")
    );
    assert!(db.delete("c", "b").unwrap());
    complete_build(&db, 1);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
    db.start_index_drop("c", "idx").unwrap();
    assert_eq!(
        db.query_index("c", "idx", &IndexQuery::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert!(matches!(
        &db.index_jobs("c").unwrap()[0],
        IndexJobStatus::Dropping {
            phase: IndexDropPhase::Entries,
            ..
        }
    ));
    assert_eq!(
        db.start_index_build("c", definition(true))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    complete_drop(&db, 1);
    assert!(db.index_jobs("c").unwrap().is_empty());
    db.start_index_build("c", definition(true)).unwrap();
    db.advance_index_build("c", "idx", 1).unwrap();
    db.cancel_index_build("c", "idx").unwrap();
    complete_drop(&db, 1);
    db.create_index("c", definition(true)).unwrap();
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
}

#[test]
fn pending_partial_index_tracks_changes_and_unsupported_old_values_can_be_fixed() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"n":[1],"active":true}))
        .unwrap();
    db.insert("c", "b", &json!({"n":2,"active":false})).unwrap();
    let mut index = definition(false);
    index.partial = Some(PartialFilter::Equals {
        path: vec!["active".into()],
        value: IndexValue::Boolean(true),
    });
    db.start_index_build("c", index).unwrap();
    assert_eq!(
        db.advance_index_build("c", "idx", 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    db.replace("c", "a", &json!({"n":1,"active":true}), false)
        .unwrap();
    db.replace("c", "b", &json!({"n":2,"active":true}), false)
        .unwrap();
    complete_build(&db, 1);
    assert_eq!(
        db.query_index("c", "idx", &IndexQuery::default())
            .unwrap()
            .documents
            .len(),
        2
    );
    db.replace("c", "a", &json!({"n":1,"active":false}), false)
        .unwrap();
    assert_eq!(
        db.query_index("c", "idx", &IndexQuery::default())
            .unwrap()
            .documents
            .len(),
        1
    );
}

#[test]
fn build_and_drop_resume_after_reopening_and_support_large_collections() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    {
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        for n in 0..420 {
            db.insert("c", &format!("{n:04}"), &json!({"n":n})).unwrap();
        }
        assert!(db.create_index("c", definition(true)).is_err());
        db.start_index_build("c", definition(true)).unwrap();
        assert_eq!(
            db.advance_index_build("c", "idx", 100).unwrap().processed,
            100
        );
    }
    {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        let db = DocumentDatabase::new(&store);
        assert!(
            matches!(&db.index_jobs("c").unwrap()[0],IndexJobStatus::Building{after:Some(id),..} if id=="0099")
        );
        complete_build(&db, 100);
        db.start_index_drop("c", "idx").unwrap();
        assert_eq!(
            db.advance_index_drop("c", "idx", 100).unwrap().processed,
            100
        );
        db.insert("c", "duplicate", &json!({"n":5})).unwrap();
    }
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert!(matches!(
        &db.index_jobs("c").unwrap()[0],
        IndexJobStatus::Dropping { .. }
    ));
    complete_drop(&db, 100);
    assert!(db.index_jobs("c").unwrap().is_empty());
    assert!(db.indexes("c").unwrap().is_empty());
}

#[test]
fn concurrent_write_conflicts_with_inflight_build_step_and_retry_sees_new_row() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"n":1})).unwrap();
    db.start_index_build("c", definition(true)).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let other = Arc::clone(&store);
    let handle = thread::spawn(move || {
        DocumentDatabase::new(&other).transaction(|tx| {
            tx.advance_index_build("c", "idx", 1)?;
            ready_tx.send(()).unwrap();
            go_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            Ok(())
        })
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    db.insert("c", "before", &json!({"n":2})).unwrap();
    go_tx.send(()).unwrap();
    assert_eq!(
        handle.join().unwrap().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(matches!(
        &db.index_jobs("c").unwrap()[0],
        IndexJobStatus::Building { after: None, .. }
    ));
    complete_build(&db, 1);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);
}

#[test]
fn stale_document_transaction_cannot_skip_a_new_build() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let other = Arc::clone(&store);
    let writer = thread::spawn(move || {
        DocumentDatabase::new(&other).transaction(|tx| {
            tx.insert("c", "a", &json!({"n":1}))?;
            ready_tx.send(()).unwrap();
            go_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            Ok(())
        })
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    db.start_index_build("c", definition(true)).unwrap();
    go_tx.send(()).unwrap();
    assert_eq!(
        writer.join().unwrap().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    db.insert("c", "a", &json!({"n":1})).unwrap();
    complete_build(&db, 1);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
}

#[test]
fn metadata_version_marks_jobs_and_invalid_legacy_job_metadata_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    let metadata_key = store
        .scan(None, None, 100)
        .unwrap()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01M"))
        .unwrap()
        .0;
    let before: serde_json::Value =
        serde_json::from_slice(&store.get(&metadata_key).unwrap().unwrap()).unwrap();
    assert_eq!(before["version"], 1);
    assert!(before.get("jobs").is_none());
    db.start_index_build("c", definition(false)).unwrap();
    let mut during: serde_json::Value =
        serde_json::from_slice(&store.get(&metadata_key).unwrap().unwrap()).unwrap();
    assert_eq!(during["version"], 2);
    assert!(during.get("jobs").is_some());
    during["version"] = json!(1);
    store
        .put(&metadata_key, &serde_json::to_vec(&during).unwrap())
        .unwrap();
    assert_eq!(
        db.index_jobs("c").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn online_index_process_kill_child() {
    let Ok(path) = std::env::var("EMDA_ONLINE_INDEX_CHILD") else {
        return;
    };
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(db.advance_index_build("c", "idx", 1).unwrap().processed, 1);
    println!("INDEX_STEP_ACKNOWLEDGED");
    use std::io::Write;
    std::io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}

#[test]
fn acknowledged_build_progress_survives_process_termination() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    {
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"n":1})).unwrap();
        db.insert("c", "b", &json!({"n":2})).unwrap();
        db.start_index_build("c", definition(true)).unwrap();
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "online_index_process_kill_child", "--nocapture"])
        .env("EMDA_ONLINE_INDEX_CHILD", &path)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (done, ready) = mpsc::channel();
    thread::spawn(move || {
        for line in std::io::BufReader::new(output)
            .lines()
            .map_while(Result::ok)
        {
            if line.contains("INDEX_STEP_ACKNOWLEDGED") {
                let _ = done.send(());
                break;
            }
        }
    });
    let result = ready.recv_timeout(Duration::from_secs(10));
    let _ = child.kill();
    let _ = child.wait();
    result.expect("child did not acknowledge step");
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert!(
        matches!(&db.index_jobs("c").unwrap()[0],IndexJobStatus::Building{after:Some(id),..} if id=="a")
    );
    complete_build(&db, 1);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);
}

#[test]
fn online_drop_process_kill_child() {
    let Ok(path) = std::env::var("EMDA_ONLINE_DROP_CHILD") else {
        return;
    };
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(db.advance_index_drop("c", "idx", 1).unwrap().processed, 1);
    println!("INDEX_DROP_STEP_ACKNOWLEDGED");
    use std::io::Write;
    std::io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}

#[test]
fn acknowledged_drop_progress_survives_process_termination() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    {
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"n":1})).unwrap();
        db.insert("c", "b", &json!({"n":2})).unwrap();
        db.create_index("c", definition(true)).unwrap();
        db.start_index_drop("c", "idx").unwrap();
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "online_drop_process_kill_child", "--nocapture"])
        .env("EMDA_ONLINE_DROP_CHILD", &path)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (done, ready) = mpsc::channel();
    thread::spawn(move || {
        for line in std::io::BufReader::new(output)
            .lines()
            .map_while(Result::ok)
        {
            if line.contains("INDEX_DROP_STEP_ACKNOWLEDGED") {
                let _ = done.send(());
                break;
            }
        }
    });
    let result = ready.recv_timeout(Duration::from_secs(10));
    let _ = child.kill();
    let _ = child.wait();
    result.expect("child did not acknowledge drop step");
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert!(matches!(
        &db.index_jobs("c").unwrap()[0],
        IndexJobStatus::Dropping {
            phase: IndexDropPhase::Entries,
            ..
        }
    ));
    complete_drop(&db, 1);
    db.create_index("c", definition(true)).unwrap();
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);
}

#[test]
fn read_set_build_steps_handle_writes_on_both_sides_of_the_cursor() {
    for (new_id, conflict) in [("0", false), ("z", true)] {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(root.path()).unwrap());
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"n":1})).unwrap();
        db.insert("c", "b", &json!({"n":2})).unwrap();
        db.start_index_build("c", definition(true)).unwrap();
        db.advance_index_build("c", "idx", 1).unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        let other = Arc::clone(&store);
        let handle = thread::spawn(move || {
            DocumentDatabase::new(&other).transaction_with_read_set(|tx| {
                tx.advance_index_build("c", "idx", 1)?;
                ready_tx.send(()).unwrap();
                go_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            })
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        db.insert("c", new_id, &json!({"n":3})).unwrap();
        go_tx.send(()).unwrap();
        let result = handle.join().unwrap();
        if conflict {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        } else {
            result.unwrap();
        }
        complete_build(&db, 1);
        assert_eq!(db.verify_collection("c").unwrap().index_keys, 6);
    }
}
