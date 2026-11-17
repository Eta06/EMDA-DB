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

const DOCUMENT_PREFIX: &[u8] = b"\0emda-document\x01";

trait RawKv {
    fn raw_put(&self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn raw_scan(&self) -> io::Result<Vec<database_engine::Entry>>;
}

impl RawKv for Store {
    fn raw_put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.put(key, value)
    }
    fn raw_scan(&self) -> io::Result<Vec<database_engine::Entry>> {
        self.scan(None, None, 1024)
    }
}

impl RawKv for CheckpointStore {
    fn raw_put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.put(key, value)
    }
    fn raw_scan(&self) -> io::Result<Vec<database_engine::Entry>> {
        self.scan(None, None, 1024)
    }
}

fn names_collection(key: &[u8], kind: u8, collection: &str) -> bool {
    let Some(rest) = key.strip_prefix(DOCUMENT_PREFIX) else {
        return false;
    };
    let Some((found, rest)) = rest.split_first() else {
        return false;
    };
    if *found != kind || rest.len() < 2 {
        return false;
    }
    let len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
    rest.get(2..2 + len) == Some(collection.as_bytes())
}

fn index_counts(entries: &[database_engine::Entry], collection: &str) -> (usize, usize) {
    (
        entries
            .iter()
            .filter(|(key, _)| names_collection(key, b'I', collection))
            .count(),
        entries
            .iter()
            .filter(|(key, _)| names_collection(key, b'U', collection))
            .count(),
    )
}

fn prepare_unique_drop(db: &DocumentDatabase<'_>, collection: &str) {
    db.create_collection(collection, CollectionSchema::default())
        .unwrap();
    db.insert(collection, "a", &json!({"n": 1})).unwrap();
    db.insert(collection, "b", &json!({"n": 2})).unwrap();
    db.create_index(collection, definition(true)).unwrap();
    db.start_index_drop(collection, "idx").unwrap();
    assert!(matches!(
        &db.index_jobs(collection).unwrap()[..],
        [IndexJobStatus::Dropping {
            phase: IndexDropPhase::Entries,
            ..
        }]
    ));
}

fn rewrite_drop(
    store: &impl RawKv,
    collection: &str,
    phase: IndexDropPhase,
    after: Option<Vec<u8>>,
) {
    let entries = store.raw_scan().unwrap();
    let (key, value) = entries
        .iter()
        .find(|(key, _)| names_collection(key, b'M', collection))
        .unwrap();
    let mut metadata: serde_json::Value = serde_json::from_slice(value).unwrap();
    let job = &mut metadata["jobs"][0];
    job["phase"] = serde_json::to_value(phase).unwrap();
    job["after"] = match &after {
        Some(cursor) => serde_json::to_value(cursor).unwrap(),
        None => serde_json::Value::Null,
    };
    store
        .raw_put(key, &serde_json::to_vec(&metadata).unwrap())
        .unwrap();
}

fn assert_skip_rejected(db: &DocumentDatabase<'_>, store: &impl RawKv, collection: &str) {
    let before = index_counts(&store.raw_scan().unwrap(), collection);
    assert_eq!(before, (2, 2));
    let error = db.advance_index_drop(collection, "idx", 128).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("skipped existing keys"),
        "{error}"
    );
    assert_eq!(index_counts(&store.raw_scan().unwrap(), collection), before);
    assert!(matches!(
        &db.index_jobs(collection).unwrap()[..],
        [IndexJobStatus::Dropping { name, .. }] if name == "idx"
    ));
    let again = db.advance_index_drop(collection, "idx", 1).unwrap_err();
    assert_eq!(again.kind(), io::ErrorKind::InvalidData);
    assert_eq!(index_counts(&store.raw_scan().unwrap(), collection), before);
}

fn exercise_drop_cursor_guard(db: &DocumentDatabase<'_>, store: &impl RawKv) {
    prepare_unique_drop(db, "ordered");
    complete_drop_named(db, "ordered");
    assert!(db.index_jobs("ordered").unwrap().is_empty());
    assert_eq!(index_counts(&store.raw_scan().unwrap(), "ordered"), (0, 0));
    db.create_index("ordered", definition(true)).unwrap();
    assert_eq!(db.verify_collection("ordered").unwrap().index_keys, 4);

    prepare_unique_drop(db, "past");
    let entries = store.raw_scan().unwrap();
    let mut entry_keys = entries
        .iter()
        .filter(|(key, _)| names_collection(key, b'I', "past"))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    entry_keys.sort();
    let mut past_end = entry_keys.last().unwrap().clone();
    past_end.push(0);
    rewrite_drop(store, "past", IndexDropPhase::Entries, Some(past_end));
    assert_skip_rejected(db, store, "past");
    assert!(matches!(
        &db.index_jobs("past").unwrap()[..],
        [IndexJobStatus::Dropping {
            phase: IndexDropPhase::Entries,
            ..
        }]
    ));

    prepare_unique_drop(db, "live");
    let entries = store.raw_scan().unwrap();
    let live = entries
        .iter()
        .filter(|(key, _)| names_collection(key, b'I', "live"))
        .map(|(key, _)| key.clone())
        .min()
        .unwrap();
    rewrite_drop(store, "live", IndexDropPhase::Entries, Some(live));
    assert_skip_rejected(db, store, "live");
    assert!(matches!(
        &db.index_jobs("live").unwrap()[..],
        [IndexJobStatus::Dropping {
            phase: IndexDropPhase::Entries,
            ..
        }]
    ));

    prepare_unique_drop(db, "phase");
    rewrite_drop(store, "phase", IndexDropPhase::Unique, None);
    assert_skip_rejected(db, store, "phase");
    assert!(matches!(
        &db.index_jobs("phase").unwrap()[..],
        [IndexJobStatus::Dropping {
            phase: IndexDropPhase::Unique,
            ..
        }]
    ));
}

fn complete_drop_named(db: &DocumentDatabase<'_>, collection: &str) {
    for _ in 0..16 {
        if db
            .advance_index_drop(collection, "idx", 1)
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("index drop did not finish");
}

#[test]
fn index_drop_refuses_a_cursor_that_skips_surviving_keys() {
    let root = tempfile::tempdir().unwrap();
    let ordinary_path = root.path().join("ordinary");
    let ordinary = Store::open(&ordinary_path).unwrap();
    exercise_drop_cursor_guard(&DocumentDatabase::new(&ordinary), &ordinary);
    drop(ordinary);
    let reopened = Store::open(&ordinary_path).unwrap();
    let db = DocumentDatabase::new(&reopened);
    assert_skip_rejected(&db, &reopened, "past");
    assert_skip_rejected(&db, &reopened, "live");
    assert_skip_rejected(&db, &reopened, "phase");
    assert_eq!(
        index_counts(&reopened.raw_scan().unwrap(), "ordered"),
        (2, 2)
    );

    let source = root.path().join("checkpoint");
    let checkpoint = root.path().join("base");
    let initial = Store::open(&source).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    exercise_drop_cursor_guard(&DocumentDatabase::with_checkpoint_store(&store), &store);
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&reopened);
    assert_skip_rejected(&db, &reopened, "past");
    assert_skip_rejected(&db, &reopened, "live");
    assert_skip_rejected(&db, &reopened, "phase");
}
