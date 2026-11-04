use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue,
    Store,
};
use serde_json::json;
use std::{
    env, fs,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

mod support;
use support::retry_lock;

fn setup(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "n".into(),
            fields: vec![vec!["n".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
}
fn write(db: &DocumentDatabase<'_>, signal: &Path) -> ! {
    db.transaction(|tx| {
        tx.insert("c", "acked", &json!({"n":7}))?;
        tx.put_raw(b"receipt", b"acked")
    })
    .unwrap();
    fs::write(signal, b"committed").unwrap();
    loop {
        thread::park();
    }
}
fn verify(db: &DocumentDatabase<'_>) {
    assert_eq!(db.get("c", "acked").unwrap(), Some(json!({"n":7})));
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
        Some(b"acked".to_vec())
    );
    let rows = db
        .query_index(
            "c",
            "n",
            &IndexQuery {
                equal: vec![IndexValue::Integer(7)],
                ..Default::default()
            },
        )
        .unwrap()
        .documents;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "acked");
}

#[test]
fn document_recovery_child() {
    let Ok(mode) = env::var("EMDA_DOCUMENT_CHILD") else {
        return;
    };
    let path = env::var_os("EMDA_DOCUMENT_PATH").unwrap();
    let signal = env::var_os("EMDA_DOCUMENT_SIGNAL").unwrap();
    if mode == "ordinary" {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        write(&DocumentDatabase::new(&store), Path::new(&signal));
    } else {
        let checkpoint = env::var_os("EMDA_DOCUMENT_CHECKPOINT").unwrap();
        let digest: [u8; 32] = fs::read(env::var_os("EMDA_DOCUMENT_DIGEST").unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        write(
            &DocumentDatabase::with_checkpoint_store(&store),
            Path::new(&signal),
        );
    }
}

#[test]
fn acknowledged_document_and_index_survive_process_termination() {
    for mode in ["ordinary", "checkpoint"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let digest_file = root.path().join("digest");
        let signal = root.path().join("signal");
        let ordinary = retry_lock(|| Store::open(&path)).unwrap();
        let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
        fs::write(&digest_file, digest).unwrap();
        if mode == "ordinary" {
            setup(&DocumentDatabase::new(&ordinary));
            drop(ordinary);
        } else {
            drop(ordinary);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            setup(&DocumentDatabase::with_checkpoint_store(&store));
        }
        let mut child = Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("document_recovery_child")
            .env("EMDA_DOCUMENT_CHILD", mode)
            .env("EMDA_DOCUMENT_PATH", &path)
            .env("EMDA_DOCUMENT_SIGNAL", &signal)
            .env("EMDA_DOCUMENT_CHECKPOINT", &checkpoint)
            .env("EMDA_DOCUMENT_DIGEST", &digest_file)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !signal.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("document child exited before acknowledgement: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("document child did not acknowledge");
            }
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        if mode == "ordinary" {
            let store = retry_lock(|| Store::open(&path)).unwrap();
            verify(&DocumentDatabase::new(&store));
        } else {
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            verify(&DocumentDatabase::with_checkpoint_store(&store));
        }
    }
}

#[test]
fn document_indexes_survive_managed_checkpoint_compaction_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("live");
    let checkpoint = root.path().join("base");
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = retry_lock(|| {
        CheckpointStore::open_with_resource_limits(
            &path,
            &checkpoint,
            &digest,
            4096,
            16,
            4096,
            None,
        )
    })
    .unwrap();
    store.rollover_managed(root.path().join("managed")).unwrap();
    store.enable_automatic_checkpoints().unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    setup(&db);
    db.transaction(|tx| {
        tx.insert("c", "acked", &json!({"n":7}))?;
        tx.put_raw(b"receipt", b"acked")
    })
    .unwrap();
    let (store, _) = store
        .compact_and_replace(root.path().join("previous"))
        .unwrap();
    verify(&DocumentDatabase::with_checkpoint_store(&store));
    assert!(store.automatic_checkpoints_enabled().unwrap());
    drop(store);
    let store = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(&path, 4096, 16, 4096, None)
    })
    .unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    verify(&db);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
}

#[test]
fn document_backup_restore_captures_one_consistent_version_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("live");
        let backup = root.path().join("backup");
        let restored = root.path().join("restored");
        let ordinary = retry_lock(|| Store::open(&path)).unwrap();
        if checkpoint_mode {
            let checkpoint = root.path().join("base");
            let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
            drop(ordinary);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            setup(&db);
            db.transaction(|tx| {
                tx.insert("c", "acked", &json!({"n":7}))?;
                tx.put_raw(b"receipt", b"acked")
            })
            .unwrap();
            store.backup(&backup).unwrap();
            db.replace("c", "acked", &json!({"n":8}), false).unwrap();
        } else {
            let db = DocumentDatabase::new(&ordinary);
            setup(&db);
            db.transaction(|tx| {
                tx.insert("c", "acked", &json!({"n":7}))?;
                tx.put_raw(b"receipt", b"acked")
            })
            .unwrap();
            ordinary.backup(&backup).unwrap();
            db.replace("c", "acked", &json!({"n":8}), false).unwrap();
        }
        Store::restore(&backup, &restored).unwrap();
        let restored = retry_lock(|| Store::open(&restored)).unwrap();
        let db = DocumentDatabase::new(&restored);
        verify(&db);
        assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
    }
}

#[test]
fn ordinary_compaction_preserves_document_and_unique_index_invariants() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("live");
    let previous = root.path().join("previous");
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    setup(&db);
    db.transaction(|tx| {
        tx.insert("c", "acked", &json!({"n":7}))?;
        tx.put_raw(b"receipt", b"acked")
    })
    .unwrap();
    let store = store.compact_and_replace(&path, &previous).unwrap();
    let db = DocumentDatabase::new(&store);
    verify(&db);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
    let old = retry_lock(|| Store::open(&previous)).unwrap();
    verify(&DocumentDatabase::new(&old));
}
