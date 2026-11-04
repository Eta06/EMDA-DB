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
        let store = Store::open(path).unwrap();
        write(&DocumentDatabase::new(&store), Path::new(&signal));
    } else {
        let checkpoint = env::var_os("EMDA_DOCUMENT_CHECKPOINT").unwrap();
        let digest: [u8; 32] = fs::read(env::var_os("EMDA_DOCUMENT_DIGEST").unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let store = CheckpointStore::open(path, checkpoint, &digest).unwrap();
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
        let ordinary = Store::open(&path).unwrap();
        let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
        fs::write(&digest_file, digest).unwrap();
        if mode == "ordinary" {
            setup(&DocumentDatabase::new(&ordinary));
            drop(ordinary);
        } else {
            drop(ordinary);
            let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
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
            let store = Store::open(&path).unwrap();
            verify(&DocumentDatabase::new(&store));
        } else {
            let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
            verify(&DocumentDatabase::with_checkpoint_store(&store));
        }
    }
}
