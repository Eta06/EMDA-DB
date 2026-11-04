mod support;
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, FieldRule, FieldType, IndexDefinition,
    Store,
};
use serde_json::json;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use support::retry_lock;

fn schema() -> CollectionSchema {
    CollectionSchema {
        fields: vec![FieldRule {
            path: vec!["email".into()],
            field_type: FieldType::String,
            required: true,
            nullable: false,
        }],
    }
}
fn finish(db: &DocumentDatabase<'_>, limit: usize) {
    for _ in 0..500 {
        if db.advance_schema_migration("c", limit).unwrap().complete {
            return;
        }
    }
    panic!("migration did not finish");
}
fn exercise(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"email":"a"})).unwrap();
    db.insert("c", "b", &json!({"other":true})).unwrap();
    db.start_schema_migration("c", schema()).unwrap();
    assert_eq!(db.schema_migration("c").unwrap().unwrap().after, None);
    assert_eq!(db.advance_schema_migration("c", 1).unwrap().processed, 1);
    db.insert("c", "0", &json!({"email":"zero"})).unwrap();
    assert_eq!(
        db.insert("c", "invalid", &json!({})).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        db.advance_schema_migration("c", 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        db.schema_migration("c").unwrap().unwrap().after.as_deref(),
        Some("a")
    );
    db.replace("c", "b", &json!({"email":"b"}), false).unwrap();
    finish(db, 1);
    assert_eq!(db.schema_migration("c").unwrap(), None);
    assert_eq!(
        db.insert("c", "invalid", &json!({})).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(db.get("c", "0").unwrap().is_some());
}

#[test]
fn incremental_schema_validation_handles_writes_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("ordinary")).unwrap();
    exercise(&DocumentDatabase::new(&store));
    let path = root.path().join("checkpoint");
    let base = root.path().join("base");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&base).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
    exercise(&DocumentDatabase::with_checkpoint_store(&store));
}

#[test]
fn cancellation_keeps_the_active_schema_and_releases_write_constraints() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.start_schema_migration("c", schema()).unwrap();
    assert_eq!(
        db.set_schema("c", CollectionSchema::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        db.insert("c", "a", &json!({})).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    db.cancel_schema_migration("c").unwrap();
    db.insert("c", "a", &json!({})).unwrap();
    assert_eq!(
        db.cancel_schema_migration("c").unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
}

#[test]
fn migration_and_incremental_index_can_progress_together() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"email":"a"})).unwrap();
    db.start_schema_migration("c", schema()).unwrap();
    db.start_index_build(
        "c",
        IndexDefinition {
            name: "email".into(),
            fields: vec![vec!["email".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "0", &json!({"email":"zero"})).unwrap();
    finish(&db, 1);
    for _ in 0..10 {
        if db.advance_index_build("c", "email", 1).unwrap().complete {
            break;
        }
    }
    assert!(db.index_jobs("c").unwrap().is_empty());
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);
}

#[test]
fn schema_job_survives_reopen_and_validates_more_than_256_documents() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    {
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        for n in 0..420 {
            db.insert("c", &format!("{n:04}"), &json!({"email":format!("{n}")}))
                .unwrap();
        }
        assert!(db.set_schema("c", schema()).is_err());
        db.start_schema_migration("c", schema()).unwrap();
        assert_eq!(
            db.advance_schema_migration("c", 100).unwrap().processed,
            100
        );
    }
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(
        db.schema_migration("c").unwrap().unwrap().after.as_deref(),
        Some("0099")
    );
    finish(&db, 100);
    assert_eq!(
        db.insert("c", "invalid", &json!({})).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn read_set_migration_detects_later_phantoms_and_covers_earlier_insertions() {
    for (new_id, conflict) in [("0", false), ("z", true)] {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(root.path()).unwrap());
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"email":"a"})).unwrap();
        db.insert("c", "b", &json!({"email":"b"})).unwrap();
        db.start_schema_migration("c", schema()).unwrap();
        db.advance_schema_migration("c", 1).unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        let other = Arc::clone(&store);
        let handle = thread::spawn(move || {
            DocumentDatabase::new(&other).transaction_with_read_set(|tx| {
                tx.advance_schema_migration("c", 1)?;
                ready_tx.send(()).unwrap();
                go_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            })
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        db.insert("c", new_id, &json!({"email":"new"})).unwrap();
        go_tx.send(()).unwrap();
        let result = handle.join().unwrap();
        if conflict {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        } else {
            result.unwrap();
        }
        finish(&db, 1);
        assert!(db.get("c", new_id).unwrap().is_some());
    }
}

#[test]
fn schema_migration_process_kill_child() {
    let Ok(path) = std::env::var("EMDA_SCHEMA_MIGRATION_CHILD") else {
        return;
    };
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(db.advance_schema_migration("c", 1).unwrap().processed, 1);
    println!("SCHEMA_STEP_ACKNOWLEDGED");
    use std::io::Write;
    std::io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}

#[test]
fn acknowledged_schema_progress_survives_process_termination() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    {
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"email":"a"})).unwrap();
        db.insert("c", "b", &json!({"email":"b"})).unwrap();
        db.start_schema_migration("c", schema()).unwrap();
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "schema_migration_process_kill_child",
            "--nocapture",
        ])
        .env("EMDA_SCHEMA_MIGRATION_CHILD", &path)
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
            if line.contains("SCHEMA_STEP_ACKNOWLEDGED") {
                let _ = done.send(());
                break;
            }
        }
    });
    let result = ready.recv_timeout(Duration::from_secs(10));
    let _ = child.kill();
    let _ = child.wait();
    result.expect("child did not acknowledge schema step");
    let store = retry_lock(|| Store::open(&path)).unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(
        db.schema_migration("c").unwrap().unwrap().after.as_deref(),
        Some("a")
    );
    finish(&db, 1);
    assert_eq!(
        db.insert("c", "invalid", &json!({})).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn incompatible_types_cannot_bypass_either_schema_during_migration() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    let old = CollectionSchema {
        fields: vec![FieldRule {
            path: vec!["n".into()],
            field_type: FieldType::Integer,
            required: true,
            nullable: false,
        }],
    };
    let new = CollectionSchema {
        fields: vec![FieldRule {
            path: vec!["n".into()],
            field_type: FieldType::String,
            required: true,
            nullable: false,
        }],
    };
    db.create_collection("c", old).unwrap();
    db.insert("c", "a", &json!({"n":1})).unwrap();
    db.start_schema_migration("c", new).unwrap();
    assert_eq!(
        db.replace("c", "a", &json!({"n":"one"}), false)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        db.replace("c", "a", &json!({"n":2}), false)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(db.delete("c", "a").unwrap());
    finish(&db, 1);
    db.insert("c", "a", &json!({"n":"one"})).unwrap();
}
