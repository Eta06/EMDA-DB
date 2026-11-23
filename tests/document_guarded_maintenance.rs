use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, FieldRule, FieldType, IndexDefinition,
    IndexQuery, MaintenanceKey, MaintenanceProgress, Store,
};
use serde_json::{Value, json};
use std::{io, path::PathBuf};

const METADATA: &[u8] = b"\0emda-document\x01M\0\x01c";
const RECEIPT: &[u8] = b"guarded-step-receipt";

struct Fixture {
    _root: tempfile::TempDir,
    source: PathBuf,
    checkpoint: Option<(PathBuf, [u8; 32])>,
}

enum Backend {
    Ordinary(Box<Store>),
    Checkpoint(Box<CheckpointStore>),
}

impl Fixture {
    fn new(checkpoint: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = checkpoint.then(|| {
            let store = Store::open(&source).unwrap();
            let base = root.path().join("checkpoint");
            let digest = store.checkpoint_to(&base).unwrap();
            (base, digest)
        });
        Self {
            _root: root,
            source,
            checkpoint,
        }
    }

    fn open(&self) -> Backend {
        if let Some((base, digest)) = &self.checkpoint {
            Backend::Checkpoint(Box::new(
                CheckpointStore::open(&self.source, base, digest).unwrap(),
            ))
        } else {
            Backend::Ordinary(Box::new(Store::open(&self.source).unwrap()))
        }
    }
}

impl Backend {
    fn docs(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }

    fn metadata(&self) -> Value {
        let bytes = match self {
            Self::Ordinary(store) => store.get(METADATA),
            Self::Checkpoint(store) => store.get(METADATA),
        }
        .unwrap()
        .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn replace_metadata(&self, metadata: &Value) {
        let bytes = serde_json::to_vec(metadata).unwrap();
        match self {
            Self::Ordinary(store) => store.put(METADATA, &bytes),
            Self::Checkpoint(store) => store.put(METADATA, &bytes),
        }
        .unwrap();
    }
}

fn index() -> IndexDefinition {
    IndexDefinition {
        name: "score".into(),
        fields: vec![vec!["n".into()]],
        unique: true,
        partial: None,
    }
}

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

fn reject<T>(result: io::Result<T>, kind: io::ErrorKind) {
    assert_eq!(
        result
            .err()
            .expect("guarded change unexpectedly succeeded")
            .kind(),
        kind
    );
}

#[test]
fn independent_progress_tokens_reject_authentic_old_index_and_schema_states() {
    for checkpoint in [false, true] {
        let fixture = Fixture::new(checkpoint);
        let store = fixture.open();
        let db = store.docs();
        let build_key = MaintenanceKey::generate().unwrap();
        let schema_key = MaintenanceKey::generate().unwrap();
        let build_start =
            MaintenanceProgress::initial_index_build("c", &index(), &build_key).unwrap();
        let schema_start =
            MaintenanceProgress::initial_schema_migration("c", &schema(), &schema_key).unwrap();
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"n": 1, "email": "a@example.test"}))
            .unwrap();
        db.insert("c", "b", &json!({"n": 2, "email": "b@example.test"}))
            .unwrap();
        db.start_index_build_guarded("c", index(), &build_key)
            .unwrap();
        db.start_schema_migration_guarded("c", schema(), &schema_key)
            .unwrap();
        let old = store.metadata();
        let advance = |db: &DocumentDatabase<'_>, build, schema| {
            db.transaction(|tx| {
                let (index, next_index) =
                    tx.advance_index_build_guarded_matching("c", "score", 1, &build_key, build)?;
                let (schema, next_schema) =
                    tx.advance_schema_migration_guarded_matching("c", 1, &schema_key, schema)?;
                assert_eq!(index.complete, schema.complete);
                assert_eq!(index.processed, schema.processed);
                Ok((next_index, next_schema))
            })
        };
        let (build_next, schema_next) = advance(&db, build_start, schema_start).unwrap();
        let build_next = MaintenanceProgress::from_bytes(build_next.unwrap().to_bytes());
        let schema_next = MaintenanceProgress::from_bytes(schema_next.unwrap().to_bytes());
        let latest = store.metadata();
        drop(store);

        let store = fixture.open();
        let db = store.docs();
        let confirm_mismatch = |build, schema| {
            let error = db
                .transaction(|tx| {
                    tx.put_raw(RECEIPT, b"uncommitted")?;
                    if build {
                        tx.advance_index_build_guarded_matching(
                            "c", "score", 1, &build_key, schema,
                        )?;
                    } else {
                        tx.advance_schema_migration_guarded_matching("c", 1, &schema_key, schema)?;
                    }
                    Ok(())
                })
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                "document maintenance progress does not match the trusted token"
            );
            assert_eq!(db.transaction(|tx| tx.get_raw(RECEIPT)).unwrap(), None);
        };
        // A stale caller fails against the newer on-disk progress.
        confirm_mismatch(true, build_start);
        confirm_mismatch(false, schema_start);
        reject(
            advance(&db, build_next, schema_start),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(store.metadata(), latest);
        // Replay old metadata including its valid original authentication tags.
        store.replace_metadata(&old);
        confirm_mismatch(true, build_next);
        confirm_mismatch(false, schema_next);
        assert_eq!(store.metadata(), old);
        store.replace_metadata(&latest);
        let (build_end, schema_end) = advance(&db, build_next, schema_next).unwrap();
        assert_eq!(
            advance(&db, build_end.unwrap(), schema_end.unwrap()).unwrap(),
            (None, None)
        );
        assert_eq!(
            db.query_index("c", "score", &IndexQuery::default())
                .unwrap()
                .documents
                .len(),
            2
        );
        assert!(db.schema_migration("c").unwrap().is_none());
    }
}

#[test]
fn guarded_index_rejects_an_altered_frontier_after_reopen_and_resumes_after_cursor_deletion() {
    for checkpoint in [false, true] {
        let fixture = Fixture::new(checkpoint);
        let store = fixture.open();
        let db = store.docs();
        let key = MaintenanceKey::generate().unwrap();
        let saved_key = key.to_bytes();
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        for (id, n) in [("a", 1), ("b", 2), ("z", 3)] {
            db.insert("c", id, &json!({"n": n})).unwrap();
        }
        db.start_index_build_guarded("c", index(), &key).unwrap();
        reject(
            db.advance_index_build("c", "score", 1),
            io::ErrorKind::PermissionDenied,
        );
        assert_eq!(
            db.advance_index_build_guarded("c", "score", 1, &key)
                .unwrap()
                .processed,
            1
        );
        let valid = store.metadata();
        assert_eq!(valid["jobs"][0]["after"], "a");
        let mut corrupt = valid.clone();
        corrupt["jobs"][0]["after"] = json!("zz");
        store.replace_metadata(&corrupt);
        drop(store);

        let store = fixture.open();
        let db = store.docs();
        let key = MaintenanceKey::from_bytes(saved_key);
        reject(
            db.transaction(|tx| {
                tx.put_raw(RECEIPT, b"discard")?;
                tx.advance_index_build_guarded("c", "score", 1, &key)
            }),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(db.transaction(|tx| tx.get_raw(RECEIPT)).unwrap(), None);
        assert_eq!(store.metadata(), corrupt);
        assert!(db.indexes("c").unwrap().is_empty());
        reject(
            db.query_index("c", "score", &IndexQuery::default()),
            io::ErrorKind::NotFound,
        );

        let mut missing = valid.clone();
        missing["jobs"][0].as_object_mut().unwrap().remove("seal");
        store.replace_metadata(&missing);
        reject(
            db.advance_index_build_guarded("c", "score", 1, &key),
            io::ErrorKind::InvalidData,
        );
        let mut definition = valid.clone();
        definition["jobs"][0]["definition"]["unique"] = json!(false);
        store.replace_metadata(&definition);
        reject(
            db.advance_index_build_guarded("c", "score", 1, &key),
            io::ErrorKind::InvalidData,
        );

        store.replace_metadata(&valid);
        let wrong_key = MaintenanceKey::generate().unwrap();
        reject(
            db.advance_index_build_guarded("c", "score", 1, &wrong_key),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(store.metadata(), valid);
        assert!(db.delete("c", "a").unwrap());
        db.insert("c", "0", &json!({"n": 0})).unwrap();
        db.insert("c", "m", &json!({"n": 4})).unwrap();
        for _ in 0..3 {
            let step = db
                .advance_index_build_guarded("c", "score", 1, &key)
                .unwrap();
            assert_eq!(step.processed, 1);
            assert!(!step.complete);
        }
        assert!(
            db.advance_index_build_guarded("c", "score", 1, &key)
                .unwrap()
                .complete
        );
        drop(store);
        let store = fixture.open();
        let db = store.docs();
        let page = db
            .query_index("c", "score", &IndexQuery::default())
            .unwrap();
        assert_eq!(
            page.documents
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["0", "b", "z", "m"]
        );
        assert_eq!(db.verify_collection("c").unwrap().documents, 4);
    }
}

#[test]
fn guarded_schema_rejects_skipped_validation_and_preserves_the_old_schema() {
    for checkpoint in [false, true] {
        let fixture = Fixture::new(checkpoint);
        let store = fixture.open();
        let db = store.docs();
        let key = MaintenanceKey::generate().unwrap();
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"email": "a"})).unwrap();
        db.insert("c", "b", &json!({})).unwrap();
        db.insert("c", "z", &json!({"email": "z"})).unwrap();
        db.start_schema_migration_guarded("c", schema(), &key)
            .unwrap();
        reject(
            db.advance_schema_migration("c", 1),
            io::ErrorKind::PermissionDenied,
        );
        assert_eq!(
            db.advance_schema_migration_guarded("c", 1, &key)
                .unwrap()
                .processed,
            1
        );
        let progress = store.metadata();

        let mut skip = progress.clone();
        skip["schema_job"]["after"] = json!("z");
        store.replace_metadata(&skip);
        drop(store);
        let store = fixture.open();
        let db = store.docs();
        reject(
            db.advance_schema_migration_guarded("c", 1, &key),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(db.get("c", "b").unwrap(), Some(json!({})));
        assert_eq!(store.metadata(), skip);

        store.replace_metadata(&progress);
        assert!(db.advance_schema_migration_guarded("c", 1, &key).is_err());
        assert_eq!(store.metadata(), progress);
        db.delete("c", "a").unwrap();
        db.replace("c", "b", &json!({"email":"b"}), false).unwrap();
        db.insert("c", "0", &json!({"email":"before"})).unwrap();
        let aborted: io::Result<()> = db.transaction(|tx| {
            tx.advance_schema_migration_guarded("c", 1, &key)?;
            Err(io::Error::other("abort guarded progress"))
        });
        assert_eq!(aborted.unwrap_err().to_string(), "abort guarded progress");
        assert_eq!(store.metadata(), progress);

        assert!(
            !db.advance_schema_migration_guarded("c", 2, &key)
                .unwrap()
                .complete
        );
        assert!(
            db.advance_schema_migration_guarded("c", 1, &key)
                .unwrap()
                .complete
        );
        drop(store);
        let store = fixture.open();
        let db = store.docs();
        assert!(db.schema_migration("c").unwrap().is_none());
        assert_eq!(db.get("c", "0").unwrap(), Some(json!({"email":"before"})));
        assert!(db.insert("c", "invalid", &json!({})).is_err());
    }
}

#[test]
fn a_fresh_guarded_job_rejects_a_saved_cursor_from_a_cancelled_job() {
    for checkpoint in [false, true] {
        let fixture = Fixture::new(checkpoint);
        let store = fixture.open();
        let db = store.docs();
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"email":"a"})).unwrap();
        let old_key = MaintenanceKey::generate().unwrap();
        db.start_schema_migration_guarded("c", schema(), &old_key)
            .unwrap();
        db.advance_schema_migration_guarded("c", 1, &old_key)
            .unwrap();
        let previous = store.metadata();
        db.cancel_schema_migration("c").unwrap();

        let new_key = MaintenanceKey::generate().unwrap();
        db.start_schema_migration_guarded("c", schema(), &new_key)
            .unwrap();
        let initial = store.metadata();
        store.replace_metadata(&previous);
        reject(
            db.advance_schema_migration_guarded("c", 1, &new_key),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(store.metadata(), previous);
        store.replace_metadata(&initial);
        assert_eq!(
            db.advance_schema_migration_guarded("c", 1, &new_key)
                .unwrap()
                .processed,
            1
        );
        assert!(
            db.advance_schema_migration_guarded("c", 1, &new_key)
                .unwrap()
                .complete
        );
    }
}

#[test]
fn restarting_an_unguarded_index_conflicts_with_a_staged_old_step_and_reopens() {
    use std::{sync::mpsc, time::Duration};

    for checkpoint in [false, true] {
        let fixture = Fixture::new(checkpoint);
        let store = fixture.open();
        let db = store.docs();
        let key = MaintenanceKey::generate().unwrap();
        let saved_key = key.to_bytes();
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"n": 1})).unwrap();
        db.insert("c", "b", &json!({"n": 2})).unwrap();
        db.start_index_build("c", index()).unwrap();
        assert_eq!(
            db.advance_index_build("c", "score", 1).unwrap().processed,
            1
        );
        let unguarded = store.metadata();
        assert_eq!(unguarded["jobs"][0]["after"], "a");

        let mut wrong = index();
        wrong.unique = false;
        reject(
            db.restart_index_build_guarded("c", &wrong, &key),
            io::ErrorKind::InvalidInput,
        );
        let aborted = db.transaction(|tx| {
            tx.restart_index_build_guarded("c", &index(), &key)?;
            tx.put_raw(RECEIPT, b"discard")?;
            Err::<(), _>(io::Error::other("abort restart"))
        });
        assert_eq!(aborted.unwrap_err().to_string(), "abort restart");
        assert_eq!(store.metadata(), unguarded);

        std::thread::scope(|scope| {
            let (staged, ready) = mpsc::channel();
            let (resume, allowed) = mpsc::channel();
            let db = &db;
            let old_step = scope.spawn(move || {
                db.transaction_with_read_set(|tx| {
                    assert_eq!(tx.advance_index_build("c", "score", 1)?.processed, 1);
                    tx.put_raw(RECEIPT, b"old step")?;
                    staged.send(()).unwrap();
                    allowed.recv_timeout(Duration::from_secs(10)).unwrap();
                    Ok(())
                })
            });
            ready.recv_timeout(Duration::from_secs(10)).unwrap();
            db.restart_index_build_guarded("c", &index(), &key).unwrap();
            resume.send(()).unwrap();
            let error = old_step.join().unwrap().unwrap_err();
            assert!(database_engine::is_transaction_conflict(&error));
        });
        let restarted = store.metadata();
        assert!(restarted["jobs"][0]["after"].is_null());
        assert!(restarted["jobs"][0]["seal"].is_array());
        assert_eq!(db.transaction(|tx| tx.get_raw(RECEIPT)).unwrap(), None);
        drop(store);

        let store = fixture.open();
        let db = store.docs();
        let key = MaintenanceKey::from_bytes(saved_key);
        assert_eq!(store.metadata(), restarted);
        reject(
            db.advance_index_build("c", "score", 1),
            io::ErrorKind::PermissionDenied,
        );
        reject(
            db.restart_index_build_guarded("c", &index(), &key),
            io::ErrorKind::InvalidInput,
        );
        for _ in 0..2 {
            let step = db
                .advance_index_build_guarded("c", "score", 1, &key)
                .unwrap();
            assert_eq!(step.processed, 1);
            assert!(!step.complete);
        }
        assert!(
            db.advance_index_build_guarded("c", "score", 1, &key)
                .unwrap()
                .complete
        );
        assert_eq!(db.verify_collection("c").unwrap().documents, 2);
        reject(
            db.insert("c", "duplicate", &json!({"n": 2})),
            io::ErrorKind::AlreadyExists,
        );
    }
}

#[test]
fn restarting_an_unguarded_migration_revalidates_documents_before_its_old_frontier() {
    for checkpoint in [false, true] {
        let fixture = Fixture::new(checkpoint);
        let store = fixture.open();
        let db = store.docs();
        let key = MaintenanceKey::generate().unwrap();
        let saved_key = key.to_bytes();
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"email": "a"})).unwrap();
        db.insert("c", "b", &json!({})).unwrap();
        db.start_schema_migration("c", schema()).unwrap();
        assert_eq!(db.advance_schema_migration("c", 1).unwrap().processed, 1);
        let mut unguarded = store.metadata();
        unguarded["schema_job"]["after"] = json!("z");
        store.replace_metadata(&unguarded);
        reject(
            db.restart_schema_migration_guarded("c", &CollectionSchema::default(), &key),
            io::ErrorKind::InvalidInput,
        );
        assert_eq!(store.metadata(), unguarded);
        db.restart_schema_migration_guarded("c", &schema(), &key)
            .unwrap();
        drop(store);

        let store = fixture.open();
        let db = store.docs();
        let key = MaintenanceKey::from_bytes(saved_key);
        assert_eq!(db.schema_migration("c").unwrap().unwrap().after, None);
        reject(
            db.advance_schema_migration("c", 1),
            io::ErrorKind::PermissionDenied,
        );
        reject(
            db.restart_schema_migration_guarded("c", &schema(), &key),
            io::ErrorKind::InvalidInput,
        );
        assert_eq!(
            db.advance_schema_migration_guarded("c", 1, &key)
                .unwrap()
                .processed,
            1
        );
        let progress = store.metadata();
        assert!(db.advance_schema_migration_guarded("c", 1, &key).is_err());
        assert_eq!(store.metadata(), progress);
        assert_eq!(db.get("c", "b").unwrap(), Some(json!({})));
        db.replace("c", "b", &json!({"email": "b"}), false).unwrap();
        assert_eq!(
            db.advance_schema_migration_guarded("c", 1, &key)
                .unwrap()
                .processed,
            1
        );
        assert!(
            db.advance_schema_migration_guarded("c", 1, &key)
                .unwrap()
                .complete
        );
        drop(store);

        let store = fixture.open();
        let db = store.docs();
        assert!(db.schema_migration("c").unwrap().is_none());
        assert!(db.insert("c", "invalid", &json!({})).is_err());
        assert_eq!(db.get("c", "b").unwrap(), Some(json!({"email": "b"})));
    }
}
