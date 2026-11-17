mod support;

use database_engine::{
    CheckpointStore, CollectionIntegrity, CollectionSchema, DocumentDatabase,
    DocumentTransactionMode, FieldRule, FieldType, IndexDefinition, IndexJobStatus, IndexQuery,
    Store, is_transaction_conflict,
};
use serde_json::{Value, json};
use std::io;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use support::retry_lock;

const SCHEMA_WITNESS: &[u8] = b"schema-publish-witness";
const INDEX_WITNESS: &[u8] = b"index-publish-witness";
const AFTER_CURSOR_ID: &str = "zz";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommittedPublication {
    Schema,
    Index,
}

fn pending_schema() -> CollectionSchema {
    CollectionSchema {
        fields: vec![FieldRule {
            path: vec!["email".into()],
            field_type: FieldType::String,
            required: true,
            nullable: false,
        }],
    }
}

fn index_definition() -> IndexDefinition {
    IndexDefinition {
        name: "idx".into(),
        fields: vec![vec!["n".into()]],
        unique: true,
        partial: None,
    }
}

fn document(email: &str, n: i64) -> Value {
    json!({"email": email, "n": n})
}

fn raw(db: &DocumentDatabase<'_>, key: &[u8]) -> Option<Vec<u8>> {
    db.transaction(|transaction| transaction.get_raw(key))
        .unwrap()
}

fn indexed_rows(db: &DocumentDatabase<'_>) -> Vec<(String, Value)> {
    db.query_index("c", "idx", &IndexQuery::default())
        .unwrap()
        .documents
        .into_iter()
        .map(|row| (row.id, row.document))
        .collect()
}

fn assert_original_documents(db: &DocumentDatabase<'_>) {
    for (id, n) in [("a", 1), ("m", 2), ("z", 3)] {
        assert_eq!(db.get("c", id).unwrap(), Some(document(id, n)));
    }
}

fn assert_pending(db: &DocumentDatabase<'_>) {
    let migration = db.schema_migration("c").unwrap().unwrap();
    assert_eq!(migration.after.as_deref(), Some("z"));
    assert_eq!(migration.schema, pending_schema());
    assert!(matches!(
        &db.index_jobs("c").unwrap()[..],
        [IndexJobStatus::Building {
            after: Some(id),
            ..
        }] if id == "z"
    ));
    assert!(db.indexes("c").unwrap().is_empty());
    assert_eq!(
        db.query_index("c", "idx", &IndexQuery::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert_original_documents(db);
}

fn assert_conflict_outcome(db: &DocumentDatabase<'_>, committed: CommittedPublication) {
    assert_eq!(db.get("c", "0").unwrap(), Some(document("zero", 4)));
    assert_original_documents(db);
    match committed {
        CommittedPublication::Schema => {
            assert_eq!(raw(db, SCHEMA_WITNESS).as_deref(), Some(b"yes".as_slice()));
            assert_eq!(raw(db, INDEX_WITNESS), None);
            assert_eq!(db.schema_migration("c").unwrap(), None);
            assert!(matches!(
                &db.index_jobs("c").unwrap()[..],
                [IndexJobStatus::Building {
                    after: Some(id),
                    ..
                }] if id == "z"
            ));
            assert!(db.indexes("c").unwrap().is_empty());
            assert_eq!(
                db.query_index("c", "idx", &IndexQuery::default())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::NotFound
            );
            assert_eq!(
                db.insert("c", "missing-email", &json!({"n": 8}))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        CommittedPublication::Index => {
            assert_eq!(raw(db, SCHEMA_WITNESS), None);
            assert_eq!(raw(db, INDEX_WITNESS).as_deref(), Some(b"yes".as_slice()));
            let migration = db.schema_migration("c").unwrap().unwrap();
            assert_eq!(migration.after.as_deref(), Some("z"));
            assert_eq!(migration.schema, pending_schema());
            assert!(db.index_jobs("c").unwrap().is_empty());
            assert_eq!(db.indexes("c").unwrap(), vec![index_definition()]);
            assert_eq!(
                indexed_rows(db),
                vec![
                    ("a".to_owned(), document("a", 1)),
                    ("m".to_owned(), document("m", 2)),
                    ("z".to_owned(), document("z", 3)),
                    ("0".to_owned(), document("zero", 4)),
                ]
            );
            assert_eq!(
                db.insert("c", "duplicate", &document("duplicate", 4))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::AlreadyExists
            );
        }
    }
}

fn finish_loser(db: &DocumentDatabase<'_>, committed: CommittedPublication) {
    match committed {
        CommittedPublication::Schema => {
            db.transaction_with_read_set(|transaction| {
                transaction.put_raw(INDEX_WITNESS, b"yes")?;
                let step = transaction.advance_index_build("c", "idx", 1)?;
                assert_eq!((step.processed, step.complete), (0, true));
                Ok(())
            })
            .unwrap();
        }
        CommittedPublication::Index => {
            db.transaction_with_read_set(|transaction| {
                transaction.put_raw(SCHEMA_WITNESS, b"yes")?;
                let step = transaction.advance_schema_migration("c", 1)?;
                assert_eq!((step.processed, step.complete), (0, true));
                Ok(())
            })
            .unwrap();
        }
    }
}

fn assert_published(db: &DocumentDatabase<'_>) {
    assert_eq!(db.schema_migration("c").unwrap(), None);
    assert!(db.index_jobs("c").unwrap().is_empty());
    assert_eq!(db.indexes("c").unwrap(), vec![index_definition()]);
    assert_eq!(raw(db, SCHEMA_WITNESS).as_deref(), Some(b"yes".as_slice()));
    assert_eq!(raw(db, INDEX_WITNESS).as_deref(), Some(b"yes".as_slice()));
    assert_eq!(db.get("c", "0").unwrap(), Some(document("zero", 4)));
    assert_original_documents(db);
    assert_eq!(
        indexed_rows(db),
        vec![
            ("a".to_owned(), document("a", 1)),
            ("m".to_owned(), document("m", 2)),
            ("z".to_owned(), document("z", 3)),
            ("0".to_owned(), document("zero", 4)),
        ]
    );
    assert_eq!(
        db.verify_collection("c").unwrap(),
        CollectionIntegrity {
            documents: 4,
            indexes: 1,
            index_keys: 8,
        }
    );
    assert_eq!(
        db.insert("c", "missing-email", &json!({"n": 8}))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        db.insert("c", "duplicate", &document("duplicate", 4))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
}

struct Release {
    schema: Option<mpsc::Sender<()>>,
    index: Option<mpsc::Sender<()>>,
}

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.schema.take() {
            let _ = sender.send(());
        }
        if let Some(sender) = self.index.take() {
            let _ = sender.send(());
        }
    }
}

fn signal(slot: &mut Option<mpsc::Sender<()>>) {
    slot.take()
        .expect("publication release was already used")
        .send(())
        .expect("publication receiver dropped");
}

fn race(db: &DocumentDatabase<'_>, first: CommittedPublication) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    for (id, n) in [("a", 1), ("m", 2), ("z", 3)] {
        db.insert("c", id, &document(id, n)).unwrap();
    }
    db.start_schema_migration("c", pending_schema()).unwrap();
    db.start_index_build("c", index_definition()).unwrap();
    let schema_step = db.advance_schema_migration("c", 3).unwrap();
    assert_eq!((schema_step.processed, schema_step.complete), (3, false));
    let index_step = db.advance_index_build("c", "idx", 3).unwrap();
    assert_eq!((index_step.processed, index_step.complete), (3, false));
    assert_pending(db);

    thread::scope(|scope| {
        let (schema_ready_tx, schema_ready_rx) = mpsc::channel();
        let (index_ready_tx, index_ready_rx) = mpsc::channel();
        let (schema_go_tx, schema_go_rx) = mpsc::channel();
        let (index_go_tx, index_go_rx) = mpsc::channel();
        let mut release = Release {
            schema: Some(schema_go_tx),
            index: Some(index_go_tx),
        };
        let schema = scope.spawn(move || {
            db.transaction_with_read_set(|transaction| {
                transaction.put_raw(SCHEMA_WITNESS, b"yes")?;
                let step = transaction.advance_schema_migration("c", 1)?;
                assert_eq!((step.processed, step.complete), (0, true));
                schema_ready_tx
                    .send(())
                    .expect("schema ready receiver dropped");
                schema_go_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("schema publication was not released");
                Ok(())
            })
        });
        let index = scope.spawn(move || {
            db.transaction_with_read_set(|transaction| {
                transaction.put_raw(INDEX_WITNESS, b"yes")?;
                let step = transaction.advance_index_build("c", "idx", 1)?;
                assert_eq!((step.processed, step.complete), (0, true));
                index_ready_tx
                    .send(())
                    .expect("index ready receiver dropped");
                index_go_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("index publication was not released");
                Ok(())
            })
        });

        let schema_ready = schema_ready_rx.recv_timeout(Duration::from_secs(10));
        let index_ready = index_ready_rx.recv_timeout(Duration::from_secs(10));
        if schema_ready.is_err() || index_ready.is_err() {
            drop(release);
            let schema_panicked = schema.join().is_err();
            let index_panicked = index.join().is_err();
            panic!(
                "maintenance publication did not stage: schema={schema_ready:?} index={index_ready:?} schema_panicked={schema_panicked} index_panicked={index_panicked}"
            );
        }

        assert_pending(db);
        assert_eq!(raw(db, SCHEMA_WITNESS), None);
        assert_eq!(raw(db, INDEX_WITNESS), None);
        db.insert("c", "0", &document("zero", 4)).unwrap();
        assert_eq!(db.get("c", "0").unwrap(), Some(document("zero", 4)));
        assert_pending(db);
        assert_eq!(raw(db, SCHEMA_WITNESS), None);
        assert_eq!(raw(db, INDEX_WITNESS), None);

        match first {
            CommittedPublication::Schema => {
                signal(&mut release.schema);
                schema
                    .join()
                    .expect("schema publication thread panicked")
                    .expect("schema publication did not commit");
                signal(&mut release.index);
                let error = index
                    .join()
                    .expect("index publication thread panicked")
                    .expect_err("index publication committed after the schema publication");
                assert!(
                    is_transaction_conflict(&error),
                    "index publication failed without a conflict: {error}"
                );
            }
            CommittedPublication::Index => {
                signal(&mut release.index);
                index
                    .join()
                    .expect("index publication thread panicked")
                    .expect("index publication did not commit");
                signal(&mut release.schema);
                let error = schema
                    .join()
                    .expect("schema publication thread panicked")
                    .expect_err("schema publication committed after the index publication");
                assert!(
                    is_transaction_conflict(&error),
                    "schema publication failed without a conflict: {error}"
                );
            }
        }
        assert_conflict_outcome(db, first);
    })
}

trait BorrowDatabase {
    fn with_database<T>(&self, action: impl FnOnce(&DocumentDatabase<'_>) -> T) -> T;
}

impl BorrowDatabase for Store {
    fn with_database<T>(&self, action: impl FnOnce(&DocumentDatabase<'_>) -> T) -> T {
        action(&DocumentDatabase::new(self))
    }
}

impl BorrowDatabase for CheckpointStore {
    fn with_database<T>(&self, action: impl FnOnce(&DocumentDatabase<'_>) -> T) -> T {
        action(&DocumentDatabase::with_checkpoint_store(self))
    }
}

fn finish_durable_pair<S>(mut open: impl FnMut() -> io::Result<S>, committed: CommittedPublication)
where
    S: BorrowDatabase,
{
    {
        let store = retry_lock(&mut open).unwrap();
        store.with_database(|db| {
            assert_conflict_outcome(db, committed);
            finish_loser(db, committed);
            assert_published(db);
        });
    }
    let store = retry_lock(&mut open).unwrap();
    store.with_database(assert_published);
}

#[test]
fn read_set_publication_keeps_schema_and_index_atomic_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    for (name, first) in [
        ("schema-first", CommittedPublication::Schema),
        ("index-first", CommittedPublication::Index),
    ] {
        let ordinary = root.path().join(format!("{name}-ordinary"));
        {
            let store = Store::open(&ordinary).unwrap();
            race(&DocumentDatabase::new(&store), first);
        }
        finish_durable_pair(|| Store::open(&ordinary), first);

        let source = root.path().join(format!("{name}-checkpoint"));
        let base = root.path().join(format!("{name}-base"));
        let initial = Store::open(&source).unwrap();
        let digest = initial.checkpoint_to(&base).unwrap();
        drop(initial);
        {
            let store = retry_lock(|| CheckpointStore::open(&source, &base, &digest)).unwrap();
            race(&DocumentDatabase::with_checkpoint_store(&store), first);
        }
        finish_durable_pair(|| CheckpointStore::open(&source, &base, &digest), first);
    }
}

fn later_document() -> Value {
    document("later", 0)
}

fn assert_index_build_at(db: &DocumentDatabase<'_>, cursor: &str) {
    assert!(matches!(
        &db.index_jobs("c").unwrap()[..],
        [IndexJobStatus::Building {
            definition,
            after: Some(id),
        }] if id == cursor && definition == &index_definition()
    ));
    assert!(db.indexes("c").unwrap().is_empty());
    assert_eq!(
        db.query_index("c", "idx", &IndexQuery::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}

fn assert_after_cursor_rollback(db: &DocumentDatabase<'_>) {
    assert_eq!(raw(db, INDEX_WITNESS), None);
    assert_index_build_at(db, "z");
    assert_original_documents(db);
    assert_eq!(
        db.get("c", AFTER_CURSOR_ID).unwrap(),
        Some(later_document())
    );
}

fn assert_after_cursor_published(db: &DocumentDatabase<'_>) {
    assert_eq!(db.schema_migration("c").unwrap(), None);
    assert!(db.index_jobs("c").unwrap().is_empty());
    assert_eq!(db.indexes("c").unwrap(), vec![index_definition()]);
    assert_eq!(raw(db, INDEX_WITNESS).as_deref(), Some(b"yes".as_slice()));
    assert_original_documents(db);
    assert_eq!(
        db.get("c", AFTER_CURSOR_ID).unwrap(),
        Some(later_document())
    );
    assert_eq!(
        indexed_rows(db),
        vec![
            (AFTER_CURSOR_ID.to_owned(), later_document()),
            ("a".to_owned(), document("a", 1)),
            ("m".to_owned(), document("m", 2)),
            ("z".to_owned(), document("z", 3)),
        ]
    );
    assert_eq!(
        db.verify_collection("c").unwrap(),
        CollectionIntegrity {
            documents: 4,
            indexes: 1,
            index_keys: 8,
        }
    );
    assert_eq!(
        db.insert("c", "duplicate", &document("duplicate", 0))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
}

fn retry_index_publication(db: &DocumentDatabase<'_>) {
    db.transaction_with_read_set(|transaction| {
        let step = transaction.advance_index_build("c", "idx", 1)?;
        assert_eq!((step.processed, step.complete), (1, false));
        Ok(())
    })
    .unwrap();
    assert_eq!(raw(db, INDEX_WITNESS), None);
    assert_index_build_at(db, AFTER_CURSOR_ID);
    assert_eq!(
        db.get("c", AFTER_CURSOR_ID).unwrap(),
        Some(later_document())
    );

    db.transaction_with_read_set(|transaction| {
        transaction.put_raw(INDEX_WITNESS, b"yes")?;
        let step = transaction.advance_index_build("c", "idx", 1)?;
        assert_eq!((step.processed, step.complete), (0, true));
        Ok(())
    })
    .unwrap();
}

fn stage_index_empty_page_and_insert_after_cursor(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    for (id, n) in [("a", 1), ("m", 2), ("z", 3)] {
        db.insert("c", id, &document(id, n)).unwrap();
    }
    db.start_index_build("c", index_definition()).unwrap();
    let advanced = db.advance_index_build("c", "idx", 3).unwrap();
    assert_eq!((advanced.processed, advanced.complete), (3, false));
    assert_index_build_at(db, "z");

    thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        let (inserted_tx, inserted_rx) = mpsc::channel();
        let mut release = Release {
            schema: None,
            index: Some(go_tx),
        };
        let publication = scope.spawn(move || {
            db.transaction_with_read_set(|transaction| {
                transaction.put_raw(INDEX_WITNESS, b"yes")?;
                let step = transaction.advance_index_build("c", "idx", 1)?;
                assert_eq!((step.processed, step.complete), (0, true));
                ready_tx.send(()).expect("index ready receiver dropped");
                go_rx
                    .recv_timeout(Duration::from_secs(30))
                    .expect("index publication was not released");
                Ok(())
            })
        });

        if ready_rx.recv_timeout(Duration::from_secs(10)).is_err() {
            drop(release);
            let panicked = publication.join().is_err();
            panic!("index empty-page publication did not stage: panicked={panicked}");
        }
        assert_eq!(raw(db, INDEX_WITNESS), None);
        assert_index_build_at(db, "z");
        assert_eq!(db.get("c", AFTER_CURSOR_ID).unwrap(), None);

        let insert = scope.spawn(move || {
            let result = db.transaction_until(
                Instant::now() + Duration::from_secs(10),
                DocumentTransactionMode::Conservative,
                |transaction| transaction.insert("c", AFTER_CURSOR_ID, &later_document()),
            );
            let _ = inserted_tx.send(result);
        });
        let inserted = match inserted_rx.recv_timeout(Duration::from_secs(12)) {
            Ok(result) => {
                insert.join().expect("insert thread panicked");
                result
            }
            Err(timeout) => {
                drop(release);
                let publication_panicked = publication.join().is_err();
                let insert_panicked = insert.join().is_err();
                panic!(
                    "insert after the cursor did not finish while publication was staged: {timeout} publication_panicked={publication_panicked} insert_panicked={insert_panicked}"
                );
            }
        };
        if let Err(error) = inserted {
            drop(release);
            let _ = publication.join();
            panic!(
                "supported insert after the cursor failed while publication was staged: {error}"
            );
        }

        assert_eq!(
            db.get("c", AFTER_CURSOR_ID).unwrap(),
            Some(later_document())
        );
        assert_eq!(raw(db, INDEX_WITNESS), None);
        assert_index_build_at(db, "z");
        signal(&mut release.index);
        let error = publication
            .join()
            .expect("index publication thread panicked")
            .expect_err("index empty-page publication committed after an insert beyond the cursor");
        assert!(
            is_transaction_conflict(&error),
            "index publication failed without a conflict: {error}"
        );
        assert_after_cursor_rollback(db);
    });
}

fn finish_after_cursor<S>(mut open: impl FnMut() -> io::Result<S>)
where
    S: BorrowDatabase,
{
    {
        let store = retry_lock(&mut open).unwrap();
        store.with_database(|db| {
            assert_after_cursor_rollback(db);
            retry_index_publication(db);
            assert_after_cursor_published(db);
        });
    }
    let store = retry_lock(&mut open).unwrap();
    store.with_database(assert_after_cursor_published);
}

#[test]
fn read_set_index_empty_page_conflicts_with_insert_after_cursor_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let ordinary = root.path().join("ordinary");
    {
        let store = Store::open(&ordinary).unwrap();
        stage_index_empty_page_and_insert_after_cursor(&DocumentDatabase::new(&store));
    }
    finish_after_cursor(|| Store::open(&ordinary));

    let source = root.path().join("checkpoint");
    let base = root.path().join("base");
    let initial = Store::open(&source).unwrap();
    let digest = initial.checkpoint_to(&base).unwrap();
    drop(initial);
    {
        let store = retry_lock(|| CheckpointStore::open(&source, &base, &digest)).unwrap();
        stage_index_empty_page_and_insert_after_cursor(&DocumentDatabase::with_checkpoint_store(
            &store,
        ));
    }
    finish_after_cursor(|| CheckpointStore::open(&source, &base, &digest));
}

fn stage_schema_empty_page_and_insert_after_cursor(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "z", &document("z", 1)).unwrap();
    db.start_schema_migration("c", pending_schema()).unwrap();
    let step = db.advance_schema_migration("c", 1).unwrap();
    assert_eq!((step.processed, step.complete), (1, false));

    thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        let mut release = Release {
            schema: Some(go_tx),
            index: None,
        };
        let publication = scope.spawn(move || {
            db.transaction_with_read_set(|tx| {
                tx.put_raw(SCHEMA_WITNESS, b"yes")?;
                let step = tx.advance_schema_migration("c", 1)?;
                assert_eq!((step.processed, step.complete), (0, true));
                ready_tx.send(()).unwrap();
                go_rx.recv_timeout(Duration::from_secs(30)).unwrap();
                Ok(())
            })
        });
        ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        db.insert("c", AFTER_CURSOR_ID, &later_document()).unwrap();
        signal(&mut release.schema);
        let error = publication
            .join()
            .unwrap()
            .expect_err("schema publication missed phantom");
        assert!(is_transaction_conflict(&error), "unexpected error: {error}");
    });
    assert_schema_rollback(db);
}

fn assert_schema_rollback(db: &DocumentDatabase<'_>) {
    assert_eq!(raw(db, SCHEMA_WITNESS), None);
    assert_eq!(
        db.schema_migration("c").unwrap().unwrap().after.as_deref(),
        Some("z")
    );
    assert_eq!(
        db.get("c", AFTER_CURSOR_ID).unwrap(),
        Some(later_document())
    );
}

fn finish_schema_after_cursor<S: BorrowDatabase>(mut open: impl FnMut() -> io::Result<S>) {
    {
        let store = retry_lock(&mut open).unwrap();
        store.with_database(|db| {
            assert_schema_rollback(db);
            let step = db.advance_schema_migration("c", 1).unwrap();
            assert_eq!((step.processed, step.complete), (1, false));
            db.transaction_with_read_set(|tx| {
                tx.put_raw(SCHEMA_WITNESS, b"yes")?;
                let step = tx.advance_schema_migration("c", 1)?;
                assert_eq!((step.processed, step.complete), (0, true));
                Ok(())
            })
            .unwrap();
        });
    }
    let store = retry_lock(&mut open).unwrap();
    store.with_database(|db| {
        assert_eq!(db.schema_migration("c").unwrap(), None);
        assert_eq!(raw(db, SCHEMA_WITNESS).as_deref(), Some(b"yes".as_slice()));
        assert_eq!(
            db.get("c", AFTER_CURSOR_ID).unwrap(),
            Some(later_document())
        );
        assert_eq!(
            db.insert("c", "invalid", &json!({})).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    });
}

#[test]
fn read_set_schema_empty_page_conflicts_with_insert_after_cursor_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let ordinary = root.path().join("ordinary");
    {
        let store = Store::open(&ordinary).unwrap();
        stage_schema_empty_page_and_insert_after_cursor(&DocumentDatabase::new(&store));
    }
    finish_schema_after_cursor(|| Store::open(&ordinary));

    let source = root.path().join("checkpoint");
    let base = root.path().join("base");
    let initial = Store::open(&source).unwrap();
    let digest = initial.checkpoint_to(&base).unwrap();
    drop(initial);
    {
        let store = retry_lock(|| CheckpointStore::open(&source, &base, &digest)).unwrap();
        stage_schema_empty_page_and_insert_after_cursor(&DocumentDatabase::with_checkpoint_store(
            &store,
        ));
    }
    finish_schema_after_cursor(|| CheckpointStore::open(&source, &base, &digest));
}
