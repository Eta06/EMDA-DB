//! DROP TABLE on both writable backends: atomic small drops, incremental
//! large drops, resumption after an interrupted drop, and reopen.
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlOutcome, SqlValue, Store,
};

fn for_both(mut f: impl FnMut(&SqlDatabase<'_>, &str)) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("ordinary")).unwrap();
    f(&SqlDatabase::new(DocumentDatabase::new(&store)), "Store");
    drop(store);
    let log = dir.path().join("checkpoint-log");
    let initial = Store::open(&log).unwrap();
    let digest = initial
        .checkpoint_to(dir.path().join("checkpoint"))
        .unwrap();
    drop(initial);
    let checkpoint = CheckpointStore::open(&log, dir.path().join("checkpoint"), &digest).unwrap();
    f(
        &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&checkpoint)),
        "CheckpointStore",
    );
}

fn count(db: &SqlDatabase<'_>, table: &str) -> usize {
    match db.execute(&format!("SELECT id FROM {table}"), &[]).unwrap() {
        SqlOutcome::Rows { rows, .. } => rows.len(),
        other => panic!("{other:?}"),
    }
}

fn fill(db: &SqlDatabase<'_>, table: &str, rows: i64) {
    for start in (0..rows).step_by(200) {
        let values = (start..(start + 200).min(rows))
            .map(|id| format!("({id}, {}, 'v{id}')", id % 13))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(&format!("INSERT INTO {table} VALUES {values}"), &[])
            .unwrap();
    }
}

#[test]
fn small_table_drops_atomically_and_its_name_can_be_reused() {
    for_both(|db, backend| {
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER UNIQUE, s TEXT)",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX t_s ON t (s)", &[]).unwrap();
        db.execute("CREATE TABLE keep (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute("INSERT INTO keep VALUES (1)", &[]).unwrap();
        fill(db, "t", 12);
        assert_eq!(
            db.execute("DROP TABLE t", &[]).unwrap(),
            SqlOutcome::TableDropped,
            "{backend}"
        );
        for statement in [
            "SELECT * FROM t",
            "INSERT INTO t VALUES (1, 1, 'x')",
            "SELECT k.id FROM keep k JOIN t ON t.id = k.id",
            "SELECT COUNT(*) FROM t",
            "DROP TABLE t",
        ] {
            assert!(
                matches!(db.execute(statement, &[]), Err(SqlError::NoSuchTable(_))),
                "{backend}: {statement}"
            );
        }
        assert_eq!(
            db.execute("DROP TABLE IF EXISTS t", &[]).unwrap(),
            SqlOutcome::TableDropped
        );
        // No rows, index entries or document collection survive.
        assert!(
            db.document_database()
                .transaction(|tx| tx.scan_documents("t", None, 10))
                .is_err(),
            "{backend}"
        );
        // The name is reusable with a different definition and no old data.
        db.execute(
            "CREATE TABLE t (id TEXT PRIMARY KEY, k INTEGER UNIQUE)",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO t VALUES ('a', 1)", &[]).unwrap();
        assert_eq!(count(db, "t"), 1, "{backend}");
        assert_eq!(count(db, "keep"), 1, "{backend}");
    });
}

#[test]
fn large_table_drops_incrementally_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("log");
    let initial = Store::open(&log).unwrap();
    let digest = initial
        .checkpoint_to(dir.path().join("checkpoint"))
        .unwrap();
    drop(initial);
    {
        let store = CheckpointStore::open(&log, dir.path().join("checkpoint"), &digest).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
        db.execute(
            "CREATE TABLE big (id INTEGER PRIMARY KEY, k INTEGER, s TEXT)",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX big_k ON big (k)", &[]).unwrap();
        fill(&db, "big", 2_000);
        // 2,000 rows and 2,000 index entries exceed one maintenance transaction.
        assert_eq!(
            db.execute("DROP TABLE big", &[]).unwrap(),
            SqlOutcome::TableDropped
        );
        assert!(matches!(
            db.execute("SELECT * FROM big", &[]),
            Err(SqlError::NoSuchTable(_))
        ));
        db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute("INSERT INTO big VALUES (7)", &[]).unwrap();
    }
    let store = CheckpointStore::open(&log, dir.path().join("checkpoint"), &digest).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
    assert_eq!(count(&db, "big"), 1);
}

#[test]
fn interrupted_large_drop_is_finished_by_the_next_drop() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("log");
    let initial = Store::open(&log).unwrap();
    let digest = initial
        .checkpoint_to(dir.path().join("checkpoint"))
        .unwrap();
    drop(initial);
    let store = CheckpointStore::open(&log, dir.path().join("checkpoint"), &digest).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
    db.execute(
        "CREATE TABLE big (id INTEGER PRIMARY KEY, k INTEGER, s TEXT)",
        &[],
    )
    .unwrap();
    fill(&db, "big", 1_500);
    // Persist the state a crash leaves after the first drop transaction:
    // the catalog entry removed and the collection marked for deletion.
    let mut catalog_key = b"\0emda-sql\x01T".to_vec();
    catalog_key.extend_from_slice(b"big");
    db.document_database()
        .transaction(|tx| {
            tx.delete_raw(&catalog_key)?;
            tx.start_collection_drop("big")
        })
        .unwrap();
    assert!(matches!(
        db.execute("SELECT * FROM big", &[]),
        Err(SqlError::NoSuchTable(_))
    ));
    // The leftover collection blocks reuse of the name until the drop finishes.
    assert!(
        db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY)", &[])
            .is_err()
    );
    assert_eq!(
        db.execute("DROP TABLE IF EXISTS big", &[]).unwrap(),
        SqlOutcome::TableDropped
    );
    // Once removed, the collection no longer exists at all.
    assert!(!matches!(
        db.document_database().collection_drop_in_progress("big"),
        Ok(true)
    ));
    db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY)", &[])
        .unwrap();
    db.execute("INSERT INTO big VALUES (?)", &[SqlValue::Integer(1)])
        .unwrap();
    assert_eq!(count(&db, "big"), 1);
}

#[test]
fn drop_table_syntax_and_errors() {
    for_both(|db, backend| {
        db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for (statement, unsupported) in [
            ("DROP TABLE a CASCADE", true),
            ("DROP TABLE a RESTRICT", true),
            ("DROP TABLE a, b", true),
            ("DROP TABLE", false),
            ("DROP TABLE IF a", false),
            ("DROP TABLE a b", false),
        ] {
            let error = db.execute(statement, &[]).unwrap_err();
            assert_eq!(
                matches!(error, SqlError::Unsupported { .. }),
                unsupported,
                "{backend}: {statement}: {error:?}"
            );
            if !unsupported {
                assert!(
                    matches!(error, SqlError::Syntax { .. }),
                    "{statement}: {error:?}"
                );
            }
        }
        assert!(matches!(
            db.execute("DROP TABLE missing", &[]),
            Err(SqlError::NoSuchTable(_))
        ));
        assert_eq!(
            db.execute("DROP TABLE \"a\";", &[]).unwrap(),
            SqlOutcome::TableDropped
        );
    });
}
