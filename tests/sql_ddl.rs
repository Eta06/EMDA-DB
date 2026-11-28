//! DROP TABLE and ALTER TABLE ... ADD COLUMN on both writable backends: atomic
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

#[test]
fn add_column_reads_null_for_existing_rows_and_is_usable_everywhere() {
    for_both(|db, backend| {
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER)", &[])
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20)", &[])
            .unwrap();
        // A cached plan for the old definition must not be reused.
        let select_all = db.prepare("SELECT * FROM t WHERE id = 1").unwrap();
        db.execute_prepared(&select_all, &[]).unwrap();
        assert_eq!(
            db.execute("ALTER TABLE t ADD COLUMN note VARCHAR(3)", &[])
                .unwrap(),
            SqlOutcome::TableAltered,
            "{backend}"
        );
        assert_eq!(
            db.execute_prepared(&select_all, &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "k".into(), "note".into()],
                rows: vec![vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(10),
                    SqlValue::Null
                ]],
            },
            "{backend}"
        );
        db.execute("ALTER TABLE t ADD flag BOOLEAN", &[]).unwrap();
        db.execute("INSERT INTO t VALUES (3, 30, 'abc', TRUE)", &[])
            .unwrap();
        assert!(matches!(
            db.execute("INSERT INTO t VALUES (4, 40, 'abcd', TRUE)", &[]),
            Err(SqlError::Constraint(_))
        ));
        db.execute("UPDATE t SET note = 'x' WHERE id = 1", &[])
            .unwrap();
        db.execute("CREATE INDEX t_note ON t (note)", &[]).unwrap();
        let rows = |sql: &str| match db.execute(sql, &[]).unwrap() {
            SqlOutcome::Rows { rows, .. } => rows,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            rows("SELECT id FROM t WHERE note IS NULL"),
            vec![vec![SqlValue::Integer(2)]]
        );
        assert_eq!(
            rows("SELECT id FROM t WHERE note = 'x'"),
            vec![vec![SqlValue::Integer(1)]]
        );
        assert_eq!(
            rows("SELECT COUNT(note), COUNT(*) FROM t"),
            vec![vec![SqlValue::Integer(2), SqlValue::Integer(3)]]
        );
        assert_eq!(
            rows("SELECT a.id FROM t a JOIN t b ON b.note = a.note ORDER BY a.id"),
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)]]
        );
    });
}

#[test]
fn add_column_rules_and_errors() {
    for_both(|db, backend| {
        db.execute("CREATE TABLE empty (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute("CREATE TABLE filled (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute("INSERT INTO filled VALUES (1)", &[]).unwrap();
        db.execute("ALTER TABLE empty ADD COLUMN must INTEGER NOT NULL", &[])
            .unwrap();
        assert!(matches!(
            db.execute("INSERT INTO empty (id) VALUES (1)", &[]),
            Err(SqlError::Constraint(_))
        ));
        assert!(
            matches!(
                db.execute("ALTER TABLE filled ADD COLUMN must INTEGER NOT NULL", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        assert!(matches!(
            db.execute("ALTER TABLE filled ADD COLUMN id TEXT", &[]),
            Err(SqlError::Syntax { .. })
        ));
        assert!(matches!(
            db.execute("ALTER TABLE missing ADD COLUMN c TEXT", &[]),
            Err(SqlError::NoSuchTable(_))
        ));
        for statement in [
            "ALTER TABLE filled RENAME TO other",
            "ALTER TABLE filled ADD CONSTRAINT u UNIQUE (id)",
            "ALTER TABLE filled ADD COLUMN c INTEGER UNIQUE",
            "ALTER TABLE filled ADD COLUMN c INTEGER PRIMARY KEY",
            "ALTER TABLE filled ADD COLUMN c INTEGER DEFAULT 0",
            "ALTER TABLE filled ADD COLUMN c INTEGER, ADD COLUMN d INTEGER",
        ] {
            assert!(
                matches!(
                    db.execute(statement, &[]),
                    Err(SqlError::Unsupported { .. })
                ),
                "{backend}: {statement}"
            );
        }
        // Truncated statements are syntax errors, never panics.
        for statement in [
            "ALTER TABLE",
            "ALTER TABLE filled",
            "ALTER TABLE filled ADD",
            "ALTER TABLE filled ADD COLUMN",
            "ALTER TABLE filled ADD COLUMN c",
            "ALTER TABLE filled ADD COLUMN c VARCHAR(",
            "ALTER TABLE filled ADD COLUMN c VARCHAR(3",
            "ALTER TABLE filled ADD COLUMN c VARCHAR(0)",
            "ALTER TABLE filled ADD COLUMN c BLOB",
            "DROP TABLE IF",
            "DROP TABLE IF EXISTS",
        ] {
            assert!(
                matches!(db.execute(statement, &[]), Err(SqlError::Syntax { .. })),
                "{backend}: {statement}: {:?}",
                db.execute(statement, &[])
            );
        }
        // A table holds at most max_columns columns.
        db.execute("CREATE TABLE wide (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for column in 1..64 {
            db.execute(
                &format!("ALTER TABLE wide ADD COLUMN c{column} INTEGER"),
                &[],
            )
            .unwrap();
        }
        assert!(matches!(
            db.execute("ALTER TABLE wide ADD COLUMN c64 INTEGER", &[]),
            Err(SqlError::Bounds(_))
        ));
    });
}

#[test]
fn drop_and_rename_columns_are_atomic_and_preserve_other_indexes() {
    for_both(|db, backend| {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "INSERT INTO parent VALUES (10)",
            "CREATE TABLE t (a TEXT NOT NULL DEFAULT 'a', id INTEGER PRIMARY KEY, keep INTEGER REFERENCES parent(id), b INTEGER, chk INTEGER CHECK (chk > 0))",
            "CREATE INDEX t_keep ON t (keep)",
            "INSERT INTO t VALUES ('one', 1, 10, NULL, 1), ('two', 2, 10, 20, 2)",
        ] {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        for sql in [
            "ALTER TABLE t DROP COLUMN id",
            "ALTER TABLE t RENAME COLUMN keep TO replacement",
            "ALTER TABLE t DROP COLUMN chk",
            "ALTER TABLE t RENAME COLUMN a TO id",
        ] {
            assert!(
                matches!(db.execute(sql, &[]), Err(SqlError::Constraint(_))),
                "{backend}: {sql}"
            );
        }
        let cached = db.prepare("SELECT a FROM t WHERE id = 1").unwrap();
        db.execute_prepared(&cached, &[]).unwrap();
        let aborted: Result<(), SqlError> = db.transaction(|tx| {
            tx.execute("ALTER TABLE t RENAME COLUMN a TO transient", &[])?;
            tx.execute("SELECT transient FROM t WHERE id = 1", &[])?;
            Err(SqlError::Constraint("rollback migration".into()))
        });
        assert!(matches!(aborted, Err(SqlError::Constraint(ref s)) if s == "rollback migration"));
        db.execute_prepared(&cached, &[]).unwrap();
        for sql in [
            "ALTER TABLE t RENAME COLUMN a TO label",
            "ALTER TABLE t DROP COLUMN b",
            "ALTER TABLE t ADD COLUMN b INTEGER",
            "INSERT INTO t (id, keep, chk) VALUES (3, 10, 3)",
        ] {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        assert!(matches!(
            db.execute_prepared(&cached, &[]),
            Err(SqlError::NoSuchColumn { .. })
        ));
        let sql = "SELECT id, label, b FROM t WHERE keep = 10 ORDER BY id";
        let SqlOutcome::Rows { rows, .. } = db.execute(sql, &[]).unwrap() else {
            panic!()
        };
        let mut statement = sqlite.prepare(sql).unwrap();
        let expected = statement
            .query_map([], |row| {
                Ok(vec![
                    SqlValue::Integer(row.get(0)?),
                    SqlValue::Text(row.get(1)?),
                    row.get::<_, Option<i64>>(2)?
                        .map_or(SqlValue::Null, SqlValue::Integer),
                ])
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, expected, "{backend}");
        assert!(matches!(
            db.execute("INSERT INTO t (id, keep) VALUES (4, 404)", &[]),
            Err(SqlError::Constraint(_))
        ));
    });
}

#[test]
fn added_columns_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path().join("s")).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
        db.execute("ALTER TABLE t ADD COLUMN r REAL", &[]).unwrap();
        db.execute("INSERT INTO t VALUES (2, 2.5)", &[]).unwrap();
    }
    let store = Store::open(dir.path().join("s")).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    assert_eq!(
        db.execute("SELECT r FROM t ORDER BY id", &[]).unwrap(),
        SqlOutcome::Rows {
            columns: vec!["r".into()],
            rows: vec![vec![SqlValue::Null], vec![SqlValue::Real(2.5)]],
        }
    );
}

#[test]
fn ddl_inside_explicit_transactions() {
    for_both(|db, backend| {
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
        // ADD COLUMN is visible to later statements in the same transaction.
        db.transaction(|tx| {
            tx.execute("ALTER TABLE t ADD COLUMN note TEXT", &[])?;
            tx.execute("INSERT INTO t VALUES (2, 'two')", &[])?;
            Ok(())
        })
        .unwrap();
        assert_eq!(count(db, "t"), 2, "{backend}");
        // A rollback discards both the column and the row.
        let rolled_back: Result<(), SqlError> = db.transaction(|tx| {
            tx.execute("ALTER TABLE t ADD COLUMN extra INTEGER", &[])?;
            tx.execute("INSERT INTO t VALUES (3, NULL, 3)", &[])?;
            Err(SqlError::Constraint("roll back".into()))
        });
        assert!(rolled_back.is_err());
        assert!(
            matches!(
                db.execute("SELECT extra FROM t", &[]),
                Err(SqlError::NoSuchColumn { .. })
            ),
            "{backend}"
        );
        assert_eq!(count(db, "t"), 2, "{backend}");
        let dropped: Result<SqlOutcome, SqlError> =
            db.transaction(|tx| tx.execute("DROP TABLE t", &[]));
        assert!(
            matches!(dropped, Err(SqlError::Unsupported { .. })),
            "{backend}: {dropped:?}"
        );
        assert_eq!(count(db, "t"), 2, "{backend}");
    });
}
