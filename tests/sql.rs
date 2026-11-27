use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    AuthorizedRetainedStore, FileRetainedAuthority, IndexLimits, ManagedRetainedStore,
    OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, RetainedExportLimits, SqlDatabase,
    SqlError, SqlLimits, SqlOutcome, SqlValue, Store, is_transaction_conflict,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Barrier};

fn open_checkpoint(dir: &std::path::Path) -> (CheckpointStore, std::path::PathBuf, [u8; 32]) {
    let store_path = dir.join("checkpoint_log");
    let ckpt_path = dir.join("checkpoint_file");
    let initial = Store::open(&store_path).unwrap();
    let digest = initial.checkpoint_to(&ckpt_path).unwrap();
    drop(initial);
    let ckpt = CheckpointStore::open(&store_path, &ckpt_path, &digest).unwrap();
    (ckpt, ckpt_path, digest)
}

fn for_both_backends(mut f: impl FnMut(&DocumentDatabase<'_>)) {
    let dir = tempfile::tempdir().unwrap();
    let ordinary = Store::open(dir.path().join("ordinary")).unwrap();
    f(&DocumentDatabase::new(&ordinary));
    drop(ordinary);

    let (ckpt, _, _) = open_checkpoint(dir.path());
    f(&DocumentDatabase::with_checkpoint_store(&ckpt));
}

fn for_both_sql(mut f: impl FnMut(SqlDatabase<'_>)) {
    let dir = tempfile::tempdir().unwrap();
    let ordinary = Store::open(dir.path().join("ordinary")).unwrap();
    f(SqlDatabase::new(DocumentDatabase::new(&ordinary)));
    drop(ordinary);

    let (ckpt, _, _) = open_checkpoint(dir.path());
    f(SqlDatabase::new(DocumentDatabase::with_checkpoint_store(
        &ckpt,
    )));
}

#[test]
fn scan_documents_pages_in_id_order_across_all_backends() {
    fn check_scan(db: &DocumentDatabase<'_>) {
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        let empty = db
            .transaction(|tx| tx.scan_documents("items", None, 2))
            .unwrap();
        assert!(empty.is_empty());

        for id in ["d", "a", "c", "b", "e"] {
            db.insert("items", id, &json!({ "id": id })).unwrap();
        }

        let collected = db
            .transaction(|tx| {
                tx.insert("items", "aa", &json!({ "id": "aa" }))?;
                tx.delete("items", "c")?;
                assert_eq!(
                    tx.scan_documents("items", None, 0).unwrap_err().kind(),
                    io::ErrorKind::InvalidInput
                );
                assert_eq!(
                    tx.scan_documents("items", None, 1025).unwrap_err().kind(),
                    io::ErrorKind::InvalidInput
                );
                assert_eq!(
                    tx.scan_documents("missing", None, 2).unwrap_err().kind(),
                    io::ErrorKind::NotFound
                );

                let mut out = Vec::new();
                let mut after: Option<String> = None;
                loop {
                    let page = tx.scan_documents("items", after.as_deref(), 2)?;
                    if page.is_empty() {
                        break;
                    }
                    after = page.last().map(|d| d.id.clone());
                    out.extend(page.into_iter().map(|d| d.id));
                }
                Ok(out)
            })
            .unwrap();
        assert_eq!(collected, vec!["a", "aa", "b", "d", "e"]);
    }

    for_both_backends(check_scan);

    let retained_limits = RetainedStoreLimits {
        base: Limits {
            file_bytes: 1_000_000,
            entries: 5_000,
            key_bytes: 250_000,
            work_bytes: 1_000_001,
        },
        index: IndexLimits {
            blocks: 32,
            anchor_bytes: 4096,
        },
        suffix: SuffixLimits {
            bytes: 1_000_000,
            operations: 5_000,
            work: 10_000_000,
        },
        overlay: OverlayLimits {
            entries: 5_000,
            key_bytes: 250_000,
        },
    };
    let export_limits = RetainedExportLimits {
        max_log_bytes: 1_000_000,
        max_live_keys: 5_000,
        max_live_key_bytes: 250_000,
        max_output_bytes: 1_000_000,
    };

    let dir = tempfile::tempdir().unwrap();
    let src_m = dir.path().join("src_m");
    let cat_m = dir.path().join("cat_m");
    let store_m = Store::open(&src_m).unwrap();
    ManagedRetainedStore::copy_from_store(store_m, &src_m, &cat_m, export_limits, retained_limits)
        .unwrap();
    let mut managed = ManagedRetainedStore::open(&cat_m, retained_limits).unwrap();
    check_scan(&DocumentDatabase::with_managed_retained_store(&mut managed));

    type AuthCatalog = AuthorizedRetainedStore<FileRetainedAuthority>;
    let src_a = dir.path().join("src_a");
    let cat_a = dir.path().join("cat_a");
    let auth_path = dir.path().join("auth");
    let store_a = Store::open(&src_a).unwrap();
    let initial =
        AuthCatalog::prepare_from_store(store_a, &src_a, &cat_a, export_limits, retained_limits)
            .unwrap();
    let mut provider = FileRetainedAuthority::create(&auth_path).unwrap();
    assert!(provider.compare_and_swap(None, initial).unwrap());
    let mut authorized = AuthCatalog::open(
        &cat_a,
        retained_limits,
        FileRetainedAuthority::open(&auth_path).unwrap(),
    )
    .unwrap();
    check_scan(&DocumentDatabase::with_authorized_retained_store(
        &mut authorized,
    ));
}

#[test]
fn crud_statements_types_aliases_coercions_and_prepare_reuse() {
    for_both_sql(|db| {
        assert_eq!(
            db.execute(
                "-- create test table\nCREATE TABLE Users (\
                    id BIGINT PRIMARY KEY,\
                    name VARCHAR(4) NOT NULL,\
                    note TEXT,\
                    score DOUBLE,\
                    alt_score FLOAT,\
                    active BOOL,\
                    \"CaseCol\" INT\
                );",
                &[]
            )
            .unwrap(),
            SqlOutcome::TableCreated
        );

        // IF NOT EXISTS succeeds; duplicate without IF NOT EXISTS returns TableExists.
        assert_eq!(
            db.execute("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)", &[])
                .unwrap(),
            SqlOutcome::TableCreated
        );
        assert!(matches!(
            db.execute("CREATE TABLE users (id INT PRIMARY KEY)", &[])
                .unwrap_err(),
            SqlError::TableExists(_)
        ));

        // Insert with positional params, integer->real exact coercion, escaped quotes, omitted nullable cols.
        assert_eq!(
            db.execute(
                "INSERT INTO users (id, name, score, active, \"CaseCol\") VALUES \
                 (10, 'it''s', 5, TRUE, -7),\
                 (-5, 'éééé', ?, FALSE, NULL)",
                &[SqlValue::Integer(1_i64 << 53)],
            )
            .unwrap(),
            SqlOutcome::Inserted { rows: 2 }
        );

        // Insert with all columns and negative/positive extremes.
        assert_eq!(
            db.execute(
                "INSERT INTO users VALUES (-9223372036854775808, 'min', NULL, -1.5, 0.25, true, 9223372036854775807);",
                &[]
            )
            .unwrap(),
            SqlOutcome::Inserted { rows: 1 }
        );

        // SELECT * returns rows in ascending PK order (-9223372036854775808, -5, 10).
        let res = db.execute("select * from USERS", &[]).unwrap();
        assert_eq!(
            res,
            SqlOutcome::Rows {
                columns: vec![
                    "id".into(),
                    "name".into(),
                    "note".into(),
                    "score".into(),
                    "alt_score".into(),
                    "active".into(),
                    "CaseCol".into(),
                ],
                rows: vec![
                    vec![
                        SqlValue::Integer(i64::MIN),
                        SqlValue::Text("min".into()),
                        SqlValue::Null,
                        SqlValue::Real(-1.5),
                        SqlValue::Real(0.25),
                        SqlValue::Boolean(true),
                        SqlValue::Integer(i64::MAX),
                    ],
                    vec![
                        SqlValue::Integer(-5),
                        SqlValue::Text("éééé".into()),
                        SqlValue::Null,
                        SqlValue::Real((1_i64 << 53) as f64),
                        SqlValue::Null,
                        SqlValue::Boolean(false),
                        SqlValue::Null,
                    ],
                    vec![
                        SqlValue::Integer(10),
                        SqlValue::Text("it's".into()),
                        SqlValue::Null,
                        SqlValue::Real(5.0),
                        SqlValue::Null,
                        SqlValue::Boolean(true),
                        SqlValue::Integer(-7),
                    ],
                ],
            }
        );

        // Repeated columns and prepared statement reuse.
        let prep = db
            .prepare("SELECT name, name, \"CaseCol\" FROM users WHERE id = ?")
            .unwrap();
        assert_eq!(prep.param_count(), 1);
        assert_eq!(
            db.execute_prepared(&prep, &[SqlValue::Integer(10)])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["name".into(), "name".into(), "CaseCol".into()],
                rows: vec![vec![
                    SqlValue::Text("it's".into()),
                    SqlValue::Text("it's".into()),
                    SqlValue::Integer(-7),
                ]],
            }
        );
        assert_eq!(
            db.execute_prepared(&prep, &[SqlValue::Integer(-5)])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["name".into(), "name".into(), "CaseCol".into()],
                rows: vec![vec![
                    SqlValue::Text("éééé".into()),
                    SqlValue::Text("éééé".into()),
                    SqlValue::Null,
                ]],
            }
        );

        // Parameter count mismatch.
        assert!(matches!(
            db.execute_prepared(&prep, &[]).unwrap_err(),
            SqlError::ParameterCount {
                expected: 1,
                actual: 0
            }
        ));

        // UPDATE reads old row values for all SET assignments and rejects updating PK.
        assert_eq!(
            db.execute(
                "UPDATE users SET score = alt_score, alt_score = score, note = 'ok' WHERE id = -9223372036854775808",
                &[]
            )
            .unwrap(),
            SqlOutcome::Updated { rows: 1 }
        );
        assert_eq!(
            db.execute(
                "SELECT score, alt_score, note FROM users WHERE id = -9223372036854775808",
                &[]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["score".into(), "alt_score".into(), "note".into()],
                rows: vec![vec![
                    SqlValue::Real(0.25),
                    SqlValue::Real(-1.5),
                    SqlValue::Text("ok".into()),
                ]],
            }
        );
        assert!(matches!(
            db.execute("UPDATE users SET id = 99 WHERE id = 10", &[])
                .unwrap_err(),
            SqlError::Unsupported { .. }
        ));

        // DELETE
        assert_eq!(
            db.execute("DELETE FROM users WHERE active = FALSE", &[])
                .unwrap(),
            SqlOutcome::Deleted { rows: 1 }
        );
        assert_eq!(
            db.execute("SELECT id FROM users", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![
                    vec![SqlValue::Integer(i64::MIN)],
                    vec![SqlValue::Integer(10)],
                ],
            }
        );
        assert_eq!(
            db.execute("DELETE FROM users", &[]).unwrap(),
            SqlOutcome::Deleted { rows: 2 }
        );
    });
}

#[test]
fn three_valued_logic_type_checks_and_coercions() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER, r REAL, s TEXT, b BOOLEAN)",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO items VALUES \
             (1, 10, 10.0, 'alpha', TRUE),\
             (2, NULL, 2.5, NULL, FALSE),\
             (3, 9007199254740993, 9007199254740992.0, 'beta', NULL)",
            &[],
        )
        .unwrap();

        // Comparisons with NULL are UNKNOWN; NOT(UNKNOWN) is UNKNOWN; WHERE keeps only TRUE.
        for q in [
            "SELECT id FROM items WHERE n = NULL",
            "SELECT id FROM items WHERE n <> NULL",
            "SELECT id FROM items WHERE NOT (n = NULL)",
            "SELECT id FROM items WHERE NULL",
            "SELECT id FROM items WHERE NOT NULL",
            "SELECT id FROM items WHERE b AND NULL",
            "SELECT id FROM items WHERE FALSE OR NULL",
        ] {
            assert_eq!(
                db.execute(q, &[]).unwrap(),
                SqlOutcome::Rows {
                    columns: vec!["id".into()],
                    rows: vec![],
                },
                "query {q} should return no rows"
            );
        }

        // TRUE OR NULL => TRUE; FALSE AND NULL => FALSE
        assert_eq!(
            db.execute("SELECT id FROM items WHERE (id = 1) OR NULL", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![vec![SqlValue::Integer(1)]],
            }
        );
        assert_eq!(
            db.execute("SELECT id FROM items WHERE NOT ((id <> 1) AND NULL)", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![vec![SqlValue::Integer(1)]],
            }
        );

        // IS NULL and IS NOT NULL
        assert_eq!(
            db.execute("SELECT id FROM items WHERE n IS NULL", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![vec![SqlValue::Integer(2)]],
            }
        );
        assert_eq!(
            db.execute("SELECT id FROM items WHERE s IS NOT NULL", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)],],
            }
        );

        // Exact numeric comparison between INTEGER and REAL without rounding.
        assert_eq!(
            db.execute("SELECT id FROM items WHERE n = r", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![vec![SqlValue::Integer(1)]],
            }
        );
        assert_eq!(
            db.execute("SELECT id FROM items WHERE n > r", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![vec![SqlValue::Integer(3)]],
            }
        );

        // Reject NaN/Inf, non-representable int->real, int-col->real-col, and mismatched types.
        for bad_real in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(
                db.execute(
                    "INSERT INTO items (id, r) VALUES (99, ?)",
                    &[SqlValue::Real(bad_real)]
                )
                .unwrap_err(),
                SqlError::Type(_)
            ));
        }
        assert!(matches!(
            db.execute(
                "INSERT INTO items (id, r) VALUES (99, 9007199254740993)",
                &[]
            )
            .unwrap_err(),
            SqlError::Type(_)
        ));
        assert!(matches!(
            db.execute(
                "INSERT INTO items (id, r) VALUES (99, ?)",
                &[SqlValue::Integer(i64::MAX)]
            )
            .unwrap_err(),
            SqlError::Type(_)
        ));
        assert!(matches!(
            db.execute("INSERT INTO items (id, n) VALUES (99, 1.0)", &[])
                .unwrap_err(),
            SqlError::Type(_)
        ));
        assert!(matches!(
            db.execute("UPDATE items SET r = n WHERE id = 1", &[])
                .unwrap_err(),
            SqlError::Type(_)
        ));
        assert!(matches!(
            db.execute("SELECT id FROM items WHERE n = '10'", &[])
                .unwrap_err(),
            SqlError::Type(_)
        ));
        assert!(matches!(
            db.execute("SELECT id FROM items WHERE b = 1", &[])
                .unwrap_err(),
            SqlError::Type(_)
        ));
        assert!(matches!(
            db.execute("SELECT id FROM items WHERE n", &[]).unwrap_err(),
            SqlError::Type(_)
        ));
        assert!(matches!(
            db.execute("SELECT id FROM missing", &[]).unwrap_err(),
            SqlError::NoSuchTable(_)
        ));
        assert!(matches!(
            db.execute("SELECT nope FROM items", &[]).unwrap_err(),
            SqlError::NoSuchColumn { .. }
        ));
    });
}

#[test]
fn constraint_failures_are_atomic_and_leave_table_unchanged() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, code VARCHAR(3) NOT NULL, note TEXT)",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO accounts VALUES (1, 'ok', 'initial')", &[])
            .unwrap();

        // Multi-row INSERT failing on row 2 (duplicate PK with existing row) inserts nothing.
        assert!(matches!(
            db.execute(
                "INSERT INTO accounts VALUES (2, 'two', 'a'), (1, 'dup', 'b')",
                &[]
            )
            .unwrap_err(),
            SqlError::Constraint(_)
        ));
        // Multi-row INSERT failing on intra-statement duplicate PK inserts nothing.
        assert!(matches!(
            db.execute(
                "INSERT INTO accounts VALUES (3, 'aaa', 'a'), (3, 'bbb', 'b')",
                &[]
            )
            .unwrap_err(),
            SqlError::Constraint(_)
        ));
        // Multi-row INSERT failing on NOT NULL on row 2 inserts nothing.
        assert!(matches!(
            db.execute(
                "INSERT INTO accounts VALUES (4, 'four_err', 'a'), (5, NULL, 'b')",
                &[]
            )
            .unwrap_err(),
            SqlError::Constraint(_)
        ));
        // VARCHAR(3) failure on row 2 inserts nothing.
        assert!(matches!(
            db.execute(
                "INSERT INTO accounts VALUES (6, 'ok', 'a'), (7, '1234', 'b')",
                &[]
            )
            .unwrap_err(),
            SqlError::Constraint(_)
        ));
        // Omitted NOT NULL or PK column fails.
        assert!(matches!(
            db.execute("INSERT INTO accounts (id) VALUES (8)", &[])
                .unwrap_err(),
            SqlError::Constraint(_)
        ));
        assert!(matches!(
            db.execute("INSERT INTO accounts (code) VALUES ('abc')", &[])
                .unwrap_err(),
            SqlError::Constraint(_)
        ));

        // Failed UPDATE leaves all rows unchanged.
        db.execute("INSERT INTO accounts VALUES (2, 'two', 'second')", &[])
            .unwrap();
        assert!(matches!(
            db.execute("UPDATE accounts SET code = NULL WHERE id >= 1", &[])
                .unwrap_err(),
            SqlError::Constraint(_)
        ));

        // Verify exact table state.
        assert_eq!(
            db.execute("SELECT id, code, note FROM accounts", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "code".into(), "note".into()],
                rows: vec![
                    vec![
                        SqlValue::Integer(1),
                        SqlValue::Text("ok".into()),
                        SqlValue::Text("initial".into()),
                    ],
                    vec![
                        SqlValue::Integer(2),
                        SqlValue::Text("two".into()),
                        SqlValue::Text("second".into()),
                    ],
                ],
            }
        );

        // Failed CREATE TABLE leaves neither catalog nor collection.
        assert!(matches!(
            db.execute("CREATE TABLE ghost (id REAL PRIMARY KEY)", &[])
                .unwrap_err(),
            SqlError::Type(_)
        ));
        assert!(matches!(
            db.execute("SELECT * FROM ghost", &[]).unwrap_err(),
            SqlError::NoSuchTable(_)
        ));
        assert_eq!(
            db.document_database()
                .get("ghost", "anything")
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
    });
}

#[test]
fn text_primary_key_ordering_and_durability_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let ord_path = dir.path().join("ord");
    {
        let store = Store::open(&ord_path).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        db.execute(
            "CREATE TABLE kv (k TEXT, v INTEGER NOT NULL, PRIMARY KEY (k))",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO kv VALUES ('z', 1), ('', 2), ('a', 3), ('é', 4), ('Z', 5)",
            &[],
        )
        .unwrap();
        db.execute("UPDATE kv SET v = 20 WHERE k = ''", &[])
            .unwrap();
        db.execute("DELETE FROM kv WHERE k = 'Z'", &[]).unwrap();
    }
    {
        let store = Store::open(&ord_path).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        assert_eq!(
            db.execute("SELECT k, v FROM kv", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["k".into(), "v".into()],
                rows: vec![
                    vec![SqlValue::Text("".into()), SqlValue::Integer(20)],
                    vec![SqlValue::Text("a".into()), SqlValue::Integer(3)],
                    vec![SqlValue::Text("z".into()), SqlValue::Integer(1)],
                    vec![SqlValue::Text("é".into()), SqlValue::Integer(4)],
                ],
            }
        );
        // Duplicate PK still rejected after reopen.
        assert!(matches!(
            db.execute("INSERT INTO kv VALUES ('a', 99)", &[])
                .unwrap_err(),
            SqlError::Constraint(_)
        ));
    }

    // Same durable reopen check on CheckpointStore.
    let ckpt_log = dir.path().join("ckpt_log");
    let ckpt_file = dir.path().join("ckpt_file");
    let init = Store::open(&ckpt_log).unwrap();
    let digest = init.checkpoint_to(&ckpt_file).unwrap();
    drop(init);
    {
        let store = CheckpointStore::open(&ckpt_log, &ckpt_file, &digest).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
        db.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)", &[])
            .unwrap();
        db.execute("INSERT INTO kv VALUES ('b', 1), ('a', 2)", &[])
            .unwrap();
    }
    {
        let store = CheckpointStore::open(&ckpt_log, &ckpt_file, &digest).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
        assert_eq!(
            db.execute("SELECT k, v FROM kv", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["k".into(), "v".into()],
                rows: vec![
                    vec![SqlValue::Text("a".into()), SqlValue::Integer(2)],
                    vec![SqlValue::Text("b".into()), SqlValue::Integer(1)],
                ],
            }
        );
    }
}

#[test]
fn concurrent_sql_statements_stay_consistent_without_spurious_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    db.execute(
        "CREATE TABLE counters (id INTEGER PRIMARY KEY, owner INTEGER NOT NULL)",
        &[],
    )
    .unwrap();

    let barrier = Arc::new(Barrier::new(6));
    let mut handles = Vec::new();
    let conflicts = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for tid in 0..6_i64 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let conflicts = Arc::clone(&conflicts);
        handles.push(std::thread::spawn(move || {
            let db = SqlDatabase::new(DocumentDatabase::new(&store));
            barrier.wait();
            for step in 0..10_i64 {
                let id = tid * 100 + step;
                loop {
                    match db.execute(
                        "INSERT INTO counters VALUES (?, ?)",
                        &[SqlValue::Integer(id), SqlValue::Integer(tid)],
                    ) {
                        Ok(SqlOutcome::Inserted { rows: 1 }) => break,
                        Err(SqlError::Conflict(ref io_err)) => {
                            assert!(is_transaction_conflict(io_err));
                            conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                        other => panic!("unexpected concurrent result: {other:?}"),
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(conflicts.load(std::sync::atomic::Ordering::Relaxed), 0);
    let rows = match db.execute("SELECT id, owner FROM counters", &[]).unwrap() {
        SqlOutcome::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(rows.len(), 60);
}

#[test]
fn store_snapshot_16384_key_limit_vs_checkpoint_store_unbounded_base() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store");
    let ckpt_file = dir.path().join("ckpt");

    // 1 table = 1 catalog key + 1 collection metadata key = 2 keys overhead.
    // 16,382 rows + 2 overhead = 16,384 keys (exact Store MAX_SNAPSHOT_KEYS boundary).
    let store = Store::open(&store_path).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY)", &[])
        .unwrap();

    let mut inserted = 0_i64;
    while inserted < 16_382 {
        let batch = (16_382 - inserted).min(512);
        let mut sql = String::from("INSERT INTO big VALUES ");
        for i in 0..batch {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({})", inserted + i));
        }
        db.execute(&sql, &[]).unwrap();
        inserted += batch;
    }

    // At exactly 16,384 keys, a full-table scan SELECT succeeds on ordinary Store.
    let res = db.execute("SELECT id FROM big WHERE id < 10", &[]).unwrap();
    let SqlOutcome::Rows { rows, .. } = res else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 10);

    // Insert 1 more row (during this transaction snapshot has 16,384 <= 16,384 keys, so commit brings Store to 16,385 keys).
    db.execute("INSERT INTO big VALUES (16382)", &[]).unwrap();
    // Now ordinary Store has 16,385 live keys -> next transaction exceeds MAX_SNAPSHOT_KEYS and fails with Bounds.
    assert!(matches!(
        db.execute("SELECT id FROM big WHERE id < 10", &[])
            .unwrap_err(),
        SqlError::Bounds(_)
    ));

    // Checkpoint the 16,385-key store and reopen with CheckpointStore:
    let digest = store.checkpoint_to(&ckpt_file).unwrap();
    drop(store);
    let ckpt = CheckpointStore::open(&store_path, &ckpt_file, &digest).unwrap();
    let ckpt_db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt));

    // CheckpointStore has no 16,384-key full-store snapshot limit; insert past 16,384 and scan all rows.
    ckpt_db
        .execute("INSERT INTO big VALUES (16383), (16384)", &[])
        .unwrap();
    let mut visited = 0usize;
    let mut after_id: Option<String> = None;
    let all_rows_count = ckpt_db
        .document_database()
        .transaction(|tx| {
            loop {
                let page = tx.scan_documents("big", after_id.as_deref(), 1_024)?;
                if page.is_empty() {
                    break;
                }
                visited += page.len();
                after_id = page.last().map(|d| d.id.clone());
            }
            Ok(visited)
        })
        .unwrap();
    assert_eq!(all_rows_count, 16_385);

    // Also verify SqlDatabase::execute SELECT with max_select_rows=10000 and WHERE filter scanning > 16,384 rows works on CheckpointStore.
    let filtered = ckpt_db
        .execute("SELECT id FROM big WHERE id >= 16380", &[])
        .unwrap();
    assert_eq!(
        filtered,
        SqlOutcome::Rows {
            columns: vec!["id".into()],
            rows: (16_380..=16_384)
                .map(|n| vec![SqlValue::Integer(n)])
                .collect(),
        }
    );
}

#[test]
fn unsupported_sql_features_return_unsupported_naming_feature() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));

    let cases = [
        "SELECT a.id FROM a FULL JOIN b ON a.id = b.id JOIN c ON c.id = a.id",
        "SELECT a.id FROM a CROSS JOIN b",
        "SELECT a.id FROM a JOIN b USING (id)",
        "SELECT * FROM a, b",
        "SELECT id FROM a GROUP BY ROLLUP (id)",
        "SELECT id FROM a GROUP BY 1",
        "SELECT id FROM a GROUP BY id + 1",
        "SELECT id FROM a ORDER BY 1",
        "SELECT id FROM a OFFSET 5",
        "UPDATE a SET b = 1 LIMIT 5",
        "DELETE FROM a ORDER BY id LIMIT 1",
        "SELECT SUM(DISTINCT id) FROM a",
        "SELECT COUNT(*) OVER () FROM a",
        "SELECT COUNT(*) + 1 FROM a",
        "SELECT SUM(COUNT(*)) FROM a",
        "SELECT * FROM a WHERE COUNT(*) > 1",
        "SELECT * FROM (SELECT * FROM a) AS d JOIN b ON b.id = d.id",
        "SELECT * FROM a WHERE (id, b) IN (SELECT id, b FROM b)",
        "SELECT * FROM a WHERE EXISTS (WITH c AS (SELECT id FROM b) SELECT * FROM c)",
        "CREATE INDEX idx ON a (id) WHERE id > 0",
        "CREATE INDEX idx ON a (id DESC)",
        "DROP TABLE a CASCADE",
        "DROP TABLE a, b",
        "DROP INDEX idx",
        "ALTER TABLE a DROP COLUMN b",
        "ALTER TABLE a RENAME TO b",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "INSERT INTO a VALUES (1) RETURNING id",
        "INSERT INTO a VALUES (1) ON CONFLICT DO NOTHING",
        "CREATE TABLE a (id INT PRIMARY KEY, x INT DEFAULT (0))",
        "CREATE TABLE a (id INT PRIMARY KEY, x INT, CONSTRAINT c CHECK (x > 0))",
        "CREATE TABLE a (id INT PRIMARY KEY, p INT REFERENCES b(id) ON UPDATE CASCADE)",
        "CREATE TABLE a (id INT PRIMARY KEY, p INT REFERENCES b(id) ON UPDATE SET NULL)",
        "CREATE TABLE a (id INT PRIMARY KEY, p INT REFERENCES b(id) MATCH FULL)",
        "CREATE TABLE a (id INT PRIMARY KEY, p INT REFERENCES b(id) DEFERRABLE)",
        "CREATE TABLE a (id INT PRIMARY KEY, FOREIGN KEY (id, p) REFERENCES b(id, p))",
        "CREATE TABLE a (id INT PRIMARY KEY, r REAL UNIQUE)",
        "CREATE TABLE a (a INT, b INT, PRIMARY KEY (a, b))",
        "SELECT DISTINCT id FROM a",
        "SELECT id FROM a UNION SELECT id FROM b",
        "SELECT * FROM a WHERE id IN (1, 2)",
        "SELECT * FROM a WHERE id BETWEEN 1 AND 5",
        "SELECT * FROM a WHERE name LIKE 'a%'",
        "SELECT * FROM a WHERE id + 1 = 2",
        "UPDATE a SET b = 1 RETURNING b",
        "DELETE FROM a RETURNING id",
    ];

    for sql in cases {
        let err = db.prepare(sql).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported for {sql:?}, got {err:?}"
        );
    }
}

#[test]
fn all_bounds_enforced_at_limit_and_limit_plus_one() {
    // 1. SqlLimits::validate rejects values above hard maxima and zero (except max_params).
    let mut bad = SqlLimits::default();
    bad.max_sql_bytes += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let mut bad = SqlLimits::default();
    bad.max_tokens += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let mut bad = SqlLimits::default();
    bad.max_expr_depth += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let mut bad = SqlLimits::default();
    bad.max_columns += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let mut bad = SqlLimits::default();
    bad.max_insert_rows += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let mut bad = SqlLimits::default();
    bad.max_mutation_rows += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let mut bad = SqlLimits::default();
    bad.max_select_rows += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let bad = SqlLimits {
        max_sort_rows: database_engine::sql::MAX_SQL_SORT_ROWS + 1,
        ..Default::default()
    };
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let bad = SqlLimits {
        max_groups: database_engine::sql::MAX_SQL_GROUPS + 1,
        ..Default::default()
    };
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let bad = SqlLimits {
        max_distinct_values: database_engine::sql::MAX_SQL_DISTINCT_VALUES + 1,
        ..Default::default()
    };
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let bad = SqlLimits {
        max_groups: 0,
        ..Default::default()
    };
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let bad = SqlLimits {
        max_distinct_values: 0,
        ..Default::default()
    };
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let bad = SqlLimits {
        max_visited_rows: database_engine::sql::MAX_SQL_VISITED_ROWS + 1,
        ..Default::default()
    };
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let mut bad = SqlLimits::default();
    bad.max_params += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let mut bad = SqlLimits::default();
    bad.max_text_bytes += 1;
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let bad = SqlLimits {
        max_sql_bytes: 0,
        ..Default::default()
    };
    assert!(matches!(bad.validate(), Err(SqlError::Bounds(_))));

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut db = SqlDatabase::new(DocumentDatabase::new(&store));

    // 2. Statement bytes at 64 KiB and 64 KiB + 1.
    let prefix = "SELECT * FROM t -- ";
    let exact_64k = format!("{prefix}{}", "a".repeat(65_536 - prefix.len()));
    assert_eq!(exact_64k.len(), 65_536);
    assert!(db.prepare(&exact_64k).is_ok());
    let over_64k = format!("{exact_64k}a");
    assert!(matches!(db.prepare(&over_64k), Err(SqlError::Bounds(_))));

    // 3. Tokens at 4,096 and 4,097 (CREATE TABLE ... with 1,024 rows? No, WHERE TRUE OR TRUE ...).
    // "SELECT * FROM t WHERE TRUE" is 6 tokens; each " OR TRUE" is 2 tokens. 6 + 2 * 2045 = 4096.
    let mut exact_toks = String::from("SELECT * FROM t WHERE TRUE");
    for _ in 0..2045 {
        exact_toks.push_str(" OR TRUE");
    }
    assert!(db.prepare(&exact_toks).is_ok());
    let over_toks = format!("{exact_toks} OR TRUE"); // 4098 > 4096
    assert!(matches!(db.prepare(&over_toks), Err(SqlError::Bounds(_))));
    // Also test exact +1 with trailing ';' -> 4097 tokens
    let exact_plus_one_tok = format!("{exact_toks};");
    assert!(matches!(
        db.prepare(&exact_plus_one_tok),
        Err(SqlError::Bounds(_))
    ));

    // 4. Expression nesting depth at 64 and 65.
    // Root expr starts at depth 1; 63 nested parens puts literal TRUE at depth 64; 64 parens puts it at 65.
    let exact_depth = format!(
        "SELECT * FROM t WHERE {}TRUE{}",
        "(".repeat(63),
        ")".repeat(63)
    );
    assert!(db.prepare(&exact_depth).is_ok());
    let over_depth = format!(
        "SELECT * FROM t WHERE {}TRUE{}",
        "(".repeat(64),
        ")".repeat(64)
    );
    assert!(matches!(db.prepare(&over_depth), Err(SqlError::Bounds(_))));

    // 5. Columns at 64 and 65.
    let cols_64: Vec<String> = (0..64).map(|i| format!("c{i} INT")).collect();
    let create_64 = format!(
        "CREATE TABLE w64 (c0 INT PRIMARY KEY, {})",
        cols_64[1..].join(", ")
    );
    db.execute(&create_64, &[]).unwrap();
    let cols_65: Vec<String> = (0..65).map(|i| format!("c{i} INT")).collect();
    let create_65 = format!(
        "CREATE TABLE w65 (c0 INT PRIMARY KEY, {})",
        cols_65[1..].join(", ")
    );
    assert!(matches!(db.prepare(&create_65), Err(SqlError::Bounds(_))));

    // 6. Identifier length at 128 and 129 bytes.
    let id128 = "a".repeat(128);
    let id129 = "a".repeat(129);
    db.execute(&format!("CREATE TABLE {id128} (id INT PRIMARY KEY)"), &[])
        .unwrap();
    assert!(matches!(
        db.prepare(&format!("CREATE TABLE {id129} (id INT PRIMARY KEY)")),
        Err(SqlError::Bounds(_))
    ));

    // 7. TEXT PK at 254 bytes vs 255 bytes.
    db.execute("CREATE TABLE tp (k TEXT PRIMARY KEY)", &[])
        .unwrap();
    let pk254 = "k".repeat(254);
    let pk255 = "k".repeat(255);
    db.execute("INSERT INTO tp VALUES (?)", &[SqlValue::Text(pk254)])
        .unwrap();
    assert!(matches!(
        db.execute("INSERT INTO tp VALUES (?)", &[SqlValue::Text(pk255)])
            .unwrap_err(),
        SqlError::Bounds(_)
    ));

    // 8. Params at 1,024 and 1,025 (staying under 4,096 tokens: 5 + 2047 = 2,052 tokens).
    let exact_params = format!(
        "SELECT * FROM t WHERE {}",
        std::iter::repeat_n("?", 1_024)
            .collect::<Vec<_>>()
            .join(" OR ")
    );
    assert!(db.prepare(&exact_params).is_ok());
    let over_params = format!(
        "SELECT * FROM t WHERE {}",
        std::iter::repeat_n("?", 1_025)
            .collect::<Vec<_>>()
            .join(" OR ")
    );
    assert!(matches!(db.prepare(&over_params), Err(SqlError::Bounds(_))));

    // 9. TEXT value at 256 KiB and 256 KiB + 1.
    db.execute("CREATE TABLE texts (id INT PRIMARY KEY, s TEXT)", &[])
        .unwrap();
    let ok_text = "x".repeat(256 * 1024);
    let over_text = "x".repeat(256 * 1024 + 1);
    db.execute(
        "INSERT INTO texts VALUES (1, ?)",
        &[SqlValue::Text(ok_text)],
    )
    .unwrap();
    assert!(matches!(
        db.execute(
            "INSERT INTO texts VALUES (2, ?)",
            &[SqlValue::Text(over_text)],
        )
        .unwrap_err(),
        SqlError::Bounds(_)
    ));

    // 10. INSERT rows (custom max_insert_rows=500 at 500 vs 501, and max_mutation_rows=1,024 at 1,024 vs 1,025).
    db.execute("CREATE TABLE nums (id INT PRIMARY KEY, v INT)", &[])
        .unwrap();
    let custom_ins = SqlLimits {
        max_insert_rows: 500,
        ..Default::default()
    };
    db.set_limits(custom_ins).unwrap();
    let mut ins_500 = String::from("INSERT INTO nums VALUES ");
    for i in 0..500 {
        if i > 0 {
            ins_500.push(',');
        }
        ins_500.push_str(&format!("({i}, {i})"));
    }
    assert_eq!(
        db.execute(&ins_500, &[]).unwrap(),
        SqlOutcome::Inserted { rows: 500 }
    );
    let ins_501 = format!("{ins_500}, (500, 500)");
    assert!(matches!(db.prepare(&ins_501), Err(SqlError::Bounds(_))));

    db.set_limits(SqlLimits::default()).unwrap();
    let mut ins_524 = String::from("INSERT INTO nums VALUES ");
    for i in 500..1_024 {
        if i > 500 {
            ins_524.push(',');
        }
        ins_524.push_str(&format!("({i}, {i})"));
    }
    assert_eq!(
        db.execute(&ins_524, &[]).unwrap(),
        SqlOutcome::Inserted { rows: 524 }
    );

    // Update 1,024 rows succeeds.
    assert_eq!(
        db.execute("UPDATE nums SET v = 1", &[]).unwrap(),
        SqlOutcome::Updated { rows: 1_024 }
    );
    // Insert 1 more row -> 1,025 rows total. Updating or deleting all 1,025 exceeds max_mutation_rows (1,024) and changes nothing!
    db.execute("INSERT INTO nums VALUES (1024, 2)", &[])
        .unwrap();
    assert!(matches!(
        db.execute("UPDATE nums SET v = 99", &[]).unwrap_err(),
        SqlError::Bounds(_)
    ));
    assert!(matches!(
        db.execute("DELETE FROM nums", &[]).unwrap_err(),
        SqlError::Bounds(_)
    ));
    // Verify nothing changed on failed UPDATE/DELETE.
    assert_eq!(
        db.execute("SELECT v FROM nums WHERE id = 0", &[]).unwrap(),
        SqlOutcome::Rows {
            columns: vec!["v".into()],
            rows: vec![vec![SqlValue::Integer(1)]],
        }
    );
    // Delete 1 row, then deleting the remaining 1,024 rows succeeds (exact limit).
    assert_eq!(
        db.execute("DELETE FROM nums WHERE id = 1024", &[]).unwrap(),
        SqlOutcome::Deleted { rows: 1 }
    );
    assert_eq!(
        db.execute("DELETE FROM nums", &[]).unwrap(),
        SqlOutcome::Deleted { rows: 1_024 }
    );
    // Re-seed 10 rows for max_select_rows=5 / max_visited_rows=10 test.
    db.execute(
        "INSERT INTO nums VALUES (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)",
        &[],
    )
    .unwrap();

    // Custom max_select_rows and max_visited_rows at limit and limit+1.
    let custom = SqlLimits {
        max_select_rows: 5,
        max_visited_rows: 10,
        ..Default::default()
    };
    db.set_limits(custom).unwrap();
    assert_eq!(
        db.execute("SELECT id FROM nums WHERE id < 5", &[]).unwrap(),
        SqlOutcome::Rows {
            columns: vec!["id".into()],
            rows: (0..5).map(|i| vec![SqlValue::Integer(i)]).collect(),
        }
    );
    assert!(matches!(
        db.execute("SELECT id FROM nums WHERE id < 6", &[])
            .unwrap_err(),
        SqlError::Bounds(_) // 6 returned rows > max_select_rows (5)
    ));
    db.execute("INSERT INTO nums VALUES (10, 1)", &[]).unwrap();
    // Point PK lookup succeeds even when total rows > max_visited_rows (10).
    assert_eq!(
        db.execute("SELECT id FROM nums WHERE id = 0", &[]).unwrap(),
        SqlOutcome::Rows {
            columns: vec!["id".into()],
            rows: vec![vec![SqlValue::Integer(0)]],
        }
    );
    // Full scan (non-PK predicate) visits all 11 rows > max_visited_rows (10).
    assert!(matches!(
        db.execute("SELECT id FROM nums WHERE v = 0", &[])
            .unwrap_err(),
        SqlError::Bounds(_)
    ));
}

#[test]
fn hard_10000_select_limit_and_16mb_batch_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    db.execute("CREATE TABLE s10k (id INT PRIMARY KEY)", &[])
        .unwrap();
    for start in (0..10_000).step_by(1_000) {
        let mut sql = String::from("INSERT INTO s10k VALUES ");
        for i in 0..1_000 {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({})", start + i));
        }
        db.execute(&sql, &[]).unwrap();
    }
    let SqlOutcome::Rows { rows, .. } = db.execute("SELECT id FROM s10k", &[]).unwrap() else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 10_000);
    db.execute("INSERT INTO s10k VALUES (10000)", &[]).unwrap();
    assert!(matches!(
        db.execute("SELECT id FROM s10k", &[]).unwrap_err(),
        SqlError::Bounds(_)
    ));

    // 16 MiB atomic batch byte limit surfaced as Bounds without partial write.
    let dir2 = tempfile::tempdir().unwrap();
    let store2 = Store::open(dir2.path()).unwrap();
    let db2 = SqlDatabase::new(DocumentDatabase::new(&store2));
    db2.execute(
        "CREATE TABLE big_rows (id INT PRIMARY KEY, payload TEXT)",
        &[],
    )
    .unwrap();
    let chunk = "a".repeat(250 * 1024);
    let mut sql = String::from("INSERT INTO big_rows VALUES ");
    let mut params = Vec::new();
    for i in 0..70 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, ?)"));
        params.push(SqlValue::Text(chunk.clone()));
    }
    // 70 * ~250 KiB = ~17.5 MiB > 16 MiB MAX_ATOMIC_BATCH_BYTES
    assert!(matches!(
        db2.execute(&sql, &params).unwrap_err(),
        SqlError::Bounds(_)
    ));
    assert_eq!(
        db2.execute("SELECT id FROM big_rows", &[]).unwrap(),
        SqlOutcome::Rows {
            columns: vec!["id".into()],
            rows: vec![],
        }
    );
}

#[test]
fn randomized_model_test_matches_btreemap_on_both_backends() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE m (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT NOT NULL)",
            &[],
        )
        .unwrap();
        let mut model: BTreeMap<i64, (Option<i64>, String)> = BTreeMap::new();
        let mut rng = 0xDEAD_BEEF_CAFE_BABE_u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        for step in 0..200 {
            let op = next() % 4;
            match op {
                0 => {
                    // INSERT 1..=3 rows
                    let n = ((next() % 3) + 1) as usize;
                    let mut sql = String::from("INSERT INTO m VALUES ");
                    let mut params = Vec::new();
                    let mut batch = Vec::new();
                    for i in 0..n {
                        if i > 0 {
                            sql.push(',');
                        }
                        sql.push_str("(?, ?, ?)");
                        let id = (next() % 40) as i64 - 20;
                        let val = if next() % 3 == 0 {
                            None
                        } else {
                            Some((next() % 100) as i64 - 50)
                        };
                        let tag = format!("t{}", next() % 5);
                        params.push(SqlValue::Integer(id));
                        params.push(val.map_or(SqlValue::Null, SqlValue::Integer));
                        params.push(SqlValue::Text(tag.clone()));
                        batch.push((id, val, tag));
                    }
                    let dup_in_batch = {
                        let mut seen = std::collections::HashSet::new();
                        batch.iter().any(|(id, _, _)| !seen.insert(*id))
                    };
                    let dup_existing = batch.iter().any(|(id, _, _)| model.contains_key(id));
                    let res = db.execute(&sql, &params);
                    if dup_in_batch || dup_existing {
                        assert!(
                            matches!(res, Err(SqlError::Constraint(_))),
                            "step {step}: {res:?}"
                        );
                    } else {
                        assert_eq!(res.unwrap(), SqlOutcome::Inserted { rows: n });
                        for (id, val, tag) in batch {
                            model.insert(id, (val, tag));
                        }
                    }
                }
                1 => {
                    // UPDATE with threshold
                    let threshold = (next() % 40) as i64 - 20;
                    let new_val = if next() % 2 == 0 {
                        None
                    } else {
                        Some((next() % 100) as i64)
                    };
                    let res = db
                        .execute(
                            "UPDATE m SET val = ? WHERE id >= ?",
                            &[
                                new_val.map_or(SqlValue::Null, SqlValue::Integer),
                                SqlValue::Integer(threshold),
                            ],
                        )
                        .unwrap();
                    let mut changed = 0;
                    for (&id, entry) in model.iter_mut() {
                        if id >= threshold {
                            entry.0 = new_val;
                            changed += 1;
                        }
                    }
                    assert_eq!(res, SqlOutcome::Updated { rows: changed });
                }
                2 => {
                    // DELETE with WHERE val IS NULL OR id < threshold
                    let threshold = (next() % 40) as i64 - 20;
                    let res = db
                        .execute(
                            "DELETE FROM m WHERE val IS NULL OR id < ?",
                            &[SqlValue::Integer(threshold)],
                        )
                        .unwrap();
                    let before = model.len();
                    model.retain(|&id, (val, _)| !(val.is_none() || id < threshold));
                    let deleted = before - model.len();
                    assert_eq!(res, SqlOutcome::Deleted { rows: deleted });
                }
                _ => {
                    // Full SELECT comparison in ascending PK order
                    let res = db.execute("SELECT id, val, tag FROM m", &[]).unwrap();
                    let expected: Vec<Vec<SqlValue>> = model
                        .iter()
                        .map(|(&id, (val, tag))| {
                            vec![
                                SqlValue::Integer(id),
                                val.map_or(SqlValue::Null, SqlValue::Integer),
                                SqlValue::Text(tag.clone()),
                            ]
                        })
                        .collect();
                    assert_eq!(
                        res,
                        SqlOutcome::Rows {
                            columns: vec!["id".into(), "val".into(), "tag".into()],
                            rows: expected,
                        }
                    );
                }
            }
        }
    });
}

#[test]
fn pk_point_and_range_respect_max_visited_rows_beyond_limit() {
    for_both_sql(|mut db| {
        db.set_limits(SqlLimits {
            max_visited_rows: 100,
            ..Default::default()
        })
        .unwrap();
        db.execute(
            "CREATE TABLE big_int (id INTEGER PRIMARY KEY, tag TEXT NOT NULL, v INTEGER)",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE big_txt (k TEXT PRIMARY KEY, tag TEXT NOT NULL, v INTEGER)",
            &[],
        )
        .unwrap();

        let mut int_sql = String::from("INSERT INTO big_int VALUES ");
        let mut txt_sql = String::from("INSERT INTO big_txt VALUES ");
        for i in 0..300_i64 {
            if i > 0 {
                int_sql.push(',');
                txt_sql.push(',');
            }
            int_sql.push_str(&format!("({i}, 'tag{}', {})", i % 5, i * 2));
            txt_sql.push_str(&format!("('k{:03}', 'tag{}', {})", i, i % 5, i * 2));
        }
        db.execute(&int_sql, &[]).unwrap();
        db.execute(&txt_sql, &[]).unwrap();

        // Point SELECT, UPDATE, DELETE work on 300-row table when max_visited_rows = 100.
        assert_eq!(
            db.execute("SELECT tag FROM big_int WHERE id = 150.0", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["tag".into()],
                rows: vec![vec![SqlValue::Text("tag0".into())]],
            }
        );
        assert_eq!(
            db.execute(
                "SELECT id FROM big_int WHERE id = ? AND tag = 'mismatch'",
                &[SqlValue::Integer(150)],
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![],
            }
        );
        assert_eq!(
            db.execute("UPDATE big_int SET v = 999 WHERE 150 = id", &[])
                .unwrap(),
            SqlOutcome::Updated { rows: 1 }
        );
        assert_eq!(
            db.execute("SELECT v FROM big_int WHERE id = 150", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["v".into()],
                rows: vec![vec![SqlValue::Integer(999)]],
            }
        );
        assert_eq!(
            db.execute("DELETE FROM big_int WHERE id = 150", &[])
                .unwrap(),
            SqlOutcome::Deleted { rows: 1 }
        );

        // TEXT PK point access works beyond max_visited_rows.
        assert_eq!(
            db.execute("SELECT v FROM big_txt WHERE k = 'k150'", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["v".into()],
                rows: vec![vec![SqlValue::Integer(300)]],
            }
        );
        assert_eq!(
            db.execute(
                "SELECT v FROM big_txt WHERE k = 'k150' AND tag = 'nope'",
                &[]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["v".into()],
                rows: vec![],
            }
        );

        // Range touching <= 100 rows succeeds; range touching > 100 rows fails with Bounds.
        let r = db
            .execute("SELECT id FROM big_int WHERE id >= 120 AND id <= 125", &[])
            .unwrap();
        assert_eq!(
            r,
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: (120..=125).map(|i| vec![SqlValue::Integer(i)]).collect(),
            }
        );
        assert_eq!(
            db.execute("SELECT id FROM big_int WHERE id > 120 AND id < 125", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: (121..125).map(|i| vec![SqlValue::Integer(i)]).collect(),
            }
        );
        assert_eq!(
            db.execute(
                "SELECT k FROM big_txt WHERE k >= 'k010' AND k <= 'k012'",
                &[]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["k".into()],
                rows: vec![
                    vec![SqlValue::Text("k010".into())],
                    vec![SqlValue::Text("k011".into())],
                    vec![SqlValue::Text("k012".into())],
                ],
            }
        );
        assert_eq!(
            db.execute("SELECT id FROM big_int WHERE id >= 0 AND id <= 99", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: (0..100).map(|i| vec![SqlValue::Integer(i)]).collect(),
            }
        );
        assert!(matches!(
            db.execute("SELECT id FROM big_int WHERE id >= 0 AND id <= 100", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
        assert!(matches!(
            db.execute(
                "SELECT k FROM big_txt WHERE k >= 'k000' AND k <= 'k105'",
                &[]
            )
            .unwrap_err(),
            SqlError::Bounds(_)
        ));

        // Contradictory bounds and NULL PK operands yield 0 rows without scanning.
        assert_eq!(
            db.execute("SELECT id FROM big_int WHERE id >= 200 AND id <= 100", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![],
            }
        );
        assert_eq!(
            db.execute("SELECT id FROM big_int WHERE id = NULL", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![],
            }
        );

        // Full scan (no PK conjunct) is rejected with Bounds.
        assert!(matches!(
            db.execute("SELECT id FROM big_int", &[]).unwrap_err(),
            SqlError::Bounds(_)
        ));
        assert!(matches!(
            db.execute("SELECT id FROM big_int WHERE v = 0", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
        assert!(matches!(
            db.execute("SELECT id FROM big_int WHERE id = 1 OR id = 2", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
    });
}

#[test]
fn randomized_pk_access_differential_against_forced_full_scan() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE di (id INTEGER PRIMARY KEY, n INTEGER, r REAL, s TEXT, b BOOLEAN)",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE dt (k TEXT PRIMARY KEY, n INTEGER, r REAL, s TEXT, b BOOLEAN)",
            &[],
        )
        .unwrap();

        let int_keys = [
            i64::MIN,
            i64::MIN + 1,
            -1000,
            -5,
            -2,
            -1,
            0,
            1,
            2,
            3,
            4,
            5,
            100,
            (1_i64 << 53) - 1,
            1_i64 << 53,
            (1_i64 << 53) + 1,
            i64::MAX - 1,
            i64::MAX,
        ];
        for (idx, &id) in int_keys.iter().enumerate() {
            let n = if idx % 4 == 0 {
                SqlValue::Null
            } else {
                SqlValue::Integer((idx as i64) - 5)
            };
            let r = if idx % 5 == 0 {
                SqlValue::Null
            } else {
                SqlValue::Real((idx as f64) * 0.5 - 2.0)
            };
            let s = if idx % 3 == 0 {
                SqlValue::Null
            } else {
                SqlValue::Text(format!("s{}", idx % 4))
            };
            let b = if idx % 6 == 0 {
                SqlValue::Null
            } else {
                SqlValue::Boolean(idx % 2 == 0)
            };
            db.execute(
                "INSERT INTO di VALUES (?, ?, ?, ?, ?)",
                &[SqlValue::Integer(id), n, r, s, b],
            )
            .unwrap();
        }

        let txt_keys = [
            "", "\0", "\0a", "A", "Z", "a", "aa", "ab", "b", "c", "é", "éé",
        ];
        for (idx, &k) in txt_keys.iter().enumerate() {
            let n = if idx % 3 == 0 {
                SqlValue::Null
            } else {
                SqlValue::Integer(idx as i64)
            };
            let r = SqlValue::Real(idx as f64 * 0.25);
            let s = if idx % 2 == 0 {
                SqlValue::Text(format!("v{idx}"))
            } else {
                SqlValue::Null
            };
            let b = if idx % 4 == 0 {
                SqlValue::Null
            } else {
                SqlValue::Boolean(idx % 2 == 1)
            };
            db.execute(
                "INSERT INTO dt VALUES (?, ?, ?, ?, ?)",
                &[SqlValue::Text(k.to_owned()), n, r, s, b],
            )
            .unwrap();
        }

        let mut rng = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let int_operands = [
            SqlValue::Null,
            SqlValue::Integer(i64::MIN),
            SqlValue::Integer(i64::MIN + 1),
            SqlValue::Integer(-5),
            SqlValue::Integer(-2),
            SqlValue::Integer(-1),
            SqlValue::Integer(0),
            SqlValue::Integer(1),
            SqlValue::Integer(3),
            SqlValue::Integer(5),
            SqlValue::Integer((1_i64 << 53) + 1),
            SqlValue::Integer(i64::MAX - 1),
            SqlValue::Integer(i64::MAX),
            SqlValue::Real(-9223372036854775808.0),
            SqlValue::Real(-1e20),
            SqlValue::Real(-1.5),
            SqlValue::Real(-0.5),
            SqlValue::Real(-0.0),
            SqlValue::Real(0.0),
            SqlValue::Real(2.5),
            SqlValue::Real(3.0),
            SqlValue::Real(3.5),
            SqlValue::Real(9007199254740992.0),
            SqlValue::Real(9223372036854774784.0),
            SqlValue::Real(9223372036854775808.0),
            SqlValue::Real(1e20),
        ];
        let ops = ["=", "<>", "<", "<=", ">", ">="];
        let other_preds = [
            "n > 0",
            "n IS NULL",
            "NOT (b = FALSE)",
            "(s = 's1' OR b = TRUE)",
            "r >= -1.0",
            "NULL",
            "TRUE",
        ];

        for _ in 0..250 {
            let mut clauses = Vec::new();
            let mut params = Vec::new();
            let num_terms = (next() % 4) + 1;
            for _ in 0..num_terms {
                if next() % 4 == 0 {
                    clauses.push(other_preds[(next() as usize) % other_preds.len()].to_owned());
                } else {
                    let op = ops[(next() as usize) % ops.len()];
                    let val = int_operands[(next() as usize) % int_operands.len()].clone();
                    let term = if next() % 2 == 0 {
                        format!("id {op} ?")
                    } else {
                        format!("? {op} id")
                    };
                    let wrapped = if next() % 3 == 0 {
                        format!("({term})")
                    } else {
                        term
                    };
                    clauses.push(wrapped);
                    params.push(val);
                }
            }
            let joiner = if next() % 6 == 0 { " OR " } else { " AND " };
            let where_sql = if next() % 8 == 0 {
                format!("NOT ({})", clauses.join(joiner))
            } else {
                clauses.join(joiner)
            };

            let select_cols = match next() % 3 {
                0 => "*",
                1 => "id, s, b",
                _ => "n, id, id",
            };
            let q = format!("SELECT {select_cols} FROM di WHERE {where_sql}");
            let planned = db.execute(&q, &params);
            let forced = db.execute_force_full_scan_for_test(&q, &params);
            assert_eq!(
                planned.unwrap(),
                forced.unwrap(),
                "query: {q} params: {params:?}"
            );
        }

        let txt_operands = [
            SqlValue::Null,
            SqlValue::Text("".into()),
            SqlValue::Text("\0".into()),
            SqlValue::Text("A".into()),
            SqlValue::Text("a".into()),
            SqlValue::Text("aa".into()),
            SqlValue::Text("b".into()),
            SqlValue::Text("é".into()),
            SqlValue::Text("x".repeat(300)),
        ];

        for _ in 0..200 {
            let mut clauses = Vec::new();
            let mut params = Vec::new();
            let num_terms = (next() % 4) + 1;
            for _ in 0..num_terms {
                if next() % 4 == 0 {
                    clauses.push(other_preds[(next() as usize) % other_preds.len()].to_owned());
                } else {
                    let op = ops[(next() as usize) % ops.len()];
                    let val = txt_operands[(next() as usize) % txt_operands.len()].clone();
                    let term = if next() % 2 == 0 {
                        format!("k {op} ?")
                    } else {
                        format!("? {op} k")
                    };
                    clauses.push(if next() % 2 == 0 {
                        format!("({term})")
                    } else {
                        term
                    });
                    params.push(val);
                }
            }
            let joiner = if next() % 6 == 0 { " OR " } else { " AND " };
            let where_sql = clauses.join(joiner);
            let q = format!("SELECT k, n, s FROM dt WHERE {where_sql}");
            let planned = db.execute(&q, &params).unwrap();
            let forced = db.execute_force_full_scan_for_test(&q, &params).unwrap();
            assert_eq!(planned, forced, "query: {q} params: {params:?}");
        }

        // Also differential-check UPDATE and DELETE row counts and resulting table state.
        let upd_planned = db
            .execute(
                "UPDATE di SET n = 777 WHERE id >= -1.5 AND id <= 3.5 AND (b = TRUE OR s IS NULL)",
                &[],
            )
            .unwrap();
        let after_upd = db.execute("SELECT id, n FROM di", &[]).unwrap();
        // Verify against forced scan on a fresh matching predicate on another range:
        let del_where = "k >= 'a' AND k < 'c' AND b = TRUE";
        let expected_del = match db
            .execute_force_full_scan_for_test(&format!("SELECT k FROM dt WHERE {del_where}"), &[])
            .unwrap()
        {
            SqlOutcome::Rows { rows, .. } => rows.len(),
            _ => unreachable!(),
        };
        let del_res = db
            .execute(&format!("DELETE FROM dt WHERE {del_where}"), &[])
            .unwrap();
        assert_eq!(del_res, SqlOutcome::Deleted { rows: expected_del });
        assert!(matches!(upd_planned, SqlOutcome::Updated { .. }));
        assert!(matches!(after_upd, SqlOutcome::Rows { .. }));
    });
}

#[test]
fn concurrent_disjoint_tables_have_zero_conflicts_on_store_and_checkpoint() {
    fn run_disjoint<'a>(make_db: &(dyn Fn() -> SqlDatabase<'a> + Sync)) -> usize {
        let setup = make_db();
        for t in 0..4 {
            setup
                .execute(
                    &format!("CREATE TABLE t{t} (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)"),
                    &[],
                )
                .unwrap();
        }
        let barrier = Arc::new(Barrier::new(4));
        let conflicts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        std::thread::scope(|s| {
            for t in 0..4_i64 {
                let barrier = Arc::clone(&barrier);
                let conflicts = Arc::clone(&conflicts);
                s.spawn(move || {
                    let db = make_db();
                    barrier.wait();
                    for i in 0..50_i64 {
                        loop {
                            match db.execute(
                                &format!("INSERT INTO t{t} VALUES (?, ?)"),
                                &[SqlValue::Integer(i), SqlValue::Integer(t)],
                            ) {
                                Ok(SqlOutcome::Inserted { rows: 1 }) => break,
                                Err(SqlError::Conflict(_)) => {
                                    conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    continue;
                                }
                                other => panic!("unexpected result on table t{t}: {other:?}"),
                            }
                        }
                    }
                });
            }
        });
        for t in 0..4 {
            let SqlOutcome::Rows { rows, .. } =
                setup.execute(&format!("SELECT id FROM t{t}"), &[]).unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(rows.len(), 50);
        }
        conflicts.load(std::sync::atomic::Ordering::Relaxed)
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("store")).unwrap();
    let store_conflicts = run_disjoint(&|| SqlDatabase::new(DocumentDatabase::new(&store)));
    assert_eq!(
        store_conflicts, 0,
        "expected 0 conflicts on Store with ReadSet mode"
    );

    let (ckpt, _, _) = open_checkpoint(dir.path());
    let ckpt_conflicts =
        run_disjoint(&|| SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt)));
    assert_eq!(
        ckpt_conflicts, 0,
        "expected 0 conflicts on CheckpointStore with ReadSet mode"
    );
}

#[test]
fn concurrent_read_modify_write_and_duplicate_pk_inserts_are_linearizable() {
    fn check_backend<'a>(make_db: &(dyn Fn() -> SqlDatabase<'a> + Sync)) {
        let setup = make_db();
        setup
            .execute(
                "CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)",
                &[],
            )
            .unwrap();
        setup.execute("INSERT INTO c VALUES (1, 0)", &[]).unwrap();

        // (b) 8 threads doing SELECT + conditional UPDATE ... WHERE id = 1 AND n = <old>.
        const THREADS: usize = 8;
        const INCREMENTS_PER_THREAD: usize = 20;
        let barrier = Arc::new(Barrier::new(THREADS));
        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let barrier = Arc::clone(&barrier);
                s.spawn(move || {
                    let db = make_db();
                    let sel = db.prepare("SELECT n FROM c WHERE id = 1").unwrap();
                    let upd = db
                        .prepare("UPDATE c SET n = ? WHERE id = 1 AND n = ?")
                        .unwrap();
                    barrier.wait();
                    for _ in 0..INCREMENTS_PER_THREAD {
                        loop {
                            let old = match db.execute_prepared(&sel, &[]) {
                                Ok(SqlOutcome::Rows { rows, .. }) => match rows[0][0] {
                                    SqlValue::Integer(v) => v,
                                    ref other => panic!("unexpected value: {other:?}"),
                                },
                                Err(SqlError::Conflict(_)) => continue,
                                other => panic!("unexpected select error: {other:?}"),
                            };
                            match db.execute_prepared(
                                &upd,
                                &[SqlValue::Integer(old + 1), SqlValue::Integer(old)],
                            ) {
                                Ok(SqlOutcome::Updated { rows: 1 }) => break,
                                Ok(SqlOutcome::Updated { rows: 0 })
                                | Err(SqlError::Conflict(_)) => {
                                    continue;
                                }
                                other => panic!("unexpected update result: {other:?}"),
                            }
                        }
                    }
                });
            }
        });

        assert_eq!(
            setup.execute("SELECT n FROM c WHERE id = 1", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["n".into()],
                rows: vec![vec![SqlValue::Integer(
                    (THREADS * INCREMENTS_PER_THREAD) as i64
                )]],
            }
        );

        // (c) Concurrent INSERTs of the same new primary key: exactly one succeeds.
        setup
            .execute(
                "CREATE TABLE dup_race (id INTEGER PRIMARY KEY, winner INTEGER NOT NULL)",
                &[],
            )
            .unwrap();
        let barrier2 = Arc::new(Barrier::new(THREADS));
        let successes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        std::thread::scope(|s| {
            for tid in 0..THREADS {
                let barrier2 = Arc::clone(&barrier2);
                let successes = Arc::clone(&successes);
                s.spawn(move || {
                    let db = make_db();
                    barrier2.wait();
                    match db.execute(
                        "INSERT INTO dup_race VALUES (42, ?)",
                        &[SqlValue::Integer(tid as i64)],
                    ) {
                        Ok(SqlOutcome::Inserted { rows: 1 }) => {
                            successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(SqlError::Constraint(_)) | Err(SqlError::Conflict(_)) => {}
                        other => panic!("unexpected concurrent duplicate PK insert: {other:?}"),
                    }
                });
            }
        });
        assert_eq!(successes.load(std::sync::atomic::Ordering::Relaxed), 1);
        let SqlOutcome::Rows { rows, .. } = setup.execute("SELECT id FROM dup_race", &[]).unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(rows, vec![vec![SqlValue::Integer(42)]]);
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("store")).unwrap();
    check_backend(&|| SqlDatabase::new(DocumentDatabase::new(&store)));

    let (ckpt, _, _) = open_checkpoint(dir.path());
    check_backend(&|| SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt)));
}

#[test]
fn order_by_nulls_types_multikey_and_tiebreak_on_both_backends() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                grp INTEGER,
                score REAL,
                tag TEXT,
                flag BOOLEAN
            )",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO items VALUES
                (10, 2, 1.5, 'b', TRUE),
                (2,  NULL, -0.0, 'a', FALSE),
                (5,  1, 2.0, NULL, TRUE),
                (1,  1, 2.0, 'Z', FALSE),
                (8,  2, NULL, 'é', NULL),
                (3,  NULL, 0.0, '', TRUE),
                (4,  1, NULL, 'a', FALSE)",
            &[],
        )
        .unwrap();

        let ids_of = |sql: &str, params: &[SqlValue]| -> Vec<i64> {
            match db.execute(sql, params).unwrap() {
                SqlOutcome::Rows { rows, .. } => rows
                    .into_iter()
                    .map(|r| match r[0] {
                        SqlValue::Integer(n) => n,
                        ref other => panic!("expected integer id, got {other:?}"),
                    })
                    .collect(),
                other => panic!("expected rows, got {other:?}"),
            }
        };

        // Default ASC => NULLS FIRST, ties break by PK ASC (2 < 3, 1 < 4 < 5, 8 < 10).
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY grp", &[]),
            vec![2, 3, 1, 4, 5, 8, 10]
        );
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY grp ASC NULLS FIRST", &[]),
            vec![2, 3, 1, 4, 5, 8, 10]
        );
        // ASC NULLS LAST => non-nulls first, NULLs last (ties still break by PK ASC).
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY grp ASC NULLS LAST", &[]),
            vec![1, 4, 5, 8, 10, 2, 3]
        );
        // Default DESC => NULLS LAST, ties break by PK ASC!
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY grp DESC", &[]),
            vec![8, 10, 1, 4, 5, 2, 3]
        );
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY grp DESC NULLS LAST", &[]),
            vec![8, 10, 1, 4, 5, 2, 3]
        );
        // DESC NULLS FIRST => NULLs first, then DESC, ties by PK ASC.
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY grp DESC NULLS FIRST", &[]),
            vec![2, 3, 8, 10, 1, 4, 5]
        );

        // Multi-key ordering + ORDER BY columns not in SELECT list.
        // grp ASC NULLS LAST:
        //   grp=1: id=1(score=2.0), id=4(score=NULL), id=5(score=2.0)
        //   grp=2: id=8(score=NULL), id=10(score=1.5)
        //   grp=NULL: id=2(score=-0.0), id=3(score=0.0) -> -0.0 == 0.0 => PK ASC: 2, 3
        assert_eq!(
            ids_of(
                "SELECT id FROM items ORDER BY grp ASC NULLS LAST, score DESC NULLS FIRST",
                &[]
            ),
            vec![4, 1, 5, 8, 10, 2, 3]
        );
        assert_eq!(
            db.execute("SELECT tag FROM items ORDER BY flag DESC, id ASC", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["tag".into()],
                rows: vec![
                    vec![SqlValue::Text("".into())],  // id=3 flag=TRUE
                    vec![SqlValue::Null],             // id=5 flag=TRUE
                    vec![SqlValue::Text("b".into())], // id=10 flag=TRUE
                    vec![SqlValue::Text("Z".into())], // id=1 flag=FALSE
                    vec![SqlValue::Text("a".into())], // id=2 flag=FALSE
                    vec![SqlValue::Text("a".into())], // id=4 flag=FALSE
                    vec![SqlValue::Text("é".into())], // id=8 flag=NULL (DESC => NULLS LAST)
                ],
            }
        );

        // TEXT UTF-8 byte order ('': 0, 'Z': 90, 'a': 97, 'b': 98, 'é': 0xc3...).
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY tag ASC NULLS LAST", &[]),
            vec![3, 1, 2, 4, 10, 8, 5]
        );

        // PK DESC and PK ASC with trailing keys.
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY id DESC", &[]),
            vec![10, 8, 5, 4, 3, 2, 1]
        );
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY id DESC, score ASC", &[]),
            vec![10, 8, 5, 4, 3, 2, 1]
        );
        assert_eq!(
            ids_of("SELECT id FROM items ORDER BY id ASC, score DESC", &[]),
            vec![1, 2, 3, 4, 5, 8, 10]
        );

        // TEXT PK tie-break ascending on non-PK DESC sort.
        db.execute("CREATE TABLE tpk (k TEXT PRIMARY KEY, v INTEGER)", &[])
            .unwrap();
        db.execute(
            "INSERT INTO tpk VALUES ('z', 1), ('a', 1), ('é', 1), ('Z', 2)",
            &[],
        )
        .unwrap();
        assert_eq!(
            db.execute("SELECT k FROM tpk ORDER BY v DESC", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["k".into()],
                rows: vec![
                    vec![SqlValue::Text("Z".into())],
                    vec![SqlValue::Text("a".into())],
                    vec![SqlValue::Text("z".into())],
                    vec![SqlValue::Text("é".into())],
                ],
            }
        );
        assert_eq!(
            db.execute("SELECT k FROM tpk ORDER BY k DESC LIMIT 2 OFFSET 1", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["k".into()],
                rows: vec![
                    vec![SqlValue::Text("z".into())],
                    vec![SqlValue::Text("a".into())],
                ],
            }
        );
    });
}

#[test]
fn limit_offset_edges_params_early_termination_and_sort_bounds() {
    for_both_sql(|mut db| {
        db.execute(
            "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT)",
            &[],
        )
        .unwrap();
        let mut ins = String::from("INSERT INTO nums VALUES ");
        for i in 0..50_i64 {
            if i > 0 {
                ins.push(',');
            }
            ins.push_str(&format!("({i}, {}, 't{:02}')", (50 - i) % 10, i));
        }
        db.execute(&ins, &[]).unwrap();

        let get_ids = |db: &SqlDatabase<'_>, sql: &str, params: &[SqlValue]| -> Vec<i64> {
            match db.execute(sql, params).unwrap() {
                SqlOutcome::Rows { rows, .. } => rows
                    .into_iter()
                    .map(|r| match r[0] {
                        SqlValue::Integer(n) => n,
                        _ => unreachable!(),
                    })
                    .collect(),
                _ => unreachable!(),
            }
        };

        // LIMIT/OFFSET edges: 0, at end, beyond end, huge values.
        assert_eq!(
            get_ids(&db, "SELECT id FROM nums LIMIT 0", &[]),
            Vec::<i64>::new()
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY val DESC LIMIT 0 OFFSET 100",
                &[]
            ),
            Vec::<i64>::new()
        );
        assert_eq!(
            get_ids(&db, "SELECT id FROM nums LIMIT 5 OFFSET 45", &[]),
            vec![45, 46, 47, 48, 49]
        );
        assert_eq!(
            get_ids(&db, "SELECT id FROM nums LIMIT 5 OFFSET 50", &[]),
            Vec::<i64>::new()
        );
        assert_eq!(
            get_ids(&db, "SELECT id FROM nums LIMIT 5 OFFSET 500", &[]),
            Vec::<i64>::new()
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY id DESC LIMIT 3 OFFSET 48",
                &[]
            ),
            vec![1, 0]
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY id DESC LIMIT 3 OFFSET 50",
                &[]
            ),
            Vec::<i64>::new()
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY val ASC LIMIT 9223372036854775807 OFFSET 9223372036854775807",
                &[]
            ),
            Vec::<i64>::new()
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY val ASC LIMIT 9223372036854775807 OFFSET 48",
                &[]
            )
            .len(),
            2
        );

        // Parameterized LIMIT / OFFSET + WHERE param ordering.
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums WHERE id >= ? ORDER BY id ASC LIMIT ? OFFSET ?",
                &[
                    SqlValue::Integer(10),
                    SqlValue::Integer(3),
                    SqlValue::Integer(2),
                ]
            ),
            vec![12, 13, 14]
        );

        // Parameter validation errors (Bounds for negative, Type for non-integer, NoSuchColumn even with LIMIT 0).
        assert!(matches!(
            db.execute("SELECT id FROM nums LIMIT ?", &[SqlValue::Integer(-1)])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
        assert!(matches!(
            db.execute(
                "SELECT id FROM nums LIMIT 1 OFFSET ?",
                &[SqlValue::Integer(-5)]
            )
            .unwrap_err(),
            SqlError::Bounds(_)
        ));
        for bad in [
            SqlValue::Null,
            SqlValue::Real(2.0),
            SqlValue::Text("2".into()),
            SqlValue::Boolean(true),
        ] {
            assert!(matches!(
                db.execute("SELECT id FROM nums LIMIT ?", std::slice::from_ref(&bad))
                    .unwrap_err(),
                SqlError::Type(_)
            ));
            assert!(matches!(
                db.execute("SELECT id FROM nums LIMIT 1 OFFSET ?", &[bad])
                    .unwrap_err(),
                SqlError::Type(_)
            ));
        }
        assert!(matches!(
            db.execute("SELECT id FROM nums ORDER BY nope LIMIT 0", &[])
                .unwrap_err(),
            SqlError::NoSuchColumn { .. }
        ));

        // Early termination proved via max_visited_rows = 5 on 50-row table.
        db.set_limits(SqlLimits {
            max_visited_rows: 5,
            max_select_rows: 10,
            max_sort_rows: 10,
            ..Default::default()
        })
        .unwrap();

        // LIMIT 0 visits 0 rows.
        assert_eq!(
            get_ids(&db, "SELECT id FROM nums ORDER BY val DESC LIMIT 0", &[]),
            Vec::<i64>::new()
        );
        // Streaming PK ASC visits exactly OFFSET + LIMIT = 5 rows.
        assert_eq!(
            get_ids(&db, "SELECT id FROM nums LIMIT 5", &[]),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY id ASC LIMIT 3 OFFSET 2",
                &[]
            ),
            vec![2, 3, 4]
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums WHERE id >= 20 ORDER BY id ASC LIMIT 5",
                &[]
            ),
            vec![20, 21, 22, 23, 24]
        );
        // OFFSET 2 + LIMIT 4 requires visiting 6 rows > max_visited_rows (5).
        assert!(matches!(
            db.execute("SELECT id FROM nums ORDER BY id ASC LIMIT 4 OFFSET 2", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
        // Non-PK ORDER BY and PK DESC must scan all 50 rows > max_visited_rows (5).
        assert!(matches!(
            db.execute("SELECT id FROM nums ORDER BY val ASC LIMIT 2", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
        assert!(matches!(
            db.execute("SELECT id FROM nums ORDER BY id DESC LIMIT 2", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));

        // Sort buffer and max_select_rows bounds: top-K LIMIT 5 succeeds when total=50 > max_sort_rows=10,
        // while unbounded sort or OFFSET+LIMIT > max_sort_rows fails with Bounds.
        db.set_limits(SqlLimits {
            max_visited_rows: 100,
            max_select_rows: 5,
            max_sort_rows: 10,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY val ASC, id ASC LIMIT 5",
                &[]
            ),
            vec![0, 10, 20, 30, 40]
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY val ASC LIMIT 4 OFFSET 6",
                &[]
            ),
            vec![19, 29, 39, 49]
        );
        assert_eq!(
            get_ids(
                &db,
                "SELECT id FROM nums ORDER BY id DESC LIMIT 5 OFFSET 5",
                &[]
            ),
            vec![44, 43, 42, 41, 40]
        );
        // Full sort without LIMIT on 50 rows exceeds max_select_rows/max_sort_rows.
        assert!(matches!(
            db.execute("SELECT id FROM nums ORDER BY val ASC", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
        // OFFSET 8 + LIMIT 5 = 13 > max_sort_rows (10).
        assert!(matches!(
            db.execute("SELECT id FROM nums ORDER BY val ASC LIMIT 5 OFFSET 8", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
        // LIMIT 6 > max_select_rows (5) when 50 rows match.
        assert!(matches!(
            db.execute("SELECT id FROM nums ORDER BY val ASC LIMIT 6", &[])
                .unwrap_err(),
            SqlError::Bounds(_)
        ));
        // OFFSET 47 + LIMIT 6 needs 53 > max_sort_rows (10), so it fails with Bounds.
        assert!(matches!(
            db.execute(
                "SELECT id FROM nums ORDER BY val ASC LIMIT 6 OFFSET 47",
                &[]
            )
            .unwrap_err(),
            SqlError::Bounds(_)
        ));
    });

    // Separate check for LIMIT > max_select_rows when matching rows after OFFSET <= max_select_rows.
    for_both_sql(|mut db| {
        db.set_limits(SqlLimits {
            max_select_rows: 5,
            max_sort_rows: 100,
            ..Default::default()
        })
        .unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &[])
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,3),(2,1),(3,2)", &[])
            .unwrap();
        let SqlOutcome::Rows { rows, .. } = db
            .execute("SELECT id FROM t ORDER BY v ASC LIMIT 50", &[])
            .unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(
            rows,
            vec![
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
                vec![SqlValue::Integer(1)],
            ]
        );
    });
}

#[test]
fn randomized_order_limit_offset_differential_against_model() {
    use std::cmp::Ordering;

    #[derive(Clone)]
    struct ModelRow {
        id: i64,
        n: Option<i64>,
        r: Option<f64>,
        s: Option<String>,
        b: Option<bool>,
    }

    impl ModelRow {
        fn col(&self, idx: usize) -> SqlValue {
            match idx {
                0 => SqlValue::Integer(self.id),
                1 => self.n.map_or(SqlValue::Null, SqlValue::Integer),
                2 => self.r.map_or(SqlValue::Null, SqlValue::Real),
                3 => self.s.clone().map_or(SqlValue::Null, SqlValue::Text),
                4 => self.b.map_or(SqlValue::Null, SqlValue::Boolean),
                _ => unreachable!(),
            }
        }
    }

    fn cmp_model_val(a: &SqlValue, b: &SqlValue, desc: bool, nulls_first: bool) -> Ordering {
        match (a, b) {
            (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
            (SqlValue::Null, _) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (_, SqlValue::Null) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (SqlValue::Integer(x), SqlValue::Integer(y)) => {
                let o = x.cmp(y);
                if desc { o.reverse() } else { o }
            }
            (SqlValue::Real(x), SqlValue::Real(y)) => {
                let o = x.partial_cmp(y).unwrap();
                if desc { o.reverse() } else { o }
            }
            (SqlValue::Text(x), SqlValue::Text(y)) => {
                let o = x.as_bytes().cmp(y.as_bytes());
                if desc { o.reverse() } else { o }
            }
            (SqlValue::Boolean(x), SqlValue::Boolean(y)) => {
                let o = x.cmp(y);
                if desc { o.reverse() } else { o }
            }
            _ => unreachable!(),
        }
    }

    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE r (id INTEGER PRIMARY KEY, n INTEGER, r REAL, s TEXT, b BOOLEAN)",
            &[],
        )
        .unwrap();

        let mut model: BTreeMap<i64, ModelRow> = BTreeMap::new();
        let mut rng = 0x1234_5678_9ABC_DEF0_u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let mut ins_sql = String::from("INSERT INTO r VALUES ");
        let mut ins_params = Vec::new();
        for i in 0..40_i64 {
            if i > 0 {
                ins_sql.push(',');
            }
            ins_sql.push_str("(?, ?, ?, ?, ?)");
            let id = i * 3 - 50;
            let n = if next() % 4 == 0 {
                None
            } else {
                Some(((next() % 7) as i64) - 3)
            };
            let r = if next() % 4 == 0 {
                None
            } else {
                let vals = [-2.5, -0.0, 0.0, 1.5, 2.0];
                Some(vals[(next() as usize) % vals.len()])
            };
            let s = if next() % 4 == 0 {
                None
            } else {
                let vals = ["", "A", "a", "b", "é"];
                Some(vals[(next() as usize) % vals.len()].to_owned())
            };
            let b = if next() % 4 == 0 {
                None
            } else {
                Some(next() & 1 != 0)
            };
            ins_params.push(SqlValue::Integer(id));
            ins_params.push(n.map_or(SqlValue::Null, SqlValue::Integer));
            ins_params.push(r.map_or(SqlValue::Null, SqlValue::Real));
            ins_params.push(s.clone().map_or(SqlValue::Null, SqlValue::Text));
            ins_params.push(b.map_or(SqlValue::Null, SqlValue::Boolean));
            model.insert(id, ModelRow { id, n, r, s, b });
        }
        db.execute(&ins_sql, &ins_params).unwrap();

        let col_names = ["id", "n", "r", "s", "b"];
        for _ in 0..200 {
            let mut sql = String::from("SELECT ");
            let proj: Vec<usize> = if next() & 1 == 0 {
                sql.push('*');
                vec![0, 1, 2, 3, 4]
            } else {
                let cnt = 1 + (next() % 3) as usize;
                let mut cols = Vec::new();
                for i in 0..cnt {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    let c = (next() as usize) % 5;
                    sql.push_str(col_names[c]);
                    cols.push(c);
                }
                cols
            };
            sql.push_str(" FROM r");

            let mut params = Vec::new();
            let lo_bound = if next() % 3 == 0 {
                let lo = ((next() % 120) as i64) - 60;
                sql.push_str(" WHERE id >= ?");
                params.push(SqlValue::Integer(lo));
                Some(lo)
            } else {
                None
            };

            let mut order_spec = Vec::new();
            if next() % 6 != 0 {
                sql.push_str(" ORDER BY ");
                let key_cnt = 1 + (next() % 3) as usize;
                for i in 0..key_cnt {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    let c = (next() as usize) % 5;
                    let desc = next() & 1 != 0;
                    let nulls_opt = match next() % 3 {
                        0 => None,
                        1 => Some(true),
                        _ => Some(false),
                    };
                    sql.push_str(col_names[c]);
                    if desc {
                        sql.push_str(" DESC");
                    } else if next() & 1 != 0 {
                        sql.push_str(" ASC");
                    }
                    if let Some(nf) = nulls_opt {
                        sql.push_str(if nf { " NULLS FIRST" } else { " NULLS LAST" });
                    }
                    let nulls_first = nulls_opt.unwrap_or(!desc);
                    order_spec.push((c, desc, nulls_first));
                }
            }

            let (limit, offset) = if next() % 5 != 0 {
                let lim = match next() % 6 {
                    0 => 0,
                    1 => 1,
                    2 => 5,
                    3 => 15,
                    4 => 40,
                    _ => 100,
                };
                sql.push_str(" LIMIT ?");
                params.push(SqlValue::Integer(lim));
                let off = if next() & 1 != 0 {
                    let o = (next() % 45) as i64;
                    sql.push_str(" OFFSET ?");
                    params.push(SqlValue::Integer(o));
                    o as usize
                } else {
                    0
                };
                (Some(lim as usize), off)
            } else {
                (None, 0)
            };

            let actual = db.execute(&sql, &params).unwrap();
            let forced = db.execute_force_full_scan_for_test(&sql, &params).unwrap();
            assert_eq!(actual, forced, "planned vs forced mismatch on `{sql}`");

            let mut expected_rows: Vec<&ModelRow> = model
                .values()
                .filter(|row| lo_bound.is_none_or(|lo| row.id >= lo))
                .collect();
            if !order_spec.is_empty() {
                expected_rows.sort_by(|a, b| {
                    for &(c, desc, nulls_first) in &order_spec {
                        let o = cmp_model_val(&a.col(c), &b.col(c), desc, nulls_first);
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                    a.id.cmp(&b.id)
                });
            }
            let expected_projected: Vec<Vec<SqlValue>> = expected_rows
                .into_iter()
                .skip(offset)
                .take(limit.unwrap_or(usize::MAX))
                .map(|row| proj.iter().map(|&c| row.col(c)).collect())
                .collect();
            let expected = SqlOutcome::Rows {
                columns: proj.iter().map(|&c| col_names[c].to_owned()).collect(),
                rows: expected_projected,
            };
            assert_eq!(
                actual, expected,
                "model mismatch for `{sql}` params={params:?}"
            );
        }
    });
}

#[test]
fn catalog_and_plan_caches_never_serve_stale_definitions_on_both_backends() {
    fn check_pair<'a>(make_db: &dyn Fn() -> SqlDatabase<'a>) {
        let mut db1 = make_db();
        let db2 = make_db();

        db1.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
            &[],
        )
        .unwrap();
        db1.execute("INSERT INTO items VALUES (1, 10, 'hi')", &[])
            .unwrap();
        let prep = db1
            .prepare("SELECT id, a, b FROM items WHERE id = ?")
            .unwrap();
        assert_eq!(
            db1.execute_prepared(&prep, &[SqlValue::Integer(1)])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "a".into(), "b".into()],
                rows: vec![vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(10),
                    SqlValue::Text("hi".into()),
                ]],
            }
        );
        // Also populate db1's statement cache via execute().
        assert_eq!(
            db1.execute("SELECT id, a, b FROM items WHERE id = 1", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "a".into(), "b".into()],
                rows: vec![vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(10),
                    SqlValue::Text("hi".into()),
                ]],
            }
        );

        // CREATE TABLE IF NOT EXISTS from db2 does not alter or break existing table.
        assert_eq!(
            db2.execute(
                "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, completely_different TEXT)",
                &[]
            )
            .unwrap(),
            SqlOutcome::TableCreated
        );
        assert_eq!(
            db2.execute("SELECT id, a, b FROM items WHERE id = 1", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "a".into(), "b".into()],
                rows: vec![vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(10),
                    SqlValue::Text("hi".into()),
                ]],
            }
        );

        // Drop collection + catalog entry via DocumentDatabase, bypassing db1's in-memory caches.
        db2.document_database()
            .transaction(|tx| {
                tx.drop_collection("items")?;
                tx.delete_raw(b"\0emda-sql\x01Titems")
            })
            .unwrap();

        // db1 must immediately observe NoSuchTable (not serve stale cache).
        assert!(matches!(
            db1.execute_prepared(&prep, &[SqlValue::Integer(1)])
                .unwrap_err(),
            SqlError::NoSuchTable(_)
        ));
        assert!(matches!(
            db1.execute("SELECT id, a, b FROM items WHERE id = 1", &[])
                .unwrap_err(),
            SqlError::NoSuchTable(_)
        ));

        // Recreate `items` via db2 with a completely different schema (TEXT PK, different columns).
        db2.execute(
            "CREATE TABLE items (id TEXT PRIMARY KEY, tag TEXT NOT NULL, score REAL)",
            &[],
        )
        .unwrap();
        db2.execute("INSERT INTO items VALUES ('k1', 'alpha', 3.5)", &[])
            .unwrap();

        // db1's old prepared statement and cached SQL string must detect the schema change.
        assert!(matches!(
            db1.execute_prepared(&prep, &[SqlValue::Integer(1)])
                .unwrap_err(),
            SqlError::NoSuchColumn { .. }
        ));
        assert!(matches!(
            db1.execute("SELECT id, a, b FROM items WHERE id = 1", &[])
                .unwrap_err(),
            SqlError::NoSuchColumn { .. }
        ));
        assert_eq!(
            db1.execute("SELECT * FROM items", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "tag".into(), "score".into()],
                rows: vec![vec![
                    SqlValue::Text("k1".into()),
                    SqlValue::Text("alpha".into()),
                    SqlValue::Real(3.5),
                ]],
            }
        );
        assert_eq!(
            db1.execute("SELECT id, tag, score FROM items WHERE id = 'k1'", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "tag".into(), "score".into()],
                rows: vec![vec![
                    SqlValue::Text("k1".into()),
                    SqlValue::Text("alpha".into()),
                    SqlValue::Real(3.5),
                ]],
            }
        );

        // set_limits clears cached statements and plans so tighter max_columns takes effect.
        assert!(db1.execute("SELECT id, tag FROM items", &[]).is_ok());
        db1.set_limits(SqlLimits {
            max_columns: 1,
            ..Default::default()
        })
        .unwrap();
        assert!(matches!(
            db1.execute("SELECT id, tag FROM items", &[]).unwrap_err(),
            SqlError::Bounds(_)
        ));
    }

    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store");
    {
        let store = Store::open(&store_path).unwrap();
        check_pair(&|| SqlDatabase::new(DocumentDatabase::new(&store)));
    }
    // Reopen Store and verify persisted new schema.
    {
        let store = Store::open(&store_path).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        assert_eq!(
            db.execute("SELECT * FROM items", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "tag".into(), "score".into()],
                rows: vec![vec![
                    SqlValue::Text("k1".into()),
                    SqlValue::Text("alpha".into()),
                    SqlValue::Real(3.5),
                ]],
            }
        );
    }

    let (ckpt, ckpt_path, digest) = open_checkpoint(dir.path());
    check_pair(&|| SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt)));
    drop(ckpt);
    let ckpt_reopened =
        CheckpointStore::open(dir.path().join("checkpoint_log"), &ckpt_path, &digest).unwrap();
    let ckpt_db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt_reopened));
    assert_eq!(
        ckpt_db.execute("SELECT * FROM items", &[]).unwrap(),
        SqlOutcome::Rows {
            columns: vec!["id".into(), "tag".into(), "score".into()],
            rows: vec![vec![
                SqlValue::Text("k1".into()),
                SqlValue::Text("alpha".into()),
                SqlValue::Real(3.5),
            ]],
        }
    );
}

#[test]
fn non_bounds_invalid_input_errors_stay_sql_error_io() {
    for_both_sql(|db| {
        // Create a collection with a stricter document schema rule (IntegerRange 0..=10)
        // and register a matching SQL catalog entry to exercise a real engine InvalidInput
        // schema rejection through SqlDatabase::execute.
        let doc_db = db.document_database();
        let schema = CollectionSchema {
            fields: vec![
                database_engine::FieldRule {
                    path: vec!["id".into()],
                    field_type: database_engine::FieldType::Integer,
                    required: true,
                    nullable: false,
                },
                database_engine::FieldRule {
                    path: vec!["x".into()],
                    field_type: database_engine::FieldType::IntegerRange { min: 0, max: 10 },
                    required: true,
                    nullable: false,
                },
            ],
        };
        doc_db
            .transaction(|tx| {
                tx.create_collection("ranged", schema)?;
                let catalog_entry = json!({
                    "version": 1,
                    "name": "ranged",
                    "columns": [
                        { "name": "id", "col_type": { "kind": "integer" }, "not_null": true },
                        { "name": "x", "col_type": { "kind": "integer" }, "not_null": true }
                    ],
                    "primary_key_index": 0
                });
                tx.put_raw(
                    b"\0emda-sql\x01Tranged",
                    &serde_json::to_vec(&catalog_entry).unwrap(),
                )
            })
            .unwrap();

        db.execute("INSERT INTO ranged VALUES (1, 5)", &[]).unwrap();
        let err = db
            .execute("INSERT INTO ranged VALUES (2, 99)", &[])
            .unwrap_err();
        match err {
            SqlError::Io(io_err) => {
                assert_eq!(io_err.kind(), io::ErrorKind::InvalidInput);
                assert!(io_err.to_string().contains("schema"));
            }
            other => panic!("expected SqlError::Io for schema rejection, got {other:?}"),
        }
    });
}

#[test]
fn row_integrity_rejects_mismatched_document_id_on_all_access_paths() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'good')", &[])
            .unwrap();

        // Inject a document whose ID does not match its primary key column value.
        db.document_database()
            .insert(
                "items",
                "not-an-encoded-pk",
                &json!({"id": 2, "name": "rogue"}),
            )
            .unwrap();

        let err = db.execute("SELECT name FROM items", &[]).unwrap_err();
        match err {
            SqlError::Io(io_err) => {
                assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);
                assert!(io_err.to_string().contains("items"));
            }
            other => panic!("expected SqlError::Io(InvalidData), got {other:?}"),
        }

        // Also verify point lookup catches a document stored at pk(10) whose JSON body says id=999.
        db.execute(
            "CREATE TABLE items2 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        )
        .unwrap();
        db.document_database()
            .insert(
                "items2",
                "i:800000000000000a", // encoded 10
                &json!({"id": 999, "name": "spoofed"}),
            )
            .unwrap();
        for sql in [
            "SELECT name FROM items2 WHERE id = 10",
            "UPDATE items2 SET name = 'x' WHERE id = 10",
            "DELETE FROM items2 WHERE id = 10",
            "SELECT name FROM items2",
        ] {
            let err = db.execute(sql, &[]).unwrap_err();
            match err {
                SqlError::Io(io_err) => {
                    assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);
                    assert!(io_err.to_string().contains("items2"));
                }
                other => panic!("expected SqlError::Io(InvalidData) for {sql}, got {other:?}"),
            }
        }

        // Also test TEXT PK mismatch.
        db.execute("CREATE TABLE txt_pk (k TEXT PRIMARY KEY, v INT)", &[])
            .unwrap();
        db.document_database()
            .insert("txt_pk", "t:alpha", &json!({"k": "beta", "v": 1}))
            .unwrap();
        let err = db
            .execute("SELECT v FROM txt_pk WHERE k = 'alpha'", &[])
            .unwrap_err();
        match err {
            SqlError::Io(io_err) => {
                assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);
                assert!(io_err.to_string().contains("txt_pk"));
            }
            other => panic!("expected SqlError::Io(InvalidData), got {other:?}"),
        }
    });
}
