//! Integration tests for SQL secondary indexes, UNIQUE constraints, NULL semantics,
//! index-assisted query planning, bounds, durability, and concurrency across backends.
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlLimits, SqlOutcome, SqlValue,
    Store,
};
use std::sync::{Arc, Barrier};
use std::thread;

fn open_checkpoint(dir: &std::path::Path) -> (CheckpointStore, std::path::PathBuf, [u8; 32]) {
    let log_dir = dir.join("checkpoint_log");
    let ckpt_path = dir.join("checkpoint.dat");
    let initial = Store::open(&log_dir).unwrap();
    let digest = initial.checkpoint_to(&ckpt_path).unwrap();
    drop(initial);
    let ckpt = CheckpointStore::open(&log_dir, &ckpt_path, &digest).unwrap();
    (ckpt, ckpt_path, digest)
}

fn for_both_sql(mut f: impl FnMut(&mut SqlDatabase<'_>, &str)) {
    let dir1 = tempfile::tempdir().unwrap();
    let store = Store::open(dir1.path()).unwrap();
    let mut db = SqlDatabase::new(DocumentDatabase::new(&store));
    f(&mut db, "Store");

    let dir2 = tempfile::tempdir().unwrap();
    let (ckpt, _, _) = open_checkpoint(dir2.path());
    let mut ckpt_db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt));
    f(&mut ckpt_db, "CheckpointStore");
}

fn verify_collection_ok(db: &SqlDatabase<'_>, table: &str) {
    db.document_database()
        .transaction(|tx| tx.verify_collection(table))
        .unwrap();
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn ddl_syntax_errors_and_limits_for_indexes_and_unique() {
    for_both_sql(|db, _| {
        assert_eq!(
            db.execute(
                "CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    code INT,
                    dept INT NOT NULL,
                    tag VARCHAR(8),
                    active BOOLEAN,
                    score REAL,
                    UNIQUE (dept, code)
                )",
                &[],
            )
            .unwrap(),
            SqlOutcome::TableCreated
        );

        assert_eq!(
            db.execute("CREATE INDEX idx_tag ON users (tag)", &[])
                .unwrap(),
            SqlOutcome::IndexCreated
        );
        assert_eq!(
            db.execute("CREATE INDEX IF NOT EXISTS idx_tag ON users (tag)", &[])
                .unwrap(),
            SqlOutcome::IndexCreated
        );
        assert_eq!(
            db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_comp ON users (dept, tag, active)",
                &[],
            )
            .unwrap(),
            SqlOutcome::IndexCreated
        );

        // Duplicate index name without IF NOT EXISTS fails with IndexExists.
        assert!(matches!(
            db.execute("CREATE INDEX idx_tag ON users (code)", &[]),
            Err(SqlError::IndexExists(_))
        ));
        assert!(matches!(
            db.execute("CREATE INDEX idx_missing_tbl ON nope (a)", &[]),
            Err(SqlError::NoSuchTable(_))
        ));
        assert!(matches!(
            db.execute("CREATE INDEX idx_missing_col ON users (nope)", &[]),
            Err(SqlError::NoSuchColumn { .. })
        ));
        // Duplicate column in index or empty column list -> Syntax.
        assert!(matches!(
            db.execute("CREATE INDEX idx_dup ON users (code, code)", &[]),
            Err(SqlError::Syntax { .. })
        ));
        assert!(matches!(
            db.execute("CREATE INDEX idx_empty ON users ()", &[]),
            Err(SqlError::Syntax { .. })
        ));
        // > 4 columns in index or UNIQUE -> Bounds.
        assert!(matches!(
            db.execute(
                "CREATE INDEX idx_5 ON users (id, email, code, dept, tag)",
                &[],
            ),
            Err(SqlError::Bounds(_))
        ));
        assert!(matches!(
            db.execute(
                "CREATE TABLE bad5 (id INT PRIMARY KEY, a INT, b INT, c INT, d INT, e INT, UNIQUE (a, b, c, d, e))",
                &[],
            ),
            Err(SqlError::Bounds(_))
        ));
        // REAL columns cannot be indexed -> Unsupported.
        assert!(matches!(
            db.execute("CREATE INDEX idx_real ON users (score)", &[]),
            Err(SqlError::Unsupported { .. })
        ));
        assert!(matches!(
            db.execute(
                "CREATE TABLE bad_real (id INT PRIMARY KEY, r REAL UNIQUE)",
                &[]
            ),
            Err(SqlError::Unsupported { .. })
        ));
        // Unsupported index clauses.
        for bad_sql in [
            "DROP INDEX idx_tag",
            "CREATE INDEX idx_w ON users (code) WHERE code > 0",
            "CREATE INDEX idx_d ON users (code DESC)",
            "CREATE INDEX idx_u ON users USING btree (code)",
        ] {
            assert!(
                matches!(db.prepare(bad_sql), Err(SqlError::Unsupported { .. })),
                "expected Unsupported for {bad_sql}"
            );
        }

        // 16 indexes per table cap.
        db.execute("CREATE TABLE many (id INT PRIMARY KEY, a INT)", &[])
            .unwrap();
        for i in 0..16 {
            db.execute(&format!("CREATE INDEX idx_{i} ON many (a)"), &[])
                .unwrap();
        }
        assert!(matches!(
            db.execute("CREATE INDEX idx_16 ON many (a)", &[]),
            Err(SqlError::Bounds(_))
        ));
    });
}

#[test]
fn unique_constraints_atomic_and_null_semantics_on_both_backends() {
    for_both_sql(|db, _| {
        db.execute(
            "CREATE TABLE t (
                id INTEGER PRIMARY KEY,
                u INT UNIQUE,
                a INT,
                b TEXT,
                UNIQUE (a, b)
            )",
            &[],
        )
        .unwrap();

        // Multiple NULLs in single-column UNIQUE and compound UNIQUE (with one or both NULL).
        assert_eq!(
            db.execute(
                "INSERT INTO t VALUES
                    (1, NULL, 10, NULL),
                    (2, NULL, 10, NULL),
                    (3, NULL, NULL, 'x'),
                    (4, NULL, NULL, 'x'),
                    (5, NULL, NULL, NULL),
                    (6, NULL, NULL, NULL),
                    (7, 100, 10, 'x')",
                &[],
            )
            .unwrap(),
            SqlOutcome::Inserted { rows: 7 }
        );
        verify_collection_ok(db, "t");

        // Single-row INSERT violating column UNIQUE -> Constraint, data unchanged.
        assert!(matches!(
            db.execute("INSERT INTO t VALUES (8, 100, 20, 'y')", &[]),
            Err(SqlError::Constraint(_))
        ));
        // Single-row INSERT violating compound UNIQUE -> Constraint, data unchanged.
        assert!(matches!(
            db.execute("INSERT INTO t VALUES (8, 200, 10, 'x')", &[]),
            Err(SqlError::Constraint(_))
        ));
        // Multi-row INSERT where row 2 violates existing -> row 1 rolled back!
        assert!(matches!(
            db.execute(
                "INSERT INTO t VALUES (8, 200, 20, 'y'), (9, 100, 30, 'z')",
                &[],
            ),
            Err(SqlError::Constraint(_))
        ));
        // Multi-row INSERT where row 2 duplicates row 1 within same statement -> neither committed!
        assert!(matches!(
            db.execute(
                "INSERT INTO t VALUES (8, 300, 20, 'y'), (9, 300, 30, 'z')",
                &[],
            ),
            Err(SqlError::Constraint(_))
        ));
        assert_eq!(
            db.execute("SELECT id FROM t WHERE id >= 8", &[]).unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into()],
                rows: vec![],
            }
        );

        // UPDATE violating unique against existing row -> atomic rollback.
        assert!(matches!(
            db.execute("UPDATE t SET u = 100 WHERE id = 1", &[]),
            Err(SqlError::Constraint(_))
        ));
        // Multi-row UPDATE setting same value -> atomic rollback.
        assert!(matches!(
            db.execute("UPDATE t SET u = 555 WHERE id <= 2", &[]),
            Err(SqlError::Constraint(_))
        ));
        // No-op self-UPDATE on unique row succeeds.
        assert_eq!(
            db.execute("UPDATE t SET u = 100 WHERE id = 7", &[])
                .unwrap(),
            SqlOutcome::Updated { rows: 1 }
        );
        // Updating unique value to NULL frees it and allows multiple NULLs.
        assert_eq!(
            db.execute("UPDATE t SET u = NULL, b = NULL WHERE id = 7", &[])
                .unwrap(),
            SqlOutcome::Updated { rows: 1 }
        );
        assert_eq!(
            db.execute("INSERT INTO t VALUES (8, 100, 10, 'x')", &[])
                .unwrap(),
            SqlOutcome::Inserted { rows: 1 }
        );
        // Deleting frees unique value.
        assert_eq!(
            db.execute("DELETE FROM t WHERE id = 8", &[]).unwrap(),
            SqlOutcome::Deleted { rows: 1 }
        );
        assert_eq!(
            db.execute("INSERT INTO t VALUES (9, 100, 10, 'x')", &[])
                .unwrap(),
            SqlOutcome::Inserted { rows: 1 }
        );

        // Indexed TEXT > 256 bytes -> Bounds, nothing changed.
        let long = "x".repeat(257);
        assert!(matches!(
            db.execute(
                "INSERT INTO t VALUES (10, 999, 99, ?)",
                &[SqlValue::Text(long.clone())],
            ),
            Err(SqlError::Bounds(_))
        ));
        assert!(matches!(
            db.execute("UPDATE t SET b = ? WHERE id = 9", &[SqlValue::Text(long)],),
            Err(SqlError::Bounds(_))
        ));

        verify_collection_ok(db, "t");
        assert_eq!(
            db.execute("SELECT id, u, a, b FROM t WHERE id = 9", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "u".into(), "a".into(), "b".into()],
                rows: vec![vec![
                    SqlValue::Integer(9),
                    SqlValue::Integer(100),
                    SqlValue::Integer(10),
                    SqlValue::Text("x".into()),
                ]],
            }
        );
    });
}

#[test]
fn populated_table_create_index_build_and_bounds() {
    for_both_sql(|db, _| {
        db.execute(
            "CREATE TABLE pop (id INTEGER PRIMARY KEY, k INT, s TEXT)",
            &[],
        )
        .unwrap();
        for i in 1..=10 {
            db.execute(
                "INSERT INTO pop VALUES (?, ?, ?)",
                &[
                    SqlValue::Integer(i),
                    SqlValue::Integer(if i <= 3 { 1 } else { i }),
                    SqlValue::Text(format!("s{i}")),
                ],
            )
            .unwrap();
        }
        // Multiple NULLs in k allowed when building unique index.
        db.execute(
            "INSERT INTO pop VALUES (11, NULL, NULL), (12, NULL, NULL)",
            &[],
        )
        .unwrap();

        // Unique violation on populated table fails with Constraint and leaves catalog/collection untouched.
        assert!(matches!(
            db.execute("CREATE UNIQUE INDEX u_k ON pop (k)", &[]),
            Err(SqlError::Constraint(_))
        ));
        assert!(db.document_database().indexes("pop").unwrap().is_empty());

        // Unique build succeeds on unique column s (with multiple NULLs).
        db.execute("CREATE UNIQUE INDEX u_s ON pop (s)", &[])
            .unwrap();
        // Non-unique build succeeds on k.
        db.execute("CREATE INDEX idx_k ON pop (k)", &[]).unwrap();
        verify_collection_ok(db, "pop");

        // Queries use new indexes and match full scan.
        for sql in [
            "SELECT id, k, s FROM pop WHERE s = 's7'",
            "SELECT id, k, s FROM pop WHERE k = 1",
            "SELECT id, k, s FROM pop WHERE k IS NULL",
        ] {
            assert_eq!(
                db.execute(sql, &[]).unwrap(),
                db.execute_force_full_scan_for_test(sql, &[]).unwrap(),
                "{sql}"
            );
        }

        // Populated table exceeding 256-row one-shot build limit fails with Bounds and builds nothing.
        db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, v INT)", &[])
            .unwrap();
        for chunk in (0..257i64).collect::<Vec<_>>().chunks(100) {
            let placeholders = vec!["(?, ?)"; chunk.len()].join(", ");
            let params: Vec<SqlValue> = chunk
                .iter()
                .flat_map(|&i| [SqlValue::Integer(i), SqlValue::Integer(i % 17)])
                .collect();
            db.execute(&format!("INSERT INTO big VALUES {placeholders}"), &params)
                .unwrap();
        }
        assert!(matches!(
            db.execute("CREATE INDEX idx_v ON big (v)", &[]),
            Err(SqlError::Bounds(_))
        ));
        assert!(db.document_database().indexes("big").unwrap().is_empty());

        // Populated table with >256-byte TEXT fails CREATE INDEX with Bounds and builds nothing.
        db.execute(
            "CREATE TABLE longtxt (id INTEGER PRIMARY KEY, txt TEXT)",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO longtxt VALUES (1, ?)",
            &[SqlValue::Text("a".repeat(257))],
        )
        .unwrap();
        assert!(matches!(
            db.execute("CREATE INDEX idx_txt ON longtxt (txt)", &[]),
            Err(SqlError::Bounds(_))
        ));
        assert!(
            db.document_database()
                .indexes("longtxt")
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn type_semantics_integer_real_text_boolean_through_indexes() {
    for_both_sql(|db, _| {
        db.execute(
            "CREATE TABLE types (
                id INTEGER PRIMARY KEY,
                n INTEGER,
                s TEXT,
                b BOOLEAN,
                extra TEXT
            )",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX idx_n ON types (n)", &[]).unwrap();
        db.execute("CREATE INDEX idx_s ON types (s)", &[]).unwrap();
        db.execute("CREATE INDEX idx_b ON types (b)", &[]).unwrap();

        let p53 = 1_i64 << 53;
        let rows = [
            (1, Some(i64::MIN), Some(""), Some(false), "m0"),
            (2, Some(-2), Some("\0"), Some(false), "m1"),
            (3, Some(3), Some("A"), Some(true), "m2"),
            (4, Some(4), Some("a"), Some(true), "m3"),
            (5, Some(p53 - 1), Some("aa"), None, "m4"),
            (6, Some(p53), Some("é"), Some(false), "m5"),
            (7, Some(p53 + 1), Some("z"), Some(true), "m6"),
            (8, Some(i64::MAX), None, None, "m7"),
            (9, None, Some("a".repeat(256).leak()), Some(true), "m8"),
        ];
        for (id, n, s, b, extra) in rows {
            db.execute(
                "INSERT INTO types VALUES (?, ?, ?, ?, ?)",
                &[
                    SqlValue::Integer(id),
                    n.map_or(SqlValue::Null, SqlValue::Integer),
                    s.map_or(SqlValue::Null, |v| SqlValue::Text(v.to_owned())),
                    b.map_or(SqlValue::Null, SqlValue::Boolean),
                    SqlValue::Text(extra.to_owned()),
                ],
            )
            .unwrap();
        }

        let queries: &[(&str, &[SqlValue])] = &[
            // Exact integer > 2^53 and i64 extremes
            (
                "SELECT id FROM types WHERE n = ?",
                &[SqlValue::Integer(p53 + 1)],
            ),
            (
                "SELECT id FROM types WHERE n = ?",
                &[SqlValue::Integer(p53)],
            ),
            (
                "SELECT id FROM types WHERE n > ?",
                &[SqlValue::Integer(p53)],
            ),
            (
                "SELECT id FROM types WHERE n >= ?",
                &[SqlValue::Integer(i64::MIN)],
            ),
            (
                "SELECT id FROM types WHERE n <= ?",
                &[SqlValue::Integer(i64::MAX)],
            ),
            (
                "SELECT id FROM types WHERE n > ?",
                &[SqlValue::Integer(i64::MAX)],
            ),
            (
                "SELECT id FROM types WHERE n < ?",
                &[SqlValue::Integer(i64::MIN)],
            ),
            // INTEGER column vs REAL operands (fractions, exact 3.0, -0.0, 2^53, 2^63)
            ("SELECT id FROM types WHERE n = 3.0", &[]),
            ("SELECT id FROM types WHERE n = 3.5", &[]),
            ("SELECT id FROM types WHERE n > 3.5", &[]),
            ("SELECT id FROM types WHERE n >= 3.5", &[]),
            ("SELECT id FROM types WHERE n < 3.5", &[]),
            ("SELECT id FROM types WHERE n <= 3.5", &[]),
            ("SELECT id FROM types WHERE 3.5 < n", &[]),
            ("SELECT id FROM types WHERE n = -0.0", &[]),
            (
                "SELECT id FROM types WHERE n = ?",
                &[SqlValue::Real(p53 as f64)],
            ),
            (
                "SELECT id FROM types WHERE n > ?",
                &[SqlValue::Real(p53 as f64)],
            ),
            (
                "SELECT id FROM types WHERE n < ?",
                &[SqlValue::Real(p53 as f64)],
            ),
            (
                "SELECT id FROM types WHERE n < ?",
                &[SqlValue::Real(9_223_372_036_854_775_808.0)],
            ),
            (
                "SELECT id FROM types WHERE n >= ?",
                &[SqlValue::Real(9_223_372_036_854_775_808.0)],
            ),
            (
                "SELECT id FROM types WHERE n >= ?",
                &[SqlValue::Real(-9_223_372_036_854_775_808.0)],
            ),
            (
                "SELECT id FROM types WHERE n < ?",
                &[SqlValue::Real(-9_223_372_036_854_775_808.0)],
            ),
            // Residual predicate must not be dropped when index matches
            ("SELECT id FROM types WHERE n = 3 AND extra = 'nope'", &[]),
            ("SELECT id FROM types WHERE n = 3 AND extra = 'm2'", &[]),
            // NULL comparisons
            ("SELECT id FROM types WHERE n = NULL", &[]),
            ("SELECT id FROM types WHERE n < ?", &[SqlValue::Null]),
            // TEXT byte order and >256-byte predicates
            ("SELECT id FROM types WHERE s >= '' AND s < 'a'", &[]),
            (
                "SELECT id FROM types WHERE s = ?",
                &[SqlValue::Text("\0".into())],
            ),
            ("SELECT id FROM types WHERE s > 'a' AND s <= 'z'", &[]),
            (
                "SELECT id FROM types WHERE s = ?",
                &[SqlValue::Text("a".repeat(257))],
            ),
            (
                "SELECT id FROM types WHERE s > ?",
                &[SqlValue::Text("a".repeat(257))],
            ),
            (
                "SELECT id FROM types WHERE s < ?",
                &[SqlValue::Text("a".repeat(257))],
            ),
            // BOOLEAN index
            ("SELECT id FROM types WHERE b = TRUE", &[]),
            ("SELECT id FROM types WHERE b = FALSE", &[]),
            ("SELECT id FROM types WHERE b > FALSE", &[]),
            ("SELECT id FROM types WHERE b < TRUE", &[]),
            ("SELECT id FROM types WHERE b > TRUE", &[]),
            ("SELECT id FROM types WHERE b < FALSE", &[]),
            ("SELECT id FROM types WHERE b", &[]),
            ("SELECT id FROM types WHERE NOT b", &[]),
        ];

        for (sql, params) in queries {
            let indexed = db.execute(sql, params).unwrap();
            let scanned = db.execute_force_full_scan_for_test(sql, params).unwrap();
            assert_eq!(indexed, scanned, "mismatch for {sql} params={params:?}");
        }
    });
}

#[test]
fn compound_indexes_nullable_trailing_order_by_and_visited_bounds() {
    for_both_sql(|db, _| {
        db.execute(
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                a INT NOT NULL,
                b INT,
                c INT NOT NULL
            )",
            &[],
        )
        .unwrap();
        // Compound index with nullable column `b` in the middle/trailing position
        db.execute("CREATE INDEX idx_ab ON items (a, b)", &[])
            .unwrap();
        // Compound index with all NOT NULL columns for ORDER BY streaming
        db.execute("CREATE INDEX idx_ac ON items (a, c)", &[])
            .unwrap();

        let rows = [
            (1, 10, Some(1), 30),
            (2, 10, None, 20),
            (3, 10, Some(2), 10),
            (4, 20, Some(1), 10),
            (5, 20, None, 5),
            (6, 10, Some(2), 10), // tie with id=3 on (a, c)
        ];
        for (id, a, b, c) in rows {
            db.execute(
                "INSERT INTO items VALUES (?, ?, ?, ?)",
                &[
                    SqlValue::Integer(id),
                    SqlValue::Integer(a),
                    b.map_or(SqlValue::Null, SqlValue::Integer),
                    SqlValue::Integer(c),
                ],
            )
            .unwrap();
        }

        let queries = [
            // Prefix on (a, b) with nullable b must NOT omit id=2 (b IS NULL)!
            "SELECT id, a, b, c FROM items WHERE a = 10",
            "SELECT id, a, b, c FROM items WHERE a = 10 AND b IS NOT NULL",
            "SELECT id, a, b, c FROM items WHERE a = 10 AND b >= 1",
            "SELECT id, a, b, c FROM items WHERE a = 10 AND b > 1 AND c = 10",
            // ORDER BY on compound index (a, c) with ties & LIMIT/OFFSET
            "SELECT id, a, c FROM items ORDER BY a ASC, c ASC LIMIT 4 OFFSET 1",
            "SELECT id, a, c FROM items WHERE a = 10 ORDER BY c ASC LIMIT 3",
            "SELECT id, a, c FROM items WHERE a = 10 ORDER BY c ASC, id ASC LIMIT 2 OFFSET 1",
            "SELECT id, a, c FROM items ORDER BY a ASC LIMIT 3", // prefix only -> must tie-break by id, not c!
            "SELECT id, a, b FROM items ORDER BY b ASC LIMIT 4", // nullable b -> NULLS FIRST!
            "SELECT id, a, c FROM items WHERE a >= 10 ORDER BY id DESC LIMIT 3 OFFSET 1",
        ];

        for sql in queries {
            assert_eq!(
                db.execute(sql, &[]).unwrap(),
                db.execute_force_full_scan_for_test(sql, &[]).unwrap(),
                "mismatch for {sql}"
            );
        }
    });

    // Exact max_visited_rows enforcement on index scans and streaming ORDER BY LIMIT beyond max_visited_rows.
    let dir = tempfile::tempdir().unwrap();
    let (ckpt, _, _) = open_checkpoint(dir.path());
    let db = SqlDatabase::with_limits(
        DocumentDatabase::with_checkpoint_store(&ckpt),
        SqlLimits {
            max_visited_rows: 10,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE big_idx (id INTEGER PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)",
        &[],
    )
    .unwrap();
    db.execute("CREATE INDEX idx_gv ON big_idx (grp, val)", &[])
        .unwrap();
    for chunk in (0..50i64).collect::<Vec<_>>().chunks(25) {
        let placeholders = vec!["(?, ?, ?)"; chunk.len()].join(", ");
        let params: Vec<SqlValue> = chunk
            .iter()
            .flat_map(|&i| {
                [
                    SqlValue::Integer(i),
                    SqlValue::Integer(i % 5),
                    SqlValue::Integer(i),
                ]
            })
            .collect();
        db.execute(
            &format!("INSERT INTO big_idx VALUES {placeholders}"),
            &params,
        )
        .unwrap();
    }
    // grp = 2 matches exactly 10 rows (within max_visited_rows = 10) while full table has 50 rows!
    let SqlOutcome::Rows { rows, .. } = db
        .execute("SELECT id FROM big_idx WHERE grp = 2", &[])
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 10);
    // Insert 1 more row in grp = 2 -> 11 rows > max_visited_rows = 10 -> Bounds (no silent truncation).
    db.execute("INSERT INTO big_idx VALUES (52, 2, 52)", &[])
        .unwrap();
    assert!(matches!(
        db.execute("SELECT id FROM big_idx WHERE grp = 2", &[]),
        Err(SqlError::Bounds(_))
    ));
    // Streaming ORDER BY grp ASC, val ASC LIMIT 5 succeeds on 51-row table with max_visited_rows = 10!
    let SqlOutcome::Rows { rows, .. } = db
        .execute(
            "SELECT id, grp, val FROM big_idx ORDER BY grp ASC, val ASC LIMIT 5",
            &[],
        )
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 5);
}

#[test]
fn randomized_indexed_vs_forced_full_scan_differential() {
    for_both_sql(|db, backend| {
        db.execute(
            "CREATE TABLE diff (
                id INTEGER PRIMARY KEY,
                a INT,
                b INT NOT NULL,
                s TEXT,
                flag BOOLEAN,
                r REAL,
                UNIQUE (a, b)
            )",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX idx_b ON diff (b)", &[]).unwrap();
        db.execute("CREATE INDEX idx_s ON diff (s)", &[]).unwrap();
        db.execute("CREATE INDEX idx_bs ON diff (b, s)", &[])
            .unwrap();
        db.execute("CREATE INDEX idx_flag ON diff (flag)", &[])
            .unwrap();

        let mut rng = Rng(0xDEAD_BEEF_1234_5678);
        let texts = ["", "a", "A", "ab", "b", "é", "z"];
        for id in 0..120i64 {
            let a = if rng.below(4) == 0 {
                SqlValue::Null
            } else {
                SqlValue::Integer(id * 3)
            };
            let b = SqlValue::Integer((rng.next() % 9) as i64 - 4);
            let s = if rng.below(5) == 0 {
                SqlValue::Null
            } else {
                SqlValue::Text(texts[(rng.next() as usize) % texts.len()].into())
            };
            let flag = match rng.below(3) {
                0 => SqlValue::Null,
                1 => SqlValue::Boolean(false),
                _ => SqlValue::Boolean(true),
            };
            let r = SqlValue::Real((rng.below(15) as f64) * 0.5 - 3.0);
            db.execute(
                "INSERT INTO diff VALUES (?, ?, ?, ?, ?, ?)",
                &[SqlValue::Integer(id), a, b, s, flag, r],
            )
            .unwrap();
        }
        verify_collection_ok(db, "diff");

        let ops = ["=", "<>", "<", "<=", ">", ">="];
        let cols = ["id", "a", "b", "s", "flag"];
        for case in 0..300 {
            let mut params = Vec::new();
            let where_sql = if rng.below(6) == 0 {
                String::new()
            } else {
                let mut clauses = Vec::new();
                let n_conj = 1 + (rng.below(3) as usize);
                for _ in 0..n_conj {
                    match rng.below(7) {
                        0 => {
                            let op = ops[(rng.next() as usize) % ops.len()];
                            let v = (rng.next() % 11) as i64 - 5;
                            clauses.push(format!("b {op} ?"));
                            params.push(SqlValue::Integer(v));
                        }
                        1 => {
                            let op = ops[(rng.next() as usize) % ops.len()];
                            let f = ((rng.next() % 17) as f64) * 0.5 - 4.0;
                            clauses.push(format!("b {op} ?"));
                            params.push(SqlValue::Real(f));
                        }
                        2 => {
                            let op = ops[(rng.next() as usize) % ops.len()];
                            let t = texts[(rng.next() as usize) % texts.len()];
                            clauses.push(format!("s {op} ?"));
                            params.push(SqlValue::Text(t.into()));
                        }
                        3 => {
                            let op = ops[(rng.next() as usize) % ops.len()];
                            let b = rng.below(2) == 0;
                            clauses.push(format!("flag {op} ?"));
                            params.push(SqlValue::Boolean(b));
                        }
                        4 => {
                            let c = cols[(rng.next() as usize) % cols.len()];
                            let neg = if rng.below(2) == 0 { "" } else { "NOT " };
                            clauses.push(format!("{c} IS {neg}NULL"));
                        }
                        5 => {
                            let op = ops[(rng.next() as usize) % ops.len()];
                            let v = (rng.next() % 140) as i64 - 10;
                            clauses.push(format!("id {op} ?"));
                            params.push(SqlValue::Integer(v));
                        }
                        _ => {
                            clauses.push("(r >= 0.0 OR b = 1)".into());
                        }
                    }
                }
                format!(" WHERE {}", clauses.join(" AND "))
            };
            let order_sql = if rng.below(3) == 0 {
                String::new()
            } else {
                let k = 1 + (rng.below(2) as usize);
                let mut items = Vec::new();
                for _ in 0..k {
                    let c = cols[(rng.next() as usize) % cols.len()];
                    let dir = ["", " ASC", " DESC"][(rng.next() as usize) % 3];
                    let nulls = ["", " NULLS FIRST", " NULLS LAST"][(rng.next() as usize) % 3];
                    items.push(format!("{c}{dir}{nulls}"));
                }
                format!(" ORDER BY {}", items.join(", "))
            };
            let limit_sql = match rng.below(3) {
                0 => String::new(),
                1 => format!(" LIMIT {}", rng.below(15)),
                _ => format!(" LIMIT {} OFFSET {}", rng.below(20), rng.below(50)),
            };
            let sql =
                format!("SELECT id, a, b, s, flag, r FROM diff{where_sql}{order_sql}{limit_sql}");
            let indexed = db.execute(&sql, &params).unwrap();
            let scanned = db.execute_force_full_scan_for_test(&sql, &params).unwrap();
            assert_eq!(
                indexed, scanned,
                "[{backend}] case {case}: mismatch for `{sql}` params={params:?}"
            );
        }
    });
}

#[test]
fn durable_reopen_and_catalog_cache_invalidation_with_indexes() {
    fn run_pair<'a>(open_db: &dyn Fn() -> SqlDatabase<'a>) {
        let db1 = open_db();
        let db2 = open_db();
        db1.execute(
            "CREATE TABLE cache_t (id INTEGER PRIMARY KEY, code INT NOT NULL, label TEXT)",
            &[],
        )
        .unwrap();
        db1.execute(
            "INSERT INTO cache_t VALUES (1, 10, 'one'), (2, 20, 'two')",
            &[],
        )
        .unwrap();
        let prep = db1
            .prepare("SELECT id, label FROM cache_t WHERE code = ?")
            .unwrap();
        assert_eq!(
            db1.execute_prepared(&prep, &[SqlValue::Integer(10)])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "label".into()],
                rows: vec![vec![SqlValue::Integer(1), SqlValue::Text("one".into())]],
            }
        );

        // db2 creates a UNIQUE index on `code`; db1 must observe catalog change immediately.
        db2.execute("CREATE UNIQUE INDEX u_code ON cache_t (code)", &[])
            .unwrap();
        assert_eq!(
            db1.execute_prepared(&prep, &[SqlValue::Integer(20)])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["id".into(), "label".into()],
                rows: vec![vec![SqlValue::Integer(2), SqlValue::Text("two".into())]],
            }
        );
        assert!(matches!(
            db1.execute("INSERT INTO cache_t VALUES (3, 10, 'dup')", &[]),
            Err(SqlError::Constraint(_))
        ));
    }

    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store");
    {
        let store = Store::open(&store_path).unwrap();
        run_pair(&|| SqlDatabase::new(DocumentDatabase::new(&store)));
    }
    {
        let store = Store::open(&store_path).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        assert!(matches!(
            db.execute("INSERT INTO cache_t VALUES (4, 20, 'dup2')", &[]),
            Err(SqlError::Constraint(_))
        ));
        verify_collection_ok(&db, "cache_t");
    }

    let (ckpt, ckpt_path, digest) = open_checkpoint(dir.path());
    run_pair(&|| SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt)));
    drop(ckpt);
    let ckpt_reopened =
        CheckpointStore::open(dir.path().join("checkpoint_log"), &ckpt_path, &digest).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt_reopened));
    assert!(matches!(
        db.execute("INSERT INTO cache_t VALUES (4, 20, 'dup2')", &[]),
        Err(SqlError::Constraint(_))
    ));
    verify_collection_ok(&db, "cache_t");
}

#[test]
fn concurrent_unique_inserts_allow_exactly_one_winner() {
    fn run_race(make_db: impl Fn() -> SqlDatabase<'static> + Send + Sync) {
        let setup = make_db();
        setup
            .execute(
                "CREATE TABLE u_race (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
                &[],
            )
            .unwrap();

        let threads = 6usize;
        let barrier = Arc::new(Barrier::new(threads));
        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let b = Arc::clone(&barrier);
            let db = make_db();
            handles.push(thread::spawn(move || {
                b.wait();
                db.execute(
                    "INSERT INTO u_race VALUES (?, 'same@example.test')",
                    &[SqlValue::Integer((t + 1) as i64)],
                )
            }));
        }

        let mut successes = 0usize;
        for h in handles {
            match h.join().unwrap() {
                Ok(SqlOutcome::Inserted { rows: 1 }) => successes += 1,
                Err(SqlError::Conflict(_)) | Err(SqlError::Constraint(_)) => {}
                other => panic!("unexpected race outcome: {other:?}"),
            }
        }
        assert_eq!(successes, 1, "exactly one thread must win unique insert");
        let SqlOutcome::Rows { rows, .. } =
            setup.execute("SELECT id, email FROM u_race", &[]).unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        verify_collection_ok(&setup, "u_race");

        // Disjoint unique values commit concurrently with zero conflicts.
        let barrier2 = Arc::new(Barrier::new(threads));
        let mut handles2 = Vec::with_capacity(threads);
        for t in 0..threads {
            let b = Arc::clone(&barrier2);
            let db = make_db();
            handles2.push(thread::spawn(move || {
                b.wait();
                db.execute(
                    "INSERT INTO u_race VALUES (?, ?)",
                    &[
                        SqlValue::Integer(100 + t as i64),
                        SqlValue::Text(format!("disjoint-{t}@example.test")),
                    ],
                )
            }));
        }
        for h in handles2 {
            assert_eq!(h.join().unwrap().unwrap(), SqlOutcome::Inserted { rows: 1 });
        }
        verify_collection_ok(&setup, "u_race");
    }

    let dir = tempfile::tempdir().unwrap();
    let store: &'static Store = Box::leak(Box::new(Store::open(dir.path().join("store")).unwrap()));
    run_race(|| SqlDatabase::new(DocumentDatabase::new(store)));

    let (ckpt, _, _) = open_checkpoint(dir.path());
    let ckpt: &'static CheckpointStore = Box::leak(Box::new(ckpt));
    run_race(|| SqlDatabase::new(DocumentDatabase::with_checkpoint_store(ckpt)));
}

/// Rows written before NULL was stored by omission hold explicit JSON `null`.
/// Building a UNIQUE index over them must still allow any number of NULLs.
#[test]
fn unique_index_over_legacy_explicit_null_rows_allows_many_nulls() {
    for_both_sql(|db, backend| {
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER, v TEXT)",
            &[],
        )
        .unwrap();
        let documents = db.document_database();
        for id in 1i64..=4 {
            let key = format!("i:{:016x}", (id ^ i64::MIN) as u64);
            let v = if id <= 2 {
                serde_json::json!("same")
            } else {
                serde_json::json!(null)
            };
            documents
                .insert("t", &key, &serde_json::json!({"id": id, "u": null, "v": v}))
                .unwrap();
        }
        db.execute("CREATE UNIQUE INDEX tu ON t (u)", &[])
            .unwrap_or_else(|error| panic!("{backend}: single-column index: {error}"));
        db.execute("CREATE UNIQUE INDEX tuv ON t (u, v)", &[])
            .unwrap_or_else(|error| panic!("{backend}: compound index: {error}"));
        verify_collection_ok(db, "t");
        let SqlOutcome::Rows { rows, .. } = db
            .execute("SELECT id FROM t WHERE u IS NULL AND v IS NULL", &[])
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.len(), 2, "{backend}");
        db.execute("INSERT INTO t VALUES (5, NULL, NULL)", &[])
            .unwrap();
        db.execute("UPDATE t SET u = 7 WHERE id = 1", &[]).unwrap();
        assert!(
            matches!(
                db.execute("UPDATE t SET u = 7 WHERE id = 2", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        db.execute("UPDATE t SET u = NULL WHERE id = 1", &[])
            .unwrap();
        db.execute("UPDATE t SET u = 7 WHERE id = 2", &[]).unwrap();
        let SqlOutcome::Rows { rows, .. } =
            db.execute("SELECT id FROM t WHERE u = 7", &[]).unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows, vec![vec![SqlValue::Integer(2)]], "{backend}");
        verify_collection_ok(db, "t");
    });
}
