use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlLimits, SqlOutcome, SqlValue,
    Store,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

fn open_checkpoint(dir: &std::path::Path) -> CheckpointStore {
    let store_path = dir.join("checkpoint_log");
    let ckpt_path = dir.join("checkpoint_file");
    let initial = Store::open(&store_path).unwrap();
    let digest = initial.checkpoint_to(&ckpt_path).unwrap();
    drop(initial);
    CheckpointStore::open(&store_path, &ckpt_path, &digest).unwrap()
}

fn for_both_sql(mut f: impl FnMut(SqlDatabase<'_>)) {
    let dir = tempfile::tempdir().unwrap();
    let ordinary = Store::open(dir.path().join("ordinary")).unwrap();
    f(SqlDatabase::new(DocumentDatabase::new(&ordinary)));
    drop(ordinary);

    let ckpt = open_checkpoint(dir.path());
    f(SqlDatabase::new(DocumentDatabase::with_checkpoint_store(
        &ckpt,
    )));
}

fn for_both_sql_with_limits(limits: SqlLimits, mut f: impl FnMut(SqlDatabase<'_>)) {
    let dir = tempfile::tempdir().unwrap();
    let ordinary = Store::open(dir.path().join("ordinary")).unwrap();
    f(SqlDatabase::with_limits(DocumentDatabase::new(&ordinary), limits).unwrap());
    drop(ordinary);

    let ckpt = open_checkpoint(dir.path());
    f(SqlDatabase::with_limits(DocumentDatabase::with_checkpoint_store(&ckpt), limits).unwrap());
}

#[test]
fn aggregates_all_types_nulls_and_empty_tables_on_both_backends() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                n INTEGER,
                r REAL,
                s TEXT,
                b BOOLEAN
            )",
            &[],
        )
        .unwrap();

        // Empty table without GROUP BY returns exactly one row.
        let empty_res = db
            .execute(
                "SELECT
                    COUNT(*) AS cnt_all,
                    COUNT(n) AS cnt_n,
                    COUNT(DISTINCT n) AS cnt_dn,
                    SUM(n) AS sum_n,
                    SUM(r) AS sum_r,
                    AVG(n) AS avg_n,
                    AVG(r) AS avg_r,
                    MIN(id) AS min_id,
                    MAX(id) AS max_id,
                    MIN(s) AS min_s,
                    MAX(b) AS max_b
                 FROM items",
                &[],
            )
            .unwrap();
        assert_eq!(
            empty_res,
            SqlOutcome::Rows {
                columns: vec![
                    "cnt_all".into(),
                    "cnt_n".into(),
                    "cnt_dn".into(),
                    "sum_n".into(),
                    "sum_r".into(),
                    "avg_n".into(),
                    "avg_r".into(),
                    "min_id".into(),
                    "max_id".into(),
                    "min_s".into(),
                    "max_b".into(),
                ],
                rows: vec![vec![
                    SqlValue::Integer(0),
                    SqlValue::Integer(0),
                    SqlValue::Integer(0),
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                ]],
            }
        );

        // Empty table with GROUP BY returns 0 rows.
        assert_eq!(
            db.execute("SELECT n, COUNT(*) FROM items GROUP BY n", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["n".into(), "count(*)".into()],
                rows: Vec::new(),
            }
        );

        // Insert rows where some columns are all-NULL first.
        db.execute(
            "INSERT INTO items VALUES
                (1, NULL, NULL, NULL, NULL),
                (2, NULL, NULL, NULL, NULL)",
            &[],
        )
        .unwrap();
        assert_eq!(
            db.execute(
                "SELECT COUNT(*), COUNT(n), COUNT(DISTINCT n), SUM(n), AVG(n), MIN(n), MAX(n), MIN(s), MAX(b) FROM items",
                &[]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec![
                    "count(*)".into(),
                    "count(n)".into(),
                    "count(distinct n)".into(),
                    "sum(n)".into(),
                    "avg(n)".into(),
                    "min(n)".into(),
                    "max(n)".into(),
                    "min(s)".into(),
                    "max(b)".into(),
                ],
                rows: vec![vec![
                    SqlValue::Integer(2),
                    SqlValue::Integer(0),
                    SqlValue::Integer(0),
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                ]],
            }
        );

        // Insert mixed typed values including duplicate values, -0.0/0.0, UTF-8 byte order, booleans.
        db.execute(
            "INSERT INTO items VALUES
                (3, 10, -0.0, 'a', FALSE),
                (4, 10, 0.0, 'Z', TRUE),
                (5, -5, 2.5, 'é', FALSE),
                (6, 2, 1.5, 'b', TRUE)",
            &[],
        )
        .unwrap();

        let res = db
            .execute(
                "SELECT
                    COUNT(*),
                    COUNT(n),
                    COUNT(DISTINCT n),
                    COUNT(DISTINCT r),
                    COUNT(DISTINCT s),
                    COUNT(DISTINCT b),
                    SUM(n),
                    SUM(r),
                    AVG(n),
                    AVG(r),
                    MIN(n),
                    MAX(n),
                    MIN(r),
                    MAX(r),
                    MIN(s),
                    MAX(s),
                    MIN(b),
                    MAX(b)
                 FROM items",
                &[],
            )
            .unwrap();
        assert_eq!(
            res,
            SqlOutcome::Rows {
                columns: vec![
                    "count(*)".into(),
                    "count(n)".into(),
                    "count(distinct n)".into(),
                    "count(distinct r)".into(),
                    "count(distinct s)".into(),
                    "count(distinct b)".into(),
                    "sum(n)".into(),
                    "sum(r)".into(),
                    "avg(n)".into(),
                    "avg(r)".into(),
                    "min(n)".into(),
                    "max(n)".into(),
                    "min(r)".into(),
                    "max(r)".into(),
                    "min(s)".into(),
                    "max(s)".into(),
                    "min(b)".into(),
                    "max(b)".into(),
                ],
                rows: vec![vec![
                    SqlValue::Integer(6),
                    SqlValue::Integer(4),
                    SqlValue::Integer(3), // 10, -5, 2 (NULLs ignored)
                    SqlValue::Integer(3), // 0.0/-0.0, 2.5, 1.5
                    SqlValue::Integer(4), // 'a', 'Z', 'é', 'b'
                    SqlValue::Integer(2), // FALSE, TRUE
                    SqlValue::Integer(17),
                    SqlValue::Real(4.0),
                    SqlValue::Real(4.25), // 17 / 4
                    SqlValue::Real(1.0),  // 4.0 / 4
                    SqlValue::Integer(-5),
                    SqlValue::Integer(10),
                    SqlValue::Real(0.0),
                    SqlValue::Real(2.5),
                    SqlValue::Text("Z".into()), // byte order: 'Z' (0x5A) < 'a' (0x61) < 'é' (0xC3)
                    SqlValue::Text("é".into()),
                    SqlValue::Boolean(false),
                    SqlValue::Boolean(true),
                ]],
            }
        );

        // WHERE matching zero rows on populated table:
        // ungrouped still returns 1 row; grouped returns 0 rows.
        assert_eq!(
            db.execute("SELECT COUNT(*), SUM(n) FROM items WHERE id > 999", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["count(*)".into(), "sum(n)".into()],
                rows: vec![vec![SqlValue::Integer(0), SqlValue::Null]],
            }
        );
        assert_eq!(
            db.execute(
                "SELECT n, COUNT(*) FROM items WHERE id > 999 GROUP BY n",
                &[]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["n".into(), "count(*)".into()],
                rows: Vec::new(),
            }
        );

        // LIMIT 0 returns 0 rows even for ungrouped aggregate.
        assert_eq!(
            db.execute("SELECT COUNT(*) FROM items LIMIT 0", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["count(*)".into()],
                rows: Vec::new(),
            }
        );
    });
}

#[test]
fn sum_overflow_avg_precision_and_type_errors_on_both_backends() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE nums (id INTEGER PRIMARY KEY, grp INT, v INT, r REAL, s TEXT, b BOOLEAN)",
            &[],
        )
        .unwrap();

        // Bind-time Type errors for SUM/AVG on TEXT and BOOLEAN even on empty table.
        for bad in [
            "SELECT SUM(s) FROM nums",
            "SELECT AVG(s) FROM nums",
            "SELECT SUM(b) FROM nums",
            "SELECT AVG(b) FROM nums",
            "SELECT COUNT(*) FROM nums HAVING SUM(s) > 0",
            "SELECT COUNT(*) FROM nums ORDER BY AVG(b)",
            "SELECT COUNT(*) FROM nums HAVING COUNT(*) = 'x'",
            "SELECT COUNT(*) FROM nums HAVING COUNT(*)",
        ] {
            let err = db.execute(bad, &[]).unwrap_err();
            assert!(
                matches!(err, SqlError::Type(_)),
                "expected Type error for `{bad}`, got {err:?}"
            );
        }

        // Exact i64::MAX and i64::MIN sums succeed; i64::MAX + 1 - 1 succeeds (order-independent i128 sum).
        db.execute(
            "INSERT INTO nums VALUES
                (1, 1, 9223372036854775807, 1.0, 'a', TRUE),
                (2, 1, 1, 1.0, 'b', FALSE),
                (3, 1, -1, 1.0, 'c', TRUE)",
            &[],
        )
        .unwrap();
        assert_eq!(
            db.execute("SELECT SUM(v) FROM nums WHERE grp = 1", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["sum(v)".into()],
                rows: vec![vec![SqlValue::Integer(i64::MAX)]],
            }
        );

        // AVG uses i128 sum before dividing so [i64::MAX, i64::MAX] doesn't overflow.
        db.execute(
            "INSERT INTO nums VALUES
                (4, 2, 9223372036854775807, 1e308, 'd', TRUE),
                (5, 2, 9223372036854775807, 1e308, 'e', FALSE)",
            &[],
        )
        .unwrap();
        assert_eq!(
            db.execute("SELECT AVG(v) FROM nums WHERE grp = 2", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["avg(v)".into()],
                rows: vec![vec![SqlValue::Real(i64::MAX as f64)]],
            }
        );

        // SUM(INTEGER) positive overflow -> Bounds error (no wraparound, no REAL promotion).
        assert!(matches!(
            db.execute("SELECT SUM(v) FROM nums WHERE grp = 2", &[]),
            Err(SqlError::Bounds(_))
        ));

        // SUM(REAL) overflow -> Bounds error (no Inf).
        assert!(matches!(
            db.execute("SELECT SUM(r) FROM nums WHERE grp = 2", &[]),
            Err(SqlError::Bounds(_))
        ));

        // SUM(INTEGER) negative overflow -> Bounds error.
        db.execute(
            "INSERT INTO nums VALUES
                (6, 3, -9223372036854775808, 0.0, 'f', FALSE),
                (7, 3, -1, 0.0, 'g', FALSE)",
            &[],
        )
        .unwrap();
        assert!(matches!(
            db.execute("SELECT SUM(v) FROM nums WHERE grp = 3", &[]),
            Err(SqlError::Bounds(_))
        ));
    });
}

#[test]
fn group_by_having_order_by_aliases_and_limit_on_both_backends() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE sales (
                id INTEGER PRIMARY KEY,
                dept TEXT,
                region BOOLEAN,
                amount INT,
                flag BOOLEAN
            )",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO sales VALUES
                (1, 'eng', TRUE, 100, TRUE),
                (2, 'eng', TRUE, 200, FALSE),
                (3, 'eng', FALSE, 50, NULL),
                (4, 'sales', TRUE, NULL, TRUE),
                (5, NULL, NULL, 300, FALSE),
                (6, NULL, NULL, 100, TRUE),
                (7, 'hr', FALSE, 40, NULL)",
            &[],
        )
        .unwrap();

        // Single-column GROUP BY with NULL group, default deterministic order (ASC NULLS FIRST).
        assert_eq!(
            db.execute(
                "SELECT dept AS department, COUNT(*) AS rows_cnt, COUNT(amount) AS non_null_cnt, SUM(amount) AS total
                 FROM sales GROUP BY dept",
                &[]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec![
                    "department".into(),
                    "rows_cnt".into(),
                    "non_null_cnt".into(),
                    "total".into(),
                ],
                rows: vec![
                    vec![
                        SqlValue::Null,
                        SqlValue::Integer(2),
                        SqlValue::Integer(2),
                        SqlValue::Integer(400),
                    ],
                    vec![
                        SqlValue::Text("eng".into()),
                        SqlValue::Integer(3),
                        SqlValue::Integer(3),
                        SqlValue::Integer(350),
                    ],
                    vec![
                        SqlValue::Text("hr".into()),
                        SqlValue::Integer(1),
                        SqlValue::Integer(1),
                        SqlValue::Integer(40),
                    ],
                    vec![
                        SqlValue::Text("sales".into()),
                        SqlValue::Integer(1),
                        SqlValue::Integer(0),
                        SqlValue::Null,
                    ],
                ],
            }
        );

        // Multi-column GROUP BY with NULLs, HAVING over aggregates & grouping cols, ORDER BY alias & agg expr, LIMIT/OFFSET.
        assert_eq!(
            db.execute(
                "SELECT dept, region, COUNT(*) AS c, SUM(amount) AS total
                 FROM sales
                 GROUP BY dept, region
                 HAVING COUNT(*) >= ? OR dept IS NULL
                 ORDER BY total DESC NULLS LAST, dept ASC",
                &[SqlValue::Integer(2)]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["dept".into(), "region".into(), "c".into(), "total".into(),],
                rows: vec![
                    vec![
                        SqlValue::Null,
                        SqlValue::Null,
                        SqlValue::Integer(2),
                        SqlValue::Integer(400),
                    ],
                    vec![
                        SqlValue::Text("eng".into()),
                        SqlValue::Boolean(true),
                        SqlValue::Integer(2),
                        SqlValue::Integer(300),
                    ],
                ],
            }
        );

        // ORDER BY aggregate expression directly (both in SELECT and NOT in SELECT), LIMIT/OFFSET after grouping.
        assert_eq!(
            db.execute(
                "SELECT dept, COUNT(*) FROM sales GROUP BY dept ORDER BY SUM(amount) DESC NULLS LAST, dept DESC LIMIT 2 OFFSET 1",
                &[]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["dept".into(), "count(*)".into()],
                rows: vec![
                    vec![SqlValue::Text("eng".into()), SqlValue::Integer(3)],
                    vec![SqlValue::Text("hr".into()), SqlValue::Integer(1)],
                ],
            }
        );

        // HAVING without GROUP BY on an aggregate query (1 global group).
        assert_eq!(
            db.execute("SELECT COUNT(*) FROM sales HAVING COUNT(*) > 5", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["count(*)".into()],
                rows: vec![vec![SqlValue::Integer(7)]],
            }
        );
        assert_eq!(
            db.execute("SELECT COUNT(*) FROM sales HAVING COUNT(*) > 100", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["count(*)".into()],
                rows: Vec::new(),
            }
        );

        // AS alias on non-aggregate plain columns + ORDER BY alias.
        assert_eq!(
            db.execute(
                "SELECT id AS sale_id, amount AS amt FROM sales WHERE dept = 'eng' ORDER BY amt DESC, sale_id ASC",
                &[]
            )
            .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["sale_id".into(), "amt".into()],
                rows: vec![
                    vec![SqlValue::Integer(2), SqlValue::Integer(200)],
                    vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                    vec![SqlValue::Integer(3), SqlValue::Integer(50)],
                ],
            }
        );
    });
}

#[test]
fn group_and_distinct_bounds_enforced_at_limit_and_limit_plus_one() {
    let limits = SqlLimits {
        max_groups: 4,
        max_distinct_values: 5,
        ..Default::default()
    };
    for_both_sql_with_limits(limits, |db| {
        db.execute(
            "CREATE TABLE b (id INTEGER PRIMARY KEY, grp INT, val TEXT)",
            &[],
        )
        .unwrap();
        // 4 distinct groups (including NULL), 5 distinct non-NULL `val`s + NULLs.
        db.execute(
            "INSERT INTO b VALUES
                (1, NULL, NULL),
                (2, NULL, 'v1'),
                (3, 1, 'v2'),
                (4, 1, 'v2'),
                (5, 2, 'v3'),
                (6, 2, 'v4'),
                (7, 3, 'v5'),
                (8, 3, NULL)",
            &[],
        )
        .unwrap();

        // Exactly at max_groups = 4 -> succeeds.
        assert_eq!(
            db.execute("SELECT grp, COUNT(*) FROM b GROUP BY grp", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["grp".into(), "count(*)".into()],
                rows: vec![
                    vec![SqlValue::Null, SqlValue::Integer(2)],
                    vec![SqlValue::Integer(1), SqlValue::Integer(2)],
                    vec![SqlValue::Integer(2), SqlValue::Integer(2)],
                    vec![SqlValue::Integer(3), SqlValue::Integer(2)],
                ],
            }
        );

        // Exactly at max_distinct_values = 5 (NULLs excluded) -> succeeds.
        assert_eq!(
            db.execute("SELECT COUNT(DISTINCT val) FROM b", &[])
                .unwrap(),
            SqlOutcome::Rows {
                columns: vec!["count(distinct val)".into()],
                rows: vec![vec![SqlValue::Integer(5)]],
            }
        );

        // Adding a 5th group and 6th distinct value -> both fail with Bounds (never truncate, even with LIMIT 1!).
        db.execute("INSERT INTO b VALUES (9, 4, 'v6')", &[])
            .unwrap();
        assert!(matches!(
            db.execute("SELECT grp, COUNT(*) FROM b GROUP BY grp LIMIT 1", &[]),
            Err(SqlError::Bounds(_))
        ));
        assert!(matches!(
            db.execute("SELECT COUNT(DISTINCT val) FROM b", &[]),
            Err(SqlError::Bounds(_))
        ));
    });
}

#[test]
fn unsupported_and_invalid_aggregate_forms_rejected() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b TEXT)",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10, 'x')", &[])
            .unwrap();

        // Unsupported forms -> SqlError::Unsupported
        let unsupported = [
            "SELECT COUNT(*) + 1 FROM t",
            "SELECT a + 1 FROM t",
            "SELECT SUM(a + 1) FROM t",
            "SELECT SUM(COUNT(*)) FROM t",
            "SELECT COUNT(DISTINCT MIN(a)) FROM t",
            "SELECT * FROM t WHERE COUNT(*) > 0",
            "SELECT * FROM t WHERE SUM(a) > 0",
            "SELECT SUM(DISTINCT a) FROM t",
            "SELECT AVG(DISTINCT a) FROM t",
            "SELECT MIN(DISTINCT a) FROM t",
            "SELECT MAX(DISTINCT a) FROM t",
            "SELECT COUNT(1) FROM t",
            "SELECT SUM(1) FROM t",
            "SELECT COUNT(*) OVER () FROM t",
            "SELECT COUNT(*) FILTER (WHERE a > 0) FROM t",
            "SELECT a FROM t GROUP BY 1",
            "SELECT a FROM t GROUP BY a + 1",
            "SELECT a FROM t GROUP BY ROLLUP (a)",
            "SELECT a FROM t GROUP BY COUNT(*)",
            "SELECT COUNT(*) FROM t ORDER BY COUNT(*) + 1",
        ];
        for sql in unsupported {
            let err = db.execute(sql, &[]).unwrap_err();
            assert!(
                matches!(err, SqlError::Unsupported { .. }),
                "expected Unsupported for `{sql}`, got {err:?}"
            );
        }

        // Standard SQL / PostgreSQL semantic & syntax rejections -> Syntax
        let syntax_errs = [
            // Non-grouped columns in SELECT / HAVING / ORDER BY (no SQLite bare-column leniency)
            "SELECT id, COUNT(*) FROM t",
            "SELECT a, b FROM t GROUP BY a",
            "SELECT * FROM t GROUP BY a",
            "SELECT * FROM t HAVING COUNT(*) > 0",
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING b = 'x'",
            "SELECT COUNT(*) FROM t GROUP BY a ORDER BY b",
            "SELECT COUNT(*) FROM t ORDER BY id",
            // HAVING cannot reference SELECT aliases (PostgreSQL rule)
            "SELECT a, COUNT(*) AS cnt FROM t GROUP BY a HAVING cnt > 0",
            // Ambiguous alias in ORDER BY
            "SELECT COUNT(*) AS x, SUM(a) AS x FROM t ORDER BY x",
            "SELECT a AS x, b AS x FROM t ORDER BY x",
            // Invalid aggregate syntax
            "SELECT COUNT() FROM t",
            "SELECT SUM(*) FROM t",
            "SELECT COUNT(DISTINCT *) FROM t",
            "SELECT * AS all_cols FROM t",
            // Out of order clauses
            "SELECT a FROM t HAVING COUNT(*) > 0 GROUP BY a",
            "SELECT a FROM t GROUP BY a WHERE a > 0",
            "SELECT a FROM t ORDER BY a GROUP BY a",
            "SELECT a FROM t LIMIT 1 GROUP BY a",
        ];
        for sql in syntax_errs {
            let err = db.execute(sql, &[]).unwrap_err();
            assert!(
                matches!(err, SqlError::Syntax { .. } | SqlError::NoSuchColumn { .. }),
                "expected Syntax/NoSuchColumn for `{sql}`, got {err:?}"
            );
        }
        // Specifically verify HAVING alias fails with NoSuchColumn (cnt not a table column)
        assert!(matches!(
            db.execute(
                "SELECT a, COUNT(*) AS cnt FROM t GROUP BY a HAVING cnt > 0",
                &[]
            ),
            Err(SqlError::NoSuchColumn { .. })
        ));
    });
}

#[test]
fn min_pk_and_index_optimizations_match_full_scan_and_respect_visit_budget() {
    for_both_sql_with_limits(
        SqlLimits {
            max_visited_rows: 3,
            ..Default::default()
        },
        |db| {
            db.execute(
                "CREATE TABLE m (id INTEGER PRIMARY KEY, grp INT NOT NULL, val INT, tag TEXT NOT NULL)",
                &[],
            )
            .unwrap();
            db.execute("CREATE INDEX idx_val ON m (val)", &[]).unwrap();
            db.execute("CREATE INDEX idx_grp_val ON m (grp, val)", &[])
                .unwrap();

            for i in 1..=20i64 {
                let val = if i <= 5 {
                    SqlValue::Null
                } else {
                    SqlValue::Integer(100 - i)
                };
                db.execute(
                    "INSERT INTO m VALUES (?, ?, ?, ?)",
                    &[
                        SqlValue::Integer(i * 10),
                        SqlValue::Integer(i % 2),
                        val,
                        SqlValue::Text(format!("t{i:02}")),
                    ],
                )
                .unwrap();
            }

            // General COUNT(*) over 20 rows exceeds max_visited_rows = 3.
            assert!(matches!(
                db.execute("SELECT COUNT(*) FROM m", &[]),
                Err(SqlError::Bounds(_))
            ));

            // MIN(id) without WHERE visits 1 row via first-entry optimization and succeeds.
            assert_eq!(
                db.execute("SELECT MIN(id) FROM m", &[]).unwrap(),
                SqlOutcome::Rows {
                    columns: vec!["min(id)".into()],
                    rows: vec![vec![SqlValue::Integer(10)]],
                }
            );

            // MIN(val) without WHERE visits 1 entry via Ready index idx_val (skipping NULLs) and succeeds.
            assert_eq!(
                db.execute("SELECT MIN(val) FROM m", &[]).unwrap(),
                SqlOutcome::Rows {
                    columns: vec!["min(val)".into()],
                    rows: vec![vec![SqlValue::Integer(80)]], // 100 - 20 = 80
                }
            );

            // MIN(val) with compound prefix WHERE grp = 1 AND val > 81 uses idx_grp_val and stops on first match.
            assert_eq!(
                db.execute("SELECT MIN(val) FROM m WHERE grp = 1 AND val > 81", &[])
                    .unwrap(),
                SqlOutcome::Rows {
                    columns: vec!["min(val)".into()],
                    rows: vec![vec![SqlValue::Integer(83)]], // i=17 -> 100 - 17 = 83
                }
            );
        },
    );

    // Equivalence check against force_full_scan on various NULL/empty/index shapes.
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE eq (id INTEGER PRIMARY KEY, grp INT NOT NULL, val INT, opt INT)",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX idx_val ON eq (val)", &[]).unwrap();
        db.execute("CREATE INDEX idx_grp_val ON eq (grp, val)", &[])
            .unwrap();
        db.execute("CREATE INDEX idx_val_opt ON eq (val, opt)", &[])
            .unwrap();

        // All NULL val rows: MIN(val) must be NULL on both optimized and full-scan paths.
        db.execute(
            "INSERT INTO eq VALUES (1, 1, NULL, NULL), (2, 1, NULL, 5)",
            &[],
        )
        .unwrap();
        for sql in [
            "SELECT MIN(id) FROM eq",
            "SELECT MIN(val) FROM eq",
            "SELECT MIN(val) FROM eq WHERE grp = 1",
        ] {
            let opt = db.execute(sql, &[]).unwrap();
            let full = db.execute_force_full_scan_for_test(sql, &[]).unwrap();
            assert_eq!(opt, full, "mismatch on `{sql}`");
        }

        // Mixed NULL and non-NULL values.
        db.execute(
            "INSERT INTO eq VALUES (3, 1, 50, NULL), (4, 2, 10, 1), (5, 2, 20, NULL), (6, 1, 30, 2)",
            &[],
        )
        .unwrap();
        for sql in [
            "SELECT MIN(id) FROM eq",
            "SELECT MIN(id) FROM eq WHERE id >= 3",
            "SELECT MIN(id) FROM eq WHERE id > 100",
            "SELECT MIN(val) FROM eq",
            "SELECT MIN(val) FROM eq WHERE val > 15",
            "SELECT MIN(val) FROM eq WHERE grp = 1 AND val > 10",
            "SELECT MIN(val) FROM eq WHERE grp = 2 AND opt IS NOT NULL",
            "SELECT MIN(val) AS mv FROM eq HAVING MIN(val) >= 10",
            "SELECT MIN(val) AS mv FROM eq HAVING MIN(val) > 100",
        ] {
            let opt = db.execute(sql, &[]).unwrap();
            let full = db.execute_force_full_scan_for_test(sql, &[]).unwrap();
            assert_eq!(opt, full, "mismatch on `{sql}`");
        }
    });
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

fn to_sqlite(value: &SqlValue) -> rusqlite::types::Value {
    use rusqlite::types::Value;
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(n) => Value::Integer(*n),
        SqlValue::Real(f) => Value::Real(*f),
        SqlValue::Text(s) => Value::Text(s.clone()),
        SqlValue::Boolean(b) => Value::Integer(*b as i64),
    }
}

fn from_sqlite_expected(value: rusqlite::types::Value, expect_bool: bool) -> SqlValue {
    use rusqlite::types::Value;
    match value {
        Value::Null => SqlValue::Null,
        Value::Integer(n) if expect_bool => SqlValue::Boolean(n != 0),
        Value::Integer(n) => SqlValue::Integer(n),
        Value::Real(f) => SqlValue::Real(f),
        Value::Text(s) => SqlValue::Text(s),
        Value::Blob(_) => panic!("unexpected blob"),
    }
}

#[test]
fn randomized_aggregates_differential_against_sqlite_on_both_backends() {
    for_both_sql(|emda| {
        let lite = rusqlite::Connection::open_in_memory().unwrap();
        let ddls = [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, g1 INT, g2 TEXT, n INT, r REAL, s TEXT, b BOOLEAN)",
            "CREATE INDEX idx_g1 ON t (g1)",
            "CREATE INDEX idx_n ON t (n)",
            "CREATE INDEX idx_g1_n ON t (g1, n)",
        ];
        for ddl in ddls {
            emda.execute(ddl, &[]).unwrap();
            lite.execute(ddl, []).unwrap();
        }

        let mut rng = Rng(0xa662_cafe_1234_9876);
        let mut val_rng = Rng(0x0000_ddba_cafe_babe);
        let texts = ["", "a", "Z", "ab", "é", "z"];
        for id in 0..250i64 {
            let mut maybe = |v: SqlValue| {
                if rng.below(5) == 0 { SqlValue::Null } else { v }
            };
            let g1 = maybe(SqlValue::Integer((val_rng.next() as i64).rem_euclid(7) - 2));
            let g2 = maybe(SqlValue::Text(texts[(val_rng.below(4)) as usize].into()));
            let n = maybe(SqlValue::Integer(
                (val_rng.next() as i64).rem_euclid(21) - 10,
            ));
            let r = maybe(SqlValue::Real((val_rng.below(17) as f64) * 0.25 - 2.0));
            let s = maybe(SqlValue::Text(texts[(val_rng.below(6)) as usize].into()));
            let b = maybe(SqlValue::Boolean(val_rng.below(2) == 0));
            let row = vec![SqlValue::Integer(id), g1, g2, n, r, s, b];
            emda.execute("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?)", &row)
                .unwrap();
            lite.execute(
                "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?)",
                rusqlite::params_from_iter(row.iter().map(to_sqlite)),
            )
            .unwrap();
        }

        let predicates = [
            "",
            "WHERE n >= -3",
            "WHERE g1 = 1 OR g2 IS NULL",
            "WHERE r <= 1.0 AND b = TRUE",
            "WHERE id >= 20 AND id < 180",
            "WHERE id > 9999",
        ];

        // 1. Ungrouped aggregate queries.
        for pred in predicates {
            let sql = format!(
                "SELECT COUNT(*), COUNT(n), COUNT(DISTINCT n), SUM(n), SUM(r), AVG(n), AVG(r), MIN(n), MAX(n), MIN(s), MAX(s), MIN(b), MAX(b) FROM t {pred}"
            );
            let SqlOutcome::Rows { rows: ours, .. } = emda.execute(&sql, &[]).unwrap() else {
                panic!("expected rows");
            };
            let SqlOutcome::Rows {
                rows: ours_full, ..
            } = emda.execute_force_full_scan_for_test(&sql, &[]).unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(ours, ours_full, "index vs full scan mismatch for `{sql}`");

            let mut stmt = lite.prepare(&sql).unwrap();
            let theirs = stmt
                .query_map([], |row| {
                    Ok(vec![
                        from_sqlite_expected(row.get(0).unwrap(), false),
                        from_sqlite_expected(row.get(1).unwrap(), false),
                        from_sqlite_expected(row.get(2).unwrap(), false),
                        from_sqlite_expected(row.get(3).unwrap(), false),
                        from_sqlite_expected(row.get(4).unwrap(), false),
                        from_sqlite_expected(row.get(5).unwrap(), false),
                        from_sqlite_expected(row.get(6).unwrap(), false),
                        from_sqlite_expected(row.get(7).unwrap(), false),
                        from_sqlite_expected(row.get(8).unwrap(), false),
                        from_sqlite_expected(row.get(9).unwrap(), false),
                        from_sqlite_expected(row.get(10).unwrap(), false),
                        from_sqlite_expected(row.get(11).unwrap(), true),
                        from_sqlite_expected(row.get(12).unwrap(), true),
                    ])
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(ours, theirs, "SQLite oracle mismatch for `{sql}`");
        }

        // 2. Randomized GROUP BY + HAVING + ORDER BY + LIMIT/OFFSET queries.
        let group_sets: [&[(&str, bool)]; 3] = [
            &[("g1", false)],
            &[("g2", false)],
            &[("g1", false), ("g2", false)],
        ];
        let havings = [
            "",
            "HAVING COUNT(*) >= 3",
            "HAVING SUM(n) > 0 AND MIN(n) IS NOT NULL",
            "HAVING COUNT(DISTINCT s) >= 2 OR MAX(b) = TRUE",
        ];
        let order_choices = [
            ("", ""),
            (
                "ORDER BY cnt DESC, total ASC NULLS FIRST",
                "ORDER BY cnt DESC, total ASC NULLS FIRST",
            ),
            (
                "ORDER BY SUM(n) DESC NULLS LAST",
                "ORDER BY SUM(n) DESC NULLS LAST",
            ),
            (
                "ORDER BY avg_r ASC NULLS LAST",
                "ORDER BY avg_r ASC NULLS LAST",
            ),
        ];

        for _ in 0..120 {
            let grp = group_sets[rng.below(group_sets.len() as u64) as usize];
            let grp_cols_sql = grp.iter().map(|(c, _)| *c).collect::<Vec<_>>().join(", ");
            let pred = predicates[rng.below(predicates.len() as u64) as usize];
            let having = havings[rng.below(havings.len() as u64) as usize];
            let (ord_emda_base, ord_lite_base) =
                order_choices[rng.below(order_choices.len() as u64) as usize];
            let tie_break = grp
                .iter()
                .map(|(c, _)| format!("{c} ASC NULLS FIRST"))
                .collect::<Vec<_>>()
                .join(", ");
            let ord_lite = if ord_lite_base.is_empty() {
                format!("ORDER BY {tie_break}")
            } else {
                format!("{ord_lite_base}, {tie_break}")
            };
            let limit = match rng.below(3) {
                0 => String::new(),
                1 => format!(" LIMIT {}", rng.below(8)),
                _ => format!(" LIMIT {} OFFSET {}", rng.below(6), rng.below(10)),
            };

            let select_list = format!(
                "{grp_cols_sql}, COUNT(*) AS cnt, COUNT(DISTINCT s) AS ds, SUM(n) AS total, AVG(r) AS avg_r, MIN(b) AS min_b"
            );
            let emda_sql = format!(
                "SELECT {select_list} FROM t {pred} GROUP BY {grp_cols_sql} {having} {ord_emda_base}{limit}"
            );
            let lite_sql = format!(
                "SELECT {select_list} FROM t {pred} GROUP BY {grp_cols_sql} {having} {ord_lite}{limit}"
            );

            let SqlOutcome::Rows { rows: ours, .. } = emda.execute(&emda_sql, &[]).unwrap() else {
                panic!("expected rows");
            };
            let SqlOutcome::Rows {
                rows: ours_full, ..
            } = emda
                .execute_force_full_scan_for_test(&emda_sql, &[])
                .unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(ours, ours_full, "index vs full scan mismatch `{emda_sql}`");

            let mut stmt = lite.prepare(&lite_sql).unwrap();
            let grp_len = grp.len();
            let theirs = stmt
                .query_map([], |row| {
                    let mut out = Vec::with_capacity(grp_len + 5);
                    for (i, &(_, expect_bool)) in grp.iter().enumerate() {
                        out.push(from_sqlite_expected(row.get(i).unwrap(), expect_bool));
                    }
                    out.push(from_sqlite_expected(row.get(grp_len).unwrap(), false));
                    out.push(from_sqlite_expected(row.get(grp_len + 1).unwrap(), false));
                    out.push(from_sqlite_expected(row.get(grp_len + 2).unwrap(), false));
                    out.push(from_sqlite_expected(row.get(grp_len + 3).unwrap(), false));
                    out.push(from_sqlite_expected(row.get(grp_len + 4).unwrap(), true));
                    Ok(out)
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(ours, theirs, "SQLite oracle mismatch `{emda_sql}`");
        }
    });
}

#[derive(Clone, Debug)]
struct ModelRow {
    g: Option<i64>,
    n: Option<i64>,
    r: Option<f64>,
    s: Option<String>,
    b: Option<bool>,
}

fn model_cmp_opt<T>(
    a: Option<&T>,
    b: Option<&T>,
    nulls_first: bool,
    desc: bool,
    cmp: impl FnOnce(&T, &T) -> Ordering,
) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(av), Some(bv)) => {
            let o = cmp(av, bv);
            if desc { o.reverse() } else { o }
        }
    }
}

#[test]
fn randomized_aggregate_model_test_on_both_backends() {
    for_both_sql(|db| {
        db.execute(
            "CREATE TABLE m (id INTEGER PRIMARY KEY, g INT, n INT, r REAL, s TEXT, b BOOLEAN)",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX idx_g ON m (g)", &[]).unwrap();
        db.execute("CREATE INDEX idx_n ON m (n)", &[]).unwrap();

        let mut model: BTreeMap<i64, ModelRow> = BTreeMap::new();
        let mut rng = Rng(0x7777_4444_1111_9999);
        let strs = ["a", "B", "é", "z"];

        for step in 0..120 {
            match step % 5 {
                0 | 1 => {
                    let id = (rng.below(60)) as i64 - 20;
                    let g = if rng.below(5) == 0 {
                        None
                    } else {
                        Some((rng.below(6)) as i64 - 2)
                    };
                    let n = if rng.below(5) == 0 {
                        None
                    } else if rng.below(25) == 0 {
                        Some(i64::MAX)
                    } else {
                        Some((rng.below(15)) as i64 - 5)
                    };
                    let r = if rng.below(5) == 0 {
                        None
                    } else {
                        Some((rng.below(9) as f64) * 0.5 - 2.0)
                    };
                    let s = if rng.below(5) == 0 {
                        None
                    } else {
                        Some(strs[rng.below(4) as usize].to_owned())
                    };
                    let b = if rng.below(5) == 0 {
                        None
                    } else {
                        Some(rng.below(2) == 0)
                    };
                    let params = [
                        SqlValue::Integer(id),
                        g.map_or(SqlValue::Null, SqlValue::Integer),
                        n.map_or(SqlValue::Null, SqlValue::Integer),
                        r.map_or(SqlValue::Null, SqlValue::Real),
                        s.clone().map_or(SqlValue::Null, SqlValue::Text),
                        b.map_or(SqlValue::Null, SqlValue::Boolean),
                    ];
                    let res = db.execute("INSERT INTO m VALUES (?, ?, ?, ?, ?, ?)", &params);
                    if let std::collections::btree_map::Entry::Vacant(e) = model.entry(id) {
                        res.unwrap();
                        e.insert(ModelRow { g, n, r, s, b });
                    } else {
                        assert!(matches!(res, Err(SqlError::Constraint(_))));
                    }
                }
                2 => {
                    let target = (rng.below(60)) as i64 - 20;
                    let res = db
                        .execute("DELETE FROM m WHERE id = ?", &[SqlValue::Integer(target)])
                        .unwrap();
                    let expected = usize::from(model.remove(&target).is_some());
                    assert_eq!(res, SqlOutcome::Deleted { rows: expected });
                }
                _ => {
                    let min_g = (rng.below(5)) as i64 - 3;
                    let min_cnt = (rng.below(4)) as i64;
                    let desc = rng.below(2) == 0;
                    let limit = rng.below(5) as usize;
                    let offset = rng.below(4) as usize;
                    let sql = format!(
                        "SELECT g, COUNT(*) AS c, COUNT(DISTINCT s) AS ds, SUM(n) AS sn, AVG(r) AS ar, MIN(s) AS mins, MAX(b) AS maxb
                         FROM m
                         WHERE g >= ? OR g IS NULL
                         GROUP BY g
                         HAVING COUNT(*) >= ?
                         ORDER BY c {}, g ASC
                         LIMIT ? OFFSET ?",
                        if desc { "DESC" } else { "ASC" }
                    );
                    let params = [
                        SqlValue::Integer(min_g),
                        SqlValue::Integer(min_cnt),
                        SqlValue::Integer(limit as i64),
                        SqlValue::Integer(offset as i64),
                    ];

                    // Compute model expectation.
                    let mut grouped: BTreeMap<Option<i64>, Vec<&ModelRow>> = BTreeMap::new();
                    for row in model.values() {
                        let keep = row.g.is_none_or(|gv| gv >= min_g);
                        if keep {
                            grouped.entry(row.g).or_default().push(row);
                        }
                    }

                    let mut overflow = false;
                    struct ExpectedGrp {
                        g: Option<i64>,
                        c: i64,
                        ds: i64,
                        sn: Option<i64>,
                        ar: Option<f64>,
                        mins: Option<String>,
                        maxb: Option<bool>,
                    }
                    let mut out_groups = Vec::new();
                    for (g, rows) in grouped {
                        let c = rows.len() as i64;
                        let mut distinct_s = HashSet::new();
                        let mut sum_n: i128 = 0;
                        let mut cnt_n: u64 = 0;
                        let mut sum_r: f64 = 0.0;
                        let mut cnt_r: u64 = 0;
                        let mut mins: Option<String> = None;
                        let mut maxb: Option<bool> = None;
                        for r in rows {
                            if let Some(sv) = &r.s {
                                distinct_s.insert(sv.clone());
                                if mins.as_ref().is_none_or(|m| sv.as_bytes() < m.as_bytes()) {
                                    mins = Some(sv.clone());
                                }
                            }
                            if let Some(nv) = r.n {
                                sum_n += i128::from(nv);
                                cnt_n += 1;
                            }
                            if let Some(rv) = r.r {
                                sum_r += rv;
                                cnt_r += 1;
                            }
                            if let Some(bv) = r.b {
                                maxb = Some(maxb.map_or(bv, |cur| cur || bv));
                            }
                        }
                        let sn = if cnt_n == 0 {
                            None
                        } else if let Ok(v) = i64::try_from(sum_n) {
                            Some(v)
                        } else {
                            overflow = true;
                            break;
                        };
                        let ar = if cnt_r == 0 {
                            None
                        } else {
                            let avg = sum_r / (cnt_r as f64);
                            Some(if avg == 0.0 { 0.0 } else { avg })
                        };
                        if c >= min_cnt {
                            out_groups.push(ExpectedGrp {
                                g,
                                c,
                                ds: distinct_s.len() as i64,
                                sn,
                                ar,
                                mins,
                                maxb,
                            });
                        }
                    }

                    let actual = db.execute(&sql, &params);
                    if overflow {
                        assert!(matches!(actual, Err(SqlError::Bounds(_))));
                    } else {
                        if limit > 0 {
                            out_groups.sort_by(|a, b| {
                                let ord = a.c.cmp(&b.c);
                                let ord = if desc { ord.reverse() } else { ord };
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                                model_cmp_opt(a.g.as_ref(), b.g.as_ref(), true, false, |x, y| {
                                    x.cmp(y)
                                })
                            });
                        }
                        let expected_rows: Vec<Vec<SqlValue>> = if limit == 0 {
                            Vec::new()
                        } else {
                            out_groups
                                .into_iter()
                                .skip(offset)
                                .take(limit)
                                .map(|eg| {
                                    vec![
                                        eg.g.map_or(SqlValue::Null, SqlValue::Integer),
                                        SqlValue::Integer(eg.c),
                                        SqlValue::Integer(eg.ds),
                                        eg.sn.map_or(SqlValue::Null, SqlValue::Integer),
                                        eg.ar.map_or(SqlValue::Null, SqlValue::Real),
                                        eg.mins.map_or(SqlValue::Null, SqlValue::Text),
                                        eg.maxb.map_or(SqlValue::Null, SqlValue::Boolean),
                                    ]
                                })
                                .collect()
                        };
                        assert_eq!(
                            actual.unwrap(),
                            SqlOutcome::Rows {
                                columns: vec![
                                    "g".into(),
                                    "c".into(),
                                    "ds".into(),
                                    "sn".into(),
                                    "ar".into(),
                                    "mins".into(),
                                    "maxb".into(),
                                ],
                                rows: expected_rows,
                            }
                        );
                    }
                }
            }
        }
    });
}
