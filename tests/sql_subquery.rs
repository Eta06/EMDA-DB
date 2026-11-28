//! Comprehensive tests for bounded, uncorrelated subqueries on both writable
//! backends (`Store` and `CheckpointStore`), including SQLite differential fuzzing.
use database_engine::sql::{
    DEFAULT_SQL_SUBQUERY_VALUES, MAX_SQL_SUBQUERY_DEPTH, MAX_SQL_SUBQUERY_VALUES,
};
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlLimits, SqlOutcome, SqlValue,
    Store,
};
use rusqlite::{Connection, types::Value as SqliteValue};

fn for_both(mut f: impl FnMut(&mut SqlDatabase<'_>, &str)) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("ordinary")).unwrap();
    let mut db = SqlDatabase::new(DocumentDatabase::new(&store));
    f(&mut db, "Store");
    drop(db);
    drop(store);

    let log = dir.path().join("checkpoint-log");
    let initial = Store::open(&log).unwrap();
    let cp_path = dir.path().join("checkpoint");
    let digest = initial.checkpoint_to(&cp_path).unwrap();
    drop(initial);
    let cp = CheckpointStore::open(&log, &cp_path, &digest).unwrap();
    let mut cp_db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&cp));
    f(&mut cp_db, "CheckpointStore");
}

fn rows(outcome: SqlOutcome) -> Vec<Vec<SqlValue>> {
    match outcome {
        SqlOutcome::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

fn cols_and_rows(outcome: SqlOutcome) -> (Vec<String>, Vec<Vec<SqlValue>>) {
    match outcome {
        SqlOutcome::Rows { columns, rows } => (columns, rows),
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn range(&mut self, bound: usize) -> usize {
        (self.next_u64() as usize) % bound
    }
}

fn to_sqlite(v: &SqlValue) -> SqliteValue {
    match v {
        SqlValue::Null => SqliteValue::Null,
        SqlValue::Integer(n) => SqliteValue::Integer(*n),
        SqlValue::Real(f) => SqliteValue::Real(*f),
        SqlValue::Text(s) => SqliteValue::Text(s.clone()),
        SqlValue::Boolean(b) => SqliteValue::Integer(i64::from(*b)),
    }
}

fn normalize_sqlite_cell(v: SqliteValue, bool_col: bool) -> SqlValue {
    match v {
        SqliteValue::Null => SqlValue::Null,
        SqliteValue::Integer(n) if bool_col => SqlValue::Boolean(n != 0),
        SqliteValue::Integer(n) => SqlValue::Integer(n),
        SqliteValue::Real(f) => SqlValue::Real(if f == 0.0 { 0.0 } else { f }),
        SqliteValue::Text(s) => SqlValue::Text(s),
        SqliteValue::Blob(_) => panic!("unexpected SQLite blob"),
    }
}

fn query_sqlite(conn: &Connection, sql: &str, bool_cols: &[bool]) -> Vec<Vec<SqlValue>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let ncols = stmt.column_count();
    let mut iter = stmt.query([]).unwrap();
    let mut out = Vec::new();
    while let Some(row) = iter.next().unwrap() {
        let mut cells = Vec::with_capacity(ncols);
        for c in 0..ncols {
            let v: SqliteValue = row.get(c).unwrap();
            let is_bool = bool_cols.get(c).copied().unwrap_or(false);
            cells.push(normalize_sqlite_cell(v, is_bool));
        }
        out.push(cells);
    }
    out
}

#[test]
fn correlated_exists_matches_sqlite_with_nulls_duplicates_and_residual_filters() {
    for_both(|db, backend| {
        let sqlite = Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE a (id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, k INTEGER, active BOOLEAN)",
            "CREATE INDEX b_k ON b(k)",
            "INSERT INTO a VALUES (1, 10), (2, 20), (3, NULL), (4, 40)",
            "INSERT INTO b VALUES (1, 10, TRUE), (2, 10, TRUE), (3, 20, FALSE), (4, NULL, TRUE)",
        ] {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        for (sql, bools) in [
            (
                "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.active = TRUE) ORDER BY id",
                [false, false],
            ),
            (
                "SELECT id FROM a WHERE NOT EXISTS (SELECT * FROM b WHERE a.k = b.k AND b.active = TRUE LIMIT 1) ORDER BY id",
                [false, false],
            ),
            (
                "SELECT id, EXISTS (SELECT 1 FROM b WHERE a.k = b.k) AS hit FROM a ORDER BY id",
                [false, true],
            ),
            (
                "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.active = TRUE AND b.id < 0) ORDER BY id",
                [false, false],
            ),
        ] {
            let actual = rows(db.execute(sql, &[]).unwrap());
            assert_eq!(
                actual,
                query_sqlite(&sqlite, sql, &bools),
                "{backend}: {sql}"
            );
        }
        db.transaction(|tx| {
            tx.execute("INSERT INTO b VALUES (5, 40, TRUE)", &[])?;
            let result = tx.execute("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.active = TRUE) ORDER BY id", &[])?;
            assert_eq!(rows(result), vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(4)]]);
            Ok(())
        }).unwrap();
    });
}

fn setup_basic_tables(db: &SqlDatabase<'_>) {
    db.execute(
        "CREATE TABLE outer_t (id INT PRIMARY KEY, k INT, r REAL, s TEXT, b BOOLEAN)",
        &[],
    )
    .unwrap();
    db.execute(
        "CREATE TABLE inner_t (id INT PRIMARY KEY, k INT, r REAL, s TEXT, b BOOLEAN)",
        &[],
    )
    .unwrap();
    db.execute("CREATE INDEX idx_outer_k ON outer_t (k)", &[])
        .unwrap();
}

#[test]
fn in_and_not_in_three_valued_logic_and_empty_sets() {
    for_both(|db, backend| {
        setup_basic_tables(db);
        let outer_rows = [
            (1, Some(10), Some(1.0), Some("a"), Some(true)),
            (2, Some(20), Some(2.5), Some("b"), Some(false)),
            (3, Some(30), None, Some("c"), None),
            (4, None, Some(2.0), None, Some(true)),
        ];
        for (id, k, r, s, b) in outer_rows {
            db.execute(
                "INSERT INTO outer_t VALUES (?, ?, ?, ?, ?)",
                &[
                    SqlValue::Integer(id),
                    k.map_or(SqlValue::Null, SqlValue::Integer),
                    r.map_or(SqlValue::Null, SqlValue::Real),
                    s.map_or(SqlValue::Null, |v| SqlValue::Text(v.into())),
                    b.map_or(SqlValue::Null, SqlValue::Boolean),
                ],
            )
            .unwrap();
        }

        // Empty set: `x IN (empty)` is always FALSE even for NULL x;
        // `x NOT IN (empty)` is always TRUE even for NULL x.
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k IN (SELECT k FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new()
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k NOT IN (SELECT k FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
                vec![SqlValue::Integer(4)],
            ],
            "{backend}: NOT IN empty set must include NULL lhs"
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id, k IN (SELECT k FROM inner_t) AS in_emp, k NOT IN (SELECT k FROM inner_t) AS nin_emp FROM outer_t ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(1), SqlValue::Boolean(false), SqlValue::Boolean(true)],
                vec![SqlValue::Integer(2), SqlValue::Boolean(false), SqlValue::Boolean(true)],
                vec![SqlValue::Integer(3), SqlValue::Boolean(false), SqlValue::Boolean(true)],
                vec![SqlValue::Integer(4), SqlValue::Boolean(false), SqlValue::Boolean(true)],
            ]
        );

        // Non-empty inner set without NULL: {10, 30}.
        db.execute("INSERT INTO inner_t VALUES (1, 10, 2.0, 'a', TRUE)", &[])
            .unwrap();
        db.execute("INSERT INTO inner_t VALUES (2, 30, 3.0, 'z', FALSE)", &[])
            .unwrap();

        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k IN (SELECT k FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)]]
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k NOT IN (SELECT k FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(2)]],
            "{backend}: NULL k is UNKNOWN under NOT IN and filtered out"
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE (k NOT IN (SELECT k FROM inner_t)) IS NULL ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(4)]]
        );

        // Inner set WITH NULL: {10, 30, NULL}.
        db.execute(
            "INSERT INTO inner_t VALUES (3, NULL, -0.0, NULL, NULL)",
            &[],
        )
        .unwrap();
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k IN (SELECT k FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)]],
            "{backend}: matching elements remain TRUE even when set has NULL"
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k NOT IN (SELECT k FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new(),
            "{backend}: NOT IN over set containing NULL is never TRUE"
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id, k IN (SELECT k FROM inner_t), k NOT IN (SELECT k FROM inner_t) FROM outer_t ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(1), SqlValue::Boolean(true), SqlValue::Boolean(false)],
                vec![SqlValue::Integer(2), SqlValue::Null, SqlValue::Null],
                vec![SqlValue::Integer(3), SqlValue::Boolean(true), SqlValue::Boolean(false)],
                vec![SqlValue::Integer(4), SqlValue::Null, SqlValue::Null],
            ]
        );

        // Numeric cross-type matching (REAL <-> INTEGER) + negative zero + high-precision i64.
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE r IN (SELECT id FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(4)]],
            "{backend}: 1.0 == 1 and 2.0 == 2 under IN"
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT 0 IN (SELECT r FROM inner_t WHERE id = 3), 2 IN (SELECT r FROM inner_t WHERE id = 1) FROM outer_t WHERE id = 1",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Boolean(true), SqlValue::Boolean(true)]]
        );
        // Large i64 beyond 2^53 must stay distinct from f64 2^53.
        db.execute("CREATE TABLE bigints (id INT PRIMARY KEY, v INT)", &[])
            .unwrap();
        db.execute("CREATE TABLE bigreals (id INT PRIMARY KEY, v REAL)", &[])
            .unwrap();
        let p53 = 1_i64 << 53;
        db.execute(
            "INSERT INTO bigints VALUES (1, ?)",
            &[SqlValue::Integer(p53 + 1)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO bigreals VALUES (1, ?)",
            &[SqlValue::Real(p53 as f64)],
        )
        .unwrap();
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM bigints WHERE v IN (SELECT v FROM bigreals)",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new(),
            "{backend}: (1<<53)+1 must not equal (1<<53) as REAL"
        );

        // Parameterized IN LHS & text PK lookup optimization (`pk IN (subquery)`).
        db.execute("CREATE TABLE tp (k TEXT PRIMARY KEY, tag INT)", &[])
            .unwrap();
        db.execute(
            "INSERT INTO tp VALUES ('alpha', 1), ('beta', 2), ('gamma', 3)",
            &[],
        )
        .unwrap();
        assert_eq!(
            rows(
                db.execute(
                    "SELECT k FROM tp WHERE k IN (SELECT s FROM outer_t WHERE k IS NOT NULL) ORDER BY k DESC",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new()
        );
        db.execute("INSERT INTO tp VALUES ('a', 10), ('c', 20)", &[])
            .unwrap();
        assert_eq!(
            rows(
                db.execute(
                    "SELECT k FROM tp WHERE k IN (SELECT s FROM outer_t WHERE k IS NOT NULL) ORDER BY k",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Text("a".into())], vec![SqlValue::Text("c".into())]]
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT ? IN (SELECT k FROM outer_t) FROM outer_t WHERE id = 1",
                    &[SqlValue::Integer(20)]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Boolean(true)]]
        );
    });
}

#[test]
fn exists_and_not_exists_forms() {
    for_both(|db, backend| {
        setup_basic_tables(db);
        db.execute("INSERT INTO outer_t VALUES (1, 10, 1.0, 'x', TRUE)", &[])
            .unwrap();
        db.execute("INSERT INTO outer_t VALUES (2, 20, 2.0, 'y', FALSE)", &[])
            .unwrap();

        // Empty inner -> EXISTS is FALSE, NOT EXISTS is TRUE.
        assert_eq!(
            rows(
                db.execute(
                    "SELECT EXISTS (SELECT * FROM inner_t), NOT EXISTS (SELECT 1 FROM inner_t) FROM outer_t WHERE id = 1",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Boolean(false), SqlValue::Boolean(true)]],
            "{backend}: EXISTS/NOT EXISTS on empty multi-column table"
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE NOT EXISTS (SELECT * FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
        );

        // Single row containing all NULLs still makes EXISTS TRUE.
        db.execute(
            "INSERT INTO inner_t VALUES (1, NULL, NULL, NULL, NULL)",
            &[],
        )
        .unwrap();
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE EXISTS (SELECT k, s FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
            "{backend}: EXISTS treats row of NULLs as existing"
        );
        // LIMIT 0 and exhausted OFFSET make EXISTS FALSE.
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE EXISTS (SELECT 1 FROM inner_t LIMIT 0)",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new()
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE EXISTS (SELECT 1 FROM inner_t LIMIT 1 OFFSET 1)",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new()
        );
        // Inner with aggregate and HAVING.
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE EXISTS (SELECT COUNT(*) FROM inner_t HAVING COUNT(*) > 5)",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new()
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT EXISTS (SELECT COUNT(*) FROM inner_t HAVING COUNT(*) >= 1) FROM outer_t WHERE id = 1",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Boolean(true)]]
        );
    });
}

#[test]
fn scalar_subqueries_all_positions_and_errors() {
    for_both(|db, backend| {
        setup_basic_tables(db);
        db.execute(
            "INSERT INTO outer_t VALUES (1, 10, 1.5, 'alice', TRUE), (2, 20, 2.5, 'bob', FALSE), (3, 30, 3.5, 'carol', NULL)",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO inner_t VALUES (1, 20, 2.5, 'match', TRUE), (2, 10, 1.5, 'other', FALSE)",
            &[],
        )
        .unwrap();

        // Scalar returning 0 rows -> NULL.
        assert_eq!(
            rows(
                db.execute(
                    "SELECT (SELECT k FROM inner_t WHERE id = 999) FROM outer_t WHERE id = 1",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Null]],
            "{backend}: 0-row scalar subquery must yield NULL"
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k = (SELECT k FROM inner_t WHERE id = 999)",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new()
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE (SELECT k FROM inner_t WHERE id = 999) IS NULL ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
            ]
        );

        // Scalar comparisons (`=`, `>`, `<`, `<=`, boolean predicate, ORDER BY alias, JOIN subquery).
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k = (SELECT MAX(k) FROM inner_t) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(2)]]
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE (SELECT b FROM inner_t WHERE id = 1) AND k >= (SELECT k FROM inner_t ORDER BY k ASC LIMIT 1) ORDER BY id",
                    &[]
                )
                .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
            ]
        );
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k = (SELECT inner_t.k FROM inner_t JOIN outer_t ON inner_t.id = outer_t.id WHERE inner_t.id = 2)",
                    &[]
                )
                .unwrap()
            ),
            vec![vec![SqlValue::Integer(1)]],
            "{backend}: JOIN inside scalar subquery"
        );

        // Scalar subquery inside SELECT list and ordered by alias.
        let (cols, res) = cols_and_rows(
            db.execute(
                "SELECT id, (SELECT MAX(k) FROM inner_t) AS mx FROM outer_t ORDER BY mx DESC, id DESC LIMIT 2",
                &[],
            )
            .unwrap(),
        );
        assert_eq!(cols, vec!["id", "mx"]);
        assert_eq!(
            res,
            vec![
                vec![SqlValue::Integer(3), SqlValue::Integer(20)],
                vec![SqlValue::Integer(2), SqlValue::Integer(20)],
            ]
        );

        // HAVING with scalar subquery and aggregate SELECT list with subquery.
        assert_eq!(
            rows(
                db.execute(
                    "SELECT k, COUNT(*) AS c, (SELECT COUNT(*) FROM inner_t) AS tot FROM outer_t GROUP BY k HAVING k >= (SELECT MAX(k) FROM inner_t) ORDER BY c, k",
                    &[]
                )
                .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(20), SqlValue::Integer(1), SqlValue::Integer(2)],
                vec![SqlValue::Integer(30), SqlValue::Integer(1), SqlValue::Integer(2)],
            ]
        );

        // INSERT VALUES with scalar subquery + UPDATE SET with scalar subquery.
        db.execute(
            "INSERT INTO outer_t VALUES (10, (SELECT MIN(k) FROM inner_t), 9.5, 'inserted', (SELECT b FROM inner_t WHERE id = 2))",
            &[],
        )
        .unwrap();
        assert_eq!(
            rows(
                db.execute("SELECT k, b FROM outer_t WHERE id = 10", &[])
                    .unwrap()
            ),
            vec![vec![SqlValue::Integer(10), SqlValue::Boolean(false)]]
        );
        db.execute(
            "UPDATE outer_t SET k = (SELECT MAX(k) FROM inner_t) WHERE id = 10",
            &[],
        )
        .unwrap();
        assert_eq!(
            rows(
                db.execute("SELECT k FROM outer_t WHERE id = 10", &[])
                    .unwrap()
            ),
            vec![vec![SqlValue::Integer(20)]]
        );

        // Multi-row scalar subquery -> SqlError::Constraint everywhere.
        for bad_sql in [
            "SELECT (SELECT k FROM inner_t) FROM outer_t WHERE id = 1",
            "SELECT * FROM outer_t WHERE k = (SELECT k FROM inner_t)",
            "SELECT * FROM outer_t WHERE EXISTS (SELECT k FROM inner_t WHERE k = (SELECT k FROM inner_t))",
            "UPDATE outer_t SET k = (SELECT k FROM inner_t) WHERE id = 10",
            "INSERT INTO outer_t VALUES (11, (SELECT k FROM inner_t), 1.0, 'bad', TRUE)",
        ] {
            assert!(
                matches!(
                    db.execute(bad_sql, &[]).unwrap_err(),
                    SqlError::Constraint(_)
                ),
                "{backend}: expected Constraint for multi-row scalar {bad_sql:?}"
            );
        }

        // Multi-column IN / scalar subquery -> SqlError::Syntax.
        for multi_col_sql in [
            "SELECT * FROM outer_t WHERE k IN (SELECT * FROM inner_t)",
            "SELECT * FROM outer_t WHERE k = (SELECT id, k FROM inner_t WHERE id = 1)",
            "SELECT (SELECT id, k FROM inner_t) FROM outer_t",
        ] {
            assert!(
                matches!(
                    db.execute(multi_col_sql, &[]).unwrap_err(),
                    SqlError::Syntax { .. }
                ),
                "{backend}: expected Syntax for multi-column subquery {multi_col_sql:?}"
            );
        }

        // Type mismatch detected even when inner table is empty.
        db.execute("CREATE TABLE empty_t (id INT PRIMARY KEY, s TEXT)", &[])
            .unwrap();
        for bad_type_sql in [
            "SELECT * FROM outer_t WHERE k = (SELECT s FROM empty_t)",
            "SELECT * FROM outer_t WHERE k IN (SELECT s FROM empty_t)",
            "SELECT * FROM outer_t WHERE (SELECT id FROM empty_t)",
            "SELECT * FROM outer_t WHERE NOT (SELECT id FROM empty_t)",
            "UPDATE outer_t SET k = (SELECT s FROM empty_t)",
        ] {
            assert!(
                matches!(
                    db.execute(bad_type_sql, &[]).unwrap_err(),
                    SqlError::Type(_)
                ),
                "{backend}: expected Type error for {bad_type_sql:?}"
            );
        }
    });
}

#[test]
fn bounds_nesting_and_correlated_errors() {
    // Validate SqlLimits::max_subquery_values.
    let def = SqlLimits::default();
    assert_eq!(def.max_subquery_values, DEFAULT_SQL_SUBQUERY_VALUES);
    assert!(def.validate().is_ok());

    let zero = SqlLimits {
        max_subquery_values: 0,
        ..Default::default()
    };
    assert!(matches!(zero.validate(), Err(SqlError::Bounds(_))));

    let over_max = SqlLimits {
        max_subquery_values: MAX_SQL_SUBQUERY_VALUES + 1,
        ..Default::default()
    };
    assert!(matches!(over_max.validate(), Err(SqlError::Bounds(_))));

    let at_max = SqlLimits {
        max_subquery_values: MAX_SQL_SUBQUERY_VALUES,
        ..Default::default()
    };
    assert!(at_max.validate().is_ok());

    for_both(|db, backend| {
        let limits = SqlLimits {
            max_subquery_values: 3,
            max_visited_rows: 10,
            ..Default::default()
        };
        db.set_limits(limits).unwrap();
        setup_basic_tables(db);
        db.execute("INSERT INTO outer_t VALUES (1, 1, 1.0, 'a', TRUE)", &[])
            .unwrap();
        db.execute("INSERT INTO outer_t VALUES (2, 2, 2.0, 'b', FALSE)", &[])
            .unwrap();
        // Insert 4 distinct values including NULL (plus duplicates).
        db.execute(
            "INSERT INTO inner_t VALUES (1, 10, 1.0, 'a', TRUE), (2, 20, 2.0, 'b', FALSE), (3, NULL, 3.0, 'c', TRUE), (4, 10, 1.0, 'a', TRUE)",
            &[],
        )
        .unwrap();

        // 3 distinct values (10, 20, NULL) across 4 scanned rows fits max_subquery_values = 3.
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE k IN (SELECT k FROM inner_t)",
                    &[]
                )
                .unwrap()
            ),
            Vec::<Vec<SqlValue>>::new()
        );
        // Add 4th distinct value -> exceeds max_subquery_values (3).
        db.execute("INSERT INTO inner_t VALUES (5, 30, 4.0, 'd', TRUE)", &[])
            .unwrap();
        assert!(
            matches!(
                db.execute(
                    "SELECT id FROM outer_t WHERE k IN (SELECT k FROM inner_t)",
                    &[]
                )
                .unwrap_err(),
                SqlError::Bounds(_)
            ),
            "{backend}: 4th distinct value must trigger Bounds rather than truncating"
        );

        // Shared max_visited_rows: max_visited_rows = 10.
        // Insert 3 more rows in outer_t (total 5 outer) + 5 inner = 10 -> OK; add 1 more inner -> 11 -> Bounds.
        db.execute(
            "INSERT INTO outer_t VALUES (3, 3, 1.0, 'c', TRUE), (4, 4, 1.0, 'd', TRUE), (5, 5, 1.0, 'e', TRUE)",
            &[],
        )
        .unwrap();
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE EXISTS (SELECT 1 FROM inner_t WHERE id = 5)",
                    &[]
                )
                .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
                vec![SqlValue::Integer(4)],
                vec![SqlValue::Integer(5)],
            ]
        );
        // Force inner full scan (5 inner) + outer full scan (5 outer) = 10 -> passes!
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE (SELECT COUNT(*) FROM inner_t WHERE k >= 0) >= 0",
                    &[]
                )
                .unwrap()
            )
            .len(),
            5
        );
        // Add 6th row to inner -> 6 inner + 5 outer = 11 > 10 -> Bounds!
        db.execute("INSERT INTO inner_t VALUES (6, 40, 5.0, 'e', TRUE)", &[])
            .unwrap();
        assert!(matches!(
            db.execute(
                "SELECT id FROM outer_t WHERE (SELECT COUNT(*) FROM inner_t WHERE k >= 0) >= 0",
                &[]
            )
            .unwrap_err(),
            SqlError::Bounds(_)
        ));
        // Meanwhile EXISTS early-stop visits only 1 inner row + 5 outer = 6 <= 10!
        assert_eq!(
            rows(
                db.execute(
                    "SELECT id FROM outer_t WHERE EXISTS (SELECT 1 FROM inner_t)",
                    &[]
                )
                .unwrap()
            )
            .len(),
            5
        );

        // Nesting limit MAX_SQL_SUBQUERY_DEPTH (= 4) succeeds; depth 5 fails with Bounds.
        assert_eq!(MAX_SQL_SUBQUERY_DEPTH, 4);
        let d4 = "SELECT id FROM outer_t WHERE k IN (SELECT k FROM inner_t WHERE k IN (SELECT k FROM inner_t WHERE k IN (SELECT k FROM inner_t WHERE k IN (SELECT k FROM inner_t))))";
        assert!(db.prepare(d4).is_ok());
        let d5 = "SELECT id FROM outer_t WHERE k IN (SELECT k FROM inner_t WHERE k IN (SELECT k FROM inner_t WHERE k IN (SELECT k FROM inner_t WHERE k IN (SELECT k FROM inner_t WHERE k IN (SELECT k FROM inner_t)))))";
        assert!(matches!(db.prepare(d5).unwrap_err(), SqlError::Bounds(_)));

        // Correlated rejection: unqualified, qualified, depth-2, GROUP BY, SELECT list, JOIN subquery.
        db.execute("CREATE TABLE t_out (id INT PRIMARY KEY, only_out INT)", &[])
            .unwrap();
        db.execute("CREATE TABLE t_mid (id INT PRIMARY KEY, only_mid INT)", &[])
            .unwrap();
        db.execute("CREATE TABLE t_in (id INT PRIMARY KEY, only_in INT)", &[])
            .unwrap();

        for corr_sql in [
            "SELECT id FROM t_out WHERE EXISTS (SELECT 1 FROM t_in WHERE only_in = only_out)",
            "SELECT id FROM t_out WHERE EXISTS (SELECT 1 FROM t_in WHERE t_in.only_in > t_out.only_out)",
            "SELECT id FROM t_out WHERE EXISTS (SELECT 1 FROM t_mid WHERE EXISTS (SELECT 1 FROM t_in WHERE only_in = only_out))",
            "SELECT id FROM t_out WHERE k IN (SELECT only_out FROM t_in)",
            "SELECT id FROM t_out WHERE id IN (SELECT id FROM t_in GROUP BY only_out)",
            "SELECT id FROM t_out WHERE id IN (SELECT t_in.id FROM t_in JOIN t_mid ON t_in.id = t_mid.id WHERE t_out.only_out = 1)",
            "SELECT id FROM t_out WHERE id IN (SELECT t_in.id FROM t_in JOIN t_mid ON t_in.id = t_mid.id WHERE only_out = 1)",
        ] {
            let sql_to_run =
                corr_sql.replace("k IN (SELECT only_out", "only_out IN (SELECT only_out");
            let err = db.execute(&sql_to_run, &[]).unwrap_err();
            assert!(
                matches!(err, SqlError::Unsupported { .. }),
                "{backend}: expected Unsupported for correlated {sql_to_run:?}, got {err:?}"
            );
        }

        // Completely nonexistent column still yields NoSuchColumn, not Unsupported.
        let err = db
            .execute(
                "SELECT id FROM t_out WHERE EXISTS (SELECT 1 FROM t_in WHERE no_such_anywhere = 1)",
                &[],
            )
            .unwrap_err();
        assert!(matches!(err, SqlError::NoSuchColumn { .. }));
    });
}

#[test]
fn planner_preservation_and_explain() {
    for_both(|db, backend| {
        setup_basic_tables(db);
        db.execute(
            "INSERT INTO outer_t VALUES (1, 10, 1.0, 'a', TRUE), (2, 10, 2.0, 'b', FALSE), (3, 20, 3.0, 'c', TRUE)",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO inner_t VALUES (1, 10, 1.0, 'a', TRUE), (2, 99, 9.0, 'z', FALSE)",
            &[],
        )
        .unwrap();

        // Outer secondary index preserved alongside subquery conjunct.
        let plan_rows = rows(
            db.execute(
                "EXPLAIN SELECT id FROM outer_t WHERE k = ? AND EXISTS (SELECT 1 FROM inner_t WHERE id = 1) ORDER BY id",
                &[SqlValue::Integer(10)],
            )
            .unwrap(),
        );
        assert_eq!(plan_rows[0][0], SqlValue::Text("subquery".into()));
        assert_eq!(plan_rows[0][1], SqlValue::Text("inner_t".into()));
        assert_eq!(plan_rows[1][0], SqlValue::Text("scan".into()));
        assert!(
            matches!(&plan_rows[1][2], SqlValue::Text(s) if s.contains("idx_outer_k")),
            "{backend}: expected outer secondary index scan alongside subquery, got {:?}",
            plan_rows
        );

        // Outer PK lookup preserved alongside IN subquery.
        let pk_plan = rows(
            db.execute(
                "EXPLAIN SELECT id FROM outer_t WHERE id = ? AND k NOT IN (SELECT k FROM inner_t)",
                &[SqlValue::Integer(2)],
            )
            .unwrap(),
        );
        assert_eq!(pk_plan[0][0], SqlValue::Text("subquery".into()));
        assert_eq!(pk_plan[1][2], SqlValue::Text("primary-key lookup".into()));

        // Nested subqueries emit nested subquery EXPLAIN lines without reading rows.
        let nested_plan = rows(
            db.execute(
                "EXPLAIN SELECT (SELECT k FROM inner_t WHERE EXISTS (SELECT 1 FROM outer_t)) FROM outer_t",
                &[],
            )
            .unwrap(),
        );
        let sub_steps = nested_plan
            .iter()
            .filter(|r| r[0] == SqlValue::Text("subquery".into()))
            .count();
        assert_eq!(sub_steps, 2, "{backend}: expected 2 subquery EXPLAIN rows");

        // Compare pk IN (subquery) point-lookup path against forced full scan (> 0 keys and > 64 keys).
        db.execute("CREATE TABLE big_pk (id INT PRIMARY KEY, v INT)", &[])
            .unwrap();
        db.execute("CREATE TABLE big_keys (id INT PRIMARY KEY, k INT)", &[])
            .unwrap();
        for i in 1..=100_i64 {
            db.execute(
                "INSERT INTO big_pk VALUES (?, ?)",
                &[SqlValue::Integer(i), SqlValue::Integer(i * 10)],
            )
            .unwrap();
        }
        for i in 1..=70_i64 {
            db.execute(
                "INSERT INTO big_keys VALUES (?, ?)",
                &[SqlValue::Integer(i), SqlValue::Integer(i)],
            )
            .unwrap();
        }
        for q in [
            "SELECT id, v FROM big_pk WHERE id IN (SELECT k FROM big_keys WHERE id <= 10) ORDER BY id DESC LIMIT 4 OFFSET 2",
            "SELECT id, v FROM big_pk WHERE id IN (SELECT k FROM big_keys) AND v >= 650 ORDER BY v DESC LIMIT 5",
            "SELECT COUNT(*) FROM big_pk WHERE id IN (SELECT k FROM big_keys WHERE id <= 5)",
        ] {
            let fast = rows(db.execute(q, &[]).unwrap());
            let slow = rows(db.execute_force_full_scan_for_test(q, &[]).unwrap());
            assert_eq!(fast, slow, "{backend}: query {q:?} diverged from full scan");
        }
    });
}

#[test]
fn transactions_update_delete_rollback_and_conflict() {
    for_both(|db, backend| {
        setup_basic_tables(db);
        db.execute(
            "INSERT INTO outer_t VALUES (1, 10, 1.0, 'a', TRUE), (2, 20, 2.0, 'b', FALSE), (3, 30, 3.0, 'c', TRUE)",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO inner_t VALUES (1, 10, 1.0, 'x', TRUE)", &[])
            .unwrap();

        // Transaction read-your-own-writes + rollback.
        let rolled_back: Result<(), SqlError> = db.transaction(|tx| {
            tx.execute("INSERT INTO inner_t VALUES (2, 30, 3.0, 'y', TRUE)", &[])?;
            let out = tx.execute(
                "UPDATE outer_t SET k = 99 WHERE k IN (SELECT k FROM inner_t)",
                &[],
            )?;
            assert!(matches!(out, SqlOutcome::Updated { rows: 2 }));
            let del = tx.execute(
                "DELETE FROM outer_t WHERE NOT EXISTS (SELECT 1 FROM inner_t WHERE k = 10)",
                &[],
            )?;
            assert!(matches!(del, SqlOutcome::Deleted { rows: 0 }));
            assert_eq!(
                rows(tx.execute("SELECT k FROM outer_t ORDER BY id", &[])?),
                vec![
                    vec![SqlValue::Integer(99)],
                    vec![SqlValue::Integer(20)],
                    vec![SqlValue::Integer(99)],
                ]
            );
            Err(SqlError::Syntax {
                offset: 0,
                message: "force rollback".into(),
            })
        });
        assert!(
            rolled_back.is_err(),
            "{backend}: rollback should return Err"
        );
        assert_eq!(
            rows(
                db.execute("SELECT k FROM outer_t ORDER BY id", &[])
                    .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(10)],
                vec![SqlValue::Integer(20)],
                vec![SqlValue::Integer(30)],
            ],
            "{backend}: rolled back subquery UPDATE must leave outer_t untouched"
        );

        // Concurrent insert into inner_t causes SqlError::Conflict on commit.
        let conflict_res = db.transaction(|tx| {
            tx.execute(
                "DELETE FROM outer_t WHERE id IN (SELECT k FROM inner_t WHERE k > 100)",
                &[],
            )?;
            db.execute("INSERT INTO inner_t VALUES (3, 200, 1.0, 'c', FALSE)", &[])
                .unwrap();
            tx.execute("UPDATE outer_t SET k = 11 WHERE id = 1", &[])?;
            Ok(())
        });
        assert!(
            matches!(conflict_res, Err(SqlError::Conflict(_))),
            "{backend}: concurrent insert into inner table must cause Conflict, got {conflict_res:?}"
        );
        assert_eq!(
            rows(
                db.execute("SELECT k FROM outer_t ORDER BY id", &[])
                    .unwrap()
            ),
            vec![
                vec![SqlValue::Integer(10)],
                vec![SqlValue::Integer(20)],
                vec![SqlValue::Integer(30)],
            ],
            "{backend}: conflicted transaction must be rolled back"
        );
    });
}

#[test]
fn randomized_differential_against_sqlite() {
    for_both(|db, backend| {
        let conn = Connection::open_in_memory().unwrap();
        let ddl = [
            "CREATE TABLE t_out (id INT PRIMARY KEY, tag INT, grp INT, val REAL, label TEXT, flag BOOLEAN)",
            "CREATE TABLE t_in (id INT PRIMARY KEY, ref_id INT, tag INT, label TEXT, flag BOOLEAN)",
            "CREATE TABLE t_aux (id INT PRIMARY KEY, ref_id INT, w INT)",
        ];
        for sql in ddl {
            db.execute(sql, &[]).unwrap();
            conn.execute(sql, []).unwrap();
        }
        db.execute("CREATE INDEX idx_out_tag ON t_out (tag)", &[])
            .unwrap();
        db.execute("CREATE INDEX idx_in_tag ON t_in (tag)", &[])
            .unwrap();

        let mut rng = Rng::new(0x517C_C1B7_2722_0A95);
        let opt_int = |rng: &mut Rng, hi: i64| -> SqlValue {
            if rng.range(4) == 0 {
                SqlValue::Null
            } else {
                SqlValue::Integer((rng.range(hi as usize) as i64) - 2)
            }
        };
        let opt_real = |rng: &mut Rng| -> SqlValue {
            if rng.range(4) == 0 {
                SqlValue::Null
            } else {
                SqlValue::Real(((rng.range(12) as f64) - 3.0) / 2.0)
            }
        };
        let opt_text = |rng: &mut Rng| -> SqlValue {
            match rng.range(5) {
                0 => SqlValue::Null,
                1 => SqlValue::Text("a".into()),
                2 => SqlValue::Text("b".into()),
                3 => SqlValue::Text("c".into()),
                _ => SqlValue::Text("d".into()),
            }
        };
        let opt_bool = |rng: &mut Rng| -> SqlValue {
            match rng.range(4) {
                0 => SqlValue::Null,
                1 => SqlValue::Boolean(false),
                _ => SqlValue::Boolean(true),
            }
        };

        for id in 1..=25_i64 {
            let p = [
                SqlValue::Integer(id),
                opt_int(&mut rng, 8),
                opt_int(&mut rng, 5),
                opt_real(&mut rng),
                opt_text(&mut rng),
                opt_bool(&mut rng),
            ];
            db.execute("INSERT INTO t_out VALUES (?, ?, ?, ?, ?, ?)", &p)
                .unwrap();
            conn.execute(
                "INSERT INTO t_out VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params_from_iter(p.iter().map(to_sqlite)),
            )
            .unwrap();
        }
        for id in 1..=16_i64 {
            let p = [
                SqlValue::Integer(id),
                opt_int(&mut rng, 28),
                opt_int(&mut rng, 8),
                opt_text(&mut rng),
                opt_bool(&mut rng),
            ];
            db.execute("INSERT INTO t_in VALUES (?, ?, ?, ?, ?)", &p)
                .unwrap();
            conn.execute(
                "INSERT INTO t_in VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params_from_iter(p.iter().map(to_sqlite)),
            )
            .unwrap();
        }
        for id in 1..=10_i64 {
            let p = [
                SqlValue::Integer(id),
                opt_int(&mut rng, 18),
                opt_int(&mut rng, 8),
            ];
            db.execute("INSERT INTO t_aux VALUES (?, ?, ?)", &p)
                .unwrap();
            conn.execute(
                "INSERT INTO t_aux VALUES (?1, ?2, ?3)",
                rusqlite::params_from_iter(p.iter().map(to_sqlite)),
            )
            .unwrap();
        }

        let inner_filters = [
            "id < 0",
            "flag = TRUE",
            "tag IS NOT NULL",
            "tag IS NULL OR label = 'a'",
            "ref_id >= 2 AND tag <= 3",
            "id > 0",
        ];

        for step in 0..80 {
            let f1 = inner_filters[rng.range(inner_filters.len())];
            let f2 = inner_filters[rng.range(inner_filters.len())];
            let pred_kind = rng.range(8);
            let pred = match pred_kind {
                0 => format!("tag IN (SELECT tag FROM t_in WHERE {f1})"),
                1 => format!("tag NOT IN (SELECT tag FROM t_in WHERE {f1})"),
                2 => format!("(tag NOT IN (SELECT tag FROM t_in WHERE {f1})) IS NULL"),
                3 => format!("id IN (SELECT ref_id FROM t_in WHERE {f1} ORDER BY id DESC LIMIT 8)"),
                4 => format!(
                    "EXISTS (SELECT 1 FROM t_in WHERE {f1}) AND NOT EXISTS (SELECT * FROM t_in WHERE {f2})"
                ),
                5 => format!("tag = (SELECT tag FROM t_in WHERE {f1} ORDER BY id LIMIT 1)"),
                6 => format!("val >= (SELECT MIN(tag) FROM t_in WHERE {f1})"),
                _ => "tag IN (SELECT t_in.tag FROM t_in JOIN t_aux ON t_in.id = t_aux.ref_id WHERE t_aux.w >= 0)".to_owned(),
            };
            let outer_extra = match rng.range(4) {
                0 => String::new(),
                1 => " AND grp = 1".to_owned(),
                2 => " OR label = 'a'".to_owned(),
                _ => " AND (flag IS NULL OR flag = TRUE)".to_owned(),
            };
            let order_limit = match rng.range(3) {
                0 => "ORDER BY id",
                1 => "ORDER BY tag DESC, id ASC LIMIT 7 OFFSET 1",
                _ => "ORDER BY id DESC LIMIT 10",
            };
            let sql = format!(
                "SELECT id, tag, flag FROM t_out WHERE ({pred}){outer_extra} {order_limit}"
            );

            let emda_fast = rows(db.execute(&sql, &[]).unwrap());
            let emda_scan = rows(db.execute_force_full_scan_for_test(&sql, &[]).unwrap());
            let sqlite_res = query_sqlite(&conn, &sql, &[false, false, true]);
            assert_eq!(
                emda_fast, sqlite_res,
                "{backend} step {step}: mismatch on {sql:?}"
            );
            assert_eq!(
                emda_scan, sqlite_res,
                "{backend} step {step} full-scan: mismatch on {sql:?}"
            );
        }

        // Differential UPDATE and DELETE with subqueries.
        let mut_stmts = [
            "UPDATE t_out SET grp = (SELECT MAX(w) FROM t_aux) WHERE tag IN (SELECT tag FROM t_in WHERE flag = TRUE)",
            "UPDATE t_out SET label = 'updated' WHERE tag NOT IN (SELECT tag FROM t_in WHERE id <= 3)",
            "DELETE FROM t_out WHERE id IN (SELECT ref_id FROM t_in WHERE tag IS NOT NULL) AND EXISTS (SELECT 1 FROM t_aux)",
        ];
        for sql in mut_stmts {
            db.execute(sql, &[]).unwrap();
            conn.execute(sql, []).unwrap();
            let check = "SELECT id, tag, grp, val, label, flag FROM t_out ORDER BY id";
            assert_eq!(
                rows(db.execute(check, &[]).unwrap()),
                query_sqlite(&conn, check, &[false, false, false, false, false, true]),
                "{backend}: post-mutation mismatch after {sql:?}"
            );
        }
    });
}

/// Subqueries whose inner or outer query reads through a secondary index
/// (equality, range, ORDER BY satisfied by the index, LIMIT) match SQLite.
#[test]
fn subqueries_over_secondary_index_access_paths_match_sqlite() {
    for_both(|db, backend| {
        let sqlite = Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE p (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, tag TEXT)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, pcode INTEGER, note TEXT)",
        ] {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        for sql in [
            "CREATE INDEX p_code ON p (code)",
            "CREATE INDEX c_pcode ON c (pcode)",
        ] {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        for id in 0..300_i64 {
            let tag = if id % 7 == 0 {
                None
            } else {
                Some(format!("t{}", id % 5))
            };
            let params = [
                SqlValue::Integer(id),
                SqlValue::Integer((id * 37) % 101),
                tag.clone().map_or(SqlValue::Null, SqlValue::Text),
            ];
            db.execute("INSERT INTO p VALUES (?, ?, ?)", &params)
                .unwrap();
            sqlite
                .execute(
                    "INSERT INTO p VALUES (?, ?, ?)",
                    rusqlite::params_from_iter(params.iter().map(to_sqlite)),
                )
                .unwrap();
            let pcode = if id % 11 == 0 {
                SqlValue::Null
            } else {
                SqlValue::Integer(id % 60)
            };
            let params = [
                SqlValue::Integer(id),
                pcode,
                SqlValue::Text(format!("n{id}")),
            ];
            db.execute("INSERT INTO c VALUES (?, ?, ?)", &params)
                .unwrap();
            sqlite
                .execute(
                    "INSERT INTO c VALUES (?, ?, ?)",
                    rusqlite::params_from_iter(params.iter().map(to_sqlite)),
                )
                .unwrap();
        }
        for sql in [
            "SELECT id, code FROM p WHERE code = 17 AND id IN (SELECT pcode FROM c WHERE pcode >= 10 AND pcode < 40) ORDER BY id",
            "SELECT id FROM p WHERE code >= 20 AND code < 30 AND EXISTS (SELECT id FROM c WHERE pcode = 5) ORDER BY id",
            "SELECT id, code FROM p WHERE code >= 5 AND code < 50 AND id NOT IN (SELECT pcode FROM c WHERE pcode >= 0 AND pcode < 30) ORDER BY code, id LIMIT 7",
            "SELECT id, code FROM p WHERE code > 90 ORDER BY code LIMIT 5",
            "SELECT id, (SELECT COUNT(*) FROM c WHERE pcode = 3) FROM p WHERE code = 3 ORDER BY id",
            "SELECT id FROM c WHERE pcode IN (SELECT code FROM p WHERE code >= 95) ORDER BY id LIMIT 4",
            "SELECT id FROM c WHERE pcode = (SELECT MIN(code) FROM p WHERE code > 40) ORDER BY id DESC",
            "SELECT note FROM c WHERE pcode >= 50 AND pcode < 55 AND pcode NOT IN (SELECT code FROM p WHERE tag IS NULL) ORDER BY pcode, id",
        ] {
            let ours = rows(db.execute(sql, &[]).unwrap());
            let expected = query_sqlite(&sqlite, sql, &[false; 3]);
            assert_eq!(ours, expected, "{backend}: {sql}");
        }
    });
}
