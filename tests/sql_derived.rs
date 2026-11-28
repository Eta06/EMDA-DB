//! Derived tables and common table expressions, compared with SQLite.
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlOutcome, SqlValue, Store,
};

fn for_both(mut f: impl FnMut(&mut SqlDatabase<'_>, &str)) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("ordinary")).unwrap();
    f(
        &mut SqlDatabase::new(DocumentDatabase::new(&store)),
        "Store",
    );
    drop(store);
    let log = dir.path().join("checkpoint-log");
    let initial = Store::open(&log).unwrap();
    let digest = initial
        .checkpoint_to(dir.path().join("checkpoint"))
        .unwrap();
    drop(initial);
    let checkpoint = CheckpointStore::open(&log, dir.path().join("checkpoint"), &digest).unwrap();
    f(
        &mut SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&checkpoint)),
        "CheckpointStore",
    );
}

fn rows(db: &SqlDatabase<'_>, sql: &str, params: &[SqlValue]) -> Vec<Vec<SqlValue>> {
    match db.execute(sql, params) {
        Ok(SqlOutcome::Rows { rows, .. }) => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

fn from_sqlite(value: rusqlite::types::Value) -> SqlValue {
    use rusqlite::types::Value;
    match value {
        Value::Null => SqlValue::Null,
        Value::Integer(n) => SqlValue::Integer(n),
        Value::Real(f) => SqlValue::Real(f),
        Value::Text(s) => SqlValue::Text(s),
        Value::Blob(_) => panic!("blob"),
    }
}

#[test]
fn derived_tables_and_ctes_match_sqlite() {
    for_both(|db, backend| {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        let mut setup = vec![
            "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept INTEGER, salary INTEGER, rate REAL, name TEXT)".to_owned(),
            "CREATE TABLE dept (id INTEGER PRIMARY KEY, title TEXT)".to_owned(),
        ];
        for id in 0..5 {
            setup.push(format!("INSERT INTO dept VALUES ({id}, 'd{id}')"));
        }
        for id in 0..60 {
            let dept = if id % 11 == 0 {
                "NULL".to_owned()
            } else {
                (id % 5).to_string()
            };
            let salary = if id % 7 == 0 {
                "NULL".to_owned()
            } else {
                (1000 + (id * 37) % 500).to_string()
            };
            setup.push(format!(
                "INSERT INTO emp VALUES ({id}, {dept}, {salary}, {}.25, 'n{:02}')",
                id % 9,
                (id * 13) % 60
            ));
        }
        for sql in &setup {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        for sql in [
            "SELECT * FROM (SELECT id, salary FROM emp WHERE dept = 2) AS d ORDER BY id",
            "SELECT d.id FROM (SELECT id, salary FROM emp WHERE salary > 1200) d WHERE d.salary < 1400 ORDER BY d.salary DESC, d.id LIMIT 5",
            "SELECT dept, COUNT(*) AS n, SUM(salary) AS total FROM (SELECT dept, salary FROM emp WHERE salary IS NOT NULL) AS s GROUP BY dept ORDER BY dept",
            "SELECT COUNT(*), MAX(total) FROM (SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept) AS t",
            "SELECT title, n FROM (SELECT d.title, COUNT(e.id) AS n FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY d.title) AS x WHERE n > 10 ORDER BY title",
            "WITH rich AS (SELECT id, name, salary FROM emp WHERE salary >= 1300) SELECT name FROM rich ORDER BY name",
            "WITH per AS (SELECT dept, AVG(rate) AS avg_rate FROM emp GROUP BY dept) SELECT dept, avg_rate FROM per WHERE avg_rate > 3.5 ORDER BY dept NULLS FIRST",
            "WITH a AS (SELECT id FROM emp WHERE id < 10), b AS (SELECT id FROM emp WHERE id > 50) SELECT COUNT(*) FROM b",
            "WITH t AS (SELECT dept, salary FROM emp) SELECT dept, MIN(salary) AS lo FROM t AS q GROUP BY dept HAVING MIN(salary) < 1100 ORDER BY lo, dept",
            "SELECT * FROM (SELECT name FROM emp ORDER BY name LIMIT 3) AS z",
            "SELECT e.id, d.title FROM (SELECT id, dept FROM emp WHERE id < 10) e JOIN dept d ON d.id = e.dept ORDER BY e.id",
            "SELECT d.id, e.id FROM dept d LEFT JOIN (SELECT id, dept FROM emp WHERE id < 2) e ON d.id = e.dept ORDER BY d.id, e.id",
            "SELECT e.id, d.title FROM emp e LEFT JOIN (SELECT title FROM dept WHERE id < 0) d ON d.title = e.name WHERE e.id < 2 ORDER BY e.id",
            "WITH a AS (SELECT id, dept FROM emp WHERE id < 5) SELECT x.id, y.id FROM a x FULL JOIN a y ON x.dept = y.dept ORDER BY x.id, y.id",
            "WITH stat AS (SELECT dept, COUNT(*) AS n FROM emp GROUP BY dept) SELECT d.title, s.n FROM dept d JOIN stat s ON s.dept = d.id ORDER BY d.id",
            "WITH a AS (SELECT id FROM emp) SELECT id FROM emp ORDER BY id LIMIT 1",
            "WITH RECURSIVE r AS (SELECT id FROM emp WHERE id < 1) SELECT id FROM r",
            "SELECT d.id, COUNT(e.id) AS n FROM (SELECT id, dept FROM emp WHERE id < 2) e RIGHT JOIN dept d ON d.id = e.dept GROUP BY d.id ORDER BY d.id",
            "SELECT d.id, e.n FROM dept d JOIN (SELECT x.id, COUNT(y.id) AS n FROM dept x LEFT JOIN emp y ON x.id = y.dept GROUP BY x.id) e ON d.id = e.id ORDER BY d.id",
        ] {
            let ours = rows(db, sql, &[]);
            let mut stmt = sqlite.prepare(sql).unwrap();
            let n = stmt.column_count();
            let theirs: Vec<Vec<SqlValue>> = stmt
                .query_map([], |row| {
                    (0..n).map(|i| row.get(i).map(from_sqlite)).collect()
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(ours, theirs, "{backend}: {sql}");
        }
        // Parameters in the outer query.
        assert_eq!(
            rows(
                db,
                "SELECT COUNT(*) FROM (SELECT id FROM emp WHERE dept = 1) AS d WHERE d.id > ?",
                &[SqlValue::Integer(30)]
            ),
            vec![vec![SqlValue::Integer(6)]],
            "{backend}"
        );
        for sql in [
            "SELECT * FROM (SELECT id FROM emp WHERE id > ?) AS d",
            "WITH dept AS (SELECT id FROM emp WHERE id < 2), nested AS (SELECT id FROM dept) SELECT id FROM nested",
            "WITH a AS (SELECT id FROM emp) SELECT id FROM a JOIN dept ON dept.id = a.id",
            "SELECT * FROM (DELETE FROM emp) AS d",
        ] {
            assert!(db.execute(sql, &[]).is_err(), "{backend}: {sql}");
        }
        assert!(matches!(
            db.execute("SELECT * FROM (SELECT nope FROM emp) AS d", &[]),
            Err(SqlError::NoSuchColumn { .. })
        ));
        let plan = rows(db, "EXPLAIN SELECT id FROM (SELECT id FROM emp) AS d", &[]);
        assert_eq!(plan[0][0], SqlValue::Text("derived".into()), "{backend}");
        let plan = rows(
            db,
            "EXPLAIN SELECT d.id FROM dept d JOIN (SELECT dept FROM emp) e ON e.dept = d.id",
            &[],
        );
        assert!(
            matches!(&plan[1][3], SqlValue::Text(detail) if detail.contains("materialized source"))
        );
    });
}

#[test]
fn recursive_ctes_match_sqlite_and_enforce_cumulative_bounds() {
    for_both(|db, backend| {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE roots (id INTEGER PRIMARY KEY)",
            "CREATE TABLE edges (id INTEGER PRIMARY KEY, src INTEGER, dst INTEGER, label TEXT)",
            "CREATE INDEX edges_src ON edges(src)",
            "INSERT INTO roots VALUES (1)",
            "INSERT INTO edges VALUES (1, 1, 2, 'a'), (2, 1, 3, 'b'), (3, 2, 4, 'c'), (4, 3, 5, 'd'), (5, 4, 6, 'e')",
        ] {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        let all = "WITH RECURSIVE walk(node) AS (
            SELECT id FROM roots UNION ALL
            SELECT e.dst FROM walk w JOIN edges e ON e.src = w.node
        ) SELECT node FROM walk ORDER BY node";
        let check = |db: &SqlDatabase<'_>, sql: &str| {
            let mut stmt = sqlite.prepare(sql).unwrap();
            let expected: Vec<_> = stmt
                .query_map([], |row| Ok(vec![from_sqlite(row.get(0)?)]))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(rows(db, sql, &[]), expected, "{backend}: {sql}");
        };
        check(db, all);
        let plan = rows(db, &format!("EXPLAIN {all}"), &[]);
        assert!(
            matches!(&plan[0][3], SqlValue::Text(detail) if detail.contains("recursive materialized source"))
        );
        let cycle = "INSERT INTO edges VALUES (6, 6, 1, 'cycle')";
        db.execute(cycle, &[]).unwrap();
        sqlite.execute(cycle, []).unwrap();
        let distinct = all.replace("UNION ALL", "UNION");
        check(db, &distinct);

        let original = *db.limits();
        db.set_limits(database_engine::SqlLimits {
            max_select_rows: 6,
            ..original
        })
        .unwrap();
        check(db, &distinct);
        assert!(matches!(db.execute(all, &[]), Err(SqlError::Bounds(_))));
        db.set_limits(database_engine::SqlLimits {
            max_select_rows: 5,
            ..original
        })
        .unwrap();
        assert!(matches!(
            db.execute(&distinct, &[]),
            Err(SqlError::Bounds(_))
        ));
        db.set_limits(database_engine::SqlLimits {
            max_visited_rows: 4,
            ..original
        })
        .unwrap();
        assert!(matches!(
            db.execute(&distinct, &[]),
            Err(SqlError::Bounds(_))
        ));
        db.set_limits(original).unwrap();

        let wrong_type = all.replace("SELECT e.dst", "SELECT e.label");
        assert!(matches!(
            db.execute(&wrong_type, &[]),
            Err(SqlError::Type(_))
        ));
        let no_children = all.replace("SELECT id FROM roots", "SELECT id FROM roots WHERE id < 0");
        assert!(rows(db, &no_children, &[]).is_empty());
    });
}
