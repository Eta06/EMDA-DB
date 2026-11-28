//! EXPLAIN for single-table, aggregate and join SELECTs. Besides checking
//! the descriptions, the tests tie them to execution: with a small
//! max_visited_rows, a query EXPLAIN reports as a lookup or narrow path runs,
//! while one it reports as a full scan exceeds the bound.
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlLimits, SqlOutcome, SqlValue,
    Store,
};

fn explain(db: &SqlDatabase<'_>, sql: &str, params: &[SqlValue]) -> Vec<Vec<String>> {
    match db.execute(&format!("EXPLAIN {sql}"), params) {
        Ok(SqlOutcome::Rows { columns, rows }) => {
            assert_eq!(columns, ["step", "table", "access", "detail"]);
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| match value {
                            SqlValue::Text(s) => s,
                            other => panic!("{other:?}"),
                        })
                        .collect()
                })
                .collect()
        }
        other => panic!("EXPLAIN {sql}: {other:?}"),
    }
}

fn access(db: &SqlDatabase<'_>, sql: &str, params: &[SqlValue]) -> String {
    explain(db, sql, params)[0][2].clone()
}

fn setup(db: &SqlDatabase<'_>) {
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER NOT NULL, code TEXT UNIQUE, v INTEGER)",
        "CREATE INDEX t_k ON t (k)",
        "CREATE TABLE u (id INTEGER PRIMARY KEY, t_id INTEGER, w INTEGER)",
        "CREATE INDEX u_t ON u (t_id)",
    ] {
        db.execute(sql, &[]).unwrap();
    }
    for start in (0..300).step_by(100) {
        let rows = (start..start + 100)
            .map(|id| format!("({id}, {}, 'c{id}', {})", id % 10, id % 7))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(&format!("INSERT INTO t VALUES {rows}"), &[])
            .unwrap();
        let rows = (start..start + 100)
            .map(|id| format!("({id}, {}, {})", id % 50, id % 3))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(&format!("INSERT INTO u VALUES {rows}"), &[])
            .unwrap();
    }
}

#[test]
fn explain_describes_single_table_and_aggregate_plans() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("s")).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    setup(&db);
    assert_eq!(
        access(&db, "SELECT * FROM t WHERE id = ?", &[SqlValue::Integer(5)]),
        "primary-key lookup"
    );
    assert!(
        access(&db, "SELECT * FROM t WHERE id >= 10 AND id < 20", &[])
            .starts_with("primary-key range")
    );
    let unique = access(&db, "SELECT * FROM t WHERE code = 'c7'", &[]);
    assert!(
        unique.starts_with("unique index ") && unique.ends_with(" lookup"),
        "{unique}"
    );
    // A key equality combined with another condition uses the general
    // planner rather than the single-key fast path.
    assert_eq!(
        access(&db, "SELECT * FROM t WHERE id = 5 AND v = 3", &[]),
        "primary-key lookup"
    );
    assert!(
        access(&db, "SELECT * FROM t WHERE k = 3", &[])
            .starts_with("index t_k: equality on 1 column")
    );
    assert_eq!(
        access(&db, "SELECT * FROM t WHERE v = 3", &[]),
        "full scan in primary-key order"
    );
    assert!(access(&db, "SELECT * FROM t WHERE id = 1 AND id = 2", &[]).starts_with("none"));
    let plan = explain(&db, "SELECT id FROM t WHERE v = 3 ORDER BY id LIMIT 5", &[]);
    assert!(
        plan[0][3].contains("WHERE checked on each row read"),
        "{plan:?}"
    );
    assert!(
        plan[0][3].contains("reading stops at OFFSET + LIMIT"),
        "{plan:?}"
    );
    let plan = explain(&db, "SELECT id FROM t ORDER BY v LIMIT 5", &[]);
    assert!(plan[0][3].contains("top OFFSET + LIMIT rows"), "{plan:?}");
    let plan = explain(
        &db,
        "SELECT k, COUNT(*) FROM t WHERE v > 1 GROUP BY k HAVING COUNT(*) > 2",
        &[],
    );
    assert_eq!(plan.len(), 2, "{plan:?}");
    assert_eq!(plan[1][0], "aggregate");
    assert!(plan[1][2].contains("groups by 1 column"), "{plan:?}");
    assert!(plan[1][3].contains("HAVING"), "{plan:?}");
}

#[test]
fn explain_describes_join_plans() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("s")).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    setup(&db);
    db.execute(
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, w INTEGER)",
        &[],
    )
    .unwrap();
    let plan = explain(
        &db,
        "SELECT t.id, u.w FROM t JOIN u ON u.t_id = t.id LEFT JOIN plain p ON p.w = u.w \
         LEFT JOIN t t2 ON t2.v > u.w WHERE t.id = 3 ORDER BY t.id",
        &[],
    );
    assert_eq!(plan.len(), 5, "{plan:?}");
    assert_eq!(plan[0][2], "primary-key lookup", "{plan:?}");
    assert_eq!(plan[1][2], "index u_t lookup", "{plan:?}");
    assert_eq!(plan[2][2], "hash table on w built once", "{plan:?}");
    assert!(plan[2][3].starts_with("LEFT JOIN"), "{plan:?}");
    assert_eq!(plan[3][2], "nested loop over every row", "{plan:?}");
    assert_eq!(plan[4][0], "order");
    assert!(plan[4][2].contains("stream"), "{plan:?}");
    let plan = explain(
        &db,
        "SELECT t.id FROM t JOIN u ON u.id = t.id ORDER BY u.w LIMIT 3",
        &[],
    );
    assert_eq!(plan[0][2], "full scan in primary-key order");
    assert_eq!(plan[1][2], "primary-key lookup");
    assert!(plan[2][2].contains("sorted"), "{plan:?}");
}

#[test]
fn explained_paths_match_execution_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("s")).unwrap();
    let mut db = SqlDatabase::new(DocumentDatabase::new(&store));
    setup(&db);
    db.set_limits(SqlLimits {
        max_visited_rows: 50,
        ..SqlLimits::default()
    })
    .unwrap();
    for (sql, narrow) in [
        ("SELECT * FROM t WHERE id = 5", true),
        ("SELECT * FROM t WHERE id >= 10 AND id < 20", true),
        ("SELECT * FROM t WHERE k = 3 AND id < 100", true),
        ("SELECT * FROM t WHERE code = 'c9'", true),
        ("SELECT * FROM t WHERE v = 3", false),
        (
            "SELECT t.id FROM t JOIN u ON u.t_id = t.id WHERE t.id = 3",
            true,
        ),
        (
            "SELECT t.id FROM t JOIN u ON u.w = t.v WHERE t.id = 3",
            false,
        ),
    ] {
        let path = access(&db, sql, &[]);
        let result = db.execute(sql, &[]);
        if narrow {
            assert!(result.is_ok(), "{sql}: EXPLAIN says {path}, got {result:?}");
        } else {
            assert!(
                matches!(result, Err(SqlError::Bounds(_))),
                "{sql}: EXPLAIN says {path}, got {result:?}"
            );
        }
    }
}

#[test]
fn explain_errors_params_and_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("log");
    let initial = Store::open(&log).unwrap();
    let digest = initial
        .checkpoint_to(dir.path().join("checkpoint"))
        .unwrap();
    drop(initial);
    let store = CheckpointStore::open(&log, dir.path().join("checkpoint"), &digest).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
    setup(&db);
    for statement in [
        "EXPLAIN INSERT INTO t VALUES (1000, 1, 'x', 1)",
        "EXPLAIN DELETE FROM t",
        "EXPLAIN CREATE TABLE z (id INTEGER PRIMARY KEY)",
    ] {
        assert!(
            matches!(
                db.execute(statement, &[]),
                Err(SqlError::Unsupported { .. })
            ),
            "{statement}"
        );
    }
    assert!(matches!(
        db.execute("EXPLAIN SELECT * FROM missing", &[]),
        Err(SqlError::NoSuchTable(_))
    ));
    assert!(matches!(
        db.execute("EXPLAIN", &[]),
        Err(SqlError::Syntax { .. })
    ));
    assert!(matches!(
        db.execute("EXPLAIN SELECT * FROM t WHERE id = ?", &[]),
        Err(SqlError::ParameterCount { .. })
    ));
    let statement = db.prepare("EXPLAIN SELECT * FROM t WHERE k = ?").unwrap();
    assert_eq!(statement.param_count(), 1);
    let planned = db
        .transaction(|tx| tx.execute_prepared(&statement, &[SqlValue::Integer(4)]))
        .unwrap();
    let SqlOutcome::Rows { rows, .. } = planned else {
        panic!()
    };
    assert_eq!(
        rows[0][2],
        SqlValue::Text("index t_k: equality on 1 column(s)".into())
    );
}

#[test]
fn explain_counts_candidates_with_snapshot_visibility_and_a_cap() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("log");
    let checkpoint = dir.path().join("checkpoint");
    let store = Store::open(&log).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    setup(&db);
    check_candidate_estimates(&db);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(db);
    drop(store);
    let store = CheckpointStore::open(&log, &checkpoint, &digest).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store));
    check_candidate_estimates(&db);
}

fn check_candidate_estimates(db: &SqlDatabase<'_>) {
    for (sql, suffix) in [
        ("SELECT * FROM t", "at least 257 (count capped)"),
        ("SELECT * FROM t WHERE id > 4 AND id <= 9", "5"),
        ("SELECT * FROM t WHERE id >= 297 AND id < 999", "3"),
        ("SELECT * FROM t WHERE id > 299", "0"),
        ("SELECT * FROM t WHERE id = 1 AND id = 2", "0"),
        ("SELECT * FROM t WHERE k = 3", "30"),
        (
            "SELECT * FROM t WHERE k >= 8 AND k <= 9 AND v = 3 LIMIT 1",
            "60",
        ),
        ("SELECT COUNT(*) FROM t WHERE k = 2", "30"),
        ("SELECT * FROM t WHERE code = 'c7'", "at most 1"),
    ] {
        let plan = explain(db, sql, &[]);
        assert!(
            plan[0][3].ends_with(&format!("candidate rows: {suffix}")),
            "{sql}: {plan:?}"
        );
    }

    let result: Result<(), SqlError> = db.transaction(|tx| {
        tx.execute("DELETE FROM t WHERE id = 3", &[])?;
        tx.execute(
            "INSERT INTO t VALUES (1001, 3, 'staged', 0), (1002, 3, 'other', 0)",
            &[],
        )?;
        let SqlOutcome::Rows { rows, .. } =
            tx.execute("EXPLAIN SELECT * FROM t WHERE k = 3", &[])?
        else {
            panic!("expected plan rows");
        };
        assert!(matches!(&rows[0][3], SqlValue::Text(s) if s.ends_with("candidate rows: 31")));
        Err(SqlError::Bounds("roll back staged estimate fixture".into()))
    });
    assert!(
        matches!(result, Err(SqlError::Bounds(ref s)) if s == "roll back staged estimate fixture")
    );
}

#[test]
fn competing_indexes_use_the_narrower_snapshot_range() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("s")).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    setup(&db);
    db.execute("CREATE INDEX t_v ON t(v)", &[]).unwrap();
    let query = "SELECT id FROM t WHERE k >= ? AND v = ?";
    let params = [SqlValue::Integer(9), SqlValue::Integer(3)];
    assert!(access(&db, query, &params).starts_with("index t_k"));
    let SqlOutcome::Rows { rows, .. } = db.execute(query, &params).unwrap() else {
        panic!("expected rows");
    };
    let expected: Vec<_> = (0..300)
        .filter(|id| id % 10 == 9 && id % 7 == 3)
        .map(|id| vec![SqlValue::Integer(id)])
        .collect();
    assert_eq!(rows, expected);
    let count = "SELECT COUNT(*) FROM t WHERE k >= ? AND v = ?";
    assert!(access(&db, count, &params).starts_with("index t_k"));
    let SqlOutcome::Rows { rows, .. } = db.execute(count, &params).unwrap() else {
        panic!("expected aggregate rows");
    };
    assert_eq!(rows, vec![vec![SqlValue::Integer(expected.len() as i64)]]);
}
