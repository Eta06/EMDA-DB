//! `CHECK` constraints on both writable backends, compared with SQLite.
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

fn rows(db: &SqlDatabase<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    match db.execute(sql, &[]) {
        Ok(SqlOutcome::Rows { rows, .. }) => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

const CREATE: &str = "CREATE TABLE t (
    id INTEGER PRIMARY KEY,
    qty INTEGER CHECK (qty >= 0),
    lo INTEGER,
    hi INTEGER,
    tag INTEGER CHECK (tag IN (SELECT id FROM o) OR tag IS NULL),
    CHECK (lo <= hi)
)";

#[test]
fn checks_reject_false_and_accept_true_or_null() {
    for_both(|db, backend| {
        // Subqueries are refused in a check.
        db.execute("CREATE TABLE o (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        assert!(matches!(
            db.execute(CREATE, &[]),
            Err(SqlError::Unsupported { .. })
        ));
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER CHECK (qty >= 0), \
             lo INTEGER, hi INTEGER, tag TEXT CHECK (tag <> ''), CHECK (lo <= hi))",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO t VALUES (1, 0, 1, 2, 'a')", &[])
            .unwrap();
        // NULL makes a check unknown, which passes.
        db.execute("INSERT INTO t VALUES (2, NULL, NULL, 5, NULL)", &[])
            .unwrap();
        for sql in [
            "INSERT INTO t VALUES (3, -1, 1, 2, 'a')",
            "INSERT INTO t VALUES (3, 1, 3, 2, 'a')",
            "INSERT INTO t VALUES (3, 1, 1, 2, '')",
            "INSERT INTO t VALUES (3, 1, 1, 2, 'a'), (4, -5, 1, 2, 'b')",
            "UPDATE t SET qty = -1 WHERE id = 1",
            "UPDATE t SET lo = 10 WHERE id = 1",
        ] {
            let error = db.execute(sql, &[]).unwrap_err();
            assert!(
                matches!(error, SqlError::Constraint(_)),
                "{backend}: {sql}: {error:?}"
            );
        }
        // A failed multi-row statement wrote nothing.
        assert_eq!(
            rows(db, "SELECT COUNT(*) FROM t"),
            vec![vec![SqlValue::Integer(2)]]
        );
        db.execute("UPDATE t SET lo = 2 WHERE id = 1", &[]).unwrap();
        // Inside a transaction a failing statement rolls the transaction back.
        let result = db.transaction(|tx| {
            tx.execute("INSERT INTO t VALUES (5, 1, 1, 1, 'x')", &[])?;
            tx.execute("UPDATE t SET hi = 0 WHERE id = 5", &[])?;
            Ok(())
        });
        assert!(matches!(result, Err(SqlError::Constraint(_))), "{backend}");
        assert_eq!(
            rows(db, "SELECT id, qty, lo, hi FROM t ORDER BY id"),
            vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(0),
                    SqlValue::Integer(2),
                    SqlValue::Integer(2)
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Integer(5)
                ],
            ],
            "{backend}"
        );
    });
}

#[test]
fn invalid_checks_are_rejected_at_create_table() {
    for_both(|db, backend| {
        for sql in [
            "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER CHECK (nope > 1))",
            "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 'x'))",
            "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > ?))",
            "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER CHECK (v))",
            "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER CHECK v > 1)",
            "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > (SELECT 1)))",
        ] {
            assert!(db.execute(sql, &[]).is_err(), "{backend}: {sql}");
            assert!(matches!(
                db.execute("SELECT id FROM a", &[]),
                Err(SqlError::NoSuchTable(_))
            ));
        }
        let many = (0..33)
            .map(|i| format!("CHECK (v <> {i})"))
            .collect::<Vec<_>>()
            .join(", ");
        assert!(matches!(
            db.execute(
                &format!("CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER, {many})"),
                &[]
            ),
            Err(SqlError::Bounds(_))
        ));
    });
}

#[test]
fn checks_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let store = Store::open(&path).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, CHECK (a < b OR b IS NULL))",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO t VALUES (1, 1, 2)", &[]).unwrap();
    }
    let store = Store::open(&path).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    assert!(matches!(
        db.execute("INSERT INTO t VALUES (2, 3, 2)", &[]),
        Err(SqlError::Constraint(_))
    ));
    db.execute("INSERT INTO t VALUES (2, 3, NULL)", &[])
        .unwrap();
}

/// Random inserts and updates succeed or fail as in SQLite, and leave the
/// same rows.
#[test]
fn random_writes_match_sqlite() {
    for_both(|db, backend| {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        let create = "CREATE TABLE r (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
                      c TEXT, CHECK (a <= b), CHECK (c <> 'bad' OR a = 0), CHECK (NOT (b > 50)))";
        db.execute(create, &[]).unwrap();
        sqlite.execute(create, []).unwrap();
        let mut state = 0x1234_5678_9abc_def1_u64;
        let mut next = |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let value = |v: u64| match v {
            0 => "NULL".to_owned(),
            v => format!("{}", v as i64 - 30),
        };
        for step in 0..400 {
            let sql = match next(3) {
                0 | 1 => format!(
                    "INSERT INTO r VALUES ({}, {}, {}, {})",
                    next(40),
                    value(next(90)),
                    value(next(90)),
                    ["NULL", "'bad'", "'ok'"][next(3) as usize]
                ),
                _ => format!(
                    "UPDATE r SET {} = {} WHERE id = {}",
                    ["a", "b"][next(2) as usize],
                    value(next(90)),
                    next(40)
                ),
            };
            let ours = db.execute(&sql, &[]);
            let theirs = sqlite.execute(&sql, []);
            assert_eq!(
                ours.is_ok(),
                theirs.is_ok(),
                "{backend} step {step}: {sql}: {ours:?} / {theirs:?}"
            );
        }
        let query = "SELECT id, a, b, c FROM r ORDER BY id";
        let ours = rows(db, query);
        let mut stmt = sqlite.prepare(query).unwrap();
        let theirs: Vec<Vec<SqlValue>> = stmt
            .query_map([], |row| {
                (0..4)
                    .map(|i| {
                        Ok(match row.get::<_, rusqlite::types::Value>(i)? {
                            rusqlite::types::Value::Null => SqlValue::Null,
                            rusqlite::types::Value::Integer(n) => SqlValue::Integer(n),
                            rusqlite::types::Value::Text(s) => SqlValue::Text(s),
                            other => panic!("{other:?}"),
                        })
                    })
                    .collect()
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(ours, theirs, "{backend}");
        assert!(ours.len() > 5, "{backend}: too few rows exercised");
    });
}
