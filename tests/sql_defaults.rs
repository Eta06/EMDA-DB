//! Column `DEFAULT` values on both writable backends, compared with SQLite.
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

fn text(s: &str) -> SqlValue {
    SqlValue::Text(s.into())
}

const CREATE: &str = "CREATE TABLE t (
    id INTEGER PRIMARY KEY,
    n INTEGER NOT NULL DEFAULT -7,
    r REAL DEFAULT 2,
    s VARCHAR(5) DEFAULT 'abc',
    b BOOLEAN NOT NULL DEFAULT TRUE,
    z TEXT DEFAULT NULL,
    plain INTEGER
)";

#[test]
fn omitted_columns_take_their_defaults_and_explicit_values_win() {
    for_both(|db, backend| {
        db.execute(CREATE, &[]).unwrap();
        db.execute("CREATE INDEX t_s ON t (s)", &[]).unwrap();
        db.execute("INSERT INTO t (id) VALUES (1)", &[]).unwrap();
        db.execute(
            "INSERT INTO t (id, n, s) VALUES (2, 5, NULL), (3, 6, 'x')",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO t VALUES (4, 1, 0.5, 'q', FALSE, 'zz', 9)", &[])
            .unwrap();
        let got = rows(db, "SELECT id, n, r, s, b, z, plain FROM t ORDER BY id");
        assert_eq!(
            got,
            vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(-7),
                    SqlValue::Real(2.0),
                    text("abc"),
                    SqlValue::Boolean(true),
                    SqlValue::Null,
                    SqlValue::Null,
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Integer(5),
                    SqlValue::Real(2.0),
                    SqlValue::Null,
                    SqlValue::Boolean(true),
                    SqlValue::Null,
                    SqlValue::Null,
                ],
                vec![
                    SqlValue::Integer(3),
                    SqlValue::Integer(6),
                    SqlValue::Real(2.0),
                    text("x"),
                    SqlValue::Boolean(true),
                    SqlValue::Null,
                    SqlValue::Null,
                ],
                vec![
                    SqlValue::Integer(4),
                    SqlValue::Integer(1),
                    SqlValue::Real(0.5),
                    text("q"),
                    SqlValue::Boolean(false),
                    text("zz"),
                    SqlValue::Integer(9),
                ],
            ],
            "{backend}"
        );
        // Defaulted values are indexed like written ones.
        assert_eq!(
            rows(db, "SELECT id FROM t WHERE s = 'abc'"),
            vec![vec![SqlValue::Integer(1)]],
            "{backend}"
        );
        // An omitted NOT NULL column without a default still fails.
        db.execute(
            "CREATE TABLE u (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
            &[],
        )
        .unwrap();
        assert!(matches!(
            db.execute("INSERT INTO u (id) VALUES (1)", &[]),
            Err(SqlError::Constraint(_))
        ));
        // Explicit NULL into a NOT NULL column with a default is still refused.
        assert!(matches!(
            db.execute("INSERT INTO t (id, n) VALUES (5, NULL)", &[]),
            Err(SqlError::Constraint(_))
        ));
        // The same prepared statement keeps applying defaults.
        let insert = db
            .prepare("INSERT INTO t (id, plain) VALUES (?, ?)")
            .unwrap();
        for id in 10..13 {
            db.execute_prepared(&insert, &[SqlValue::Integer(id), SqlValue::Integer(id)])
                .unwrap();
        }
        assert_eq!(
            rows(db, "SELECT COUNT(*) FROM t WHERE n = -7 AND b = TRUE"),
            vec![vec![SqlValue::Integer(4)]],
            "{backend}"
        );
    });
}

#[test]
fn invalid_defaults_are_rejected_at_create_table() {
    for_both(|db, backend| {
        for (sql, expect) in [
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 'x')",
                "type",
            ),
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v BOOLEAN DEFAULT 1)",
                "type",
            ),
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 1.5)",
                "type",
            ),
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v VARCHAR(2) DEFAULT 'abc')",
                "any",
            ),
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT NULL)",
                "constraint",
            ),
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY DEFAULT 1)",
                "unsupported",
            ),
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER DEFAULT (1 + 1))",
                "unsupported",
            ),
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT DEFAULT CURRENT_TIMESTAMP)",
                "unsupported",
            ),
            (
                "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 1 DEFAULT 2)",
                "syntax",
            ),
        ] {
            let error = db.execute(sql, &[]).unwrap_err();
            let ok = match expect {
                "type" => matches!(error, SqlError::Type(_)),
                "constraint" => matches!(error, SqlError::Constraint(_)),
                "unsupported" => matches!(error, SqlError::Unsupported { .. }),
                "syntax" => matches!(error, SqlError::Syntax { .. }),
                _ => true,
            };
            assert!(ok, "{backend}: {sql}: {error:?}");
            assert!(matches!(
                db.execute("SELECT id FROM a", &[]),
                Err(SqlError::NoSuchTable(_))
            ));
        }
        // ADD COLUMN with a default is not supported.
        db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        assert!(matches!(
            db.execute("ALTER TABLE c ADD COLUMN v INTEGER DEFAULT 1", &[]),
            Err(SqlError::Unsupported { .. })
        ));
    });
}

#[test]
fn defaults_survive_reopen_and_match_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    let statements = [
        CREATE,
        "INSERT INTO t (id) VALUES (1)",
        "INSERT INTO t (id, n) VALUES (2, 3)",
        "INSERT INTO t (id, s, z) VALUES (3, 'de', 'w')",
        "INSERT INTO t (id, r, b) VALUES (4, -1.25, FALSE)",
    ];
    {
        let store = Store::open(&path).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        for sql in statements {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
    }
    let store = Store::open(&path).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    db.execute("INSERT INTO t (id) VALUES (5)", &[]).unwrap();
    sqlite.execute("INSERT INTO t (id) VALUES (5)", []).unwrap();
    let query = "SELECT id, n, r, s, b, z, plain FROM t ORDER BY id";
    let ours = rows(&db, query);
    let mut stmt = sqlite.prepare(query).unwrap();
    let theirs: Vec<Vec<SqlValue>> = stmt
        .query_map([], |row| {
            let get = |i: usize| -> rusqlite::Result<SqlValue> {
                Ok(match row.get::<_, rusqlite::types::Value>(i)? {
                    rusqlite::types::Value::Null => SqlValue::Null,
                    rusqlite::types::Value::Integer(n) if i == 4 => SqlValue::Boolean(n != 0),
                    rusqlite::types::Value::Integer(n) => SqlValue::Integer(n),
                    rusqlite::types::Value::Real(f) => SqlValue::Real(f),
                    rusqlite::types::Value::Text(s) => SqlValue::Text(s),
                    rusqlite::types::Value::Blob(_) => unreachable!(),
                })
            };
            (0..7).map(get).collect()
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ours, theirs);
}
