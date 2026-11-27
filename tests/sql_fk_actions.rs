//! `ON DELETE CASCADE` and `ON DELETE SET NULL` on both writable backends,
//! compared with SQLite with foreign keys enabled.
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlLimits, SqlOutcome, SqlValue,
    Store,
};

fn for_both(limits: SqlLimits, mut f: impl FnMut(&SqlDatabase<'_>, &str)) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("ordinary")).unwrap();
    f(
        &SqlDatabase::with_limits(DocumentDatabase::new(&store), limits).unwrap(),
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
        &SqlDatabase::with_limits(DocumentDatabase::with_checkpoint_store(&checkpoint), limits)
            .unwrap(),
        "CheckpointStore",
    );
}

fn rows(db: &SqlDatabase<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    match db.execute(sql, &[]) {
        Ok(SqlOutcome::Rows { rows, .. }) => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

fn ints(db: &SqlDatabase<'_>, sql: &str) -> Vec<Option<i64>> {
    rows(db, sql)
        .into_iter()
        .map(|row| match row[0] {
            SqlValue::Integer(n) => Some(n),
            SqlValue::Null => None,
            ref other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn cascades_and_set_null_follow_the_declared_actions() {
    for_both(SqlLimits::default(), |db, backend| {
        for sql in [
            "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, p INTEGER REFERENCES p(id) ON DELETE CASCADE)",
            "CREATE INDEX c_p ON c (p)",
            "CREATE TABLE g (id INTEGER PRIMARY KEY, c INTEGER REFERENCES c(id) ON DELETE CASCADE)",
            "CREATE TABLE n (id INTEGER PRIMARY KEY, c INTEGER REFERENCES c(id) ON DELETE SET NULL, \
             code TEXT REFERENCES p(code) ON DELETE SET NULL)",
            "INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO c VALUES (10, 1), (11, 1), (12, 2), (13, NULL)",
            "INSERT INTO g VALUES (100, 10), (101, 11), (102, 12)",
            "INSERT INTO n VALUES (200, 10, 'a'), (201, 12, 'b'), (202, NULL, 'c')",
        ] {
            db.execute(sql, &[]).unwrap();
        }
        // Deleting parent 1 cascades to c 10 and 11, then to g 100 and 101,
        // and sets n.c to NULL for row 200 and n.code for the deleted 'a'.
        db.execute("DELETE FROM p WHERE id = 1", &[]).unwrap();
        assert_eq!(
            ints(db, "SELECT id FROM c ORDER BY id"),
            [Some(12), Some(13)],
            "{backend}"
        );
        assert_eq!(
            ints(db, "SELECT id FROM g ORDER BY id"),
            [Some(102)],
            "{backend}"
        );
        assert_eq!(
            rows(db, "SELECT id, c, code FROM n ORDER BY id"),
            vec![
                vec![SqlValue::Integer(200), SqlValue::Null, SqlValue::Null],
                vec![
                    SqlValue::Integer(201),
                    SqlValue::Integer(12),
                    SqlValue::Text("b".into())
                ],
                vec![
                    SqlValue::Integer(202),
                    SqlValue::Null,
                    SqlValue::Text("c".into())
                ],
            ],
            "{backend}"
        );
        // A RESTRICT reference below a cascade still blocks the whole delete.
        db.execute(
            "CREATE TABLE r (id INTEGER PRIMARY KEY, g INTEGER REFERENCES g(id) ON DELETE RESTRICT)",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO r VALUES (300, 102)", &[]).unwrap();
        assert!(matches!(
            db.execute("DELETE FROM p WHERE id = 2", &[]),
            Err(SqlError::Constraint(_))
        ));
        assert_eq!(
            ints(db, "SELECT id FROM p ORDER BY id"),
            [Some(2), Some(3)],
            "{backend}"
        );
        assert_eq!(
            ints(db, "SELECT id FROM g ORDER BY id"),
            [Some(102)],
            "{backend}"
        );
        db.execute("DELETE FROM r", &[]).unwrap();
        db.execute("DELETE FROM p WHERE id = 2", &[]).unwrap();
        assert_eq!(
            ints(db, "SELECT id FROM c ORDER BY id"),
            [Some(13)],
            "{backend}"
        );
        assert!(ints(db, "SELECT id FROM g").is_empty(), "{backend}");
    });
}

#[test]
fn a_self_referencing_tree_is_deleted_whole() {
    for_both(SqlLimits::default(), |db, backend| {
        db.execute(
            "CREATE TABLE node (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES node(id) ON DELETE CASCADE)",
            &[],
        )
        .unwrap();
        // A chain of 10 under node 0, plus a separate root 100.
        db.execute("INSERT INTO node VALUES (0, NULL), (100, NULL)", &[])
            .unwrap();
        for id in 1..10 {
            db.execute(&format!("INSERT INTO node VALUES ({id}, {})", id - 1), &[])
                .unwrap();
        }
        db.execute("DELETE FROM node WHERE id = 0", &[]).unwrap();
        assert_eq!(ints(db, "SELECT id FROM node"), [Some(100)], "{backend}");
        // A chain deeper than the nesting bound fails and changes nothing.
        db.execute("INSERT INTO node VALUES (1000, NULL)", &[])
            .unwrap();
        for id in 1001..1020 {
            db.execute(&format!("INSERT INTO node VALUES ({id}, {})", id - 1), &[])
                .unwrap();
        }
        assert!(matches!(
            db.execute("DELETE FROM node WHERE id = 1000", &[]),
            Err(SqlError::Bounds(_))
        ));
        assert_eq!(
            ints(db, "SELECT COUNT(*) FROM node"),
            [Some(21)],
            "{backend}"
        );
    });
}

#[test]
fn set_null_respects_checks_and_not_null() {
    for_both(SqlLimits::default(), |db, backend| {
        db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        assert!(matches!(
            db.execute(
                "CREATE TABLE bad (id INTEGER PRIMARY KEY, p INTEGER NOT NULL REFERENCES p(id) ON DELETE SET NULL)",
                &[]
            ),
            Err(SqlError::Constraint(_))
        ));
        db.execute(
            "CREATE TABLE c (id INTEGER PRIMARY KEY, p INTEGER REFERENCES p(id) ON DELETE SET NULL, \
             CHECK (p IS NOT NULL OR id < 0))",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO p VALUES (1)", &[]).unwrap();
        db.execute("INSERT INTO c VALUES (5, 1)", &[]).unwrap();
        assert!(matches!(
            db.execute("DELETE FROM p WHERE id = 1", &[]),
            Err(SqlError::Constraint(_))
        ));
        assert_eq!(ints(db, "SELECT p FROM c"), [Some(1)], "{backend}");
    });
}

#[test]
fn cascades_are_bounded_by_the_mutation_limit() {
    let limits = SqlLimits {
        max_mutation_rows: 20,
        ..SqlLimits::default()
    };
    for_both(limits, |db, backend| {
        db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute(
            "CREATE TABLE c (id INTEGER PRIMARY KEY, p INTEGER REFERENCES p(id) ON DELETE CASCADE)",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO p VALUES (1), (2)", &[]).unwrap();
        let children = (0..21)
            .map(|id| format!("({id}, 1)"))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(&format!("INSERT INTO c VALUES {children}"), &[])
            .unwrap();
        assert!(matches!(
            db.execute("DELETE FROM p WHERE id = 1", &[]),
            Err(SqlError::Bounds(_))
        ));
        assert_eq!(ints(db, "SELECT COUNT(*) FROM c"), [Some(21)], "{backend}");
        db.execute("DELETE FROM c WHERE id = 0", &[]).unwrap();
        db.execute("DELETE FROM p WHERE id = 1", &[]).unwrap();
        assert_eq!(ints(db, "SELECT COUNT(*) FROM c"), [Some(0)], "{backend}");
    });
}

#[test]
fn random_deletes_match_sqlite() {
    for_both(SqlLimits::default(), |db, backend| {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        sqlite.execute_batch("PRAGMA foreign_keys = ON").unwrap();
        let schema = [
            "CREATE TABLE a (id INTEGER PRIMARY KEY)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, a INTEGER REFERENCES a(id) ON DELETE CASCADE)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, b INTEGER REFERENCES b(id) ON DELETE SET NULL, \
             a INTEGER REFERENCES a(id) ON DELETE RESTRICT)",
            "CREATE TABLE d (id INTEGER PRIMARY KEY, d INTEGER REFERENCES d(id) ON DELETE CASCADE, \
             b INTEGER REFERENCES b(id) ON DELETE CASCADE)",
        ];
        for sql in schema {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let opt = |v: u64, n: u64| {
            if v % 5 == 0 {
                "NULL".to_owned()
            } else {
                (v % n).to_string()
            }
        };
        for step in 0..600 {
            let sql = match next(10) {
                0 | 1 => format!("INSERT INTO a VALUES ({})", next(10)),
                2 | 3 => format!("INSERT INTO b VALUES ({}, {})", next(20), opt(next(50), 10)),
                4 => format!(
                    "INSERT INTO c VALUES ({}, {}, {})",
                    next(20),
                    opt(next(50), 20),
                    opt(next(50), 10)
                ),
                5 | 6 => format!(
                    "INSERT INTO d VALUES ({}, {}, {})",
                    next(30),
                    opt(next(50), 30),
                    opt(next(50), 20)
                ),
                7 | 8 => format!("DELETE FROM a WHERE id = {}", next(10)),
                _ => format!("DELETE FROM b WHERE id = {}", next(20)),
            };
            let ours = db.execute(&sql, &[]);
            let theirs = sqlite.execute(&sql, []);
            assert_eq!(
                ours.is_ok(),
                theirs.is_ok(),
                "{backend} step {step}: {sql}: {ours:?} / {theirs:?}"
            );
        }
        for table in ["a", "b", "c", "d"] {
            let cols = match table {
                "a" => "id",
                "b" => "id, a",
                _ => "id, b, a",
            };
            let cols = if table == "d" { "id, d, b" } else { cols };
            let query = format!("SELECT {cols} FROM {table} ORDER BY id");
            let ours = rows(db, &query);
            let n = ours.first().map_or(0, Vec::len);
            let mut stmt = sqlite.prepare(&query).unwrap();
            let theirs: Vec<Vec<SqlValue>> = stmt
                .query_map([], |row| {
                    (0..n)
                        .map(|i| {
                            Ok(match row.get::<_, Option<i64>>(i)? {
                                Some(v) => SqlValue::Integer(v),
                                None => SqlValue::Null,
                            })
                        })
                        .collect()
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(ours, theirs, "{backend}: table {table}");
        }
    });
}
