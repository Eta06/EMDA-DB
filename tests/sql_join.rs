//! INNER and LEFT JOIN tests on both writable backends, including a
//! randomized differential test against SQLite.
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlLimits, SqlOutcome, SqlValue,
    Store,
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

fn rows(db: &SqlDatabase<'_>, sql: &str, params: &[SqlValue]) -> Vec<Vec<SqlValue>> {
    match db.execute(sql, params) {
        Ok(SqlOutcome::Rows { rows, .. }) => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

fn int(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

fn text(s: &str) -> SqlValue {
    SqlValue::Text(s.into())
}

fn setup(db: &SqlDatabase<'_>) {
    for sql in [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, team INTEGER)",
        "CREATE TABLE teams (id INTEGER PRIMARY KEY, title TEXT)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)",
        "INSERT INTO teams VALUES (1, 'red'), (2, 'blue'), (3, NULL)",
        "INSERT INTO users VALUES (10, 'ada', 1), (11, 'bob', 2), (12, 'cy', NULL), (13, 'di', 9)",
        "INSERT INTO orders VALUES (100, 10, 5.5), (101, 10, 2.0), (102, 11, 7.0), (103, NULL, 1.0)",
    ] {
        db.execute(sql, &[]).unwrap();
    }
}

#[test]
fn inner_and_left_joins_follow_sql_semantics() {
    for_both(|db, backend| {
        setup(db);
        // Primary-key point access; the unmatched team 9 and NULL team drop out.
        assert_eq!(
            rows(
                db,
                "SELECT u.name, t.title FROM users u JOIN teams t ON t.id = u.team",
                &[]
            ),
            vec![
                vec![text("ada"), text("red")],
                vec![text("bob"), text("blue")]
            ],
            "{backend}"
        );
        // LEFT JOIN extends unmatched rows with NULLs.
        assert_eq!(
            rows(
                db,
                "SELECT u.name, t.title FROM users AS u LEFT OUTER JOIN teams AS t ON u.team = t.id",
                &[]
            ),
            vec![
                vec![text("ada"), text("red")],
                vec![text("bob"), text("blue")],
                vec![text("cy"), SqlValue::Null],
                vec![text("di"), SqlValue::Null],
            ],
            "{backend}"
        );
        // A condition in ON keeps the left row; the same condition in WHERE removes it.
        assert_eq!(
            rows(
                db,
                "SELECT u.id FROM users u LEFT JOIN teams t ON t.id = u.team AND t.title = 'red' ORDER BY u.id",
                &[]
            )
            .len(),
            4,
            "{backend}"
        );
        assert_eq!(
            rows(
                db,
                "SELECT u.id FROM users u LEFT JOIN teams t ON t.id = u.team WHERE t.title = 'red'",
                &[]
            ),
            vec![vec![int(10)]],
            "{backend}"
        );
        // IS NULL on the right side finds unmatched rows.
        assert_eq!(
            rows(
                db,
                "SELECT u.name FROM users u LEFT JOIN teams t ON t.id = u.team WHERE t.id IS NULL",
                &[]
            ),
            vec![vec![text("cy")], vec![text("di")]],
            "{backend}"
        );
        // Hash access on a non-key column, three tables, ORDER BY and LIMIT.
        assert_eq!(
            rows(
                db,
                "SELECT o.id, u.name, t.title FROM orders o JOIN users u ON u.id = o.user_id \
                 JOIN teams t ON t.id = u.team ORDER BY o.total DESC LIMIT 2",
                &[]
            ),
            vec![
                vec![int(102), text("bob"), text("blue")],
                vec![int(100), text("ada"), text("red")],
            ],
            "{backend}"
        );
        assert_eq!(
            rows(
                db,
                "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > ? ORDER BY o.total",
                &[SqlValue::Real(1.5)]
            ),
            vec![
                vec![text("ada"), SqlValue::Real(2.0)],
                vec![text("ada"), SqlValue::Real(5.5)],
                vec![text("bob"), SqlValue::Real(7.0)],
            ],
            "{backend}"
        );
        // Non-equality ON uses a nested loop.
        assert_eq!(
            rows(
                db,
                "SELECT a.id, b.id FROM teams a JOIN teams b ON a.id < b.id ORDER BY a.id, b.id",
                &[]
            ),
            vec![
                vec![int(1), int(2)],
                vec![int(1), int(3)],
                vec![int(2), int(3)],
            ],
            "{backend}"
        );
        // Stars expand in table order; t.* selects one table.
        let all = rows(
            db,
            "SELECT * FROM users u JOIN teams t ON t.id = u.team WHERE u.id = 10",
            &[],
        );
        assert_eq!(
            all,
            vec![vec![int(10), text("ada"), int(1), int(1), text("red")]],
            "{backend}"
        );
        assert_eq!(
            rows(
                db,
                "SELECT t.* FROM users u JOIN teams t ON t.id = u.team WHERE u.id = 11",
                &[]
            ),
            vec![vec![int(2), text("blue")]],
            "{backend}"
        );
        // INTEGER keys match equal REAL values; fractional values match nothing.
        assert_eq!(
            rows(
                db,
                "SELECT t.title FROM orders o JOIN teams t ON t.id = o.total",
                &[]
            ),
            vec![vec![text("blue")], vec![text("red")]],
            "{backend}"
        );
    });
}

#[test]
fn join_errors_are_classified() {
    for_both(|db, backend| {
        setup(db);
        let err = |sql: &str| db.execute(sql, &[]).unwrap_err();
        assert!(
            matches!(
                err("SELECT id FROM users u JOIN teams t ON t.id = u.team"),
                SqlError::Syntax { .. }
            ),
            "{backend}: ambiguous column"
        );
        assert!(matches!(
            err("SELECT x.id FROM users u JOIN teams t ON t.id = u.team"),
            SqlError::Syntax { .. }
        ));
        assert!(matches!(
            err("SELECT u.id FROM users u JOIN teams t ON t.id = t2.id"),
            SqlError::Syntax { .. }
        ));
        assert!(matches!(
            err("SELECT u.id FROM users u JOIN teams u ON u.id = u.id"),
            SqlError::Syntax { .. }
        ));
        assert!(matches!(
            err("SELECT u.nope FROM users u JOIN teams t ON t.id = u.team"),
            SqlError::NoSuchColumn { .. }
        ));
        assert!(matches!(
            err("SELECT u.id FROM users u JOIN missing m ON m.id = u.id"),
            SqlError::NoSuchTable(_)
        ));
        assert!(matches!(
            err("SELECT u.id FROM users u JOIN teams t ON t.title = u.team"),
            SqlError::Type(_)
        ));
        assert!(matches!(
            err("SELECT u.id FROM users u JOIN teams t ON t.id"),
            SqlError::Type(_)
        ));
        for unsupported in [
            "SELECT u.id FROM users u RIGHT JOIN teams t ON t.id = u.team",
            "SELECT u.id FROM users u FULL JOIN teams t ON t.id = u.team",
            "SELECT u.id FROM users u CROSS JOIN teams t",
            "SELECT u.id FROM users u NATURAL JOIN teams t",
            "SELECT u.id FROM users u JOIN teams t USING (id)",
            "SELECT u.id FROM users u, teams t JOIN orders o ON o.id = u.id",
            "SELECT COUNT(*) FROM users u JOIN teams t ON t.id = u.team",
            "SELECT u.id AS x FROM users u JOIN teams t ON t.id = u.team",
            "SELECT u.id FROM users u JOIN teams t ON t.id = u.team GROUP BY u.id",
            "SELECT u.id FROM users u JOIN teams t ON t.id IN (1, 2)",
        ] {
            assert!(
                matches!(err(unsupported), SqlError::Unsupported { .. }),
                "{backend}: {unsupported}: {:?}",
                err(unsupported)
            );
        }
        assert!(matches!(
            err("SELECT u.id FROM users u JOIN teams t"),
            SqlError::Syntax { .. }
        ));
    });
}

#[test]
fn join_bounds_fail_instead_of_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("s")).unwrap();
    let limits = SqlLimits {
        max_visited_rows: 100,
        ..SqlLimits::default()
    };
    let db = SqlDatabase::with_limits(DocumentDatabase::new(&store), limits).unwrap();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)", &[])
        .unwrap();
    let values = (0..40)
        .map(|i| format!("({i}, {})", i % 2))
        .collect::<Vec<_>>();
    db.execute(&format!("INSERT INTO a VALUES {}", values.join(", ")), &[])
        .unwrap();
    // 40 + 40 rows read and 20 matches per row: the combined rows exceed 100.
    assert!(matches!(
        db.execute("SELECT x.id FROM a x JOIN a y ON y.v = x.v", &[]),
        Err(SqlError::Bounds(_))
    ));
    // Point access reads one row per left row: 40 + 40 stays within 100.
    assert_eq!(
        rows(&db, "SELECT x.id FROM a x JOIN a y ON y.id = x.id", &[]).len(),
        40
    );
    // LIMIT without ORDER BY stops reading early.
    assert_eq!(
        rows(
            &db,
            "SELECT x.id, y.id FROM a x JOIN a y ON y.v = x.v LIMIT 3",
            &[]
        ),
        vec![
            vec![int(0), int(0)],
            vec![int(0), int(2)],
            vec![int(0), int(4)],
        ]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT x.id FROM a x JOIN a y ON y.id = x.id LIMIT 0",
            &[]
        )
        .len(),
        0
    );
}

#[test]
fn joins_see_their_own_snapshot_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path().join("s")).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        setup(&db);
    }
    let store = Store::open(dir.path().join("s")).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    assert_eq!(
        rows(
            &db,
            "SELECT u.name, t.title FROM users u JOIN teams t ON t.id = u.team",
            &[]
        )
        .len(),
        2
    );
    let statement = db
        .prepare("SELECT u.name FROM users u JOIN teams t ON t.id = u.team WHERE t.id = ?")
        .unwrap();
    assert_eq!(statement.param_count(), 1);
    let SqlOutcome::Rows { rows, .. } = db.execute_prepared(&statement, &[int(2)]).unwrap() else {
        panic!()
    };
    assert_eq!(rows, vec![vec![text("bob")]]);
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

fn from_sqlite(value: rusqlite::types::Value) -> SqlValue {
    use rusqlite::types::Value;
    match value {
        Value::Null => SqlValue::Null,
        Value::Integer(n) => SqlValue::Integer(n),
        Value::Real(f) => SqlValue::Real(f),
        Value::Text(s) => SqlValue::Text(s),
        Value::Blob(_) => panic!("unexpected blob"),
    }
}

#[test]
fn random_joins_match_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("s")).unwrap();
    let emda = SqlDatabase::new(DocumentDatabase::new(&store));
    let lite = rusqlite::Connection::open_in_memory().unwrap();
    let mut rng = Rng(0x7a11_5eed_0bad_cafe);
    for (name, rows) in [("a", 40i64), ("b", 30), ("c", 20)] {
        let ddl =
            format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, k INTEGER, s TEXT, r REAL)");
        emda.execute(&ddl, &[]).unwrap();
        lite.execute(&ddl, []).unwrap();
        for id in 0..rows {
            let mut maybe = |value: SqlValue| {
                if rng.below(5) == 0 {
                    SqlValue::Null
                } else {
                    value
                }
            };
            let k = maybe(SqlValue::Integer(id % 7));
            let s = maybe(SqlValue::Text(["x", "y", "z"][(id % 3) as usize].into()));
            let r = maybe(SqlValue::Real((id % 5) as f64));
            let row = vec![SqlValue::Integer(id), k, s, r];
            let insert = format!("INSERT INTO {name} VALUES (?, ?, ?, ?)");
            emda.execute(&insert, &row).unwrap();
            lite.execute(
                &insert,
                rusqlite::params_from_iter(row.iter().map(to_sqlite)),
            )
            .unwrap();
        }
    }
    // Index access for joined tables and the first table: a complete
    // single-column index, and a compound index whose trailing column can be
    // NULL, which must not be used for leading-column lookups.
    for ddl in [
        "CREATE INDEX a_k ON a (k)",
        "CREATE INDEX b_k ON b (k)",
        "CREATE INDEX c_ks ON c (k, s)",
        "CREATE INDEX c_s ON c (s)",
    ] {
        emda.execute(ddl, &[]).unwrap();
        lite.execute(ddl, []).unwrap();
    }
    let on_shapes = [
        "{r}.id = {l}.k",
        "{l}.k = {r}.id",
        "{r}.k = {l}.k",
        "{r}.s = {l}.s AND {r}.id > {l}.id",
        "{r}.r = {l}.k",
        "{r}.id < {l}.k",
        "{r}.k = {l}.k OR {r}.s = 'x'",
        "{r}.k IS NULL",
        "{r}.s = {l}.s",
        "{r}.k = {l}.r",
        "NOT ({r}.k = {l}.k)",
    ];
    let filters = [
        "",
        "WHERE a.k > 2",
        "WHERE b.id IS NULL",
        "WHERE a.s = 'y' OR b.r >= 2",
        "WHERE a.id < 30 AND b.k IS NOT NULL",
        "WHERE a.id = 7",
        "WHERE a.k = 3",
        "WHERE a.k = 3.0 AND b.id >= 0",
        "WHERE a.s = 'z'",
        "WHERE 2 = a.k OR a.id = 1",
    ];
    for round in 0..300 {
        let joins = 1 + rng.below(2) as usize;
        let mut sql = String::from(" FROM a");
        let mut tables = vec!["a"];
        for (index, table) in ["b", "c"].into_iter().take(joins).enumerate() {
            let kind = if rng.below(2) == 0 {
                "JOIN"
            } else {
                "LEFT JOIN"
            };
            let left = tables[rng.below(index as u64 + 1) as usize];
            let shape = on_shapes[rng.below(on_shapes.len() as u64) as usize];
            let on = shape.replace("{l}", left).replace("{r}", table);
            sql.push_str(&format!(" {kind} {table} ON {on}"));
            tables.push(table);
        }
        let filter = filters[rng.below(filters.len() as u64) as usize];
        sql.push(' ');
        sql.push_str(filter);
        // Every row is ordered by every table's key; SQLite then agrees on
        // order, including LEFT JOIN NULLs, which sort first in both.
        let order = tables
            .iter()
            .map(|t| format!("{t}.id"))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = tables
            .iter()
            .flat_map(|t| [format!("{t}.id"), format!("{t}.k"), format!("{t}.s")])
            .collect::<Vec<_>>()
            .join(", ");
        let limit = if round % 3 == 0 {
            format!(" LIMIT {} OFFSET {}", rng.below(15), rng.below(10))
        } else {
            String::new()
        };
        let query = format!("SELECT {columns}{sql} ORDER BY {order}{limit}");
        let ours = rows(&emda, &query, &[]);
        let mut statement = lite.prepare(&query).unwrap();
        let width = tables.len() * 3;
        let theirs = statement
            .query_map([], |row| {
                Ok((0..width)
                    .map(|index| from_sqlite(row.get(index).unwrap()))
                    .collect::<Vec<_>>())
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(ours, theirs, "{query}");
        // Without ORDER BY, the same rows in some order.
        if limit.is_empty() {
            let mut unordered = rows(&emda, &format!("SELECT {columns}{sql}"), &[]);
            let key = |row: &Vec<SqlValue>| format!("{row:?}");
            unordered.sort_by_key(key);
            let mut expected = theirs.clone();
            expected.sort_by_key(key);
            assert_eq!(unordered, expected, "{query} without ORDER BY");
        }
    }
}

#[test]
fn compound_index_with_nullable_trailing_column_is_not_used_for_lookups() {
    for_both(|db, backend| {
        for sql in [
            "CREATE TABLE p (id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE q (id INTEGER PRIMARY KEY, k INTEGER, tag TEXT)",
            "CREATE INDEX q_k_tag ON q (k, tag)",
            "INSERT INTO p VALUES (1, 5)",
            "INSERT INTO q VALUES (10, 5, NULL), (11, 5, 'x'), (12, 6, 'y')",
        ] {
            db.execute(sql, &[]).unwrap();
        }
        // Row 10 is missing from the compound index; a lookup through it would drop it.
        assert_eq!(
            rows(
                db,
                "SELECT q.id FROM p JOIN q ON q.k = p.k ORDER BY q.id",
                &[]
            ),
            vec![vec![int(10)], vec![int(11)]],
            "{backend}"
        );
        assert_eq!(
            rows(
                db,
                "SELECT q.id, p.id FROM q LEFT JOIN p ON p.k = q.k WHERE q.k = 5 ORDER BY q.id",
                &[]
            ),
            vec![vec![int(10), int(1)], vec![int(11), int(1)]],
            "{backend}"
        );
    });
}

/// Scanned rows decode the same whether they take the flat reader or, for
/// text with escapes or non-ASCII characters, the full decoder.
#[test]
fn scanned_rows_with_escaped_text_join_like_plain_rows() {
    for_both(|db, backend| {
        db.execute(
            "CREATE TABLE a (id INTEGER PRIMARY KEY, k INTEGER, label TEXT, score REAL, ok BOOLEAN)",
            &[],
        )
        .unwrap();
        db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)", &[])
            .unwrap();
        let labels = [
            "plain",
            "quote \" inside",
            "line\nbreak",
            "back\\slash",
            "tab\there",
            "é unicode",
            "\u{1} control",
        ];
        for (id, label) in labels.iter().enumerate() {
            let id = id as i64;
            db.execute(
                "INSERT INTO a VALUES (?, ?, ?, ?, ?)",
                &[
                    int(id),
                    int(id),
                    text(label),
                    SqlValue::Real(id as f64 * 0.5),
                    SqlValue::Boolean(id % 2 == 0),
                ],
            )
            .unwrap();
            db.execute("INSERT INTO b VALUES (?, ?)", &[int(100 + id), int(id)])
                .unwrap();
        }
        // The join is on a column of a that is neither its key nor indexed,
        // so every row of a is read by a table scan.
        let got = rows(
            db,
            "SELECT b.id, a.label, a.score, a.ok FROM b JOIN a ON a.k = b.a_id ORDER BY b.id",
            &[],
        );
        let expected: Vec<Vec<SqlValue>> = labels
            .iter()
            .enumerate()
            .map(|(id, label)| {
                vec![
                    int(100 + id as i64),
                    text(label),
                    SqlValue::Real(id as f64 * 0.5),
                    SqlValue::Boolean(id % 2 == 0),
                ]
            })
            .collect();
        assert_eq!(got, expected, "{backend}");
    });
}
