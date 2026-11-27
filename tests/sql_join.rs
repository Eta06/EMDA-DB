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
            "SELECT u.id FROM users u FULL JOIN teams t ON t.id = u.team JOIN orders o ON o.id = u.id",
            "SELECT u.id FROM users u FULL JOIN teams t ON t.id = u.team JOIN orders o ON o.id = u.id",
            "SELECT u.id FROM users u CROSS JOIN teams t",
            "SELECT u.id FROM users u NATURAL JOIN teams t",
            "SELECT u.id FROM users u JOIN teams t USING (id)",
            "SELECT u.id FROM users u, teams t JOIN orders o ON o.id = u.id",
            "SELECT u.id AS x FROM users u JOIN teams t ON t.id = u.team",
            "SELECT UPPER(u.id) FROM users u JOIN teams t ON t.id = u.team",
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
        let key_order = tables
            .iter()
            .map(|t| format!("{t}.id"))
            .collect::<Vec<_>>()
            .join(", ");
        // Exercise PK-ordered streaming, non-PK ORDER BY (SortAll / TopK),
        // and non-projected ORDER BY columns (e.g. r).
        let order = match round % 5 {
            1 => format!("b.k DESC NULLS LAST, {key_order}"),
            2 => format!("a.s ASC NULLS FIRST, {key_order}"),
            3 => format!("b.r DESC NULLS FIRST, {key_order}"),
            _ => key_order.clone(),
        };
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

#[test]
fn top_k_order_by_limit_respects_bounds_and_stability() {
    let dir = tempfile::tempdir().unwrap();
    let limits = SqlLimits {
        max_sort_rows: 10,
        ..SqlLimits::default()
    };
    let check = |db: &SqlDatabase<'_>, backend: &str| {
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER, tag TEXT)",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE u (id INTEGER PRIMARY KEY, t_id INTEGER, note TEXT)",
            &[],
        )
        .unwrap();
        let t_vals: Vec<String> = (0..50)
            .map(|i| format!("({i}, {}, 't{i}')", (i * 17) % 13))
            .collect();
        db.execute(&format!("INSERT INTO t VALUES {}", t_vals.join(", ")), &[])
            .unwrap();
        let u_vals: Vec<String> = (0..50).map(|i| format!("({i}, {i}, 'n{i}')")).collect();
        db.execute(&format!("INSERT INTO u VALUES {}", u_vals.join(", ")), &[])
            .unwrap();

        // Top-K LIMIT 5 succeeds on 50 matching rows even with max_sort_rows = 10,
        // including ordering by a non-projected column and deterministic tie-breaking.
        let top = rows(
            db,
            "SELECT t.id, u.note FROM t JOIN u ON u.id = t.id ORDER BY t.score DESC LIMIT 5 OFFSET 2",
            &[],
        );
        let mut expected: Vec<(i64, i64)> = (0..50).map(|i| ((i * 17) % 13, i)).collect();
        expected.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let expected_rows: Vec<Vec<SqlValue>> = expected[2..7]
            .iter()
            .map(|&(_, id)| vec![int(id), text(&format!("n{id}"))])
            .collect();
        assert_eq!(top, expected_rows, "{backend}");

        // Unbounded ORDER BY on 50 rows exceeds max_sort_rows = 10.
        assert!(
            matches!(
                db.execute(
                    "SELECT t.id FROM t JOIN u ON u.id = t.id ORDER BY t.score DESC",
                    &[]
                ),
                Err(SqlError::Bounds(_))
            ),
            "{backend}"
        );
        // OFFSET + LIMIT = 13 > max_sort_rows = 10 also exceeds max_sort_rows.
        assert!(
            matches!(
                db.execute(
                    "SELECT t.id FROM t JOIN u ON u.id = t.id ORDER BY t.score DESC LIMIT 5 OFFSET 8",
                    &[]
                ),
                Err(SqlError::Bounds(_))
            ),
            "{backend}"
        );
        // EXPLAIN describes the top-K sort path.
        let plan = rows(
            db,
            "EXPLAIN SELECT t.id FROM t JOIN u ON u.id = t.id ORDER BY t.score DESC LIMIT 5",
            &[],
        );
        assert!(
            matches!(&plan[2][2], SqlValue::Text(s) if s.contains("top") && s.contains("sorted")),
            "{backend}: {plan:?}"
        );
    };

    let store = Store::open(dir.path().join("ordinary")).unwrap();
    let db = SqlDatabase::with_limits(DocumentDatabase::new(&store), limits).unwrap();
    check(&db, "Store");
    drop(store);

    let log = dir.path().join("checkpoint-log");
    let initial = Store::open(&log).unwrap();
    let digest = initial
        .checkpoint_to(dir.path().join("checkpoint"))
        .unwrap();
    drop(initial);
    let checkpoint = CheckpointStore::open(&log, dir.path().join("checkpoint"), &digest).unwrap();
    let db = SqlDatabase::with_limits(DocumentDatabase::with_checkpoint_store(&checkpoint), limits)
        .unwrap();
    check(&db, "CheckpointStore");
}

#[test]
fn cached_join_plan_revalidates_params_limits_and_ddl() {
    for_both(|db, backend| {
        setup(db);
        let sql = "SELECT u.name, u.name, t.title FROM users u JOIN teams t ON t.id = u.team WHERE u.id = ?";
        let stmt = db.prepare(sql).unwrap();

        // Repeated calls exercise the cached plan, including duplicate projected columns.
        for (id, name, title) in [(10, "ada", "red"), (11, "bob", "blue")] {
            let SqlOutcome::Rows { rows: got, .. } =
                db.execute_prepared(&stmt, &[int(id)]).unwrap()
            else {
                panic!("{backend}")
            };
            assert_eq!(
                got,
                vec![vec![text(name), text(name), text(title)]],
                "{backend}"
            );
        }
        // NULL parameter matches nothing and succeeds.
        let SqlOutcome::Rows { rows: got, .. } =
            db.execute_prepared(&stmt, &[SqlValue::Null]).unwrap()
        else {
            panic!("{backend}")
        };
        assert!(got.is_empty(), "{backend}");
        // Wrong parameter type on a cached plan must still fail with SqlError::Type.
        assert!(
            matches!(
                db.execute_prepared(&stmt, &[text("not-an-int")]),
                Err(SqlError::Type(_))
            ),
            "{backend}"
        );

        // Self-join reuses catalog & metadata cleanly.
        assert_eq!(
            rows(
                db,
                "SELECT a.id, b.name FROM users a JOIN users b ON b.id = a.id WHERE a.id = ?",
                &[int(10)]
            ),
            vec![vec![int(10), text("ada")]],
            "{backend}"
        );

        // DDL (CREATE INDEX / ALTER TABLE ADD COLUMN) invalidates cached plans.
        let idx_sql = "SELECT u.name, t.title FROM users u JOIN teams t ON t.id = u.team WHERE u.team = ? ORDER BY u.id";
        assert_eq!(
            rows(db, idx_sql, &[int(1)]),
            vec![vec![text("ada"), text("red")]],
            "{backend}"
        );
        db.execute("CREATE INDEX users_team ON users (team)", &[])
            .unwrap();
        assert_eq!(
            rows(db, idx_sql, &[int(1)]),
            vec![vec![text("ada"), text("red")]],
            "{backend}"
        );
        db.execute("ALTER TABLE teams ADD COLUMN extra TEXT", &[])
            .unwrap();
        assert_eq!(
            rows(
                db,
                "SELECT u.name, t.extra FROM users u JOIN teams t ON t.id = u.team WHERE u.id = 10",
                &[]
            ),
            vec![vec![text("ada"), SqlValue::Null]],
            "{backend}"
        );
    });
}

#[test]
fn text_pk_unique_index_compound_index_and_multipage_index_joins() {
    for_both(|db, backend| {
        for sql in [
            "CREATE TABLE kv (k TEXT PRIMARY KEY, val TEXT NOT NULL)",
            "CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE, grp INTEGER NOT NULL, seq INTEGER NOT NULL, k_ref TEXT)",
            "CREATE INDEX items_grp_seq ON items (grp, seq)",
            "CREATE INDEX items_grp ON items (grp)",
            "INSERT INTO kv VALUES ('k1', 'plain'), ('k2', 'quote \" and\nline'), ('k3', 'é unicode')",
        ] {
            db.execute(sql, &[]).unwrap();
        }
        // Insert 280 items in grp=7 (> 256 index page size) plus extra rows with
        // reversed seq order to test compound index ordering vs PK ordering.
        for chunk_start in (0..280i64).step_by(70) {
            let vals: Vec<String> = (chunk_start..chunk_start + 70)
                .map(|i| {
                    let k_ref = match i % 4 {
                        0 => "'k1'",
                        1 => "'k2'",
                        2 => "'k3'",
                        _ => "'missing'",
                    };
                    format!("({i}, 'c{i}', 7, {}, {k_ref})", 1000 - i)
                })
                .collect();
            db.execute(
                &format!("INSERT INTO items VALUES {}", vals.join(", ")),
                &[],
            )
            .unwrap();
        }

        // Batched TEXT PK lookup across > 256 index rows (multi-page index scan +
        // escaped text in point lookup + LEFT JOIN for missing key).
        let got = rows(
            db,
            "SELECT i.id, kv.val FROM items i LEFT JOIN kv ON kv.k = i.k_ref WHERE i.grp = 7 ORDER BY i.id",
            &[],
        );
        assert_eq!(got.len(), 280, "{backend}");
        assert_eq!(got[0], vec![int(0), text("plain")], "{backend}");
        assert_eq!(
            got[1],
            vec![int(1), text("quote \" and\nline")],
            "{backend}"
        );
        assert_eq!(got[2], vec![int(2), text("é unicode")], "{backend}");
        assert_eq!(got[3], vec![int(3), SqlValue::Null], "{backend}");
        assert_eq!(got[279], vec![int(279), SqlValue::Null], "{backend}");

        // UNIQUE secondary index lookup on flat rows (verifies unique-owner check).
        assert_eq!(
            rows(
                db,
                "SELECT i.id, kv.val FROM items i JOIN kv ON kv.k = i.k_ref WHERE i.code = ?",
                &[text("c1")]
            ),
            vec![vec![int(1), text("quote \" and\nline")]],
            "{backend}"
        );

        // Compound index with NOT NULL trailing column: equality on `grp` returns
        // entries in `(seq, id)` order, NOT `id` order; ORDER BY i.id must still
        // return ascending `i.id`.
        db.execute("DROP INDEX items_grp ON items", &[]).unwrap();
        let ordered = rows(
            db,
            "SELECT i.id, kv.k FROM items i JOIN kv ON kv.k = i.k_ref WHERE i.grp = 7 ORDER BY i.id LIMIT 6",
            &[],
        );
        assert_eq!(
            ordered,
            vec![
                vec![int(0), text("k1")],
                vec![int(1), text("k2")],
                vec![int(2), text("k3")],
                vec![int(4), text("k1")],
                vec![int(5), text("k2")],
                vec![int(6), text("k3")],
            ],
            "{backend}"
        );

        // Oversized (> 254 bytes) TEXT PK lookup matches nothing without error.
        let long_key = "x".repeat(300);
        assert!(
            rows(
                db,
                "SELECT kv.k FROM kv JOIN items i ON i.id = 0 WHERE kv.k = ?",
                &[SqlValue::Text(long_key)]
            )
            .is_empty(),
            "{backend}"
        );
    });
}

#[test]
fn typed_hash_joins_cover_all_types_and_numeric_coercion() {
    for_both(|db, backend| {
        for sql in [
            "CREATE TABLE l (id INTEGER PRIMARY KEY, i INTEGER, r REAL, s TEXT, b BOOLEAN)",
            "CREATE TABLE r (id INTEGER PRIMARY KEY, i INTEGER, r REAL, s TEXT, b BOOLEAN)",
            "INSERT INTO l VALUES (1, 0, -0.0, 'a', true), (2, 2, 2.5, 'b', false), (3, NULL, NULL, NULL, NULL)",
            "INSERT INTO r VALUES (10, 0, 0.0, 'a', true), (11, 2, 2.0, 'a', true), (12, 3, 2.5, 'b', false), (13, NULL, NULL, NULL, NULL)",
        ] {
            db.execute(sql, &[]).unwrap();
        }
        // INTEGER build column probed by REAL (-0.0 matches 0, 2.5 matches nothing).
        assert_eq!(
            rows(
                db,
                "SELECT l.id, r.id FROM l JOIN r ON r.i = l.r ORDER BY l.id, r.id",
                &[]
            ),
            vec![vec![int(1), int(10)]],
            "{backend}"
        );
        // REAL build column probed by INTEGER (0 matches 0.0, 2 matches 2.0).
        assert_eq!(
            rows(
                db,
                "SELECT l.id, r.id FROM l JOIN r ON r.r = l.i ORDER BY l.id, r.id",
                &[]
            ),
            vec![vec![int(1), int(10)], vec![int(2), int(11)]],
            "{backend}"
        );
        // REAL build column probed by REAL (-0.0 matches 0.0, 2.5 matches 2.5).
        assert_eq!(
            rows(
                db,
                "SELECT l.id, r.id FROM l JOIN r ON r.r = l.r ORDER BY l.id, r.id",
                &[]
            ),
            vec![vec![int(1), int(10)], vec![int(2), int(12)]],
            "{backend}"
        );
        // TEXT hash join preserves build insertion order on ties.
        assert_eq!(
            rows(
                db,
                "SELECT l.id, r.id FROM l JOIN r ON r.s = l.s WHERE l.id = 1",
                &[]
            ),
            vec![vec![int(1), int(10)], vec![int(1), int(11)]],
            "{backend}"
        );
        // BOOLEAN hash join.
        assert_eq!(
            rows(
                db,
                "SELECT l.id, r.id FROM l JOIN r ON r.b = l.b ORDER BY l.id, r.id",
                &[]
            ),
            vec![
                vec![int(1), int(10)],
                vec![int(1), int(11)],
                vec![int(2), int(12)],
            ],
            "{backend}"
        );
    });
}

/// `RIGHT JOIN` as the first join returns what SQLite returns, with the
/// written column order for `*`, alone and followed by further joins.
#[test]
fn right_joins_match_sqlite() {
    for_both(|db, backend| {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE a (id INTEGER PRIMARY KEY, b_id INTEGER, label TEXT)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, b_id INTEGER)",
            "INSERT INTO a VALUES (1, 10, 'x'), (2, 10, 'y'), (3, 99, 'z'), (4, NULL, 'w')",
            "INSERT INTO b VALUES (10, 'ten'), (20, 'twenty'), (30, NULL)",
            "INSERT INTO c VALUES (100, 20), (101, 10), (102, 40)",
        ] {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        for sql in [
            "SELECT * FROM a RIGHT JOIN b ON a.b_id = b.id ORDER BY b.id, a.id",
            "SELECT a.label, b.name FROM a RIGHT OUTER JOIN b ON a.b_id = b.id ORDER BY b.id, a.id",
            "SELECT b.id, a.id FROM a RIGHT JOIN b ON a.b_id = b.id WHERE a.id IS NULL ORDER BY b.id",
            "SELECT b.id, a.id, c.id FROM a RIGHT JOIN b ON a.b_id = b.id LEFT JOIN c ON c.b_id = b.id ORDER BY b.id, a.id, c.id",
            "SELECT b.id, c.id FROM a RIGHT JOIN b ON a.b_id = b.id JOIN c ON c.b_id = b.id ORDER BY b.id, c.id",
            "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.b_id = b.id AND a.label <> 'x' ORDER BY b.id, a.id LIMIT 3",
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
        for sql in [
            "SELECT a.id FROM a JOIN b ON a.b_id = b.id RIGHT JOIN c ON c.b_id = b.id",
            "SELECT a.id FROM a CROSS JOIN b",
        ] {
            assert!(
                matches!(db.execute(sql, &[]), Err(SqlError::Unsupported { .. })),
                "{backend}: {sql}"
            );
        }
    });
}

/// Aggregates, `GROUP BY` and `HAVING` over joins return what SQLite
/// returns, on both backends.
#[test]
fn aggregates_over_joins_match_sqlite() {
    for_both(|db, backend| {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        let mut setup = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".to_owned(),
            "CREATE TABLE u (id INTEGER PRIMARY KEY, t_id INTEGER, score REAL, tag TEXT)"
                .to_owned(),
            "CREATE TABLE o (id INTEGER PRIMARY KEY, u_id INTEGER, amount INTEGER)".to_owned(),
            "CREATE INDEX o_u ON o (u_id)".to_owned(),
        ];
        for id in 0..6 {
            setup.push(format!("INSERT INTO t VALUES ({id}, 'team{}')", id % 4));
        }
        for id in 0..40 {
            let t = if id % 9 == 0 {
                "NULL".to_owned()
            } else {
                (id % 7).to_string()
            };
            let score = if id % 5 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}.5", id % 11)
            };
            let tag = ["'a'", "'b'", "NULL"][id % 3];
            setup.push(format!("INSERT INTO u VALUES ({id}, {t}, {score}, {tag})"));
        }
        for id in 0..120 {
            let amount = if id % 13 == 0 {
                "NULL".to_owned()
            } else {
                ((id * 37) % 50).to_string()
            };
            setup.push(format!(
                "INSERT INTO o VALUES ({id}, {}, {amount})",
                (id * 7) % 45
            ));
        }
        for sql in &setup {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        for sql in [
            "SELECT COUNT(*), SUM(o.amount), MIN(o.amount), MAX(o.amount) FROM u JOIN o ON o.u_id = u.id",
            "SELECT u.tag, COUNT(*) AS n, SUM(o.amount) AS total FROM u JOIN o ON o.u_id = u.id GROUP BY u.tag ORDER BY u.tag",
            "SELECT t.name, COUNT(u.id), AVG(u.score) FROM t LEFT JOIN u ON u.t_id = t.id GROUP BY t.name ORDER BY t.name",
            "SELECT t.id, COUNT(DISTINCT u.tag) AS tags FROM t JOIN u ON u.t_id = t.id GROUP BY t.id ORDER BY t.id",
            "SELECT u.t_id, SUM(o.amount) AS total FROM u JOIN o ON o.u_id = u.id GROUP BY u.t_id HAVING SUM(o.amount) > 150 ORDER BY total DESC, u.t_id",
            "SELECT u.t_id, COUNT(*) AS n FROM u JOIN o ON o.u_id = u.id WHERE o.amount IS NOT NULL GROUP BY u.t_id HAVING COUNT(*) >= 10 AND MAX(o.amount) < 49 ORDER BY n, u.t_id LIMIT 3",
            "SELECT t.name, u.tag, COUNT(o.id) AS n FROM t JOIN u ON u.t_id = t.id LEFT JOIN o ON o.u_id = u.id GROUP BY t.name, u.tag ORDER BY t.name, u.tag NULLS FIRST",
            "SELECT COUNT(*) FROM t JOIN u ON u.t_id = t.id WHERE t.id > 100",
            "SELECT u.tag, MIN(u.score) AS lo FROM t RIGHT JOIN u ON u.t_id = t.id GROUP BY u.tag ORDER BY lo DESC NULLS LAST, u.tag",
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
        for sql in [
            "SELECT u.tag, o.amount FROM u JOIN o ON o.u_id = u.id GROUP BY u.tag",
            "SELECT u.tag FROM u JOIN o ON o.u_id = u.id WHERE COUNT(*) > 1 GROUP BY u.tag",
            "SELECT SUM(u.tag) FROM u JOIN o ON o.u_id = u.id",
            "SELECT * FROM u JOIN o ON o.u_id = u.id GROUP BY u.tag",
        ] {
            assert!(db.execute(sql, &[]).is_err(), "{backend}: {sql}");
        }
        let plan = rows(
            db,
            "EXPLAIN SELECT u.tag, COUNT(*) FROM u JOIN o ON o.u_id = u.id GROUP BY u.tag",
            &[],
        );
        assert_eq!(
            plan.last().map(|row| row[0].clone()),
            Some(SqlValue::Text("aggregate".into())),
            "{backend}"
        );
    });
}

/// `FULL JOIN` as the only join returns what SQLite returns.
#[test]
fn full_joins_match_sqlite() {
    for_both(|db, backend| {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE a (id INTEGER PRIMARY KEY, k INTEGER, label TEXT)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, k INTEGER, name TEXT)",
            "INSERT INTO a VALUES (1, 10, 'x'), (2, 10, 'y'), (3, 30, 'z'), (4, NULL, 'w')",
            "INSERT INTO b VALUES (7, 10, 'ten'), (8, 20, 'twenty'), (9, NULL, 'none'), (10, 40, NULL)",
        ] {
            db.execute(sql, &[]).unwrap();
            sqlite.execute(sql, []).unwrap();
        }
        for sql in [
            "SELECT * FROM a FULL JOIN b ON a.k = b.k ORDER BY a.id, b.id",
            "SELECT a.label, b.name FROM a FULL OUTER JOIN b ON a.k = b.k ORDER BY b.name NULLS FIRST, a.label",
            "SELECT a.id, b.id FROM a FULL JOIN b ON a.k = b.k WHERE a.id IS NULL OR b.id IS NULL ORDER BY a.id, b.id",
            "SELECT a.id, b.id FROM a FULL JOIN b ON a.k = b.k AND b.name <> 'ten' ORDER BY a.id DESC, b.id",
            "SELECT a.id, b.id FROM a FULL JOIN b ON a.k = b.k ORDER BY b.id NULLS LAST, a.id LIMIT 3 OFFSET 2",
            "SELECT b.name FROM a FULL JOIN b ON a.k = b.k WHERE b.k > 15 ORDER BY b.name",
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
        for sql in [
            "SELECT a.id FROM a FULL JOIN b ON a.k = b.k JOIN b c ON c.id = b.id",
            "SELECT a.id FROM a JOIN b ON a.k = b.k FULL JOIN b c ON c.id = b.id",
            "SELECT COUNT(*) FROM a FULL JOIN b ON a.k = b.k",
        ] {
            assert!(
                matches!(db.execute(sql, &[]), Err(SqlError::Unsupported { .. })),
                "{backend}: {sql}"
            );
        }
        let plan = rows(
            db,
            "EXPLAIN SELECT a.id FROM a FULL JOIN b ON a.k = b.k",
            &[],
        );
        assert_eq!(
            plan.last().map(|r| r[0].clone()),
            Some(SqlValue::Text("full".into()))
        );
    });
}
