//! Differential test of SELECT with WHERE, ORDER BY, LIMIT and OFFSET
//! against SQLite. Both engines use NULLs-smallest ordering by default and
//! byte-order text comparison. SQLite does not promise an order for ties, so
//! its queries append the primary key, which is EMDA's documented tie-break.
use database_engine::{DocumentDatabase, SqlDatabase, SqlOutcome, SqlValue, Store};

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

fn from_sqlite(value: rusqlite::types::Value, boolean: bool) -> SqlValue {
    use rusqlite::types::Value;
    match value {
        Value::Null => SqlValue::Null,
        Value::Integer(n) if boolean => SqlValue::Boolean(n != 0),
        Value::Integer(n) => SqlValue::Integer(n),
        Value::Real(f) => SqlValue::Real(f),
        Value::Text(s) => SqlValue::Text(s),
        Value::Blob(_) => panic!("unexpected blob"),
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

#[test]
fn select_order_limit_offset_matches_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("emda")).unwrap();
    let emda = SqlDatabase::new(DocumentDatabase::new(&store));
    let lite = rusqlite::Connection::open_in_memory().unwrap();
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, r REAL, s TEXT, b BOOLEAN)";
    emda.execute(ddl, &[]).unwrap();
    lite.execute(ddl, []).unwrap();

    let mut rng = Rng(0x5eed_1234_abcd_0001);
    let texts = ["", "a", "A", "ab", "b", "é", "z", "aa"];
    let mut values = Rng(0x0000_ddba_11c0_ffee);
    let mut rng_values = std::iter::from_fn(move || Some(values.next()));
    for id in 0..300i64 {
        let mut maybe = |value: SqlValue| {
            if rng.below(5) == 0 {
                SqlValue::Null
            } else {
                value
            }
        };
        let (n, r, t, b) = (
            rng_values.next().unwrap(),
            rng_values.next().unwrap(),
            rng_values.next().unwrap(),
            rng_values.next().unwrap(),
        );
        let row = vec![
            SqlValue::Integer(id * 7 % 307 - 150),
            maybe(SqlValue::Integer(n as i64 % 9 - 4)),
            maybe(SqlValue::Real((r % 13) as f64 / 4.0 - 1.5)),
            maybe(SqlValue::Text(texts[(t % 8) as usize].into())),
            maybe(SqlValue::Boolean(b % 2 == 0)),
        ];
        let insert = "INSERT INTO t VALUES (?, ?, ?, ?, ?)";
        emda.execute(insert, &row).unwrap();
        lite.execute(
            insert,
            rusqlite::params_from_iter(row.iter().map(to_sqlite)),
        )
        .unwrap();
    }

    let columns = ["id", "n", "r", "s", "b"];
    let predicates = [
        "",
        "WHERE n > 0",
        "WHERE r <= 0.5 OR s = 'a'",
        "WHERE NOT (b = TRUE) OR n IS NULL",
        "WHERE id >= -20 AND id < 40",
        "WHERE s IS NOT NULL AND id > 100",
    ];
    let mut compared = 0;
    for _ in 0..400 {
        let keys = 1 + rng.below(3) as usize;
        let mut order = Vec::new();
        for _ in 0..keys {
            let column = columns[rng.below(5) as usize];
            let direction = ["", " ASC", " DESC"][rng.below(3) as usize];
            let nulls = ["", " NULLS FIRST", " NULLS LAST"][rng.below(3) as usize];
            order.push(format!("{column}{direction}{nulls}"));
        }
        let predicate = predicates[rng.below(predicates.len() as u64) as usize];
        let limit = match rng.below(4) {
            0 => String::new(),
            1 => format!(" LIMIT {}", rng.below(20)),
            _ => format!(" LIMIT {} OFFSET {}", rng.below(40), rng.below(320)),
        };
        let order = order.join(", ");
        let emda_sql = format!("SELECT id, n, r, s, b FROM t {predicate} ORDER BY {order}{limit}");
        let lite_sql =
            format!("SELECT id, n, r, s, b FROM t {predicate} ORDER BY {order}, id ASC{limit}");
        let SqlOutcome::Rows { rows: ours, .. } = emda.execute(&emda_sql, &[]).unwrap() else {
            panic!("expected rows for {emda_sql}");
        };
        let mut statement = lite.prepare(&lite_sql).unwrap();
        let theirs = statement
            .query_map([], |row| {
                Ok((0..5)
                    .map(|index| from_sqlite(row.get(index).unwrap(), index == 4))
                    .collect::<Vec<_>>())
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(ours, theirs, "{emda_sql}");
        compared += 1;
    }
    assert_eq!(compared, 400);
}
