//! Unit tests for SQL lexer, parser, primary key encoding, numeric comparisons,
//! randomized parser resilience, and commit-fault unknown-outcome propagation.
use super::*;
use crate::store::fault_tests::{Fault, open_versioned_fault_store};
use crate::{DocumentDatabase, Store};
use std::fs;
use std::sync::{Arc, Mutex};

#[test]
fn primary_key_encoding_preserves_total_order() {
    let ints = [
        i64::MIN,
        i64::MIN + 1,
        -1000,
        -1,
        0,
        1,
        1000,
        i64::MAX - 1,
        i64::MAX,
    ];
    let encoded: Vec<String> = ints
        .iter()
        .map(|&n| {
            value::encode_primary_key(&SqlValue::Integer(n), value::SqlType::Integer).unwrap()
        })
        .collect();
    for pair in encoded.windows(2) {
        assert!(pair[0] < pair[1], "{} should be < {}", pair[0], pair[1]);
    }

    let texts = ["", "\0", "A", "Z", "a", "aa", "b", "é"];
    let encoded_t: Vec<String> = texts
        .iter()
        .map(|&s| {
            value::encode_primary_key(
                &SqlValue::Text(s.to_owned()),
                value::SqlType::Text { max_chars: None },
            )
            .unwrap()
        })
        .collect();
    for pair in encoded_t.windows(2) {
        assert!(pair[0] < pair[1], "{} should be < {}", pair[0], pair[1]);
    }
}

#[test]
fn exact_i64_f64_conversion_and_comparison_edge_cases() {
    assert!(value::exact_i64_to_f64(0).is_ok());
    assert!(value::exact_i64_to_f64(i64::MIN).is_ok());
    assert!(value::exact_i64_to_f64(1_i64 << 53).is_ok());
    assert!(value::exact_i64_to_f64(-(1_i64 << 53)).is_ok());
    assert!(value::exact_i64_to_f64((1_i64 << 53) + 1).is_err());
    assert!(value::exact_i64_to_f64(i64::MAX).is_err());

    use std::cmp::Ordering::*;
    assert_eq!(
        value::compare_i64_f64((1_i64 << 53) + 1, (1_i64 << 53) as f64),
        Greater
    );
    assert_eq!(
        value::compare_i64_f64((1_i64 << 53) - 1, (1_i64 << 53) as f64),
        Less
    );
    assert_eq!(value::compare_i64_f64(i64::MIN, i64::MIN as f64), Equal);
    assert_eq!(
        value::compare_i64_f64(i64::MAX, 9_223_372_036_854_775_808.0),
        Less
    );
    assert_eq!(value::compare_i64_f64(i64::MIN, -1e20), Greater);
    assert_eq!(value::compare_i64_f64(-2, -1.5), Less);
    assert_eq!(value::compare_i64_f64(-1, -1.5), Greater);
}

#[test]
fn malformed_inputs_return_syntax_or_bounds_without_panicking() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));

    let bad_inputs = [
        "",
        "   -- just a comment\n  ",
        ";",
        "SELECT * FROM t;;",
        "SELECT * FROM t; SELECT * FROM t",
        "SELECT 'unterminated",
        "SELECT \"unterminated FROM t",
        "SELECT \"\" FROM t",
        "SELECT * FROM t WHERE x = 1e",
        "SELECT * FROM t WHERE x = 1e+",
        "SELECT * FROM t WHERE x = 1.2.3",
        "SELECT * FROM t WHERE x = 123abc",
        "SELECT * FROM t WHERE x = 1e999",
        "SELECT * FROM t WHERE x = 9223372036854775808",
        "SELECT * FROM t WHERE x = -9223372036854775809",
        "SELECT * FROM t WHERE x ! 1",
        "SELECT * FROM t WHERE x | 1",
        "SELECT * FROM t WHERE x @ 1",
        "SELECT * FROM t WHERE x = $1",
        "/* block comment */ SELECT * FROM t",
        "CREATE TABLE t ()",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, id TEXT)",
        "INSERT INTO t () VALUES ()",
        "INSERT INTO t VALUES ()",
        "INSERT INTO t (a, a) VALUES (1, 2)",
        "INSERT INTO t (a) VALUES (1, 2)",
        "UPDATE t SET a = 1, a = 2",
        "SELECT * FROM t WHERE a = 1 = 2",
        "SELECT * FROM t WHERE a IS 1",
        "SELECT *, a FROM t",
    ];

    for sql in bad_inputs {
        let err = db.prepare(sql).unwrap_err();
        assert!(
            matches!(err, SqlError::Syntax { .. } | SqlError::Bounds(_)),
            "expected Syntax or Bounds for {sql:?}, got {err:?}"
        );
    }
}

#[test]
fn deterministic_randomized_parser_fuzzer_never_panics() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));

    let fragments = [
        "SELECT",
        "FROM",
        "WHERE",
        "INSERT",
        "INTO",
        "VALUES",
        "UPDATE",
        "SET",
        "DELETE",
        "CREATE",
        "TABLE",
        "IF",
        "NOT",
        "EXISTS",
        "PRIMARY",
        "KEY",
        "INTEGER",
        "REAL",
        "TEXT",
        "BOOLEAN",
        "VARCHAR",
        "NULL",
        "TRUE",
        "FALSE",
        "AND",
        "OR",
        "IS",
        "*",
        ",",
        ";",
        "(",
        ")",
        "=",
        "<>",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "-",
        "?",
        "0",
        "9223372036854775808",
        "1.5",
        "'abc'",
        "'unterm",
        "\"q\"",
        "\"\"",
        "--cmt\n",
        "JOIN",
        "ORDER",
        "BY",
        "LIMIT",
        " ",
        "\n",
        "!",
        "@",
        "a",
    ];

    let mut rng = 0x5EED_CAFE_1234_5678_u64;
    let mut next_u64 = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };

    for _ in 0..2_000 {
        let count = (next_u64() % 24) as usize;
        let mut s = String::new();
        for _ in 0..count {
            let idx = (next_u64() as usize) % fragments.len();
            s.push_str(fragments[idx]);
            if next_u64() % 3 == 0 {
                s.push(' ');
            }
        }
        let _ = db.prepare(&s);
    }

    // Also mutate raw ASCII/UTF-8 bytes deterministically.
    let base = b"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);";
    for _ in 0..1_000 {
        let mut buf = base.to_vec();
        let edits = (next_u64() % 6) as usize;
        for _ in 0..edits {
            let pos = (next_u64() as usize) % buf.len();
            buf[pos] = (next_u64() & 0xff) as u8;
        }
        if let Ok(s) = std::str::from_utf8(&buf) {
            let _ = db.prepare(s);
        }
    }
}

#[test]
fn outcome_unknown_propagates_unchanged_on_commit_io_fault() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join("records.log"),
        include_bytes!("../../../tests/fixtures/v2-empty-records.log"),
    )
    .unwrap();
    fs::write(
        path.join("commit.state"),
        include_bytes!("../../../tests/fixtures/v2-empty-commit.state"),
    )
    .unwrap();

    let armed = Arc::new(Mutex::new(None));
    let store = open_versioned_fault_store(&path, Arc::clone(&armed)).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();

    *armed.lock().unwrap() = Some(Fault::SyncAfter);
    let err = db
        .execute("INSERT INTO t VALUES (1, 'hello')", &[])
        .unwrap_err();
    assert!(
        matches!(err, SqlError::OutcomeUnknown(_)),
        "expected OutcomeUnknown, got {err:?}"
    );
    assert!(err.as_io_error().is_some());
}

#[test]
fn classify_io_error_distinguishes_bounds_from_other_invalid_input() {
    use std::io;

    let schema_err = io::Error::new(
        io::ErrorKind::InvalidInput,
        "field does not match its schema type",
    );
    match exec::classify_io_error(schema_err) {
        SqlError::Io(err) => {
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(err.to_string(), "field does not match its schema type");
        }
        other => panic!("expected SqlError::Io, got {other:?}"),
    }

    let missing_err = io::Error::new(io::ErrorKind::InvalidInput, "required field is missing");
    assert!(matches!(
        exec::classify_io_error(missing_err),
        SqlError::Io(_)
    ));

    for msg in [
        "document exceeds 1 MiB",
        "snapshot key count limit exceeded",
        "atomic batch exceeds 16 MiB encoded payload",
        "transaction write count limit exceeded",
    ] {
        let kind =
            if msg.contains("1 MiB") || msg.contains("atomic batch") || msg.contains("write count")
            {
                io::ErrorKind::InvalidInput
            } else {
                io::ErrorKind::Other
            };
        let err = io::Error::new(kind, msg);
        assert!(
            matches!(exec::classify_io_error(err), SqlError::Bounds(_)),
            "expected SqlError::Bounds for {msg}"
        );
    }
}
