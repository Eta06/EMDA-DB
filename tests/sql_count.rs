//! COUNT(*) without WHERE counts row keys instead of reading rows. These
//! tests compare it with a forced full scan, inside transactions with staged
//! writes, across checkpoint base and overlay, and under concurrency.
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlLimits, SqlOutcome, SqlValue,
    Store,
};
use std::sync::Arc;

fn count(db: &SqlDatabase<'_>, sql: &str) -> i64 {
    match db.execute(sql, &[]).unwrap() {
        SqlOutcome::Rows { rows, .. } => match rows.as_slice() {
            [row] => match row[0] {
                SqlValue::Integer(n) => n,
                ref other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

fn scanned(db: &SqlDatabase<'_>, sql: &str) -> i64 {
    match db.execute_force_full_scan_for_test(sql, &[]).unwrap() {
        SqlOutcome::Rows { rows, .. } => match rows[0][0] {
            SqlValue::Integer(n) => n,
            ref other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

fn exercise(db: &SqlDatabase<'_>, backend: &str) {
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", &[])
        .unwrap();
    db.execute("CREATE TABLE other (id INTEGER PRIMARY KEY)", &[])
        .unwrap();
    db.execute("INSERT INTO other VALUES (1), (2), (3)", &[])
        .unwrap();
    assert_eq!(count(db, "SELECT COUNT(*) FROM t"), 0, "{backend}");
    for start in (0..3_000).step_by(250) {
        let rows = (start..start + 250)
            .map(|id| format!("({id}, {})", id % 7))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(&format!("INSERT INTO t VALUES {rows}"), &[])
            .unwrap();
    }
    db.execute("DELETE FROM t WHERE id >= 100 AND id < 160", &[])
        .unwrap();
    db.execute("UPDATE t SET v = 99 WHERE id < 50", &[])
        .unwrap();
    for sql in [
        "SELECT COUNT(*) FROM t",
        "SELECT COUNT(*) AS n, COUNT(*) FROM t",
        "SELECT COUNT(*) FROM other",
    ] {
        assert_eq!(count(db, sql), scanned(db, sql), "{backend}: {sql}");
    }
    assert_eq!(count(db, "SELECT COUNT(*) FROM t"), 2_940, "{backend}");
    // Staged writes in the same transaction are counted.
    db.transaction(|tx| {
        tx.execute("INSERT INTO t VALUES (5000, 1), (5001, 1)", &[])?;
        tx.execute("DELETE FROM t WHERE id = 0", &[])?;
        let SqlOutcome::Rows { rows, .. } = tx.execute("SELECT COUNT(*) FROM t", &[])? else {
            panic!()
        };
        assert_eq!(rows[0][0], SqlValue::Integer(2_941), "{backend}");
        Err::<(), _>(SqlError::Constraint("roll back".into()))
    })
    .unwrap_err();
    assert_eq!(count(db, "SELECT COUNT(*) FROM t"), 2_940, "{backend}");
    // OFFSET past the single row and LIMIT 0 return nothing.
    match db
        .execute("SELECT COUNT(*) FROM t LIMIT 1 OFFSET 1", &[])
        .unwrap()
    {
        SqlOutcome::Rows { rows, .. } => assert!(rows.is_empty()),
        other => panic!("{other:?}"),
    }
}

#[test]
fn key_count_matches_full_scan_on_both_backends() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("s")).unwrap();
    exercise(&SqlDatabase::new(DocumentDatabase::new(&store)), "Store");

    // A managed checkpoint store rolls rows into its checkpoint base, so the
    // count merges verified base blocks with the suffix overlay.
    let log = dir.path().join("managed");
    drop(Store::open(&log).unwrap());
    CheckpointStore::rebuild_managed_selection(&log, dir.path().join("base")).unwrap();
    let checkpoint = Arc::new(CheckpointStore::open_managed(&log).unwrap());
    checkpoint.enable_automatic_checkpoints().unwrap();
    exercise(
        &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&checkpoint)),
        "CheckpointStore",
    );
}

#[test]
fn key_count_respects_the_visit_bound() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("s")).unwrap();
    let limits = SqlLimits {
        max_visited_rows: 100,
        ..SqlLimits::default()
    };
    let db = SqlDatabase::with_limits(DocumentDatabase::new(&store), limits).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
        .unwrap();
    let rows = (0..100)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(&format!("INSERT INTO t VALUES {rows}"), &[])
        .unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 100);
    db.execute("INSERT INTO t VALUES (100)", &[]).unwrap();
    assert!(matches!(
        db.execute("SELECT COUNT(*) FROM t", &[]),
        Err(SqlError::Bounds(_))
    ));
}

#[test]
fn a_concurrent_insert_conflicts_with_a_counting_transaction() {
    for checkpoint in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store;
        let checkpoint_store;
        let db = if checkpoint {
            let log = dir.path().join("log");
            let initial = Store::open(&log).unwrap();
            let digest = initial.checkpoint_to(dir.path().join("cp")).unwrap();
            drop(initial);
            checkpoint_store = CheckpointStore::open(&log, dir.path().join("cp"), &digest).unwrap();
            SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&checkpoint_store))
        } else {
            store = Store::open(dir.path().join("s")).unwrap();
            SqlDatabase::new(DocumentDatabase::new(&store))
        };
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER)", &[])
            .unwrap();
        db.execute("INSERT INTO t VALUES (1), (2)", &[]).unwrap();
        // The transaction records how many rows it counted; an insert that
        // commits in between must make it conflict instead of committing a
        // stale count.
        let result = db.transaction(|tx| {
            let SqlOutcome::Rows { rows, .. } = tx.execute("SELECT COUNT(*) FROM t", &[])? else {
                panic!()
            };
            db.execute("INSERT INTO t VALUES (3)", &[]).unwrap();
            tx.execute("INSERT INTO log VALUES (1, ?)", &[rows[0][0].clone()])?;
            Ok(())
        });
        assert!(
            matches!(result, Err(SqlError::Conflict(_))),
            "checkpoint={checkpoint}: {result:?}"
        );
        assert_eq!(count(&db, "SELECT COUNT(*) FROM log"), 0);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 3);
    }
}
