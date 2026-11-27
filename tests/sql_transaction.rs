//! Integration tests for multi-statement serializable SQL transactions:
//! atomicity, rollback, poisoning, read-your-own-writes, isolation anomalies
//! (lost update, write skew, phantoms), bounds, concurrency with retry, and
//! SIGKILL crash durability across `Store` and `CheckpointStore`.

use database_engine::sql::SqlTransaction;
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlOutcome, SqlValue, Store,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackendKind {
    Ordinary,
    Checkpoint,
}

impl BackendKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Checkpoint => "checkpoint",
        }
    }

    fn from_str(raw: &str) -> Self {
        match raw {
            "ordinary" => Self::Ordinary,
            "checkpoint" => Self::Checkpoint,
            other => panic!("unknown backend: {other}"),
        }
    }
}

enum OpenBackend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl OpenBackend {
    fn open(dir: &Path, kind: BackendKind) -> Self {
        let sp = dir.join("store");
        match kind {
            BackendKind::Ordinary => Self::Ordinary(Store::open(&sp).unwrap()),
            BackendKind::Checkpoint => {
                let cp = dir.join("checkpoint");
                let digest_file = dir.join("digest");
                if !digest_file.exists() {
                    let initial = Store::open(&sp).unwrap();
                    let digest = initial.checkpoint_to(&cp).unwrap();
                    drop(initial);
                    fs::write(&digest_file, digest).unwrap();
                }
                let digest: [u8; 32] = fs::read(&digest_file).unwrap().try_into().unwrap();
                Self::Checkpoint(CheckpointStore::open(&sp, &cp, &digest).unwrap())
            }
        }
    }

    fn sql(&self) -> SqlDatabase<'_> {
        match self {
            Self::Ordinary(store) => SqlDatabase::new(DocumentDatabase::new(store)),
            Self::Checkpoint(store) => {
                SqlDatabase::new(DocumentDatabase::with_checkpoint_store(store))
            }
        }
    }
}

fn for_each_backend(mut f: impl FnMut(BackendKind, &SqlDatabase<'_>)) {
    for kind in [BackendKind::Ordinary, BackendKind::Checkpoint] {
        let dir = tempfile::tempdir().unwrap();
        let backend = OpenBackend::open(dir.path(), kind);
        let db = backend.sql();
        f(kind, &db);
    }
}

fn query_i64(db: &SqlDatabase<'_>, sql: &str, params: &[SqlValue]) -> i64 {
    match db.execute(sql, params).unwrap() {
        SqlOutcome::Rows { rows, .. } => match rows[0][0] {
            SqlValue::Integer(n) => n,
            ref other => panic!("expected INTEGER, got {other:?}"),
        },
        other => panic!("expected Rows, got {other:?}"),
    }
}

fn tx_query_i64(tx: &mut SqlTransaction<'_>, sql: &str, params: &[SqlValue]) -> i64 {
    match tx.execute(sql, params).unwrap() {
        SqlOutcome::Rows { rows, .. } => match rows[0][0] {
            SqlValue::Integer(n) => n,
            ref other => panic!("expected INTEGER, got {other:?}"),
        },
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[test]
fn textual_begin_commit_rollback_point_to_closure_api() {
    let dir = tempfile::tempdir().unwrap();
    let backend = OpenBackend::open(dir.path(), BackendKind::Ordinary);
    let db = backend.sql();

    for sql in [
        "BEGIN",
        "BEGIN TRANSACTION",
        "START TRANSACTION",
        "COMMIT",
        "END",
        "ROLLBACK",
    ] {
        let err = db.execute(sql, &[]).unwrap_err();
        match err {
            SqlError::Unsupported { feature, .. } => {
                assert!(
                    feature.contains("SqlDatabase::transaction"),
                    "expected pointer to closure API in {feature:?}"
                );
            }
            other => panic!("expected Unsupported for {sql:?}, got {other:?}"),
        }
    }
}

#[test]
fn commit_rollback_and_panic_atomicity_across_backends() {
    for_each_backend(|kind, db| {
        // 1. Multi-statement commit including CREATE TABLE, prepared INSERT, UPDATE, DELETE.
        let created = db
            .transaction(|tx| {
                tx.execute(
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, tag TEXT UNIQUE, qty INTEGER NOT NULL)",
                    &[],
                )?;
                tx.execute(
                    "INSERT INTO items VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
                    &[],
                )?;
                tx.execute("UPDATE items SET qty = 25 WHERE id = 2", &[])?;
                tx.execute("DELETE FROM items WHERE id = 3", &[])?;
                Ok(tx_query_i64(tx, "SELECT SUM(qty) FROM items", &[]))
            })
            .unwrap();
        assert_eq!(created, 35, "backend={kind:?}");
        assert_eq!(query_i64(db, "SELECT SUM(qty) FROM items", &[]), 35);

        let prepared_ins = db.prepare("INSERT INTO items VALUES (?, ?, ?)").unwrap();
        let prepared_sel = db.prepare("SELECT qty FROM items WHERE id = ?").unwrap();
        db.transaction(|tx| {
            tx.execute_prepared(
                &prepared_ins,
                &[
                    SqlValue::Integer(4),
                    SqlValue::Text("d".into()),
                    SqlValue::Integer(5),
                ],
            )?;
            let outcome = tx.execute_prepared(&prepared_sel, &[SqlValue::Integer(4)])?;
            assert_eq!(
                outcome,
                SqlOutcome::Rows {
                    columns: vec!["qty".into()],
                    rows: vec![vec![SqlValue::Integer(5)]],
                }
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(query_i64(db, "SELECT SUM(qty) FROM items", &[]), 40);

        // 2. Returning Err rolls back all staged writes and staged CREATE TABLE.
        let err = db
            .transaction(|tx| {
                tx.execute("CREATE TABLE temp_tbl (id INTEGER PRIMARY KEY)", &[])?;
                tx.execute("INSERT INTO temp_tbl VALUES (1)", &[])?;
                tx.execute("INSERT INTO items VALUES (5, 'e', 100)", &[])?;
                tx.execute("UPDATE items SET qty = 999 WHERE id = 1", &[])?;
                Err::<(), SqlError>(SqlError::Constraint("caller abort".into()))
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::Constraint(_)));
        assert_eq!(query_i64(db, "SELECT SUM(qty) FROM items", &[]), 40);
        assert!(matches!(
            db.execute("SELECT * FROM temp_tbl", &[]).unwrap_err(),
            SqlError::NoSuchTable(_)
        ));
        // Recreating the rolled-back table with a different schema must succeed cleanly.
        db.execute("CREATE TABLE temp_tbl (id TEXT PRIMARY KEY, val REAL)", &[])
            .unwrap();

        // 3. Panicking inside the closure unwinds and rolls back all staged writes.
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            let _ = db.transaction(|tx| -> Result<(), SqlError> {
                tx.execute("INSERT INTO items VALUES (6, 'f', 500)", &[])?;
                tx.execute("UPDATE items SET qty = 0 WHERE id = 1", &[])?;
                panic!("boom inside transaction");
            });
        }));
        assert!(panicked.is_err());
        assert_eq!(query_i64(db, "SELECT SUM(qty) FROM items", &[]), 40);
        assert_eq!(query_i64(db, "SELECT COUNT(*) FROM items", &[]), 3);
    });
}

#[test]
fn read_your_own_writes_index_join_and_aggregate_across_backends() {
    for_each_backend(|kind, db| {
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, dept TEXT NOT NULL, score INTEGER NOT NULL)",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX idx_dept ON users (dept)", &[])
            .unwrap();
        db.execute(
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL)",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX idx_task_user ON tasks (user_id)", &[])
            .unwrap();

        db.execute(
            "INSERT INTO users VALUES (1, 'ops', 10), (2, 'eng', 20), (3, 'eng', 30)",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO tasks VALUES (100, 1, 'deploy'), (101, 2, 'build')",
            &[],
        )
        .unwrap();

        db.transaction(|tx| {
            // UPDATE indexed column then SELECT with index.
            tx.execute("UPDATE users SET dept = 'ml', score = 99 WHERE id = 2", &[])?;
            let old_dept = tx.execute("SELECT id FROM users WHERE dept = 'eng'", &[])?;
            assert_eq!(
                old_dept,
                SqlOutcome::Rows {
                    columns: vec!["id".into()],
                    rows: vec![vec![SqlValue::Integer(3)]],
                },
                "backend={kind:?}"
            );
            let new_dept = tx.execute("SELECT id, score FROM users WHERE dept = 'ml'", &[])?;
            assert_eq!(
                new_dept,
                SqlOutcome::Rows {
                    columns: vec!["id".into(), "score".into()],
                    rows: vec![vec![SqlValue::Integer(2), SqlValue::Integer(99)]],
                }
            );

            // INSERT then JOIN (both index-lookup and hash/loop paths see staged writes).
            tx.execute("INSERT INTO users VALUES (4, 'ml', 40)", &[])?;
            tx.execute("INSERT INTO tasks VALUES (102, 4, 'train')", &[])?;
            let joined = tx.execute(
                "SELECT u.id, t.title FROM users u JOIN tasks t ON t.user_id = u.id WHERE u.dept = 'ml' ORDER BY u.id",
                &[],
            )?;
            assert_eq!(
                joined,
                SqlOutcome::Rows {
                    columns: vec!["id".into(), "title".into()],
                    rows: vec![
                        vec![SqlValue::Integer(2), SqlValue::Text("build".into())],
                        vec![SqlValue::Integer(4), SqlValue::Text("train".into())],
                    ],
                }
            );

            // DELETE then aggregate (COUNT, SUM, GROUP BY).
            tx.execute("DELETE FROM users WHERE id = 1", &[])?;
            let agg = tx.execute(
                "SELECT dept, COUNT(*), SUM(score) FROM users GROUP BY dept ORDER BY dept",
                &[],
            )?;
            assert_eq!(
                agg,
                SqlOutcome::Rows {
                    columns: vec!["dept".into(), "count(*)".into(), "sum(score)".into()],
                    rows: vec![
                        vec![
                            SqlValue::Text("eng".into()),
                            SqlValue::Integer(1),
                            SqlValue::Integer(30),
                        ],
                        vec![
                            SqlValue::Text("ml".into()),
                            SqlValue::Integer(2),
                            SqlValue::Integer(139),
                        ],
                    ],
                }
            );
            Ok(())
        })
        .unwrap();

        assert_eq!(query_i64(db, "SELECT COUNT(*) FROM users", &[]), 3);
        assert_eq!(query_i64(db, "SELECT SUM(score) FROM users", &[]), 169);
    });
}

#[test]
fn error_poisoning_and_online_ddl_rejection() {
    for_each_backend(|_, db| {
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, s VARCHAR(4))",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10, 'ok')", &[])
            .unwrap();

        // 1. Constraint error propagated via ? aborts earlier staged writes.
        let err = db
            .transaction(|tx| {
                tx.execute("INSERT INTO t VALUES (2, 20, 'two')", &[])?;
                tx.execute("INSERT INTO t VALUES (1, 99, 'dup')", &[])?;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::Constraint(_)));
        assert_eq!(query_i64(db, "SELECT COUNT(*) FROM t", &[]), 1);

        // 2. Type error caught and ignored inside closure poisons transaction so commit fails.
        let err = db
            .transaction(|tx| {
                tx.execute("INSERT INTO t VALUES (2, 20, 'two')", &[])?;
                let type_err = tx
                    .execute("INSERT INTO t VALUES (3, 'not-an-int', 'bad')", &[])
                    .unwrap_err();
                assert!(matches!(type_err, SqlError::Type(_)));

                // Subsequent execute on poisoned tx fails immediately with TransactionAborted.
                let follow_up = tx
                    .execute("INSERT INTO t VALUES (4, 40, 'four')", &[])
                    .unwrap_err();
                assert!(matches!(follow_up, SqlError::TransactionAborted(_)));

                // Even returning Ok(()) must refuse to commit.
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::TransactionAborted(_)));
        assert_eq!(query_i64(db, "SELECT COUNT(*) FROM t", &[]), 1);

        // 3. Constraint error caught and ignored inside closure also poisons transaction.
        let err = db
            .transaction(|tx| {
                tx.execute("UPDATE t SET n = 999 WHERE id = 1", &[])?;
                let c_err = tx
                    .execute("INSERT INTO t VALUES (5, 50, 'toolong')", &[])
                    .unwrap_err();
                assert!(matches!(c_err, SqlError::Constraint(_)));
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::TransactionAborted(_)));
        assert_eq!(query_i64(db, "SELECT n FROM t WHERE id = 1", &[]), 10);

        // 4. Online index DDL is rejected inside an explicit transaction with Unsupported.
        let err = db
            .transaction(|tx| {
                tx.execute("INSERT INTO t VALUES (2, 20, 'two')", &[])?;
                tx.execute("CREATE INDEX idx_n ON t (n)", &[])?;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
        assert_eq!(query_i64(db, "SELECT COUNT(*) FROM t", &[]), 1);

        db.execute("CREATE INDEX idx_n ON t (n)", &[]).unwrap();
        let err = db
            .transaction(|tx| {
                tx.execute("DROP INDEX idx_n ON t", &[])?;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    });
}

#[test]
fn isolation_prevents_lost_update_write_skew_and_phantoms() {
    for kind in [BackendKind::Ordinary, BackendKind::Checkpoint] {
        // (a) Lost update
        {
            let dir = tempfile::tempdir().unwrap();
            let backend = OpenBackend::open(dir.path(), kind);
            let db = backend.sql();
            db.execute(
                "CREATE TABLE acc (id INTEGER PRIMARY KEY, bal INTEGER NOT NULL)",
                &[],
            )
            .unwrap();
            db.execute("INSERT INTO acc VALUES (1, 100)", &[]).unwrap();

            let barrier = Arc::new(Barrier::new(2));
            let res1;
            let res2;
            (res1, res2) = thread::scope(|s| {
                let h1 = s.spawn(|| {
                    db.transaction(|tx| {
                        let bal = tx_query_i64(tx, "SELECT bal FROM acc WHERE id = 1", &[]);
                        barrier.wait();
                        tx.execute(
                            "UPDATE acc SET bal = ? WHERE id = 1",
                            &[SqlValue::Integer(bal + 25)],
                        )?;
                        Ok(())
                    })
                });
                let h2 = s.spawn(|| {
                    db.transaction(|tx| {
                        let bal = tx_query_i64(tx, "SELECT bal FROM acc WHERE id = 1", &[]);
                        barrier.wait();
                        tx.execute(
                            "UPDATE acc SET bal = ? WHERE id = 1",
                            &[SqlValue::Integer(bal - 40)],
                        )?;
                        Ok(())
                    })
                });
                (h1.join().unwrap(), h2.join().unwrap())
            });
            match (&res1, &res2) {
                (Ok(()), Err(SqlError::Conflict(_))) => {
                    assert_eq!(query_i64(&db, "SELECT bal FROM acc WHERE id = 1", &[]), 125);
                }
                (Err(SqlError::Conflict(_)), Ok(())) => {
                    assert_eq!(query_i64(&db, "SELECT bal FROM acc WHERE id = 1", &[]), 60);
                }
                other => panic!("unexpected lost-update outcome on {kind:?}: {other:?}"),
            }
        }

        // (b) Write skew (classic two-doctor on-call example), tested both unindexed and indexed.
        for with_index in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let backend = OpenBackend::open(dir.path(), kind);
            let db = backend.sql();
            db.execute(
                "CREATE TABLE doctors (id INTEGER PRIMARY KEY, on_call BOOLEAN NOT NULL)",
                &[],
            )
            .unwrap();
            if with_index {
                db.execute("CREATE INDEX idx_on_call ON doctors (on_call)", &[])
                    .unwrap();
            }
            db.execute("INSERT INTO doctors VALUES (1, TRUE), (2, TRUE)", &[])
                .unwrap();

            let barrier = Arc::new(Barrier::new(2));
            let (r1, r2) = thread::scope(|s| {
                let h1 = s.spawn(|| {
                    db.transaction(|tx| {
                        let cnt = tx_query_i64(
                            tx,
                            "SELECT COUNT(*) FROM doctors WHERE on_call = TRUE",
                            &[],
                        );
                        barrier.wait();
                        if cnt >= 2 {
                            tx.execute("UPDATE doctors SET on_call = FALSE WHERE id = 1", &[])?;
                        }
                        Ok(())
                    })
                });
                let h2 = s.spawn(|| {
                    db.transaction(|tx| {
                        let cnt = tx_query_i64(
                            tx,
                            "SELECT COUNT(*) FROM doctors WHERE on_call = TRUE",
                            &[],
                        );
                        barrier.wait();
                        if cnt >= 2 {
                            tx.execute("UPDATE doctors SET on_call = FALSE WHERE id = 2", &[])?;
                        }
                        Ok(())
                    })
                });
                (h1.join().unwrap(), h2.join().unwrap())
            });
            let ok_count = usize::from(r1.is_ok()) + usize::from(r2.is_ok());
            let conflict_count = usize::from(matches!(r1, Err(SqlError::Conflict(_))))
                + usize::from(matches!(r2, Err(SqlError::Conflict(_))));
            assert_eq!(
                (ok_count, conflict_count),
                (1, 1),
                "write skew must conflict on {kind:?} (with_index={with_index}): r1={r1:?} r2={r2:?}"
            );
            assert_eq!(
                query_i64(
                    &db,
                    "SELECT COUNT(*) FROM doctors WHERE on_call = TRUE",
                    &[]
                ),
                1
            );
        }

        // (c) Phantom insert into a scanned range (PK range and secondary-index range).
        {
            let dir = tempfile::tempdir().unwrap();
            let backend = OpenBackend::open(dir.path(), kind);
            let db = backend.sql();
            db.execute(
                "CREATE TABLE ledger (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, amount INTEGER NOT NULL)",
                &[],
            )
            .unwrap();
            db.execute("CREATE INDEX idx_bucket ON ledger (bucket)", &[])
                .unwrap();
            db.execute(
                "CREATE TABLE summary (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)",
                &[],
            )
            .unwrap();
            db.execute("INSERT INTO ledger VALUES (10, 5, 100), (20, 5, 200)", &[])
                .unwrap();

            // Tx1 scans PK range [10, 20]; Tx2 inserts phantom id=15 before Tx1 commits.
            let scanned = Arc::new(Barrier::new(2));
            let inserted = Arc::new(Barrier::new(2));
            let (r_scan, r_ins) = thread::scope(|s| {
                let h1 = s.spawn(|| {
                    db.transaction(|tx| {
                        let sum = tx_query_i64(
                            tx,
                            "SELECT SUM(amount) FROM ledger WHERE id >= 10 AND id <= 20",
                            &[],
                        );
                        scanned.wait();
                        inserted.wait();
                        tx.execute(
                            "INSERT INTO summary VALUES (1, ?)",
                            &[SqlValue::Integer(sum)],
                        )?;
                        Ok(())
                    })
                });
                let h2 = s.spawn(|| {
                    scanned.wait();
                    let res = db.execute("INSERT INTO ledger VALUES (15, 5, 50)", &[]);
                    inserted.wait();
                    res
                });
                (h1.join().unwrap(), h2.join().unwrap())
            });
            r_ins.unwrap();
            assert!(
                matches!(r_scan, Err(SqlError::Conflict(_))),
                "phantom PK range insert must conflict on {kind:?}, got {r_scan:?}"
            );
            assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM summary", &[]), 0);

            // Also test phantom insert into a secondary index equality/range scan.
            let scanned2 = Arc::new(Barrier::new(2));
            let inserted2 = Arc::new(Barrier::new(2));
            let (r_idx_scan, r_idx_ins) = thread::scope(|s| {
                let h1 = s.spawn(|| {
                    db.transaction(|tx| {
                        let sum = tx_query_i64(
                            tx,
                            "SELECT SUM(amount) FROM ledger WHERE bucket = 5",
                            &[],
                        );
                        scanned2.wait();
                        inserted2.wait();
                        tx.execute(
                            "INSERT INTO summary VALUES (2, ?)",
                            &[SqlValue::Integer(sum)],
                        )?;
                        Ok(())
                    })
                });
                let h2 = s.spawn(|| {
                    scanned2.wait();
                    let res = db.execute("INSERT INTO ledger VALUES (17, 5, 75)", &[]);
                    inserted2.wait();
                    res
                });
                (h1.join().unwrap(), h2.join().unwrap())
            });
            r_idx_ins.unwrap();
            assert!(
                matches!(r_idx_scan, Err(SqlError::Conflict(_))),
                "phantom index scan insert must conflict on {kind:?}, got {r_idx_scan:?}"
            );
            assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM summary", &[]), 0);
        }

        // (d) Disjoint writes in concurrent transactions commit with zero conflicts.
        {
            let dir = tempfile::tempdir().unwrap();
            let backend = OpenBackend::open(dir.path(), kind);
            let db = backend.sql();
            db.execute(
                "CREATE TABLE kv (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
                &[],
            )
            .unwrap();
            db.execute("INSERT INTO kv VALUES (1, 10), (2, 20)", &[])
                .unwrap();

            let barrier = Arc::new(Barrier::new(2));
            let (r1, r2) = thread::scope(|s| {
                let h1 = s.spawn(|| {
                    db.transaction(|tx| {
                        let v = tx_query_i64(tx, "SELECT v FROM kv WHERE id = 1", &[]);
                        barrier.wait();
                        tx.execute(
                            "UPDATE kv SET v = ? WHERE id = 1",
                            &[SqlValue::Integer(v + 1)],
                        )?;
                        Ok(())
                    })
                });
                let h2 = s.spawn(|| {
                    db.transaction(|tx| {
                        let v = tx_query_i64(tx, "SELECT v FROM kv WHERE id = 2", &[]);
                        barrier.wait();
                        tx.execute(
                            "UPDATE kv SET v = ? WHERE id = 2",
                            &[SqlValue::Integer(v + 1)],
                        )?;
                        Ok(())
                    })
                });
                (h1.join().unwrap(), h2.join().unwrap())
            });
            r1.unwrap();
            r2.unwrap();
            assert_eq!(query_i64(&db, "SELECT SUM(v) FROM kv", &[]), 32);
        }
    }
}

#[test]
fn transaction_bounds_and_retry_validation() {
    for_each_backend(|kind, db| {
        db.execute(
            "CREATE TABLE b (id INTEGER PRIMARY KEY, txt TEXT NOT NULL)",
            &[],
        )
        .unwrap();

        // 1. Staged-write count limit (1,024 keys across multiple statements) surfaces as Bounds and rolls back.
        let err = db
            .transaction(|tx| {
                for batch in 0..2 {
                    let mut sql = String::from("INSERT INTO b VALUES ");
                    let mut params = Vec::with_capacity(1200);
                    for i in 0..600 {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str("(?, ?)");
                        let id = batch * 600 + i;
                        params.push(SqlValue::Integer(id));
                        params.push(SqlValue::Text("x".into()));
                    }
                    tx.execute(&sql, &params)?;
                }
                Ok(())
            })
            .unwrap_err();
        assert!(
            matches!(err, SqlError::Bounds(_)),
            "expected Bounds on >1024 staged keys on {kind:?}, got {err:?}"
        );
        assert_eq!(query_i64(db, "SELECT COUNT(*) FROM b", &[]), 0);

        // 2. Staged-write byte limit (16 MiB across multiple statements) surfaces as Bounds and rolls back.
        let payload = "a".repeat(128 * 1024);
        let err = db
            .transaction(|tx| {
                for id in 0..140_i64 {
                    tx.execute(
                        "INSERT INTO b VALUES (?, ?)",
                        &[SqlValue::Integer(id), SqlValue::Text(payload.clone())],
                    )?;
                }
                Ok(())
            })
            .unwrap_err();
        assert!(
            matches!(err, SqlError::Bounds(_)),
            "expected Bounds on >16 MiB staged bytes on {kind:?}, got {err:?}"
        );
        assert_eq!(query_i64(db, "SELECT COUNT(*) FROM b", &[]), 0);

        // 3. Read-set dependency limit (2,048 distinct point reads across statements) surfaces as Bounds.
        let sel = db.prepare("SELECT txt FROM b WHERE id = ?").unwrap();
        let err = db
            .transaction(|tx| {
                for id in 0..2_100_i64 {
                    tx.execute_prepared(&sel, &[SqlValue::Integer(id)])?;
                }
                Ok(())
            })
            .unwrap_err();
        assert!(
            matches!(err, SqlError::Bounds(_)),
            "expected Bounds on >2048 read-set points on {kind:?}, got {err:?}"
        );

        // 4. transaction_with_retry(0, ...) rejects 0 attempts with Bounds.
        let err = db.transaction_with_retry(0, |_| Ok(())).unwrap_err();
        assert!(matches!(err, SqlError::Bounds(_)));
    });
}

#[test]
fn concurrent_bank_transfer_invariant_with_retry() {
    const ACCOUNTS: i64 = 8;
    const INITIAL_BAL: i64 = 1_000;
    const THREADS: usize = 4;
    const TRANSFERS_PER_THREAD: usize = 30;

    for kind in [BackendKind::Ordinary, BackendKind::Checkpoint] {
        let dir = tempfile::tempdir().unwrap();
        let backend = OpenBackend::open(dir.path(), kind);
        let db = backend.sql();

        db.execute(
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, bal INTEGER NOT NULL)",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE ledger (seq INTEGER PRIMARY KEY, src INTEGER NOT NULL, dst INTEGER NOT NULL, amt INTEGER NOT NULL)",
            &[],
        )
        .unwrap();
        for id in 0..ACCOUNTS {
            db.execute(
                "INSERT INTO accounts VALUES (?, ?)",
                &[SqlValue::Integer(id), SqlValue::Integer(INITIAL_BAL)],
            )
            .unwrap();
        }

        let sel_acc = db.prepare("SELECT bal FROM accounts WHERE id = ?").unwrap();
        let upd_acc = db
            .prepare("UPDATE accounts SET bal = ? WHERE id = ?")
            .unwrap();
        let ins_led = db
            .prepare("INSERT INTO ledger VALUES (?, ?, ?, ?)")
            .unwrap();

        thread::scope(|s| {
            for tid in 0..THREADS {
                let sel_acc = &sel_acc;
                let upd_acc = &upd_acc;
                let ins_led = &ins_led;
                let db = &db;
                s.spawn(move || {
                    for iter in 0..TRANSFERS_PER_THREAD {
                        let seq = (tid * TRANSFERS_PER_THREAD + iter) as i64;
                        let src = (seq * 3 + tid as i64) % ACCOUNTS;
                        let dst = (src + 1 + (iter as i64 % (ACCOUNTS - 1))) % ACCOUNTS;
                        let amt = 1 + (seq % 17);

                        db.transaction_with_retry(128, |tx| {
                            let src_bal =
                                match tx.execute_prepared(sel_acc, &[SqlValue::Integer(src)])? {
                                    SqlOutcome::Rows { rows, .. } => match rows[0][0] {
                                        SqlValue::Integer(n) => n,
                                        _ => unreachable!(),
                                    },
                                    _ => unreachable!(),
                                };
                            let dst_bal =
                                match tx.execute_prepared(sel_acc, &[SqlValue::Integer(dst)])? {
                                    SqlOutcome::Rows { rows, .. } => match rows[0][0] {
                                        SqlValue::Integer(n) => n,
                                        _ => unreachable!(),
                                    },
                                    _ => unreachable!(),
                                };
                            if src_bal >= amt {
                                tx.execute_prepared(
                                    upd_acc,
                                    &[SqlValue::Integer(src_bal - amt), SqlValue::Integer(src)],
                                )?;
                                tx.execute_prepared(
                                    upd_acc,
                                    &[SqlValue::Integer(dst_bal + amt), SqlValue::Integer(dst)],
                                )?;
                            }
                            tx.execute_prepared(
                                ins_led,
                                &[
                                    SqlValue::Integer(seq),
                                    SqlValue::Integer(src),
                                    SqlValue::Integer(dst),
                                    SqlValue::Integer(amt),
                                ],
                            )?;
                            Ok(())
                        })
                        .unwrap();
                    }
                });
            }
        });

        let expected_total = ACCOUNTS * INITIAL_BAL;
        assert_eq!(
            query_i64(&db, "SELECT SUM(bal) FROM accounts", &[]),
            expected_total,
            "balance invariant broken on {kind:?}"
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM ledger", &[]),
            (THREADS * TRANSFERS_PER_THREAD) as i64
        );
    }
}

// ---------------------------------------------------------------------------
// Process-kill (SIGKILL) crash drill for multi-statement transactions
// ---------------------------------------------------------------------------

const ENV_ROLE: &str = "EMDA_SQL_TX_CRASH_ROLE";
const ENV_DIR: &str = "EMDA_SQL_TX_CRASH_DIR";
const ENV_BACKEND: &str = "EMDA_SQL_TX_CRASH_BACKEND";
const ENV_SEED: &str = "EMDA_SQL_TX_CRASH_SEED";
const ENV_START_SEQ: &str = "EMDA_SQL_TX_CRASH_START_SEQ";

const CRASH_ACCOUNTS: i64 = 16;
const CRASH_INIT_BAL: i64 = 10_000;
const CRASH_KILLS_PER_BACKEND: usize = 8;
const ORDINARY_CRASH_SEED: u64 = 0x7A11_5EED_0001_0001;
const CHECKPOINT_CRASH_SEED: u64 = 0x7A11_5EED_0002_0002;

#[derive(Clone, PartialEq, Eq, Debug)]
struct CrashModel {
    accounts: BTreeMap<i64, i64>,
    ledger: BTreeMap<i64, (i64, i64, i64, String)>,
}

impl CrashModel {
    fn new() -> Self {
        let mut accounts = BTreeMap::new();
        for id in 0..CRASH_ACCOUNTS {
            accounts.insert(id, CRASH_INIT_BAL);
        }
        Self {
            accounts,
            ledger: BTreeMap::new(),
        }
    }

    fn apply(&mut self, seed: u64, seq: u64) {
        let r = mix(seed, seq);
        let src = (r % (CRASH_ACCOUNTS as u64)) as i64;
        let dst = (src + 1 + ((r >> 8) % ((CRASH_ACCOUNTS - 1) as u64)) as i64) % CRASH_ACCOUNTS;
        let amt = 1 + ((r >> 16) % 50) as i64;
        let tag = format!("tx-{seq}-{:08x}", r as u32);
        *self.accounts.get_mut(&src).unwrap() -= amt;
        *self.accounts.get_mut(&dst).unwrap() += amt;
        self.ledger.insert(seq as i64, (src, dst, amt, tag));
        if seq > 2 && seq % 4 == 0 {
            self.ledger.remove(&((seq - 2) as i64));
        }
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn mix(seed: u64, seq: u64) -> u64 {
    splitmix64(seed ^ splitmix64(seq))
}

fn retry_lock<T>(mut open: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match open() {
            Ok(v) => return Ok(v),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Opens a store after a kill. A kill during an append can leave an
/// incomplete, unacknowledged final record, which open refuses; the drill
/// then applies the explicit repair and opens again.
fn open_repairing<T>(sp: &Path, mut open: impl FnMut() -> io::Result<T>) -> T {
    match retry_lock(&mut open) {
        Ok(v) => v,
        Err(error) if error.to_string().contains("incomplete log record") => {
            let removed = retry_lock(|| Store::discard_incomplete_tail(sp)).unwrap();
            assert!(
                removed.is_some(),
                "an incomplete record was reported but not found"
            );
            eprintln!("[sql_transaction] removed a torn final record of {removed:?} bytes");
            retry_lock(open).unwrap()
        }
        Err(error) => panic!("open after kill failed: {error}"),
    }
}

fn open_crash_backend(dir: &Path, kind: BackendKind) -> OpenBackend {
    let sp = dir.join("store");
    match kind {
        BackendKind::Ordinary => OpenBackend::Ordinary(open_repairing(&sp, || Store::open(&sp))),
        BackendKind::Checkpoint => {
            let cp = dir.join("checkpoint");
            let digest: [u8; 32] = fs::read(dir.join("digest")).unwrap().try_into().unwrap();
            OpenBackend::Checkpoint(open_repairing(&sp, || {
                CheckpointStore::open(&sp, &cp, &digest)
            }))
        }
    }
}

fn run_crash_tx(db: &SqlDatabase<'_>, seed: u64, seq: u64) {
    let r = mix(seed, seq);
    let src = (r % (CRASH_ACCOUNTS as u64)) as i64;
    let dst = (src + 1 + ((r >> 8) % ((CRASH_ACCOUNTS - 1) as u64)) as i64) % CRASH_ACCOUNTS;
    let amt = 1 + ((r >> 16) % 50) as i64;
    let tag = format!("tx-{seq}-{:08x}", r as u32);

    db.transaction(|tx| {
        let src_bal = tx_query_i64(
            tx,
            "SELECT bal FROM accounts WHERE id = ?",
            &[SqlValue::Integer(src)],
        );
        let dst_bal = tx_query_i64(
            tx,
            "SELECT bal FROM accounts WHERE id = ?",
            &[SqlValue::Integer(dst)],
        );
        tx.execute(
            "UPDATE accounts SET bal = ? WHERE id = ?",
            &[SqlValue::Integer(src_bal - amt), SqlValue::Integer(src)],
        )?;
        tx.execute(
            "UPDATE accounts SET bal = ? WHERE id = ?",
            &[SqlValue::Integer(dst_bal + amt), SqlValue::Integer(dst)],
        )?;
        tx.execute(
            "INSERT INTO ledger (seq, src, dst, amt, tag) VALUES (?, ?, ?, ?, ?)",
            &[
                SqlValue::Integer(seq as i64),
                SqlValue::Integer(src),
                SqlValue::Integer(dst),
                SqlValue::Integer(amt),
                SqlValue::Text(tag.clone()),
            ],
        )?;
        if seq > 2 && seq % 4 == 0 {
            tx.execute(
                "DELETE FROM ledger WHERE seq = ?",
                &[SqlValue::Integer((seq - 2) as i64)],
            )?;
        }
        let observed_total = tx_query_i64(tx, "SELECT SUM(bal) FROM accounts", &[]);
        assert_eq!(observed_total, CRASH_ACCOUNTS * CRASH_INIT_BAL);
        Ok(())
    })
    .unwrap_or_else(|err| panic!("crash worker tx seq={seq} failed: {err}"));
}

#[test]
fn sql_tx_crash_worker() {
    if std::env::var(ENV_ROLE).ok().as_deref() != Some("worker") {
        return;
    }
    let dir = PathBuf::from(std::env::var(ENV_DIR).expect("missing dir"));
    let backend = BackendKind::from_str(&std::env::var(ENV_BACKEND).expect("missing backend"));
    let seed: u64 = std::env::var(ENV_SEED).unwrap().parse().unwrap();
    let start_seq: u64 = std::env::var(ENV_START_SEQ).unwrap().parse().unwrap();

    let opened = open_crash_backend(&dir, backend);
    let db = opened.sql();
    let stdout = io::stdout();

    for seq in start_seq.. {
        run_crash_tx(&db, seed, seq);
        let line = format!("ACK {seq}\n");
        let mut out = stdout.lock();
        if out.write_all(line.as_bytes()).is_err() || out.flush().is_err() {
            break;
        }
    }
}

fn observe_crash_state(db: &SqlDatabase<'_>) -> CrashModel {
    let mut accounts = BTreeMap::new();
    match db.execute("SELECT id, bal FROM accounts", &[]).unwrap() {
        SqlOutcome::Rows { rows, .. } => {
            for r in rows {
                let (SqlValue::Integer(id), SqlValue::Integer(bal)) = (&r[0], &r[1]) else {
                    panic!("unexpected account row: {r:?}");
                };
                accounts.insert(*id, *bal);
            }
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
    let mut ledger = BTreeMap::new();
    match db
        .execute("SELECT seq, src, dst, amt, tag FROM ledger", &[])
        .unwrap()
    {
        SqlOutcome::Rows { rows, .. } => {
            for r in rows {
                let (
                    SqlValue::Integer(seq),
                    SqlValue::Integer(src),
                    SqlValue::Integer(dst),
                    SqlValue::Integer(amt),
                    SqlValue::Text(tag),
                ) = (&r[0], &r[1], &r[2], &r[3], &r[4])
                else {
                    panic!("unexpected ledger row: {r:?}");
                };
                ledger.insert(*seq, (*src, *dst, *amt, tag.clone()));
            }
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
    CrashModel { accounts, ledger }
}

fn run_tx_crash_drill(kind: BackendKind, seed: u64) {
    let dir = tempfile::tempdir().unwrap();
    {
        let backend = OpenBackend::open(dir.path(), kind);
        let db = backend.sql();
        db.transaction(|tx| {
            tx.execute(
                "CREATE TABLE accounts (id INTEGER PRIMARY KEY, bal INTEGER NOT NULL)",
                &[],
            )?;
            tx.execute(
                "CREATE TABLE ledger (seq INTEGER PRIMARY KEY, src INTEGER NOT NULL, dst INTEGER NOT NULL, amt INTEGER NOT NULL, tag TEXT NOT NULL)",
                &[],
            )?;
            for id in 0..CRASH_ACCOUNTS {
                tx.execute(
                    "INSERT INTO accounts VALUES (?, ?)",
                    &[SqlValue::Integer(id), SqlValue::Integer(CRASH_INIT_BAL)],
                )?;
            }
            Ok(())
        })
        .unwrap();
    }

    let mut model = CrashModel::new();
    let mut committed_seq: u64 = 0;

    for round in 0..CRASH_KILLS_PER_BACKEND {
        let start_seq = committed_seq + 1;
        let target_acks = 3 + (mix(seed ^ 0xBEEF, round as u64) % 6);

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "sql_tx_crash_worker", "--nocapture"])
            .env(ENV_ROLE, "worker")
            .env(ENV_DIR, dir.path())
            .env(ENV_BACKEND, kind.as_str())
            .env(ENV_SEED, seed.to_string())
            .env(ENV_START_SEQ, start_seq.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let child_stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel::<u64>();
        let reader = thread::spawn(move || {
            let mut buf_reader = BufReader::new(child_stdout);
            let mut line = String::new();
            loop {
                line.clear();
                match buf_reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let seq = line
                            .ends_with('\n')
                            .then(|| line.trim_end().strip_prefix("ACK "))
                            .flatten()
                            .and_then(|rest| rest.parse::<u64>().ok());
                        if let Some(seq) = seq {
                            let _ = tx.send(seq);
                        }
                    }
                }
            }
        });

        let deadline = Instant::now() + Duration::from_secs(20);
        let mut acks_this_round = 0u64;
        let mut last_ack = committed_seq;
        while acks_this_round < target_acks {
            let rem = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            match rx.recv_timeout(rem) {
                Ok(seq) => {
                    assert_eq!(seq, last_ack + 1);
                    last_ack = seq;
                    acks_this_round += 1;
                }
                Err(_) => {
                    let _ = child.kill();
                    let out = child.wait_with_output().unwrap();
                    let _ = reader.join();
                    panic!(
                        "worker timed out on {kind:?} round={round}: stderr={}",
                        String::from_utf8_lossy(&out.stderr)
                    );
                }
            }
        }

        let jitter_us = match round % 4 {
            0 => 0,
            1 => 300,
            2 => 1_000,
            _ => 2_500,
        };
        if jitter_us > 0 {
            thread::sleep(Duration::from_micros(jitter_us));
        }

        let _ = child.kill();
        let _ = child.wait().unwrap();
        reader.join().unwrap();

        while let Ok(seq) = rx.try_recv() {
            assert_eq!(seq, last_ack + 1);
            last_ack = seq;
        }

        for seq in start_seq..=last_ack {
            model.apply(seed, seq);
        }
        let mut model_in_flight = model.clone();
        model_in_flight.apply(seed, last_ack + 1);
        assert_ne!(model, model_in_flight);

        let reopened = open_crash_backend(dir.path(), kind);
        let db = reopened.sql();
        let observed = observe_crash_state(&db);
        if observed == model {
            committed_seq = last_ack;
        } else if observed == model_in_flight {
            model = model_in_flight;
            committed_seq = last_ack + 1;
        } else {
            panic!(
                "multi-statement transaction atomicity violation on {kind:?} round={round} last_ack={last_ack}"
            );
        }
        assert_eq!(
            query_i64(&db, "SELECT SUM(bal) FROM accounts", &[]),
            CRASH_ACCOUNTS * CRASH_INIT_BAL
        );

        // Verify post-reopen multi-statement writability and rollback without disturbing model.
        let err = db
            .transaction(|tx| {
                tx.execute("UPDATE accounts SET bal = 0 WHERE id = 0", &[])?;
                Err::<(), SqlError>(SqlError::Constraint("probe rollback".into()))
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::Constraint(_)));
        assert_eq!(observe_crash_state(&db), model);
    }
}

#[test]
fn sql_tx_crash_ordinary_store() {
    run_tx_crash_drill(BackendKind::Ordinary, ORDINARY_CRASH_SEED);
}

#[test]
fn sql_tx_crash_checkpoint_store() {
    run_tx_crash_drill(BackendKind::Checkpoint, CHECKPOINT_CRASH_SEED);
}
