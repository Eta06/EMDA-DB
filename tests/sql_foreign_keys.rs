//! FOREIGN KEY constraints across `Store` and `CheckpointStore`:
//! DDL validation, index/table drop protection, runtime enforcement,
//! child-side index vs bounded scan, self-references, transactions/rollback,
//! reopen persistence, legacy catalog compatibility, read-set concurrency,
//! and a randomized differential oracle against SQLite (`PRAGMA foreign_keys=ON`).
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, FieldRule, FieldType, SqlDatabase,
    SqlError, SqlLimits, SqlOutcome, SqlValue, Store,
};
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;

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

#[test]
fn ddl_syntax_target_validation_and_errors() {
    for_both(|db, backend| {
        db.execute(
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE,
                flag BOOLEAN UNIQUE,
                note TEXT,
                a INTEGER,
                b INTEGER,
                UNIQUE (a, b)
            )",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX parents_note_idx ON parents (note)", &[])
            .unwrap();
        db.execute("CREATE UNIQUE INDEX parents_a_uq ON parents (a)", &[])
            .unwrap();

        // Column-level and table-level forms with omitted/explicit column, actions, and constraints.
        assert_eq!(
            db.execute(
                "CREATE TABLE child_ok (
                    id INTEGER PRIMARY KEY,
                    p_omitted INTEGER REFERENCES parents NOT NULL,
                    p_pk INTEGER NOT NULL REFERENCES parents(id) ON DELETE NO ACTION ON UPDATE RESTRICT,
                    p_code VARCHAR(16) UNIQUE REFERENCES parents(code) ON DELETE RESTRICT ON UPDATE NO ACTION,
                    p_flag BOOLEAN,
                    p_a BIGINT REFERENCES parents(a),
                    FOREIGN KEY (p_flag) REFERENCES parents(flag)
                )",
                &[],
            )
            .unwrap(),
            SqlOutcome::TableCreated,
            "{backend}"
        );
        // Table-level FOREIGN KEY declared before its column.
        assert_eq!(
            db.execute(
                "CREATE TABLE child_early (
                    FOREIGN KEY (pid) REFERENCES parents(id),
                    id INTEGER PRIMARY KEY,
                    pid INTEGER
                )",
                &[],
            )
            .unwrap(),
            SqlOutcome::TableCreated,
            "{backend}"
        );
        // PRIMARY KEY column can also carry REFERENCES.
        assert_eq!(
            db.execute(
                "CREATE TABLE child_pk_fk (
                    id INTEGER PRIMARY KEY REFERENCES parents(id),
                    v TEXT
                )",
                &[],
            )
            .unwrap(),
            SqlOutcome::TableCreated,
            "{backend}"
        );

        // IF NOT EXISTS succeeds cleanly when table already exists, even with missing parent.
        assert_eq!(
            db.execute(
                "CREATE TABLE IF NOT EXISTS child_ok (id INTEGER PRIMARY KEY, x INT REFERENCES missing(id))",
                &[],
            )
            .unwrap(),
            SqlOutcome::TableCreated,
            "{backend}"
        );

        // Missing parent table -> NoSuchTable.
        assert!(
            matches!(
                db.execute(
                    "CREATE TABLE bad_tbl (id INT PRIMARY KEY, p INT REFERENCES missing(id))",
                    &[],
                ),
                Err(SqlError::NoSuchTable(_))
            ),
            "{backend}"
        );

        // Missing parent column -> NoSuchColumn.
        assert!(
            matches!(
                db.execute(
                    "CREATE TABLE bad_col (id INT PRIMARY KEY, p INT REFERENCES parents(nope))",
                    &[],
                ),
                Err(SqlError::NoSuchColumn { .. })
            ),
            "{backend}"
        );

        // Target column must be PK or Ready single-column UNIQUE index -> Constraint.
        for sql in [
            // plain unindexed column
            "CREATE TABLE bad_u1 (id INT PRIMARY KEY, p TEXT REFERENCES parents(note))",
            // non-unique indexed column
            "CREATE TABLE bad_u2 (id INT PRIMARY KEY, p INT REFERENCES parents(b))",
            // column only in compound UNIQUE (a, b) without single-col unique on b
            "CREATE TABLE bad_u3 (id INT PRIMARY KEY, b INT REFERENCES parents(b))",
        ] {
            assert!(
                matches!(db.execute(sql, &[]), Err(SqlError::Constraint(_))),
                "{backend}: {sql}"
            );
        }

        // Building index is NOT Ready so cannot be referenced.
        db.document_database()
            .transaction(|tx| {
                tx.start_index_build(
                    "parents",
                    database_engine::IndexDefinition {
                        name: "uq_b_building".into(),
                        fields: vec![vec!["b".into()]],
                        unique: true,
                        partial: None,
                    },
                )?;
                let raw = tx.get_raw(b"\0emda-sql\x01Tparents")?.unwrap();
                let mut def: serde_json::Value = serde_json::from_slice(&raw).unwrap();
                def["indexes"].as_array_mut().unwrap().push(json!({
                    "name": "uq_b_building",
                    "columns": [5],
                    "unique": true,
                    "state": "building",
                    "is_constraint": false
                }));
                tx.put_raw(
                    b"\0emda-sql\x01Tparents",
                    &serde_json::to_vec(&def).unwrap(),
                )
            })
            .unwrap();
        assert!(
            matches!(
                db.execute(
                    "CREATE TABLE bad_building (id INT PRIMARY KEY, b INT REFERENCES parents(b))",
                    &[],
                ),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        // Type mismatch -> Type.
        for sql in [
            "CREATE TABLE bad_t1 (id INT PRIMARY KEY, p TEXT REFERENCES parents(id))",
            "CREATE TABLE bad_t2 (id INT PRIMARY KEY, p INT REFERENCES parents(code))",
            "CREATE TABLE bad_t3 (id INT PRIMARY KEY, p REAL REFERENCES parents(id))",
            "CREATE TABLE bad_t4 (id INT PRIMARY KEY, p BOOLEAN REFERENCES parents(id))",
            "CREATE TABLE bad_t5 (id INT PRIMARY KEY, p INT REFERENCES parents(flag))",
        ] {
            assert!(
                matches!(db.execute(sql, &[]), Err(SqlError::Type(_))),
                "{backend}: {sql}"
            );
        }

        // Unsupported FK clauses -> Unsupported.
        for sql in [
            "CREATE TABLE u3 (id INT PRIMARY KEY, p INT REFERENCES parents(id) ON DELETE SET DEFAULT)",
            "CREATE TABLE u6 (id INT PRIMARY KEY, p INT REFERENCES parents(id) ON UPDATE SET DEFAULT)",
            "CREATE TABLE u7 (id INT PRIMARY KEY, a INT, b INT, FOREIGN KEY (a, b) REFERENCES parents(a, b))",
            "CREATE TABLE u8 (id INT PRIMARY KEY, a INT REFERENCES parents(a, b))",
            "CREATE TABLE u9 (id INT PRIMARY KEY, p INT REFERENCES parents(id) MATCH FULL)",
            "CREATE TABLE u10 (id INT PRIMARY KEY, p INT REFERENCES parents(id) DEFERRABLE)",
            "CREATE TABLE u11 (id INT PRIMARY KEY, p INT REFERENCES parents(id) NOT DEFERRABLE)",
            "CREATE TABLE u12 (id INT PRIMARY KEY, p INT REFERENCES parents(id) INITIALLY DEFERRED)",
            "CREATE TABLE u13 (id INT PRIMARY KEY, p INT, CONSTRAINT fk_p FOREIGN KEY (p) REFERENCES parents(id))",
            "ALTER TABLE child_ok ADD COLUMN extra INT REFERENCES parents(id)",
            "ALTER TABLE child_ok ADD FOREIGN KEY (p_pk) REFERENCES parents(id)",
        ] {
            assert!(
                matches!(db.execute(sql, &[]), Err(SqlError::Unsupported { .. })),
                "{backend}: expected Unsupported for {sql}"
            );
        }

        // Malformed FK syntax -> Syntax.
        for sql in [
            "CREATE TABLE s1 (id INT PRIMARY KEY, FOREIGN KEY () REFERENCES parents(id))",
            "CREATE TABLE s2 (id INT PRIMARY KEY, p INT, FOREIGN KEY (p))",
            "CREATE TABLE s3 (id INT PRIMARY KEY, p INT, FOREIGN KEY (missing_col) REFERENCES parents(id))",
            "CREATE TABLE s4 (id INT PRIMARY KEY, p INT REFERENCES parents())",
            "CREATE TABLE s5 (id INT PRIMARY KEY, p INT REFERENCES parents(id) ON DELETE)",
            "CREATE TABLE s6 (id INT PRIMARY KEY, p INT REFERENCES parents(id) ON DELETE BOGUS)",
            "CREATE TABLE s7 (id INT PRIMARY KEY, p INT REFERENCES parents(id) ON DELETE SET BOGUS)",
            "CREATE TABLE s8 (id INT PRIMARY KEY, p INT REFERENCES parents(id) ON DELETE NO ACTION ON DELETE RESTRICT)",
            "CREATE TABLE s9 (id INT PRIMARY KEY, p INT REFERENCES parents(id) ON UPDATE NO ACTION ON UPDATE RESTRICT)",
        ] {
            assert!(
                matches!(db.execute(sql, &[]), Err(SqlError::Syntax { .. })),
                "{backend}: expected Syntax for {sql}, got {:?}",
                db.execute(sql, &[])
            );
        }

        // > 64 FKs exceeds limit -> Bounds.
        let mut many_fks = String::from("CREATE TABLE too_many_fks (id INT PRIMARY KEY");
        for _ in 0..65 {
            many_fks.push_str(", FOREIGN KEY (id) REFERENCES parents(id)");
        }
        many_fks.push(')');
        assert!(matches!(db.prepare(&many_fks), Err(SqlError::Bounds(_))));
    });
}

#[test]
fn drop_table_and_drop_index_protection() {
    for_both(|db, backend| {
        db.execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT)",
            &[],
        )
        .unwrap();
        db.execute("CREATE UNIQUE INDEX u_code ON parents (code)", &[])
            .unwrap();
        db.execute("CREATE UNIQUE INDEX u_code_2 ON parents (code)", &[])
            .unwrap();
        db.execute(
            "CREATE TABLE edges (
                id INTEGER PRIMARY KEY,
                src INTEGER REFERENCES parents(id),
                dst INTEGER REFERENCES parents(id),
                p_code TEXT REFERENCES parents(code)
            )",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE other_child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id))",
            &[],
        )
        .unwrap();

        // DROP TABLE on referenced parent fails even when empty and even with IF EXISTS.
        assert!(
            matches!(
                db.execute("DROP TABLE parents", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        assert!(
            matches!(
                db.execute("DROP TABLE IF EXISTS parents", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        // Two unique indexes cover `code`: dropping one is allowed, dropping the last is refused.
        assert_eq!(
            db.execute("DROP INDEX u_code ON parents", &[]).unwrap(),
            SqlOutcome::IndexDropped,
            "{backend}"
        );
        assert!(
            matches!(
                db.execute("DROP INDEX u_code_2 ON parents", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        assert!(
            matches!(
                db.execute("DROP INDEX IF EXISTS u_code_2 ON parents", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        // Dropping only one child leaves `other_child` still protecting `parents`.
        db.execute("DROP TABLE edges", &[]).unwrap();
        assert!(
            matches!(
                db.execute("DROP TABLE parents", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        // Once `edges` (which referenced `code`) is gone, `u_code_2` can be dropped.
        assert_eq!(
            db.execute("DROP INDEX u_code_2 ON parents", &[]).unwrap(),
            SqlOutcome::IndexDropped,
            "{backend}"
        );

        // Dropping the remaining child allows dropping `parents`.
        db.execute("DROP TABLE other_child", &[]).unwrap();
        assert_eq!(
            db.execute("DROP TABLE parents", &[]).unwrap(),
            SqlOutcome::TableDropped,
            "{backend}"
        );
    });
}

#[test]
fn large_child_incremental_drop_detaches_parent() {
    for_both(|db, backend| {
        db.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute("INSERT INTO parents VALUES (1), (2)", &[])
            .unwrap();
        db.execute(
            "CREATE TABLE big_child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id))",
            &[],
        )
        .unwrap();
        for start in (0..1_200).step_by(200) {
            let values = (start..start + 200)
                .map(|id| format!("({id}, {})", (id % 2) + 1))
                .collect::<Vec<_>>()
                .join(", ");
            db.execute(&format!("INSERT INTO big_child VALUES {values}"), &[])
                .unwrap();
        }
        assert!(matches!(
            db.execute("DROP TABLE parents", &[]),
            Err(SqlError::Constraint(_))
        ));
        assert_eq!(
            db.execute("DROP TABLE big_child", &[]).unwrap(),
            SqlOutcome::TableDropped,
            "{backend}"
        );
        assert_eq!(
            db.execute("DROP TABLE parents", &[]).unwrap(),
            SqlOutcome::TableDropped,
            "{backend}"
        );
    });
}

#[test]
fn insert_update_delete_enforcement_and_null_semantics() {
    for_both(|db, backend| {
        db.execute(
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE,
                active BOOLEAN UNIQUE,
                label TEXT
            )",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE t_parents (
                id TEXT PRIMARY KEY
            )",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                pid INTEGER REFERENCES parents(id),
                p_code TEXT REFERENCES parents(code),
                p_active BOOLEAN REFERENCES parents(active),
                t_ref TEXT REFERENCES t_parents(id),
                note TEXT
            )",
            &[],
        )
        .unwrap();

        // NULL FK values succeed even when parent tables are empty.
        db.execute("INSERT INTO children (id) VALUES (1)", &[])
            .unwrap();
        db.execute(
            "INSERT INTO children VALUES (2, NULL, NULL, NULL, NULL, 'ok')",
            &[],
        )
        .unwrap();

        // Orphan child insert fails -> Constraint.
        assert!(
            matches!(
                db.execute(
                    "INSERT INTO children VALUES (3, 1, NULL, NULL, NULL, 'bad')",
                    &[]
                ),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        db.execute(
            "INSERT INTO parents VALUES (10, 'alpha', TRUE, 'first'), (20, NULL, NULL, 'null_uq')",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO t_parents VALUES ('tp1')", &[])
            .unwrap();

        // Valid insert against PK, UNIQUE TEXT, UNIQUE BOOLEAN, and TEXT PK.
        db.execute(
            "INSERT INTO children VALUES (3, 10, 'alpha', TRUE, 'tp1', 'ok')",
            &[],
        )
        .unwrap();

        // Multi-row INSERT where row 2 fails FK must roll back row 1 atomically!
        assert!(
            matches!(
                db.execute(
                    "INSERT INTO children VALUES
                        (4, 10, 'alpha', TRUE, 'tp1', 'row1'),
                        (5, 10, 'missing_code', TRUE, 'tp1', 'row2')",
                    &[]
                ),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        assert!(rows(db, "SELECT id FROM children WHERE id >= 4", &[]).is_empty());

        // Oversized TEXT FK referencing TEXT PK (>254B) or TEXT UNIQUE (>256B) fails as Constraint.
        let too_long_pk = "x".repeat(255);
        assert!(
            matches!(
                db.execute(
                    "INSERT INTO children VALUES (6, NULL, NULL, NULL, ?, 'long_pk')",
                    &[text(&too_long_pk)]
                ),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        let too_long_uq = "y".repeat(257);
        assert!(
            matches!(
                db.execute(
                    "INSERT INTO children VALUES (6, NULL, ?, NULL, NULL, 'long_uq')",
                    &[text(&too_long_uq)]
                ),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        // Parent row with NULL in UNIQUE columns is not referenced by NULL child FKs: update/delete succeeds.
        db.execute("DELETE FROM parents WHERE id = 20", &[])
            .unwrap();

        // Updating unrelated column on referenced parent succeeds.
        db.execute("UPDATE parents SET label = 'renamed' WHERE id = 10", &[])
            .unwrap();

        // No-op UPDATE on referenced UNIQUE column succeeds.
        db.execute("UPDATE parents SET code = 'alpha' WHERE id = 10", &[])
            .unwrap();

        // Updating referenced parent UNIQUE column to new value or NULL fails atomically.
        assert!(
            matches!(
                db.execute("UPDATE parents SET code = 'beta' WHERE id = 10", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        assert!(
            matches!(
                db.execute("UPDATE parents SET active = NULL WHERE id = 10", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        // Deleting referenced parent fails atomically.
        db.execute(
            "INSERT INTO parents VALUES (30, 'gamma', FALSE, 'unref')",
            &[],
        )
        .unwrap();
        assert!(
            matches!(
                db.execute("DELETE FROM parents WHERE id >= 10", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        assert_eq!(rows(db, "SELECT id FROM parents ORDER BY id", &[]).len(), 2);

        // Updating child FK to invalid parent fails and rolls back all matched rows.
        assert!(
            matches!(
                db.execute("UPDATE children SET pid = 999 WHERE id <= 3", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );
        assert_eq!(
            rows(db, "SELECT pid FROM children WHERE id = 3", &[]),
            vec![vec![int(10)]]
        );

        // Clearing child FK values unblocks parent update/delete.
        db.execute(
            "UPDATE children SET pid = NULL, p_code = NULL, p_active = NULL, t_ref = NULL WHERE id = 3",
            &[],
        )
        .unwrap();
        db.execute("UPDATE parents SET code = 'beta' WHERE id = 10", &[])
            .unwrap();
        db.execute("DELETE FROM parents WHERE id = 10", &[])
            .unwrap();
        db.execute("DELETE FROM t_parents WHERE id = 'tp1'", &[])
            .unwrap();

        // Deleting/updating 0 rows succeeds even with invalid FK target literal.
        db.execute("UPDATE children SET pid = 999 WHERE id = 999", &[])
            .unwrap();
        db.execute("DELETE FROM parents WHERE id = 999", &[])
            .unwrap();
    });
}

fn for_both_with_limits(limits: SqlLimits, mut f: impl FnMut(&mut SqlDatabase<'_>, &str)) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("ordinary")).unwrap();
    let mut db = SqlDatabase::with_limits(DocumentDatabase::new(&store), limits).unwrap();
    f(&mut db, "Store");
    drop(db);
    drop(store);

    let log = dir.path().join("checkpoint-log");
    let initial = Store::open(&log).unwrap();
    let digest = initial
        .checkpoint_to(dir.path().join("checkpoint"))
        .unwrap();
    drop(initial);
    let checkpoint = CheckpointStore::open(&log, dir.path().join("checkpoint"), &digest).unwrap();
    let mut ckpt_db =
        SqlDatabase::with_limits(DocumentDatabase::with_checkpoint_store(&checkpoint), limits)
            .unwrap();
    f(&mut ckpt_db, "CheckpointStore");
}

#[test]
fn child_index_vs_bounded_scan_and_compound_null_safety() {
    let low_limits = SqlLimits {
        max_visited_rows: 40,
        ..SqlLimits::default()
    };
    for_both_with_limits(low_limits, |db, backend| {
        db.execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, code INTEGER UNIQUE)",
            &[],
        )
        .unwrap();
        db.execute(
            "INSERT INTO parents VALUES (1, 101), (2, 102), (99, 199)",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                pid INTEGER REFERENCES parents(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
                pcode INTEGER REFERENCES parents(code) ON UPDATE RESTRICT,
                opt INTEGER,
                req INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        for i in 0..80 {
            let p = if i == 0 { 1 } else { 2 };
            db.execute(
                "INSERT INTO children VALUES (?, ?, ?, NULL, ?)",
                &[int(i), int(p), int(100 + p), int(i)],
            )
            .unwrap();
        }

        // No child index: deleting/updating unreferenced parent 99 visits > 40 rows -> Bounds.
        assert!(
            matches!(
                db.execute("DELETE FROM parents WHERE id = 99", &[]),
                Err(SqlError::Bounds(_))
            ),
            "{backend}"
        );
        assert!(
            matches!(
                db.execute("UPDATE parents SET code = 999 WHERE id = 99", &[]),
                Err(SqlError::Bounds(_))
            ),
            "{backend}"
        );

        // Compound index with nullable trailing column `(pid, opt)` MUST NOT be used,
        // because row 0 has (pid=1, opt=NULL) which is omitted from the index!
        db.execute("CREATE INDEX idx_pid_opt ON children (pid, opt)", &[])
            .unwrap();
        assert!(
            matches!(
                db.execute("DELETE FROM parents WHERE id = 99", &[]),
                Err(SqlError::Bounds(_))
            ),
            "{backend}: compound index with nullable trailing col must fall back to scan"
        );
        // Also verify with default limits that deleting parent 1 catches child row 0 (pid=1, opt=NULL)!
        db.set_limits(SqlLimits::default()).unwrap();
        assert!(
            matches!(
                db.execute("DELETE FROM parents WHERE id = 1", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}: must not miss child row omitted from nullable compound index"
        );

        // Compound index with NOT NULL trailing column `(pid, req)` and `(pcode, req)` IS safe!
        db.set_limits(low_limits).unwrap();
        db.execute("CREATE INDEX idx_pid_req ON children (pid, req)", &[])
            .unwrap();
        db.execute("CREATE INDEX idx_pcode_req ON children (pcode, req)", &[])
            .unwrap();
        db.execute("UPDATE parents SET code = 999 WHERE id = 99", &[])
            .unwrap();
        db.execute("DELETE FROM parents WHERE id = 99", &[])
            .unwrap();
        assert!(matches!(
            db.execute("DELETE FROM parents WHERE id = 1", &[]),
            Err(SqlError::Constraint(_))
        ));

        // Child FK column that is child's PRIMARY KEY also uses O(1) lookup beyond max_visited_rows.
        db.execute(
            "CREATE TABLE pk_child (id INTEGER PRIMARY KEY REFERENCES parents(id))",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO parents VALUES (50, 500)", &[])
            .unwrap();
        db.execute("DELETE FROM parents WHERE id = 50", &[])
            .unwrap();
    });
}

#[test]
fn self_referencing_foreign_keys() {
    for_both(|db, backend| {
        db.execute(
            "CREATE TABLE emp (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE,
                mgr_id INTEGER REFERENCES emp(id),
                mgr_code TEXT REFERENCES emp(code)
            )",
            &[],
        )
        .unwrap();

        // Multi-row INSERT with child listed before parent (forward reference within one statement).
        db.execute(
            "INSERT INTO emp VALUES (2, 'bob', 1, 'alice'), (1, 'alice', NULL, NULL)",
            &[],
        )
        .unwrap();

        // Self-loop row referencing itself.
        db.execute("INSERT INTO emp VALUES (3, 'self', 3, 'self')", &[])
            .unwrap();

        // Orphan self-ref fails.
        assert!(
            matches!(
                db.execute("INSERT INTO emp VALUES (4, 'bad', 99, NULL)", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        // Deleting parent 1 alone fails because row 2 references it.
        assert!(
            matches!(
                db.execute("DELETE FROM emp WHERE id = 1", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        // Updating parent 1's code fails because row 2 references 'alice'.
        assert!(
            matches!(
                db.execute("UPDATE emp SET code = 'alice2' WHERE id = 1", &[]),
                Err(SqlError::Constraint(_))
            ),
            "{backend}"
        );

        // Updating self-loop's code and mgr_code together succeeds at statement end.
        db.execute(
            "UPDATE emp SET code = 'self2', mgr_code = 'self2' WHERE id = 3",
            &[],
        )
        .unwrap();

        // Deleting self-loop row succeeds at statement end.
        db.execute("DELETE FROM emp WHERE id = 3", &[]).unwrap();

        // Deleting both parent 1 and child 2 in one statement succeeds at statement end.
        assert_eq!(
            db.execute("DELETE FROM emp WHERE id <= 2", &[]).unwrap(),
            SqlOutcome::Deleted { rows: 2 },
            "{backend}"
        );

        // Self-referencing table can be dropped directly.
        assert_eq!(
            db.execute("DROP TABLE emp", &[]).unwrap(),
            SqlOutcome::TableDropped,
            "{backend}"
        );
    });
}

#[test]
fn transactions_rollback_reopen_and_legacy_compatibility() {
    for_both(|db, backend| {
        db.execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, tag TEXT UNIQUE)",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id))",
            &[],
        )
        .unwrap();

        // Staged parent + child in one transaction commits cleanly.
        db.transaction(|tx| {
            tx.execute("INSERT INTO parents VALUES (1, 'p1')", &[])?;
            tx.execute("INSERT INTO children VALUES (10, 1)", &[])?;
            Ok(())
        })
        .unwrap();

        // Deleting parent after staged child insert fails and rolls back both.
        let err = db
            .transaction(|tx| {
                tx.execute("INSERT INTO parents VALUES (2, 'p2')", &[])?;
                tx.execute("INSERT INTO children VALUES (11, 2)", &[])?;
                tx.execute("DELETE FROM parents WHERE id = 2", &[])?;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::Constraint(_)), "{backend}: {err:?}");
        assert_eq!(rows(db, "SELECT id FROM parents ORDER BY id", &[]).len(), 1);
        assert_eq!(
            rows(db, "SELECT id FROM children ORDER BY id", &[]).len(),
            1
        );

        // Deleting parent first then inserting child fails in same transaction.
        db.execute("INSERT INTO parents VALUES (3, 'p3')", &[])
            .unwrap();
        let err = db
            .transaction(|tx| {
                tx.execute("DELETE FROM parents WHERE id = 3", &[])?;
                tx.execute("INSERT INTO children VALUES (12, 3)", &[])?;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::Constraint(_)), "{backend}");
        assert_eq!(rows(db, "SELECT id FROM parents ORDER BY id", &[]).len(), 2);

        // Caught FK Constraint error inside transaction poisons the transaction.
        let err = db
            .transaction(|tx| {
                tx.execute("INSERT INTO parents VALUES (4, 'p4')", &[])?;
                let inner = tx
                    .execute("INSERT INTO children VALUES (13, 999)", &[])
                    .unwrap_err();
                assert!(matches!(inner, SqlError::Constraint(_)));
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::TransactionAborted(_)), "{backend}");
        assert!(rows(db, "SELECT id FROM parents WHERE id = 4", &[]).is_empty());

        // CREATE TABLE with FK + INSERT inside transaction that rolls back leaves parent clean.
        let err = db
            .transaction(|tx| {
                tx.execute(
                    "CREATE TABLE temp_c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id))",
                    &[],
                )?;
                tx.execute("INSERT INTO temp_c VALUES (1, 1)", &[])?;
                Err::<(), _>(SqlError::Constraint("abort".into()))
            })
            .unwrap_err();
        assert!(matches!(err, SqlError::Constraint(_)));
        assert!(matches!(
            db.execute("SELECT * FROM temp_c", &[]),
            Err(SqlError::NoSuchTable(_))
        ));
        db.execute("DELETE FROM parents WHERE id = 3", &[]).unwrap();

        // Legacy catalog entry without FK fields loads and can even become an FK parent.
        db.document_database()
            .transaction(|tx| {
                tx.create_collection(
                    "legacy",
                    CollectionSchema {
                        fields: vec![FieldRule {
                            path: vec!["id".into()],
                            field_type: FieldType::Integer,
                            required: true,
                            nullable: false,
                        }],
                    },
                )?;
                let legacy_json = json!({
                    "version": 1,
                    "name": "legacy",
                    "columns": [
                        { "name": "id", "col_type": { "kind": "integer" }, "not_null": true }
                    ],
                    "primary_key_index": 0
                });
                tx.put_raw(
                    b"\0emda-sql\x01Tlegacy",
                    &serde_json::to_vec(&legacy_json).unwrap(),
                )
            })
            .unwrap();
        db.execute("INSERT INTO legacy VALUES (7)", &[]).unwrap();
        db.execute(
            "CREATE TABLE legacy_child (id INT PRIMARY KEY, lid INT REFERENCES legacy(id))",
            &[],
        )
        .unwrap();
        assert!(matches!(
            db.execute("INSERT INTO legacy_child VALUES (1, 8)", &[]),
            Err(SqlError::Constraint(_))
        ));
        db.execute("INSERT INTO legacy_child VALUES (1, 7)", &[])
            .unwrap();
        assert!(matches!(
            db.execute("DROP TABLE legacy", &[]),
            Err(SqlError::Constraint(_))
        ));
        db.execute("DROP TABLE legacy_child", &[]).unwrap();
        db.execute("DROP TABLE legacy", &[]).unwrap();
    });

    // Reopen persistence on both Store and CheckpointStore.
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store");
    {
        let store = Store::open(&store_path).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        db.execute(
            "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id), pcode TEXT REFERENCES p(code))",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO p VALUES (1, 'one')", &[]).unwrap();
        db.execute("INSERT INTO c VALUES (10, 1, 'one')", &[])
            .unwrap();
    }
    {
        let store = Store::open(&store_path).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::new(&store));
        assert!(matches!(
            db.execute("INSERT INTO c VALUES (11, 2, 'one')", &[]),
            Err(SqlError::Constraint(_))
        ));
        assert!(matches!(
            db.execute("DELETE FROM p WHERE id = 1", &[]),
            Err(SqlError::Constraint(_))
        ));
        assert!(matches!(
            db.execute("UPDATE p SET code = 'two' WHERE id = 1", &[]),
            Err(SqlError::Constraint(_))
        ));
        assert!(matches!(
            db.execute("DROP TABLE p", &[]),
            Err(SqlError::Constraint(_))
        ));
        db.execute("DROP TABLE c", &[]).unwrap();
        db.execute("DROP TABLE p", &[]).unwrap();
    }

    let log = dir.path().join("ckpt-log");
    let cp = dir.path().join("ckpt");
    let initial = Store::open(&log).unwrap();
    let digest = initial.checkpoint_to(&cp).unwrap();
    drop(initial);
    {
        let ckpt = CheckpointStore::open(&log, &cp, &digest).unwrap();
        let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt));
        db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        db.execute(
            "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
            &[],
        )
        .unwrap();
        db.execute("INSERT INTO p VALUES (1)", &[]).unwrap();
        db.execute("INSERT INTO c VALUES (10, 1)", &[]).unwrap();
    }
    let ckpt = CheckpointStore::open(&log, &cp, &digest).unwrap();
    let db = SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt));
    assert!(matches!(
        db.execute("DELETE FROM p WHERE id = 1", &[]),
        Err(SqlError::Constraint(_))
    ));
    assert!(matches!(
        db.execute("DROP TABLE p", &[]),
        Err(SqlError::Constraint(_))
    ));
}

#[test]
fn concurrent_fk_checks_conflict_in_read_set_on_both_backends() {
    for checkpoint in [false, true] {
        for child_indexed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store;
            let ckpt;
            let db = if checkpoint {
                let log = dir.path().join("log");
                let initial = Store::open(&log).unwrap();
                let digest = initial.checkpoint_to(dir.path().join("cp")).unwrap();
                drop(initial);
                ckpt = CheckpointStore::open(&log, dir.path().join("cp"), &digest).unwrap();
                SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&ckpt))
            } else {
                store = Store::open(dir.path().join("s")).unwrap();
                SqlDatabase::new(DocumentDatabase::new(&store))
            };

            db.execute(
                "CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
                &[],
            )
            .unwrap();
            db.execute(
                "CREATE TABLE children (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id), pcode TEXT REFERENCES parents(code))",
                &[],
            )
            .unwrap();
            if child_indexed {
                db.execute("CREATE INDEX idx_child_pid ON children (pid)", &[])
                    .unwrap();
                db.execute("CREATE INDEX idx_child_pcode ON children (pcode)", &[])
                    .unwrap();
            }
            db.execute("INSERT INTO parents VALUES (1, 'c1'), (2, 'c2')", &[])
                .unwrap();

            // 1. Concurrent DELETE of parent 1 conflicts with a transaction inserting child referencing parent 1.
            let res1 = db.transaction(|tx| {
                tx.execute("INSERT INTO children VALUES (10, 1, 'c1')", &[])?;
                db.execute("DELETE FROM parents WHERE id = 1", &[]).unwrap();
                Ok(())
            });
            assert!(
                matches!(res1, Err(SqlError::Conflict(_))),
                "checkpoint={checkpoint} indexed={child_indexed}: {res1:?}"
            );
            assert!(rows(&db, "SELECT id FROM children", &[]).is_empty());
            assert_eq!(
                rows(&db, "SELECT id FROM parents ORDER BY id", &[]),
                vec![vec![int(2)]]
            );

            // 2. Concurrent INSERT into child referencing parent 2 conflicts with transaction deleting parent 2.
            let res2 = db.transaction(|tx| {
                tx.execute("DELETE FROM parents WHERE id = 2", &[])?;
                db.execute("INSERT INTO children VALUES (20, 2, 'c2')", &[])
                    .unwrap();
                Ok(())
            });
            assert!(
                matches!(res2, Err(SqlError::Conflict(_))),
                "checkpoint={checkpoint} indexed={child_indexed}: {res2:?}"
            );
            assert_eq!(rows(&db, "SELECT id FROM parents", &[]), vec![vec![int(2)]]);
            assert_eq!(
                rows(&db, "SELECT id FROM children", &[]),
                vec![vec![int(20)]]
            );

            // 3. Concurrent INSERT referencing parent 3's UNIQUE code conflicts with transaction updating parent 3's code.
            db.execute("INSERT INTO parents VALUES (3, 'c3')", &[])
                .unwrap();
            let res3 = db.transaction(|tx| {
                tx.execute("UPDATE parents SET code = 'c3_new' WHERE id = 3", &[])?;
                db.execute("INSERT INTO children VALUES (30, NULL, 'c3')", &[])
                    .unwrap();
                Ok(())
            });
            assert!(
                matches!(res3, Err(SqlError::Conflict(_))),
                "checkpoint={checkpoint} indexed={child_indexed}: {res3:?}"
            );
            assert_eq!(
                rows(&db, "SELECT code FROM parents WHERE id = 3", &[]),
                vec![vec![text("c3")]]
            );

            // 4. When child is indexed, inserting a child referencing disjoint parent 3 does NOT conflict
            //    with deleting unreferenced parent 4!
            if child_indexed {
                db.execute("INSERT INTO parents VALUES (4, 'c4')", &[])
                    .unwrap();
                let res4 = db.transaction(|tx| {
                    tx.execute("DELETE FROM parents WHERE id = 4", &[])?;
                    db.execute("INSERT INTO children VALUES (40, 3, 'c3')", &[])
                        .unwrap();
                    Ok(())
                });
                assert!(
                    res4.is_ok(),
                    "disjoint indexed child insert should not conflict: {res4:?}"
                );
                assert!(rows(&db, "SELECT id FROM parents WHERE id = 4", &[]).is_empty());
            }
        }
    }
}

#[test]
fn multithreaded_concurrent_fk_stress_preserves_referential_integrity() {
    for_both(|db, backend| {
        db.execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, code INTEGER UNIQUE)",
            &[],
        )
        .unwrap();
        db.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id))",
            &[],
        )
        .unwrap();
        db.execute("CREATE INDEX idx_c_pid ON children (pid)", &[])
            .unwrap();
        for p in 1..=6 {
            db.execute("INSERT INTO parents VALUES (?, ?)", &[int(p), int(p * 10)])
                .unwrap();
        }

        let barrier = Arc::new(Barrier::new(4));
        thread::scope(|s| {
            for tid in 0..4_i64 {
                let barrier = Arc::clone(&barrier);
                s.spawn(move || {
                    barrier.wait();
                    for step in 0..40_i64 {
                        let p = ((tid * 7 + step) % 6) + 1;
                        let cid = tid * 100 + step;
                        let res = match step % 3 {
                            0 => db.transaction_with_retry(16, |tx| {
                                match tx.execute(
                                    "INSERT INTO children VALUES (?, ?)",
                                    &[int(cid), int(p)],
                                ) {
                                    Ok(_) => Ok(()),
                                    Err(SqlError::Constraint(_)) => {
                                        Err(SqlError::Constraint("expected".into()))
                                    }
                                    Err(e) => Err(e),
                                }
                            }),
                            1 => db.transaction_with_retry(16, |tx| {
                                match tx.execute("DELETE FROM parents WHERE id = ?", &[int(p)]) {
                                    Ok(_) => Ok(()),
                                    Err(SqlError::Constraint(_)) => {
                                        Err(SqlError::Constraint("expected".into()))
                                    }
                                    Err(e) => Err(e),
                                }
                            }),
                            _ => db.transaction_with_retry(16, |tx| {
                                tx.execute(
                                    "INSERT INTO parents VALUES (?, ?)",
                                    &[int(p + 100), int((p + 100) * 10)],
                                )?;
                                Ok(())
                            }),
                        };
                        match res {
                            Ok(()) | Err(SqlError::Constraint(_)) => {}
                            Err(other) => panic!("{backend}: unexpected error {other:?}"),
                        }
                    }
                });
            }
        });

        let orphans = rows(
            db,
            "SELECT c.id FROM children c LEFT JOIN parents p ON p.id = c.pid WHERE c.pid IS NOT NULL AND p.id IS NULL",
            &[],
        );
        assert!(
            orphans.is_empty(),
            "{backend}: found orphan rows {orphans:?}"
        );
    });
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

fn sqlite_query(lite: &rusqlite::Connection, sql: &str, booleans: &[bool]) -> Vec<Vec<SqlValue>> {
    let mut stmt = lite.prepare(sql).unwrap();
    stmt.query_map([], |row| {
        Ok(booleans
            .iter()
            .enumerate()
            .map(|(i, &is_bool)| from_sqlite(row.get(i).unwrap(), is_bool))
            .collect::<Vec<_>>())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

#[test]
fn randomized_differential_fk_sequences_match_sqlite() {
    for_both(|emda, backend| {
        let lite = rusqlite::Connection::open_in_memory().unwrap();
        lite.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        let ddls = [
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE,
                flag BOOLEAN
            )",
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                pid INTEGER REFERENCES parents(id),
                pcode TEXT REFERENCES parents(code),
                note INTEGER
            )",
            "CREATE INDEX idx_children_pid ON children (pid)",
            "CREATE TABLE emp (
                id INTEGER PRIMARY KEY,
                mgr_id INTEGER REFERENCES emp(id),
                tag TEXT
            )",
        ];
        for ddl in ddls {
            emda.execute(ddl, &[]).unwrap();
            lite.execute(ddl, []).unwrap();
        }

        let mut rng = Rng(0xF00D_CAFE_BEEF_1234);
        let codes = ["a", "b", "c", "d", "e", "f"];
        let tags = ["t0", "t1", "t2", "t3"];

        let exec_both = |sql: &str, params: &[SqlValue], step: usize| {
            let emda_res = emda.execute(sql, params);
            let lite_res = lite.execute(
                sql,
                rusqlite::params_from_iter(params.iter().map(to_sqlite)),
            );
            assert_eq!(
                emda_res.is_ok(),
                lite_res.is_ok(),
                "{backend} step={step} sql={sql} params={params:?} emda={emda_res:?} lite={lite_res:?}"
            );
        };

        for step in 0..450_usize {
            match rng.below(9) {
                // Insert parent (1 or 2 rows)
                0 => {
                    let count = if rng.below(3) == 0 { 2 } else { 1 };
                    let mut params = Vec::new();
                    let mut placeholders = Vec::new();
                    for _ in 0..count {
                        let id = rng.below(10) as i64;
                        let code = if rng.below(4) == 0 {
                            SqlValue::Null
                        } else {
                            text(codes[rng.below(codes.len() as u64) as usize])
                        };
                        let flag = if rng.below(4) == 0 {
                            SqlValue::Null
                        } else {
                            SqlValue::Boolean(rng.below(2) == 0)
                        };
                        params.push(int(id));
                        params.push(code);
                        params.push(flag);
                        placeholders.push("(?, ?, ?)");
                    }
                    let sql = format!("INSERT INTO parents VALUES {}", placeholders.join(", "));
                    exec_both(&sql, &params, step);
                }
                // Insert child (1 or 2 rows, referencing indexed pid and unindexed pcode)
                1 | 2 => {
                    let count = if rng.below(3) == 0 { 2 } else { 1 };
                    let mut params = Vec::new();
                    let mut placeholders = Vec::new();
                    for _ in 0..count {
                        let id = rng.below(16) as i64;
                        let pid = if rng.below(4) == 0 {
                            SqlValue::Null
                        } else {
                            int(rng.below(10) as i64)
                        };
                        let pcode = if rng.below(4) == 0 {
                            SqlValue::Null
                        } else {
                            text(codes[rng.below(codes.len() as u64) as usize])
                        };
                        let note = int(rng.below(100) as i64);
                        params.push(int(id));
                        params.push(pid);
                        params.push(pcode);
                        params.push(note);
                        placeholders.push("(?, ?, ?, ?)");
                    }
                    let sql = format!("INSERT INTO children VALUES {}", placeholders.join(", "));
                    exec_both(&sql, &params, step);
                }
                // Update child FK columns
                3 => {
                    let target_id = rng.below(16) as i64;
                    let pid = if rng.below(4) == 0 {
                        SqlValue::Null
                    } else {
                        int(rng.below(10) as i64)
                    };
                    let pcode = if rng.below(4) == 0 {
                        SqlValue::Null
                    } else {
                        text(codes[rng.below(codes.len() as u64) as usize])
                    };
                    exec_both(
                        "UPDATE children SET pid = ?, pcode = ? WHERE id = ?",
                        &[pid, pcode, int(target_id)],
                        step,
                    );
                }
                // Update parent UNIQUE code or non-key flag
                4 => {
                    let target_id = rng.below(10) as i64;
                    if rng.below(3) == 0 {
                        let flag = if rng.below(3) == 0 {
                            SqlValue::Null
                        } else {
                            SqlValue::Boolean(rng.below(2) == 0)
                        };
                        exec_both(
                            "UPDATE parents SET flag = ? WHERE id = ?",
                            &[flag, int(target_id)],
                            step,
                        );
                    } else {
                        let code = if rng.below(4) == 0 {
                            SqlValue::Null
                        } else {
                            text(codes[rng.below(codes.len() as u64) as usize])
                        };
                        exec_both(
                            "UPDATE parents SET code = ? WHERE id = ?",
                            &[code, int(target_id)],
                            step,
                        );
                    }
                }
                // Delete parent (single or range)
                5 => {
                    if rng.below(4) == 0 {
                        let lo = rng.below(10) as i64;
                        let hi = lo + (rng.below(4) as i64);
                        exec_both(
                            "DELETE FROM parents WHERE id >= ? AND id <= ?",
                            &[int(lo), int(hi)],
                            step,
                        );
                    } else {
                        let target_id = rng.below(10) as i64;
                        exec_both("DELETE FROM parents WHERE id = ?", &[int(target_id)], step);
                    }
                }
                // Delete child
                6 => {
                    if rng.below(4) == 0 {
                        let pid = rng.below(10) as i64;
                        exec_both("DELETE FROM children WHERE pid = ?", &[int(pid)], step);
                    } else {
                        let target_id = rng.below(16) as i64;
                        exec_both("DELETE FROM children WHERE id = ?", &[int(target_id)], step);
                    }
                }
                // Self-referencing emp insert / update
                7 => {
                    if rng.below(2) == 0 {
                        let count = if rng.below(3) == 0 { 2 } else { 1 };
                        let mut params = Vec::new();
                        let mut placeholders = Vec::new();
                        for _ in 0..count {
                            let id = rng.below(8) as i64;
                            let mgr = if rng.below(3) == 0 {
                                SqlValue::Null
                            } else {
                                int(rng.below(8) as i64)
                            };
                            let tag = text(tags[rng.below(tags.len() as u64) as usize]);
                            params.push(int(id));
                            params.push(mgr);
                            params.push(tag);
                            placeholders.push("(?, ?, ?)");
                        }
                        let sql = format!("INSERT INTO emp VALUES {}", placeholders.join(", "));
                        exec_both(&sql, &params, step);
                    } else {
                        let id = rng.below(8) as i64;
                        let mgr = if rng.below(3) == 0 {
                            SqlValue::Null
                        } else {
                            int(rng.below(8) as i64)
                        };
                        exec_both(
                            "UPDATE emp SET mgr_id = ? WHERE id = ?",
                            &[mgr, int(id)],
                            step,
                        );
                    }
                }
                // Self-referencing emp delete (single or range)
                _ => {
                    let lo = rng.below(8) as i64;
                    let hi = lo + (rng.below(4) as i64);
                    exec_both(
                        "DELETE FROM emp WHERE id >= ? AND id <= ?",
                        &[int(lo), int(hi)],
                        step,
                    );
                }
            }

            if step % 50 == 49 || step == 449 {
                assert_eq!(
                    rows(emda, "SELECT id, code, flag FROM parents ORDER BY id", &[]),
                    sqlite_query(
                        &lite,
                        "SELECT id, code, flag FROM parents ORDER BY id",
                        &[false, false, true]
                    ),
                    "{backend} step={step}: parents mismatch"
                );
                assert_eq!(
                    rows(
                        emda,
                        "SELECT id, pid, pcode, note FROM children ORDER BY id",
                        &[]
                    ),
                    sqlite_query(
                        &lite,
                        "SELECT id, pid, pcode, note FROM children ORDER BY id",
                        &[false, false, false, false]
                    ),
                    "{backend} step={step}: children mismatch"
                );
                assert_eq!(
                    rows(emda, "SELECT id, mgr_id, tag FROM emp ORDER BY id", &[]),
                    sqlite_query(
                        &lite,
                        "SELECT id, mgr_id, tag FROM emp ORDER BY id",
                        &[false, false, false]
                    ),
                    "{backend} step={step}: emp mismatch"
                );
            }
        }
    });
}
