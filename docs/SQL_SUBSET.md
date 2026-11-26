# Embedded SQL Subset (Slice 1)

This document describes the experimental embedded SQL subset in `database_engine::sql`. It runs in-process over `DocumentDatabase` and the shared witnessed storage and transaction engine (`Store`, `CheckpointStore`, and the retained backends).

**This interface is NOT PostgreSQL-compatible, SQLite-compatible, or wire-protocol compatible.** There is no TCP server, wire protocol, session state, or multi-statement SQL transaction block. Read the [document model](DOCUMENT_MODEL.md) and [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md) before using it.

## Entry point and example

```rust
use database_engine::{DocumentDatabase, SqlDatabase, SqlOutcome, SqlValue, Store};

fn example(store: &Store) -> Result<(), Box<dyn std::error::Error>> {
    let db = SqlDatabase::new(DocumentDatabase::new(store));
    db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL)",
        &[],
    )?;
    db.execute(
        "INSERT INTO users (id, name, score) VALUES (?, ?, ?)",
        &[
            SqlValue::Integer(1),
            SqlValue::Text("Ada".into()),
            SqlValue::Integer(42), // exact integer literal/param coercion into REAL
        ],
    )?;
    let stmt = db.prepare("SELECT name, score FROM users WHERE id = ?")?;
    let outcome = db.execute_prepared(&stmt, &[SqlValue::Integer(1)])?;
    assert_eq!(
        outcome,
        SqlOutcome::Rows {
            columns: vec!["name".into(), "score".into()],
            rows: vec![vec![SqlValue::Text("Ada".into()), SqlValue::Real(42.0)]],
        }
    );
    Ok(())
}
```

- `SqlDatabase::new(DocumentDatabase)` uses `SqlLimits::default()`.
- `SqlDatabase::with_limits` and `SqlDatabase::set_limits` configure custom bounds up to hard maxima.
- `prepare` lexes and parses a single statement (allowing an optional trailing `;`; rejecting multiple statements).
- `execute_prepared` and `execute` run the statement inside one `DocumentDatabase` transaction in `DocumentTransactionMode::ReadSet`.

## Supported grammar

Keywords are case-insensitive. Unquoted identifiers (`[A-Za-z_][A-Za-z0-9_]*`) fold to ASCII lowercase. Double-quoted identifiers (`"Name"`, with `""` escaping `"`) preserve case. Identifiers must be 1–128 UTF-8 bytes. `--` starts a line comment outside strings and quoted identifiers.

```text
statement      := (create_table | insert | select | update | delete) [";"]

create_table   := CREATE TABLE [IF NOT EXISTS] ident "(" table_item ("," table_item)* ")"
table_item     := col_def | PRIMARY KEY "(" ident ")"
col_def        := ident data_type (NOT NULL | PRIMARY KEY)*
data_type      := INTEGER | INT | BIGINT
                | REAL | DOUBLE | FLOAT
                | TEXT | VARCHAR | VARCHAR "(" positive_int ")"
                | BOOLEAN | BOOL

insert         := INSERT INTO ident ["(" ident ("," ident)* ")"]
                  VALUES tuple ("," tuple)*
tuple          := "(" expr ("," expr)* ")"

select         := SELECT ("*" | ident ("," ident)*) FROM ident [WHERE expr]

update         := UPDATE ident SET assign ("," assign)* [WHERE expr]
assign         := ident "=" expr

delete         := DELETE FROM ident [WHERE expr]

expr           := or_expr
or_expr        := and_expr (OR and_expr)*
and_expr       := not_expr (AND not_expr)*
not_expr       := NOT not_expr | cmp_expr
cmp_expr       := primary [ ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") primary
                            | IS [NOT] NULL ]
primary        := literal
                | "-" numeric_literal
                | "?"
                | ident
                | "(" expr ")"
literal        := integer_lit | real_lit | string_lit | TRUE | FALSE | NULL
```

Notes on specific constructs:
- `VARCHAR(n)` enforces a maximum length of `n` UTF-8 characters (`1 <= n <= max_text_bytes`).
- Updating a `PRIMARY KEY` column in `UPDATE ... SET` is rejected as `SqlError::Unsupported`.
- Recognized SQL outside this grammar (`JOIN`, comma joins, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`, `UNION`/`INTERSECT`/`EXCEPT`, aggregates/function calls, subqueries, `CREATE INDEX`, `DROP`, `ALTER`, `BEGIN`/`COMMIT`/`ROLLBACK`, `RETURNING`, `ON CONFLICT`, `DEFAULT`, `CHECK`, `FOREIGN KEY`/`REFERENCES`, `UNIQUE`, `IN`, `BETWEEN`, `LIKE`, `CASE`, `CAST`, arithmetic operators, qualified `table.column`, aliases `AS`) fails with `SqlError::Unsupported` naming the feature.

## Types, coercion, and NULL semantics

Runtime values use `SqlValue`:
- `Null`
- `Integer(i64)` (`INTEGER`, `INT`, `BIGINT`)
- `Real(f64)` (`REAL`, `DOUBLE`, `FLOAT`; must be finite—`NaN`, `+Inf`, and `-Inf` are rejected as `SqlError::Type`)
- `Text(String)` (`TEXT`, `VARCHAR`, `VARCHAR(n)`)
- `Boolean(bool)` (`BOOLEAN`, `BOOL`)

Type rules:
- **No implicit coercion except**: an `INTEGER` literal or `INTEGER` parameter (`?`) may be stored into a `REAL` column if and only if it is **exactly** representable as IEEE 754 `f64` (`(n as f64) as i128 == n as i128`). Non-representable integers such as `(1 << 53) + 1` or `i64::MAX` fail with `SqlError::Type`. Assigning an `INTEGER` column expression into a `REAL` column is a `Type` error.
- **Comparisons**:
  - `INTEGER` and `REAL` compare numerically with exact precision (no `i64 -> f64` rounding).
  - `TEXT` compares by UTF-8 byte order.
  - `BOOLEAN` compares `FALSE < TRUE`.
  - Comparisons with `NULL` (literal or parameter) are permitted and evaluate to `UNKNOWN` (`SqlValue::Null`).
  - All other mismatched comparison types fail with `SqlError::Type` at bind time before scanning rows (even on an empty table).
- **Three-valued logic (3VL)**:
  - `NOT UNKNOWN` -> `UNKNOWN`.
  - `FALSE AND UNKNOWN` -> `FALSE`; `TRUE AND UNKNOWN` -> `UNKNOWN`.
  - `TRUE OR UNKNOWN` -> `TRUE`; `FALSE OR UNKNOWN` -> `UNKNOWN`.
  - `IS NULL` and `IS NOT NULL` always yield `TRUE` or `FALSE`.
  - `WHERE` keeps a row if and only if the predicate evaluates to `TRUE`. `WHERE` / `AND` / `OR` / `NOT` reject non-boolean non-null operands with `SqlError::Type`.

## Constraints, storage mapping, and ordering guarantee

- Every table is stored as one `DocumentDatabase` collection plus a durable catalog entry stored atomically in the same transaction under the raw-key prefix `\0emda-sql\x01T<table_name>`.
- **Do not write SQL tables through `DocumentDatabase` or the raw key-value APIs.** Raw keys under `\0emda-sql\x01T` are reserved by convention only (only `\0emda-document\x01` is enforced as reserved by the document layer). Writing a SQL collection directly through `DocumentDatabase` or raw keys can bypass SQL constraints (`VARCHAR(n)`), corrupt catalog entries, or introduce mismatched document IDs.
- **Row integrity check**: On every row decode (across point, range, and full-scan paths), the SQL layer verifies that the document ID equals the canonical encoding of the row's primary-key column value. Any mismatch returns `SqlError::Io` (`io::ErrorKind::InvalidData`) naming the table.
- Creating a table also installs a matching `CollectionSchema` (`required: true`, `nullable: !col.not_null`) as defense in depth.
- Every table must declare **exactly one** `PRIMARY KEY` column of type `INTEGER` or `TEXT`. The primary key column is implicitly `NOT NULL`.
- Duplicate primary keys (either against existing rows or within a multi-row `INSERT`) and `NOT NULL` / `VARCHAR(n)` violations fail with `SqlError::Constraint` and roll back the entire statement.
- **Primary-key ordering guarantee**:
  - `INTEGER` primary keys encode `(value ^ i64::MIN) as u64` as `i:<16 lowercase hex digits>`.
  - `TEXT` primary keys encode `t:<utf8_bytes>` (maximum `254` UTF-8 bytes to fit the 256-byte document ID limit; longer keys fail with `SqlError::Bounds`).
  - Ascending document ID byte order equals ascending SQL primary-key order (`i64` numerical order for `INTEGER`; UTF-8 byte order for `TEXT`).
  - **Slice 1 guarantees that `SELECT` without `ORDER BY` returns rows in ascending primary-key order**.

## Access paths (`SELECT`, `UPDATE`, `DELETE`)

After bind-time validation (table/column resolution and expression type checking), the planner selects one of four access paths:

- **Empty short-circuit**: If a top-level `AND` conjunction (nested parentheses allowed) contains a `NULL` comparison on the primary key (`pk <op> NULL` or `NULL <op> pk` for `=`, `<>`, `<`, `<=`, `>`, `>=`), contradictory primary-key bounds (such as `id >= 10 AND id <= 5`), or an impossible `INTEGER` primary-key equality (such as `id = 3.5` or an out-of-`i64`-range `REAL`), the statement visits zero rows without scanning and returns an empty result (`0` rows).
- **Point access (`DocumentTransaction::get`)**: When the `WHERE` expression is a top-level `AND` conjunction (nested parentheses allowed) containing `pk = <literal or ?>` (in either operand order) and consistent with any other primary-key conjuncts, the engine fetches only that single document by its encoded primary-key ID and then evaluates the full `WHERE` predicate on it. Cross-type comparisons (`INTEGER` primary key vs exact finite `REAL` such as `id = 3.0`) use the exact same semantics as a full scan.
- **Range access (`DocumentTransaction::scan_documents`)**: When the `WHERE` expression is a top-level `AND` conjunction containing one or more primary-key inequality conjuncts (`>`, `>=`, `<`, `<=` against a literal or `?` parameter in either operand order) without `=`, the planner computes tight intersected lower and upper encoded-ID bounds (rounding `REAL` bounds exactly for `INTEGER` primary keys; using UTF-8 byte order for `TEXT` primary keys) and scans only that ID range via `DocumentTransaction::scan_documents`, starting after the lower bound and stopping at the first document ID past the upper bound.
- **Full-collection scan (`DocumentTransaction::scan_documents`)**: Statements without `WHERE` and predicates outside the top-level conjunction forms above (such as top-level `OR`, `NOT`, `<>`, `IS [NOT] NULL`, or non-PK filters) scan the collection in ascending ID order.

`SqlLimits::max_visited_rows` counts only rows actually read by the chosen access path, so point lookups and bounded ranges over a subset within `max_visited_rows` succeed even when the table contains more than `max_visited_rows` total rows. During scans, unreferenced columns are not decoded into `SqlValue`s and expression evaluation borrows directly from the decoded document without cloning rows.

## Bounds (`SqlLimits`)

All bounds live in `SqlLimits` (implementing `Default`), configurable via `SqlDatabase::with_limits` or `SqlDatabase::set_limits`. Values outside `[1, hard_max]` (`[0, hard_max]` for `max_params`) are rejected with `SqlError::Bounds`.

| Bound | Default | Hard maximum | Enforced on |
| --- | ---: | ---: | --- |
| `max_sql_bytes` | 65,536 (64 KiB) | 65,536 | `prepare` / `execute` input length |
| `max_tokens` | 4,096 | 4,096 | Non-EOF lexical tokens in `prepare` |
| `max_expr_depth` | 64 | 64 | Recursive expression nesting depth |
| `max_columns` | 64 | 64 | Columns per table / `INSERT` / `SELECT` / `UPDATE` |
| `max_insert_rows` | 1,024 | 1,024 | Rows per `INSERT` statement |
| `max_mutation_rows` | 1,024 | 1,024 | Rows modified per `UPDATE` or `DELETE` |
| `max_select_rows` | 10,000 | 10,000 | Rows returned per `SELECT` |
| `max_visited_rows` | 32,768 | 131,072 | Rows visited while scanning in one statement |
| `max_params` | 1,024 | 1,024 | Positional `?` parameters |
| `max_text_bytes` | 262,144 (256 KiB) | 262,144 | UTF-8 bytes of a `TEXT` literal/param/value |
| Identifier length | 128 | 128 | Table and column names (UTF-8 bytes) |
| `TEXT` primary key bytes | 254 | 254 | Document ID 256-byte limit (`"t:"` + key) |
| Row serialized bytes | 1 MiB | 1 MiB | Document layer `MAX_DOCUMENT_BYTES` |
| Atomic batch bytes | 16 MiB | 16 MiB | Storage `MAX_ATOMIC_BATCH_BYTES` per statement |

Statements never silently truncate: exceeding any bound fails with `SqlError::Bounds` and commits nothing.

## Error classes (`SqlError`)

- `Syntax { offset, message }`: lexical or grammar error with byte offset.
- `Unsupported { feature, offset }`: recognized SQL outside slice 1.
- `Type(String)`: type mismatch, non-finite float, or non-exact `INTEGER` -> `REAL` coercion.
- `Constraint(String)`: duplicate primary key, `NOT NULL`, `VARCHAR(n)` overflow, or missing/invalid `PRIMARY KEY` definition.
- `NoSuchTable(String)` / `NoSuchColumn { table, column }`: unknown table or column.
- `TableExists(String)`: `CREATE TABLE` on an existing table/collection without `IF NOT EXISTS`.
- `Bounds(String)`: exceeded `SqlLimits`, positively identified storage/document size or batch/quota limits, or `Store` snapshot budget.
- `ParameterCount { expected, actual }`: wrong number of `params` passed to `execute` / `execute_prepared`.
- `Conflict(io::Error)`: pre-commit transaction conflict (`database_engine::is_transaction_conflict` is `true`).
- `OutcomeUnknown(io::Error)`: commit I/O failure whose persistence is uncertain; caller must reconcile before retrying.
- `Io(io::Error)`: other storage/I/O errors (including non-bounds `InvalidInput` rejections such as document schema validation failures, `InvalidData` row-integrity failures, and non-conflict `WouldBlock` admission limits).

## Atomicity, durability, and backend behavior

Every SQL statement runs inside a single `DocumentDatabase::transaction_detailed` call using `DocumentTransactionMode::ReadSet`:
- **Read-set concurrency**: Point gets, inserts, updates, and deletes record precise point dependencies (and scans record range dependencies) instead of conflicting globally on unrelated writes. On both `Store` and `CheckpointStore`, concurrent statements touching disjoint tables or disjoint primary keys commit with zero conflicts; overlapping read/write sets (such as concurrent updates to the same row or concurrent inserts of the same key) are rejected pre-commit as `SqlError::Conflict(err)`. Retained backends serialize transactions on their exclusive handle.
- **Atomicity**: A failure at any point (including row 2 of a multi-row `INSERT` or `UPDATE`) aborts the transaction; partial writes are never committed.
- **Durability**: Successful writes commit in one synchronized `DB02` atomic batch record and survive drop and reopen.
- **Conflicts and unknown outcomes**: Pre-commit conflicts surface as `SqlError::Conflict(err)` and commit I/O failures surface as `SqlError::OutcomeUnknown(err)` unchanged. The SQL layer **never retries automatically** and **never reports success for an uncertain commit**.
- **`Store` vs `CheckpointStore` snapshot boundary**:
  - Ordinary `Store` transactions capture an in-memory key-index snapshot capped at **16,384 live keys and 4 MiB of live key bytes** (`MAX_SNAPSHOT_KEYS` / `MAX_SNAPSHOT_KEY_BYTES`). Once a `Store` holds more than 16,384 live keys (including 2 keys per SQL table for collection metadata and catalog entry), subsequent `Store` SQL statements fail before execution with `SqlError::Bounds` (`snapshot key count limit exceeded`).
  - `CheckpointStore` transactions do not capture a full-store snapshot and can hold and scan tables beyond 16,384 rows (subject to `CheckpointStore`'s suffix-overlay limits between checkpoint rollovers and `SqlLimits::max_visited_rows`).

## Implemented vs planned

- **Implemented (Slice 1)**: `CREATE TABLE [IF NOT EXISTS]`, `INSERT`, `SELECT` (`*` or column list, `WHERE`), `UPDATE`, `DELETE`, `SqlValue`/`SqlLimits`/`SqlError`/`SqlStatement` prepared reuse, 3VL expressions, exact numeric comparisons, ascending PK order guarantee, primary-key point (`=`) and range (`>`, `>=`, `<`, `<=`) access paths with bounded full-collection scan fallback via `DocumentTransaction::scan_documents`, `ReadSet` optimistic concurrency, and row PK/document-ID integrity verification.
- **Planned (later slices)**: `ORDER BY`, `LIMIT` / `OFFSET`, secondary indexes (`CREATE INDEX`) and cost-based SQL query planning, `JOIN`, `GROUP BY` / aggregates, subqueries, multi-statement explicit SQL transactions (`BEGIN` / `COMMIT` / `ROLLBACK`), schema migrations (`ALTER` / `DROP`), and `EXPLAIN` / SQL diagnostics.
