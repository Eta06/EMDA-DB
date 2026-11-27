# Embedded SQL Subset

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
statement      := (create_table | create_index | drop_index | insert | select | update | delete) [";"]

create_table   := CREATE TABLE [IF NOT EXISTS] ident "(" table_item ("," table_item)* ")"
table_item     := col_def
                | PRIMARY KEY "(" ident ")"
                | UNIQUE "(" ident ("," ident)* ")"
col_def        := ident data_type (NOT NULL | PRIMARY KEY | UNIQUE)*
data_type      := INTEGER | INT | BIGINT
                | REAL | DOUBLE | FLOAT
                | TEXT | VARCHAR | VARCHAR "(" positive_int ")"
                | BOOLEAN | BOOL

create_index   := CREATE [UNIQUE] INDEX [IF NOT EXISTS] ident ON ident "(" ident ("," ident)* ")"
drop_index     := DROP INDEX [IF EXISTS] ident ON ident

insert         := INSERT INTO ident ["(" ident ("," ident)* ")"]
                  VALUES tuple ("," tuple)*
tuple          := "(" expr ("," expr)* ")"

select         := SELECT ("*" | ident ("," ident)*) FROM ident
                  [WHERE expr]
                  [ORDER BY order_item ("," order_item)*]
                  [LIMIT limit_val [OFFSET limit_val]]
order_item     := ident [ASC | DESC] [NULLS FIRST | NULLS LAST]
limit_val      := non_neg_integer_lit | "?"

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
- `ORDER BY` column positions (`ORDER BY 1`), expressions or qualified names in `ORDER BY`, standalone `OFFSET` without `LIMIT`, and `ORDER BY` / `LIMIT` / `OFFSET` on `UPDATE` or `DELETE` are rejected as `SqlError::Unsupported`.
- `LIMIT` and `OFFSET` values must be non-negative `INTEGER` literals or `?` parameters (`SqlError::Bounds` for negative `INTEGER` values; `SqlError::Type` for `NULL`, `REAL`, `TEXT`, or `BOOLEAN`; `SqlError::Syntax` for out-of-`i64`-range integer literals).
- Secondary indexes (`CREATE [UNIQUE] INDEX` and `UNIQUE` constraints) support `INTEGER`, `TEXT`/`VARCHAR(n)`, and `BOOLEAN` columns (1–4 columns per index, up to 16 indexes per table). `DROP INDEX [IF EXISTS] name ON table` drops an explicit secondary index cleanly, and refuses dropping an index that backs a table or column `UNIQUE` constraint with `SqlError::Constraint`. `REAL` columns in indexes/unique constraints, partial `CREATE INDEX ... WHERE ...`, `ASC`/`DESC` index ordering, `USING ...`, `DROP INDEX name` (without `ON table`), `DROP INDEX ... CONCURRENTLY`/`CASCADE`/`RESTRICT`, and `DROP TABLE` fail with `SqlError::Unsupported`.
- Recognized SQL outside this grammar (`JOIN`, comma joins, `GROUP BY`, `HAVING`, `DISTINCT`, `UNION`/`INTERSECT`/`EXCEPT`, aggregates/function calls, subqueries, `DROP TABLE`, `ALTER`, `BEGIN`/`COMMIT`/`ROLLBACK`, `RETURNING`, `ON CONFLICT`, `DEFAULT`, `CHECK`, `FOREIGN KEY`/`REFERENCES`, `IN`, `BETWEEN`, `LIKE`, `CASE`, `CAST`, arithmetic operators, qualified `table.column`, aliases `AS`) fails with `SqlError::Unsupported` naming the feature.

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
- Creating a table also installs a matching `CollectionSchema` (`required: col.not_null`, `nullable: !col.not_null`) as defense in depth. SQL `NULL` values are stored by omitting the field from the document JSON object so document-layer secondary and unique indexes omit any row where an indexed column is `NULL`. Rows written by earlier revisions may hold an explicit JSON `null` instead; reads treat both forms as `NULL`, and `CREATE INDEX` rewrites such rows in bounded steps (`<= 128` rows per transaction) to omit the indexed fields before starting the index build.
- Every table must declare **exactly one** `PRIMARY KEY` column of type `INTEGER` or `TEXT`. The primary key column is implicitly `NOT NULL`.
- Duplicate primary keys (either against existing rows or within a multi-row `INSERT`), `UNIQUE` violations, and `NOT NULL` / `VARCHAR(n)` violations fail with `SqlError::Constraint` and roll back the entire statement.
- **Secondary indexes and `UNIQUE` constraints**:
  - `CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (col, ...)` and column/table `UNIQUE` constraints map directly onto document-layer single/compound scalar indexes (`1..=4` columns, up to `16` indexes per table, index names scoped per table).
  - **NULL semantics**: Following PostgreSQL and SQLite, `UNIQUE` permits multiple `NULL` values (including compound tuples such as `(1, NULL)` and `(1, NULL)`), because SQL `NULL` is stored as an absent JSON field and omitted from document indexes.
  - **Populated-table index builds**:
    - Small tables (`<= 256` rows and `<= 32 MiB` of scanned document bytes) build atomically in a single `ReadSet` transaction via `DocumentTransaction::create_index`, updating both document index metadata/keys and the SQL catalog entry together in one `DB02` commit record.
    - Larger populated tables automatically switch to the document layer's bounded resumable online index build (`start_index_build` + `advance_index_build` in `Conservative` transactions of up to `128` rows per commit):
      1. Indexed explicit JSON `null` fields on legacy rows are rewritten away in bounded chunks (`<= 128` rows per commit).
      2. The index is recorded in the SQL catalog in `building` state (`"state": "building"`) alongside the document-layer background job (`start_index_build`), then advanced to completion inside `execute`.
      3. **Planner safety**: The SQL query planner never uses an index in `building` or `dropping` state (`IndexState::Ready` is required), falling back to primary-key or full-scan paths until the build commits its `ready` catalog transition. Concurrent `INSERT` / `UPDATE` / `DELETE` transactions maintain any active `building` job automatically via the document layer and conflict cleanly on overlapping keys; maintenance steps retry confirmed pre-commit conflicts (`SqlError::Conflict`) up to 256 times per step and never retry `SqlError::OutcomeUnknown`.
      4. **Mid-build failures and crashes**: If a unique violation or other failure (outside `OutcomeUnknown`) occurs mid-build, SQL cleans up the partial document index (`cancel_index_build` + bounded `advance_index_drop`) and removes the `building` catalog entry before returning `SqlError::Constraint` (or the underlying `Bounds`/`Io` error). If the process crashes (`SIGKILL`) mid-build, readers see all committed rows via primary-key/full scans without using the incomplete index, and either re-running `CREATE [UNIQUE] INDEX` resumes and completes (or cleans up on unique violation) the `building` index, or `DROP INDEX name ON table` cancels and cleans it up.
  - **`DROP INDEX [IF EXISTS] name ON table`**:
    - Indexes declared explicitly via `CREATE [UNIQUE] INDEX` may be dropped cleanly. Indexes backing a table or column `UNIQUE` constraint (`is_constraint: true`) are protected and reject `DROP INDEX` (even with `IF EXISTS`) with `SqlError::Constraint`.
    - Small indexes (`<= 768` index keys) drop atomically with the catalog entry in one transaction via `DocumentTransaction::drop_index`; larger indexes transition the catalog entry to `dropping`, call `DocumentTransaction::start_index_drop`, and drive `advance_index_drop` in bounded `128`-key steps before removing the catalog entry.
  - **String and key limits**: Indexed `TEXT` values are bounded to `256` UTF-8 bytes (`MAX_INDEX_STRING_BYTES`), and encoded index keys are bounded to `1,024` bytes; exceeding either fails with `SqlError::Bounds` and leaves no partial index or catalog entry.
- **Primary-key ordering guarantee**:
  - `INTEGER` primary keys encode `(value ^ i64::MIN) as u64` as `i:<16 lowercase hex digits>`.
  - `TEXT` primary keys encode `t:<utf8_bytes>` (maximum `254` UTF-8 bytes to fit the 256-byte document ID limit; longer keys fail with `SqlError::Bounds`).
  - Ascending document ID byte order equals ascending SQL primary-key order (`i64` numerical order for `INTEGER`; UTF-8 byte order for `TEXT`).
  - `SELECT` without `ORDER BY` returns rows in ascending primary-key order.

## `ORDER BY`, `LIMIT`, and `OFFSET` semantics and execution

- **NULL ordering**: By default, `NULL` sorts as smallest—`NULLS FIRST` for `ASC` and `NULLS LAST` for `DESC` (matching SQLite). Explicit `NULLS FIRST` or `NULLS LAST` overrides the default.
- **Value comparison**:
  - `INTEGER` and `REAL` compare numerically with exact precision (finite `REAL` with `-0.0 == 0.0`).
  - `TEXT` compares by UTF-8 byte order.
  - `BOOLEAN` compares `FALSE < TRUE`.
- **Deterministic tie-breaking**: Multi-key `ORDER BY` keys are compared left-to-right; remaining ties break deterministically by **primary key ascending**.
- **Unprojected sort keys**: `ORDER BY` may reference any column of the table, even if it is not in the `SELECT` projection list.
- **Execution strategies**:
  - `LIMIT 0` returns `0` rows without scanning or reading rows (after bind-time validation of table, columns, `WHERE`, `ORDER BY`, `LIMIT`, and `OFFSET`).
  - **Streaming ascending PK path**: `SELECT` with no `ORDER BY` or with primary key `ASC` as its first sort key streams from the chosen access path in ascending PK order and stops immediately after `OFFSET + LIMIT` matching rows without sorting or extra row visits. When `WHERE` is absent and `LIMIT` is present, scan page size is also capped to the remaining needed rows.
  - **Descending PK path**: `ORDER BY pk DESC` uses a bounded `VecDeque` ring buffer of `OFFSET + LIMIT` rows when `LIMIT` is present, or collects within `max_select_rows` / `max_sort_rows` and reverses when `LIMIT` is absent.
  - **General `ORDER BY` path**: Non-PK sorts use a bounded top-K max-heap (`BinaryHeap`) retaining at most `OFFSET + LIMIT` rows (bounded by `max_sort_rows`) when `LIMIT` is present, or a full bounded sort (under `max_select_rows` and `max_sort_rows`) when `LIMIT` is absent. Exceeding `max_sort_rows` or `max_select_rows` fails with `SqlError::Bounds` and never silently truncates.

## Statement and catalog caching

`SqlDatabase` maintains bounded in-memory caches for parsed statements (in `execute`), decoded `TableDef` catalog entries, and table-bound execution plans:
- Every statement still runs inside a `ReadSet` transaction and reads `\0emda-sql\x01T<table>` via `tx.get_raw` in the same transaction, comparing the stored catalog bytes against the cached entry.
- Read-set validation covers the catalog key on commit, and any catalog change or drop invalidates the cached `TableDef` and any dependent bound plans immediately so stale definitions are never served.
- `SqlDatabase::set_limits` clears cached statements and bound plans so updated limits take effect immediately.

- **Access-path priority (`SELECT`, `UPDATE`, `DELETE`)**:
  After bind-time validation (table/column resolution and expression type checking), the planner flattens top-level `AND` conjunctions (nested parentheses allowed) and selects one of five access paths:
  1. **Empty short-circuit**: Any `col <op> NULL` comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`), contradictory bounds on any column (such as `id >= 10 AND id <= 5` or `x > 4 AND x < 5` on `INTEGER`), impossible `INTEGER` vs `REAL` equality (such as `n = 3.5` or `n = 2^63`), or `TEXT` equality exceeding `254` bytes (PK) / `256` bytes (secondary index) visits zero rows and returns an empty result.
  2. **Primary-key point access (`DocumentTransaction::get`)**: `pk = <lit|?>` (or tight integer bounds `lo == hi`) fetches only that single document by its encoded primary-key ID and evaluates the full `WHERE` predicate.
  3. **Unique-index equality (`DocumentTransaction::query_index`)**: A `UNIQUE` index whose every column is constrained by `=` visits at most one row via the unique index.
  4. **Primary-key range vs secondary index (`DocumentTransaction::scan_documents` / `query_index`)**: Secondary indexes are eligible when a leading equality prefix (`col = v`, with `0` or more columns) is followed by an optional range (`<`, `<=`, `>`, `>=`) on the next column, and every trailing index column is guaranteed non-`NULL` (`NOT NULL` or `WHERE` proof such as `IS NOT NULL` or comparison with a non-`NULL` value). `INTEGER` bounds against `REAL` operands (including `i64` values above `2^53` and fractional `REAL`s) are converted to exact `i64` bounds matching scan semantics. When both a PK range and a secondary index apply, a small integer PK range (`<= 8` keys) prefers PK range, otherwise non-boolean index equality or a two-sided index range over a one-sided PK range prefers the index; otherwise PK range wins.
  5. **Index-order `ORDER BY ... LIMIT` streaming and full-collection scan fallback**: If `WHERE` did not select a point, range, or index path, `SELECT` with `LIMIT` and `ORDER BY` matching an index whose columns are all guaranteed non-`NULL` (with all `ASC` directions and exact PK tie-break compatibility) streams directly from `query_index` and stops after `OFFSET + LIMIT` matches. Otherwise, execution falls back to a full-collection scan in ascending ID order.
  - Whenever an index scan does not naturally yield rows in the required `ORDER BY` (or default ascending PK) order, matching rows are sorted (or bounded top-K heaped when `LIMIT` is present) with deterministic ascending PK tie-breaking. Every non-streaming path evaluates the full `WHERE` predicate as a residual filter.

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
| `max_sort_rows` | 32,768 | 131,072 | Rows buffered in `ORDER BY` sort / top-K heap |
| `max_visited_rows` | 32,768 | 131,072 | Rows visited while scanning in one statement |
| `max_params` | 1,024 | 1,024 | Positional `?` parameters |
| `max_text_bytes` | 262,144 (256 KiB) | 262,144 | UTF-8 bytes of a `TEXT` literal/param/value |
| Identifier length | 128 | 128 | Table, column, and index names (UTF-8 bytes) |
| `TEXT` primary key bytes | 254 | 254 | Document ID 256-byte limit (`"t:"` + key) |
| Indexed `TEXT` bytes | 256 | 256 | Document-layer `MAX_INDEX_STRING_BYTES` |
| Columns per index | 4 | 4 | Document-layer `MAX_INDEX_FIELDS` |
| Indexes per table | 16 | 16 | Document-layer collection index cap |
| Online `CREATE`/`DROP` step | 128 rows/keys | 128 rows/keys | Bounded chunk size for resumable index build, drop, and explicit-NULL rewrite |
| Row serialized bytes | 1 MiB | 1 MiB | Document layer `MAX_DOCUMENT_BYTES` |
| Atomic batch bytes | 16 MiB | 16 MiB | Storage `MAX_ATOMIC_BATCH_BYTES` per statement |

Statements never silently truncate: exceeding any bound fails with `SqlError::Bounds` and commits nothing.

## Error classes (`SqlError`)

- `Syntax { offset, message }`: lexical or grammar error with byte offset.
- `Unsupported { feature, offset }`: recognized SQL outside slice 1.
- `Type(String)`: type mismatch, non-finite float, or non-exact `INTEGER` -> `REAL` coercion.
- `Constraint(String)`: duplicate primary key, `UNIQUE` violation, `NOT NULL`, `VARCHAR(n)` overflow, missing/invalid `PRIMARY KEY` definition, or attempting `DROP INDEX` on a constraint-backed index.
- `NoSuchTable(String)` / `NoSuchColumn { table, column }` / `NoSuchIndex { table, index }`: unknown table, column, or index.
- `TableExists(String)` / `IndexExists(String)`: `CREATE TABLE` or `CREATE [UNIQUE] INDEX` on an existing table or index without `IF NOT EXISTS`.
- `Bounds(String)`: exceeded `SqlLimits`, positively identified storage/document size or batch/quota limits, or `Store` snapshot budget.
- `ParameterCount { expected, actual }`: wrong number of `params` passed to `execute` / `execute_prepared`.
- `Conflict(io::Error)`: pre-commit transaction conflict (`database_engine::is_transaction_conflict` is `true`).
- `OutcomeUnknown(io::Error)`: commit I/O failure whose persistence is uncertain; caller must reconcile before retrying.
- `Io(io::Error)`: other storage/I/O errors (including non-bounds `InvalidInput` rejections such as document schema validation failures, `InvalidData` row-integrity failures, and non-conflict `WouldBlock` admission limits).

## Atomicity, durability, and backend behavior

Every DML statement (`INSERT`, `UPDATE`, `DELETE`, `SELECT`) and small DDL statement runs inside a single `DocumentDatabase::transaction_detailed` call using `DocumentTransactionMode::ReadSet` (while multi-step online `CREATE INDEX` / `DROP INDEX` / explicit-NULL rewrite steps use `DocumentTransactionMode::Conservative`):
- **Read-set concurrency**: Point gets, inserts, updates, and deletes record precise point dependencies (and scans record range dependencies) instead of conflicting globally on unrelated writes. On both `Store` and `CheckpointStore`, concurrent statements touching disjoint tables or disjoint primary keys commit with zero conflicts; overlapping read/write sets (such as concurrent updates to the same row or concurrent inserts of the same key) are rejected pre-commit as `SqlError::Conflict(err)`. Retained backends serialize transactions on their exclusive handle.
- **Atomicity**: A failure at any point (including row 2 of a multi-row `INSERT` or `UPDATE`) aborts the transaction; partial writes are never committed.
- **Durability**: Successful writes commit in one synchronized `DB02` atomic batch record and survive drop and reopen.
- **Conflicts and unknown outcomes**: Pre-commit conflicts on single-transaction statements surface as `SqlError::Conflict(err)` and commit I/O failures surface as `SqlError::OutcomeUnknown(err)` unchanged. DML statements **never retry automatically** and **never report success for an uncertain commit**; multi-step online `CREATE INDEX` and `DROP INDEX` steps retry confirmed pre-commit `Conflict` errors up to 256 times per step and never retry `OutcomeUnknown`.
- **`Store` vs `CheckpointStore` snapshot boundary**:
  - Ordinary `Store` transactions capture an in-memory key-index snapshot capped at **16,384 live keys and 4 MiB of live key bytes** (`MAX_SNAPSHOT_KEYS` / `MAX_SNAPSHOT_KEY_BYTES`). Once a `Store` holds more than 16,384 live keys (including 2 keys per SQL table for collection metadata and catalog entry, plus index keys), subsequent `Store` SQL statements fail before execution with `SqlError::Bounds` (`snapshot key count limit exceeded`).
  - `CheckpointStore` transactions do not capture a full-store snapshot and can hold and index tables well beyond 16,384 rows (for managed checkpoint configurations, automatic checkpoint rollover keeps the suffix overlay bounded).

## Implemented vs planned

- **Implemented**: `CREATE TABLE [IF NOT EXISTS]` (including column-level `UNIQUE` and table-level `UNIQUE (col, ...)`), `CREATE [UNIQUE] INDEX [IF NOT EXISTS]` (single-tx for small tables and bounded resumable online build for large populated tables), `DROP INDEX [IF EXISTS] name ON table`, `INSERT`, `SELECT` (`*` or column list, `WHERE`, `ORDER BY` with `ASC`/`DESC`/`NULLS FIRST`/`NULLS LAST`, `LIMIT`/`OFFSET`), `UPDATE`, `DELETE`, `SqlValue`/`SqlLimits`/`SqlError`/`SqlStatement` prepared reuse, per-`SqlDatabase` catalog/statement/plan caching, compiled index templates, 3VL expressions, exact numeric comparisons, ascending PK order guarantee and streaming `ORDER BY pk ASC`, index-order `ORDER BY ... LIMIT` streaming, bounded `ORDER BY pk DESC` and top-K / full sort, primary-key point (`=`) and range (`>`, `>=`, `<`, `<=`) access paths, single and compound secondary index equality/range access via `DocumentTransaction::query_index`, bounded full-collection scan fallback via `DocumentTransaction::scan_documents`, `ReadSet` optimistic concurrency, and row PK/document-ID integrity verification.
- **Planned (later slices)**: `JOIN`, `GROUP BY` / aggregates, subqueries, multi-statement explicit SQL transactions (`BEGIN` / `COMMIT` / `ROLLBACK`), schema migrations (`ALTER TABLE` / `DROP TABLE`), and `EXPLAIN` / SQL diagnostics.
