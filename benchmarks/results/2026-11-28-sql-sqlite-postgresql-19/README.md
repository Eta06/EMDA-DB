# Embedded SQL against SQLite and PostgreSQL 19.0 at c7ac663d

The SQL workloads of `examples/sql_bench.rs` (10,000 rows) and
`examples/sql_join_bench.rs` (2,000 users) on EMDA and SQLite, and the same
tables, indexes, rows and queries on PostgreSQL 19.0, the newest stable
PostgreSQL release on the measurement date, through
[`scripts/benchmarks/sql_postgres.py`](../../../scripts/benchmarks/sql_postgres.py).
Two runs of each harness, interleaved in one session, at `c7ac663d`.
Nothing else ran on the host during the measurement. This run replaces
the [PostgreSQL 17.12 run](../2026-11-28-sql-sqlite-postgresql/README.md)
as the headline comparison; that run stays as a historical baseline.

Host: macOS 27.0, Apple M6, 16 GiB, internal SSD with APFS, Rust 1.100.0.

| Engine | Version | Deployment | Isolation | Durable acknowledgement |
| --- | --- | --- | --- | --- |
| EMDA `Store` and `CheckpointStore` | `c7ac663d` | In process | Serializable (read-set validation) | Log and witness synced before return (`F_BARRIERFSYNC` + `F_FULLFSYNC` here) |
| SQLite (rusqlite) | 3.49.1, bundled by `libsqlite3-sys` 0.33.0 | In process | Serializable | WAL, `synchronous=FULL`, `fullfsync=ON`, `checkpoint_fullfsync=ON` |
| PostgreSQL | 19.0 (Homebrew bottle), `pgbench` 19.0 with `-M prepared`, one client | Server over a private Unix socket | `default_transaction_isolation = serializable` | `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, `wal_sync_method=fsync_writethrough` |

The first row measures PostgreSQL's protocol and socket round trip alone
(`SELECT 1` through the same prepared-statement path). It is part of every
other PostgreSQL latency below and is not paid by the in-process engines.
Statement parsing is not compared with PostgreSQL, because `pgbench` uses
prepared statements.

## Results (p50)

Ranges cover both runs and, for EMDA, both backends where both ran.

| Workload | EMDA | SQLite | PostgreSQL 19.0 | Fastest p50 |
| --- | ---: | ---: | ---: | --- |
| Socket round trip (`SELECT 1`) | — | — | 9–14 µs | PostgreSQL only |
| Primary-key SELECT | 22.5–25.5 µs | 12.0–13.6 µs | 11.0–15.0 µs | PostgreSQL |
| One-row INSERT | 3.94 ms–3.95 ms | 3.01 ms–3.02 ms | 3.05 ms–3.14 ms | SQLite |
| One-row UPDATE | 3.97 ms–3.99 ms | 3.05 ms | 3.96 ms–3.97 ms | SQLite |
| One-row DELETE | 3.97 ms–3.99 ms | 3.03 ms–3.06 ms | 3.97 ms–3.98 ms | SQLite |
| Ten point UPDATEs in one transaction | 1.17 ms–3.97 ms | 1.43 ms–3.95 ms | 3.99 ms | EMDA |
| Predicate scan of 10,000 rows | 2.62 ms–3.08 ms | 520–800 µs | 488–694 µs | PostgreSQL |
| Indexed equality (EMDA `CheckpointStore`) | 33.8–35.4 µs | 15.5–16.0 µs | 20.0 µs | SQLite |
| Ten-row index range (EMDA `CheckpointStore`) | 55.3–58.5 µs | 13.0–13.5 µs | 19.0–19.5 µs | SQLite |
| Insert with one index (EMDA `CheckpointStore`) | 3.89 ms–3.90 ms | 3.94 ms–3.95 ms | 3.02 ms–3.92 ms | PostgreSQL |
| Insert with three indexes (EMDA `CheckpointStore`) | 3.99 ms | 3.97 ms–3.98 ms | 3.89 ms–3.95 ms | PostgreSQL |
| `COUNT(*)` | 34.2–123 µs | 2.8–4.7 µs | 362–368 µs | SQLite |
| `SUM(...) WHERE` | 1.67 ms–2.19 ms | 252–461 µs | 340–341 µs | SQLite |
| `GROUP BY` over 100 groups | 1.93 ms–2.39 ms | 1.28 ms–1.98 ms | 693–704 µs | PostgreSQL |
| Point join | 4.6–14.0 µs | 1.4–2.4 µs | 13.0–14.0 µs | SQLite |
| One-to-many join | 6.0–19.4 µs | 1.8–3.0 µs | 11.0–11.5 µs | SQLite |
| Join of one team's users, ordered | 44.9–123 µs | 9.9–18.6 µs | 31.5–33.5 µs | SQLite |
| Three-table join with `LIMIT` | 45.4–252 µs | 8.0–14.7 µs | 36.0–39.0 µs | SQLite |

What the ranges hide, per run (raw lines in the archive):

- **Wins for EMDA.** `COUNT(*)` was faster than PostgreSQL in every sample
  (`Store` 34–35 µs, `CheckpointStore` 105–123 µs, PostgreSQL
  362–368 µs). In the second run the ten-update transaction on EMDA's
  `Store` took 1.17 ms, against 1.43 ms for SQLite and 3.99 ms for
  PostgreSQL; in the first run all three took 3.95–3.99 ms. In the first
  run EMDA's `Store` also beat PostgreSQL on the point join (4.6 against
  13–14 µs) and the one-to-many join (6.0 against 11–11.5 µs); in the
  second run its point join tied PostgreSQL and its one-to-many join was
  slower.
- **Losses for EMDA.** Every other workload was slower on EMDA than on the
  fastest engine. Single-row durable writes were about 1.3× SQLite. Index
  reads, scans, `SUM` and the ordered and three-table joins were 2–31×
  slower than the fastest engine (up to 31× for the three-table join on
  `CheckpointStore`), and `CheckpointStore` joins were
  slower than `Store` joins.
- **Variation.** EMDA `Store` join latencies differed about threefold
  between the two runs, more than the other engines' did.

The indexed workload ran on EMDA's `CheckpointStore` only: with three
indexes the table exceeds the 16,384-key snapshot limit of the ordinary
`Store`.

## Workload differences

- PostgreSQL picks random keys with `\set ... random()`; the Rust harnesses
  step through keys deterministically. Both stay inside the same tables.
- PostgreSQL's DELETE finds each row it removes, one inserted earlier, with
  `id = (SELECT min(id) ... WHERE id >= 1000000)`, an extra primary-key
  range probe the Rust harnesses do not make.
- The predicate scan returned 3,303 rows on EMDA and SQLite in every run
  and 3,293–3,299 on PostgreSQL, whose random `UPDATE` keys set a different
  number of scores negative.

## Not measured

MySQL, MariaDB, MongoDB and Redis are not installed on this host, and no
SQL results exist for them. The [ten-engine byte-key baseline](../../../docs/LOCAL_COMPARISON_2026_11_03.md)
and the [document comparison](../../../docs/DOCUMENT_COMPARISON_2026_11_04.md)
are the latest measurements that include Redis and MongoDB.

## Limits

One machine and filesystem, two runs of each harness, warm caches, one
client, and synthetic tables. The ranges do not establish a ranking on other
workloads or hardware.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every JSON line, the 18 `pgbench` scripts, the
PostgreSQL driver and its reported settings, the environment, versions,
binary and script hashes, and `SHA256SUMS`. To reproduce:

```sh
cargo run --release --locked --example sql_bench -- <dir> 10000 100
cargo run --release --locked --example sql_join_bench -- <dir> 2000 100
python3 -m scripts.benchmarks.sql_postgres <postgresql-19-bin-dir> <work-dir> <out.jsonl>
```
