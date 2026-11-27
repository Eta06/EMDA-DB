# Embedded SQL against SQLite and PostgreSQL at 75f07870

The SQL workloads of `examples/sql_bench.rs` (10,000 rows) and
`examples/sql_join_bench.rs` (2,000 users) on EMDA and SQLite, and the same
tables, indexes, rows and queries on PostgreSQL through
[`scripts/benchmarks/sql_postgres.py`](../../../scripts/benchmarks/sql_postgres.py).
Two runs of each harness, back to back in one session, at `75f07870`, whose
engine code is identical to `12e6de8f`. Nothing else ran during the
measurement.

Host: macOS 27.0, Apple M6, 16 GiB, internal SSD with APFS, Rust 1.100.0.

| Engine | Deployment | Isolation | Durable acknowledgement |
| --- | --- | --- | --- |
| EMDA `Store` and `CheckpointStore` | In process | Serializable (read-set validation) | Log and witness synced before return (`F_BARRIERFSYNC` + `F_FULLFSYNC` here) |
| SQLite 3.49.1 (rusqlite) | In process | Serializable | WAL, `synchronous=FULL`, `fullfsync=ON`, `checkpoint_fullfsync=ON` |
| PostgreSQL 17.12 (`pgbench -M prepared`, one client) | Server over a private Unix socket | `default_transaction_isolation = serializable` | `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, `wal_sync_method=fsync_writethrough` |

PostgreSQL latencies include the client protocol and a socket round trip,
which the in-process engines do not pay; that cost is part of what is
measured. Statement parsing is not compared with PostgreSQL, because
`pgbench` uses prepared statements.

## Results (p50)

Ranges cover both runs and, for EMDA, both backends where both ran.

| Workload | EMDA | SQLite | PostgreSQL 17 | Fastest |
| --- | ---: | ---: | ---: | --- |
| Primary-key SELECT | 24–27 µs | 12.5–13.8 µs | 12 µs | PostgreSQL, SQLite |
| One-row INSERT | 3.90–3.95 ms | 3.80 ms | 2.06–3.71 ms | PostgreSQL |
| One-row UPDATE | 3.94–3.97 ms | 3.05–3.13 ms | 1.01–3.00 ms | PostgreSQL |
| One-row DELETE | 3.91–3.99 ms | 3.17–3.24 ms | 1.10–2.20 ms | PostgreSQL |
| Ten point UPDATEs in one transaction | 3.97–3.98 ms | 3.97 ms | 2.83–3.99 ms | PostgreSQL |
| Predicate scan of 10,000 rows | 2.50–3.15 ms | 0.95–1.17 ms | 0.68–0.70 ms | PostgreSQL |
| Indexed equality (EMDA `CheckpointStore`) | 37–40 µs | 15.4–16.0 µs | 16.0–19.5 µs | SQLite |
| Ten-row index range (EMDA `CheckpointStore`) | 46–61 µs | 12.9–13.1 µs | 18–20 µs | SQLite |
| Insert with one index (EMDA `CheckpointStore`) | 3.88–3.94 ms | 3.83–3.94 ms | 2.78–3.12 ms | PostgreSQL |
| Insert with three indexes (EMDA `CheckpointStore`) | 4.00–4.01 ms | 3.83–3.98 ms | 2.07–3.97 ms | PostgreSQL |
| `COUNT(*)` | 33–134 µs | 2.3–4.2 µs | 228–372 µs | SQLite; EMDA ahead of PostgreSQL |
| `SUM(...) WHERE` | 1.92–2.20 ms | 0.22–0.32 ms | 0.33–0.39 ms | SQLite |
| `GROUP BY` over 100 groups | 2.16–2.37 ms | 1.02–1.53 ms | 0.72–0.81 ms | PostgreSQL |
| Point join | 10–21 µs | 2.0–3.3 µs | 14–15 µs | SQLite |
| One-to-many join | 14–28 µs | 2.7–4.4 µs | 11–24 µs | SQLite |
| Join of one team's users, ordered | 94–149 µs | 16–27 µs | 32–42 µs | SQLite |
| Three-table join with `LIMIT` | 94–281 µs | 13–21 µs | 35–36 µs | SQLite |

EMDA was faster than PostgreSQL on `COUNT(*)` in every sample (EMDA `Store`
33–35 µs, `CheckpointStore` 121–134 µs), and on the point join in one of
the two `Store` runs (10.3 against 14–15 µs; 17.5 µs in the other). It was
slower than both other engines on every other workload. Durable writes cost each engine one to
four milliseconds, dominated by drive-cache flushes. PostgreSQL's single
writes varied most between the two runs (for example UPDATE 1.0 against
3.0 ms).

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
  and 3,298–3,301 on PostgreSQL, whose random `UPDATE` keys set a different
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

The [raw archive] holds every JSON line, the 17 `pgbench` scripts, the
PostgreSQL driver and its reported settings, the environment, binary and
script hashes, and `SHA256SUMS`. To reproduce:

```sh
cargo run --release --locked --example sql_bench -- <dir> 10000 100
cargo run --release --locked --example sql_join_bench -- <dir> 2000 100
python3 -m scripts.benchmarks.sql_postgres <postgresql-bin-dir> <work-dir> <out.jsonl>
```
