# Performance results

Each table compares engines within one recorded workload, at the revision
and on the host its report names. Follow the report links for engine
settings, raw samples and reproduction instructions; the
[benchmark catalog](BENCHMARKS.md) describes how results are recorded and
the [comparison method](COMPARISON_METHOD.md) how adapters are matched.
Historical tables describe the build they measured, not current `main`.
Wins and losses are both shown. No result here is a general ranking.

MySQL and MariaDB have no results in any table on this page. No valid
measurement of either has been recorded.

## SQL against SQLite and PostgreSQL

Same-session run at `75f07870` (engine code identical to `12e6de8f`) on
macOS (Apple M6, APFS). EMDA and SQLite run in process; PostgreSQL 17.12 is
a local server reached through `pgbench` with prepared statements over a
private Unix socket, so its figures include the protocol round trip. All
three sync every acknowledged write through to the drive (`F_FULLFSYNC`,
SQLite `fullfsync`, PostgreSQL `fsync_writethrough`) and run serializable
transactions. p50 ranges over two runs and both EMDA backends.

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

EMDA beat PostgreSQL on `COUNT(*)` in every sample and on the point join in
one of two `Store` runs, and was slower than both engines on every other
workload. The indexed rows ran on EMDA's `CheckpointStore` only.
[Report, workload differences and raw results](../benchmarks/results/2026-11-28-sql-sqlite-postgresql/README.md)

## Embedded SQL against SQLite at `12e6de8f`

Paired run at `12e6de8f` on macOS (Apple M6, APFS). Both engines sync every
acknowledged write (SQLite WAL with `synchronous=FULL` and `fullfsync`).
p50 ranges over two runs and both EMDA backends. The SQL fixture has
10,000 rows; the join fixture 2,000 users.

| Operation | EMDA | SQLite | EMDA / SQLite |
| :-- | --: | --: | --: |
| Parse a statement | 7.1–7.8 µs | 11.8–13.5 µs | 0.5–0.7 |
| Primary-key SELECT | 23–24 µs | 13 µs | 1.8–1.9 |
| One-row INSERT | 3.93–3.94 ms | 3.07–3.89 ms | 1.0–1.3 |
| One-row UPDATE | 3.97–3.98 ms | 3.10–3.92 ms | 1.0–1.3 |
| One-row DELETE | 3.96–3.98 ms | 3.11–3.14 ms | 1.3 |
| Ten point UPDATEs in one transaction | 3.97–3.98 ms | 3.08–3.98 ms | 1.0–1.3 |
| Predicate scan of 10,000 rows | 2.83–3.13 ms | 0.97–1.17 ms | 2.4–3.2 |
| Indexed equality (`CheckpointStore`) | 34–37 µs | 15 µs | 2.2–2.5 |
| Ten-row index range (`CheckpointStore`) | 55–59 µs | 13 µs | 4.1–4.6 |
| Insert with one index (`CheckpointStore`) | 2.88–2.96 ms | 3.06–3.90 ms | 0.7–1.0 |
| Insert with three indexes (`CheckpointStore`) | 2.98–3.97 ms | 3.03–3.08 ms | 1.0–1.3 |
| `COUNT(*)` | 36–143 µs | 4.0–5.1 µs | 7–36 |
| `SUM(...) WHERE` | 1.91–2.25 ms | 0.37–0.38 ms | 5.0–6.2 |
| `GROUP BY` over 100 groups | 2.10–2.36 ms | 1.85–1.87 ms | 1.1–1.3 |
| Point join | 11–21 µs | 4.5–5.5 µs | 2.0–4.6 |
| One-to-many join | 12–28 µs | 5.9–7.3 µs | 1.7–4.7 |
| Join of one team's users, ordered | 110–156 µs | 38–44 µs | 2.5–4.1 |
| Three-table join with `LIMIT` | 106–281 µs | 29–35 µs | 3.1–9.6 |

EMDA parsed faster and won the one-index insert; durable writes were within
about 1.3×. Every read was slower, from about 1.1× (`GROUP BY`) to 36×
(`COUNT(*)` on `CheckpointStore`). The indexed rows ran on `CheckpointStore`
only, because with three indexes the table exceeds the 16,384-key snapshot
limit of the ordinary `Store`.
[Report and raw results](../benchmarks/results/2026-11-27-sql-vs-sqlite-12e6de8f/README.md)

### SQL baseline at `b739f6dc`

The same harness earlier the same day, before the count, scan, index and
join work. Kept as the reference for those changes.

| Operation | EMDA | SQLite |
| :-- | --: | --: |
| Parse a statement | 6–7 µs | 9–13 µs |
| Primary-key SELECT | 26–28 µs | 13 µs |
| Single-row INSERT, UPDATE or DELETE | 3.9–4.0 ms | 2.8–3.9 ms |
| Ten point UPDATEs in one transaction | 1.5–4.0 ms | 1.1–3.1 ms |
| Predicate scan | 4.1–4.3 ms | 0.7–0.8 ms |
| Indexed equality (CheckpointStore) | 23–42 µs | 14–16 µs |
| Ten-row index range (CheckpointStore) | 47–62 µs | 12–14 µs |
| Insert with one or three indexes (CheckpointStore) | 1.1–4.0 ms | 2.1–3.9 ms |
| `COUNT(*)` | 3.1–3.3 ms | 4–6 µs |
| `SUM(...) WHERE` | 3.3–3.4 ms | 0.38–0.46 ms |
| `GROUP BY` over 100 groups | 3.6–3.7 ms | 1.9–2.1 ms |
| Point join | 12–29 µs | 2–5.5 µs |
| One-to-many join | 18–42 µs | 2.7–7.1 µs |
| Ordered join of one team's users | 350–460 µs | 16–44 µs |
| Three-table join with `LIMIT` | 370–420 µs | 11–35 µs |

[Report and raw results](../benchmarks/results/2026-11-27-sql-vs-sqlite/README.md)

## Byte-key operations across ten engines

Historical Linux VM baseline, November 3, EMDA 0.1.0. Values are median
operations per second. The first column uses one client. The other workload
columns use four. Values are 64 bytes unless noted.

| Engine | Mixed · 1 client | Mixed · 4 clients | Reads only | Writes only | Mixed · 4 KiB | CRUD cycle |
| :-- | --: | --: | --: | --: | --: | --: |
| EMDA 0.1.0 | 4,862 | 10,397 | 30,000 | 5,825 | 5,709 | 7,547 |
| SQLite 3.49.1 (Rust) | 3,701 | 4,514 | 28,963 | 3,148 | 3,869 | 3,478 |
| SQLite 3.53.1 (Python) | 4,046 | 4,694 | 17,011 | 3,060 | 3,075 | 3,298 |
| PostgreSQL 18.6 | 3,710 | 6,868 | 13,745 | 5,074 | 4,218 | 4,591 |
| Redis 8.2.11 | 3,103 | 6,280 | 16,408 | 5,438 | 3,203 | 3,507 |
| MongoDB 8.0.34 | 1,607 | 3,786 | 5,798 | 3,046 | 2,944 | 2,579 |
| Turso / pyturso 0.8.3 | 2,933 | 1,929 | 8,215 | 1,405 | 1,404 | 1,247 |
| RocksDB 9.8.4 | 4,698 | 4,482 | 26,513 | 2,721 | 3,241 | 2,987 |
| LMDB 0.9.36 | 3,847 | 4,339 | 27,993 | 2,337 | 4,020 | 3,127 |
| DuckDB 2.0.0 | 595 | 1,249 | 2,942 | 833 | 1,172 | 929 |
| FoundationDB 7.3.79 | 767 | 2,494 | 5,797 | 2,019 | 2,195 | 1,887 |

The harness mixes in-process adapters and loopback services, and durability
settings are engine-specific, so the adapters are not matched; the report
lists each adapter's settings, read and write p50/p99, and run-to-run
variation.
[Full byte-key matrix](LOCAL_COMPARISON_2026_11_03.md)

## JSON document operations

Historical single-client Linux VM baseline, November 4–5. Values are p50
microseconds. CRUD used a small hot dataset. The compound range query is
from a separate run over 1,000 documents with ten matches.

| Operation | EMDA | SQLite | PostgreSQL | MongoDB |
| :-- | --: | --: | --: | --: |
| Create | 262 | 228 | 526 | 874 |
| Read by ID | 5.64 | 3.29 | 233 | 479 |
| Replace | 259 | 230 | 581 | 917 |
| Unique index lookup | 8.24 | 3.45 | 243 | 583 |
| Delete | 264 | 227 | 533 | 913 |
| Compound index range | 23.56 | 14.09 | 184.81 | 578.75 |

EMDA and SQLite ran in-process. PostgreSQL and MongoDB figures include
Python driver and loopback overhead, so those columns are not matched with
the in-process ones.
[Document CRUD](DOCUMENT_COMPARISON_2026_11_04.md) ·
[Compound range](COMPOUND_RANGE_CURRENT_2026_11_05.md)

## Serializable transactions

Eight clients ran durable, serializable two-key transactions on disjoint
keys on macOS, through EMDA's ordinary store transaction API.

| Engine | Transactions/s | p99 latency |
| :-- | --: | --: |
| EMDA ordinary store, read-set validation | 642–1,114 | 14.1–35.4 ms |
| SQLite | 261–487 | 5.8–14.9 ms |
| PostgreSQL 17 | 1,254–2,766 | 7.7–10.1 ms |

EMDA led SQLite on throughput here. PostgreSQL led on throughput, and both
engines had lower median latency with one client. Shared-key workloads
showed more retries and much higher tail latency in EMDA and PostgreSQL.
[Transaction benchmark and raw results](../benchmarks/results/2026-11-26-transaction-engine-comparison/README.md)

## Recovery and resources

- **Clean reopen, 262,144 initial keys.** On macOS, EMDA's ordinary store
  reached the first read in a median 80.88 ms at 33.70 MiB peak process RSS.
  Retained storage took 50.27 ms at 2.92 MiB. SQLite took 1.20 ms at 3.17 MiB.
  The report includes Linux, full-scan and write results.
  [Recovery comparison](../benchmarks/results/2026-11-22-recovery-sqlite-matrix/README.md)
- **Retained storage, 1,048,576 keys.** A warm-cache macOS reference workload
  used 9.7–11.5 MB peak worker RSS and a fresh process reached the first read
  after cleanup in 103–132 ms. Some write stalls exceeded one second.
  [Reference workload and raw results](../benchmarks/results/2026-11-26-reference-deployment-r1/README.md)

## Durability evidence

- **Physical power cut.** One Raspberry Pi 5 run on Samsung NVMe with ext4
  recovered all 497 acknowledged atomic batches (994 values) after power
  returned. [Power-cut test](../benchmarks/results/2026-11-26-physical-power-cut/README.md)
- **Off-host restore.** An ordinary-store backup created on macOS restored
  and verified all 99 expected records on a separate Linux host.
  [Restore test](../benchmarks/results/2026-11-26-offhost-store-backup-restore/README.md)
