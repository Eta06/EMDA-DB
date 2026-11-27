# Embedded SQL against SQLite at 12e6de8f

Two runs each of `examples/sql_bench.rs` (10,000 rows, 100 operations) and
`examples/sql_join_bench.rs` (2,000 users, 100 operations) at `12e6de8f`,
with the same harness, sizes and settings as the
[`b739f6dc` baseline](../2026-11-27-sql-vs-sqlite/README.md). No other
benchmark or build ran during the measurement.

Host: macOS 27.0, Apple M6, 16 GiB, internal SSD with APFS, Rust 1.100.0,
release build. SQLite 3.49.1 (bundled by `libsqlite3-sys` 0.33.0) through
rusqlite in WAL mode with `synchronous=FULL`, `fullfsync=ON` and
`checkpoint_fullfsync=ON`. EMDA synchronizes every acknowledged write with
`F_FULLFSYNC` (barrier-ordered V2 on this volume). Both engines get the same
secondary indexes in the indexed and join workloads, and every query
returned the same row counts on both engines.

## Results (p50)

Ranges cover both runs and, where both ran, both EMDA backends (`Store` and
`CheckpointStore`). The ratio is EMDA divided by SQLite; below 1 means
EMDA was faster.

| Workload | EMDA | SQLite | EMDA / SQLite |
| --- | ---: | ---: | ---: |
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
| Three-table join with LIMIT | 106–281 µs | 29–35 µs | 3.1–9.6 |

EMDA parsed faster in every sample and won the one-index insert. Durable
single-row writes and the ten-update transaction were within about 1.3× of
SQLite. Every read workload was slower, from about 1.1× (`GROUP BY`) to
36× (`COUNT(*)` on `CheckpointStore`; 7–9× on `Store`).

Against the `b739f6dc` baseline, EMDA's `COUNT(*)` fell from 3.1–3.3 ms
to 36–143 µs. `SUM(...) WHERE` fell from 3.3–3.4 to 1.9–2.3 ms, the
predicate scan from 4.1–4.3 to 2.8–3.1 ms, and the three-table join from
370–420 to 106–281 µs. In this pair SQLite's join latencies were also about
2–5× higher than in some earlier sessions on the same machine, so compare
the join rows within this run rather than across runs.

The indexed workload on the ordinary `Store` did not run at 10,000 rows:
with three indexes the table exceeds the documented 16,384-key limit on a
`Store` transaction's snapshot. That workload ran on `CheckpointStore` only.

## Limits

One machine and filesystem, two runs, warm caches, and synthetic tables.
The ranges do not establish a ranking on other workloads or hardware.
No MySQL, MariaDB, PostgreSQL, MongoDB or Redis results were taken for
these SQL workloads.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every JSON line, the environment, the binary and
source hashes, and `SHA256SUMS`. To reproduce:

```sh
cargo run --release --locked --example sql_bench -- <dir> 10000 100
cargo run --release --locked --example sql_join_bench -- <dir> 2000 100
```
