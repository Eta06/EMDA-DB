# Embedded SQL against SQLite at b739f6dc

Two runs each of `examples/sql_bench.rs` (10,000 rows, 100 operations) and
`examples/sql_join_bench.rs` (2,000 users, 100 operations) at `b739f6dc`, on
an otherwise idle machine.

Host: macOS 27.0, Apple M6, 16 GiB, internal SSD with APFS, Rust 1.100.0,
release build. SQLite 3.49.1 through rusqlite in WAL mode with
`synchronous=FULL`, `fullfsync=ON` and `checkpoint_fullfsync=ON`. EMDA
synchronizes every acknowledged write with `F_FULLFSYNC` (barrier-ordered V2
on this volume). Both engines get the same secondary indexes in the indexed
and join workloads.

## Results (p50)

| Workload | EMDA (`Store` / `CheckpointStore`) | SQLite |
| --- | ---: | ---: |
| Parse a statement | 6–7 µs | 9–13 µs |
| Primary-key SELECT | 26–28 µs | 13 µs |
| One-row INSERT, UPDATE or DELETE | 3.9–4.0 ms | 2.8–3.9 ms |
| Ten point UPDATEs in one transaction | 1.5–4.0 ms | 1.1–3.1 ms |
| Predicate scan of 10,000 rows | 4.1–4.3 ms | 0.7–0.8 ms |
| Indexed equality (`CheckpointStore`) | 23–42 µs | 14–16 µs |
| Ten-row index range (`CheckpointStore`) | 47–62 µs | 12–14 µs |
| Insert with one or three indexes (`CheckpointStore`) | 1.1–4.0 ms | 2.1–3.9 ms |
| `COUNT(*)` | 3.1–3.3 ms | 4–6 µs |
| `SUM(...) WHERE` | 3.3–3.4 ms | 0.38–0.46 ms |
| `GROUP BY` over 100 groups | 3.6–3.7 ms | 1.9–2.1 ms |
| Point join | 12–29 µs | 2–5.5 µs |
| One-to-many join | 18–42 µs | 2.7–7.1 µs |
| Join of one team's users, ordered | 350–460 µs | 16–44 µs |
| Three-table join with LIMIT | 370–420 µs | 11–35 µs |

EMDA parses faster. Durable writes and single-commit transactions are within
about 1.3× of SQLite; flush times varied about threefold between runs on
both engines. Reads are 2× (point) to 600× (`COUNT(*)`) slower. The largest
costs are decoding every row as a JSON document in scans and aggregates,
and reading every row for `COUNT(*)`.

The indexed workload on the ordinary `Store` did not run at 10,000 rows:
with three indexes the table exceeds the 16,384-key limit on a `Store`
transaction's snapshot.

## Limits

One machine and filesystem, two runs, warm caches, and synthetic tables.
The ranges are across the two runs and both EMDA backends; they do not
establish a ranking on other workloads or hardware.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every JSON line, the environment and binary
hashes, and `SHA256SUMS`.
