# Grouped integer statistics comparison, 2026-11-04

On this small read-only workload, SQLite's grouped scan had lower latency than the prototype. The engines use different data representations, so these results do not establish a general performance ranking.

The workload groups 64 or 256 rows into eight string groups and calculates row counts plus counts, sums, minima and maxima for two signed-integer fields, with some null values in the second field. Every timed result is checked against the other engine outside the timing interval. The prototype reads and validates JSON documents inside a transaction; SQLite uses normalized columns and a prepared statement with a full table scan and temporary B-tree. The SQLite adapter does not parse JSON, so the work is not identical. Average calculation, writes, concurrency, independent backup/restore and power failures are not measured.

Each result below is the median of four round medians, in microseconds. Each round contains 30 measured queries per engine after five warmups. Execution order alternates within each process; the two runs were performed separately.

| Documents | Prototype, first | SQLite, first | Prototype, repeat | SQLite, repeat |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 70.06 | 27.72 | 70.17 | 27.78 |
| 256 | 271.45 | 89.64 | 268.27 | 88.44 |

For 256 documents, the prototype's four first-run medians ranged from 269.16 to 275.99 µs and its repeat medians from 266.73 to 273.68 µs. SQLite's corresponding ranges were 88.90–89.93 µs and 87.82–89.31 µs. With only 30 samples per round, the nearest-rank p99 is the maximum observed sample: prototype round p99s ranged from 310.35 to 425.02 µs in the first run and 330.46 to 471.67 µs in the repeat; SQLite ranged from 115.64 to 189.47 µs and 111.95 to 277.31 µs. These tails are highly sensitive to interruption and are not service-level guarantees.

The benchmark used revision `8b2fe82`, SQLite 3.49.1 with WAL and `synchronous=FULL`, a local tmpfs, and an Intel Xeon Platinum 8573C VM with a four-CPU cgroup quota. The harness pinned the benchmark and driver to separate available CPUs, recorded VM telemetry and checked `SCAN docs; USE TEMP B-TREE FOR GROUP BY`. No cgroup throttling was recorded in either run; the highest observed VM steal fraction was 7.69% in the first run and zero in the repeat. Setup and the prototype reopen check were outside timers. The VM and shared process prevent per-engine resource attribution.

The [first run](../benchmarks/results/2026-11-04-aggregate-stats-primary.json) and [repeat](../benchmarks/results/2026-11-04-aggregate-stats-repeat.json) contain every sample, configuration, executable hash and telemetry. The reproducible harness is `examples/aggregate_stats_bench.rs` with `scripts/aggregate_stats_compare.py`. A separate [23,960-input model fuzz run](../benchmarks/results/2026-11-04-aggregate-stats-fuzz.json) checks generated document changes, filters, grouping and reopenings on both writable backends; it is not an exhaustive correctness or crash test.
