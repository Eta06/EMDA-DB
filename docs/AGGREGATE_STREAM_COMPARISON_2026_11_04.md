# Streaming grouped-statistics comparison, 2026-11-04

Streaming matching documents reduced sampled prototype latency on both tested collection sizes. SQLite's normalized-table scan remained faster. The comparison covers one small, read-only workload and does not establish a universal ranking.

The prior implementation first retained all matching decoded documents, then grouped them. The candidate groups each matching document during the complete scan. It still validates all records, observes the complete transaction range, enforces the same scan and filter budgets, and returns scan or filter errors ahead of an earlier aggregation error. It retains groups and the current scan page; this is not a total-memory limit.

Both binaries execute the same in-process benchmark: eight string groups, two signed-integer fields with some nulls, and count, sum, minimum and maximum. Each result is checked against SQLite outside the timer. SQLite uses a normalized table and a prepared `GROUP BY` full scan with a temporary B-tree; the prototype reads JSON documents and uses a transaction. SQLite does not parse JSON, and the workload does not measure writes, concurrency, power loss, or an independent restore.

Four runs were ordered baseline A, candidate A, candidate B, baseline B. Each contains four rounds of 100 timed queries per engine for each size after five warmups, alternating engine order within a process. The table shows the median of the four round medians in microseconds.

| Documents | Baseline A | Candidate A | Candidate B | Baseline B | SQLite range across runs |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 66.66 | 55.66 | 56.10 | 66.86 | 27.62–27.99 |
| 256 | 269.15 | 209.79 | 210.80 | 269.23 | 88.59–89.69 |

At 256 documents, the candidate median was about 22% lower than the neighboring baseline in each ordering. The median of the four round p99 values was 418.59/418.01 µs in baseline A/B and 299.19/390.42 µs in candidate A/B. Each p99 is based on only 100 samples per round and was variable; it is not a service-level bound. At 64 documents the candidate median fell about 16%. Actual production benefit depends on document size, filtering and workload.

The baseline executable was built from revision `8b2fe82` (the same runtime source as driver revision `7c84244`) and the candidate from the runtime source of `3088fe6`. The candidate benchmark binary was built before the commit was amended to add array-filter fuzz cases; runtime source was unchanged. The binary hashes, all samples and VM telemetry are in [baseline A](../benchmarks/results/2026-11-04-aggregate-stream-baseline-a.json), [candidate A](../benchmarks/results/2026-11-04-aggregate-stream-candidate-a.json), [candidate B](../benchmarks/results/2026-11-04-aggregate-stream-candidate-b.json) and [baseline B](../benchmarks/results/2026-11-04-aggregate-stream-baseline-b.json). Both engines used local tmpfs, SQLite 3.49.1 WAL with `synchronous=FULL`, and separate pinned benchmark/driver CPUs on an Intel Xeon Platinum 8573C VM with a four-CPU cgroup quota. No cgroup throttling was recorded; maximum observed VM steal ranged from 3.57% to 4.55% across runs. Setup and the prototype reopen check were outside timing.

A separate [19,782-input model fuzz run](../benchmarks/results/2026-11-04-aggregate-stream-fuzz.json) exercised document changes, grouping, array filters and reopenings on both writable backends. Targeted tests additionally cover later filter and scan errors taking precedence over an earlier aggregation error, transaction phantoms and recovery. These checks do not establish physical power-loss behavior.
