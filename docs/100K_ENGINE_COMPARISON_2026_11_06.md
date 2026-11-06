# 100,000-key local comparison — 6 November 2026

On this four-client, warm-key workload, the prototype had higher measured
throughput than the other configurations tested. SQLite had a lower median
write latency in both workloads. These are two exploratory runs per
configuration on one shared VM, not a general database ranking.

The clean source revision was `a2870949277595739f176921318e4e61e3ba5189`.
Each invocation initialized 100,000 eight-byte keys with 64-byte values,
then checked every value's format and checksum after measurement. Four
clients ran either 1,000 mixed operations each with a 50% write probability,
or 750 writes each. The mixed workload reports actual completed operations;
the random choice does not require an exact 50/50 split. Values written by
concurrent clients are not checked against an externally ordered last-write
history in this benchmark.

Each table entry is the median of two run-level results; latency is in
microseconds. The [raw runs](../benchmarks/results/2026-11-06-100k-engine-matrix.json.gz)
and [summary](../benchmarks/results/2026-11-06-100k-engine-summary.json)
retain individual values and ranges.

| Configuration | Mixed ops/s | Read p50 | Write p50 | Write p99 |
| --- | ---: | ---: | ---: | ---: |
| Prototype | 9,455 | 72 | 688 | 1,601 |
| SQLite | 4,201 | 58 | 303 | 36,419 |
| PostgreSQL | 5,580 | 310 | 867 | 7,390 |
| MongoDB | 3,615 | 638 | 1,377 | 3,362 |

| Configuration | Write-only ops/s | Write p50 | Write p99 |
| --- | ---: | ---: | ---: |
| Prototype | 5,341 | 704 | 1,658 |
| SQLite | 3,312 | 241 | 6,249 |
| PostgreSQL | 4,399 | 817 | 2,332 |
| MongoDB | 2,651 | 1,343 | 3,870 |

The prototype used V3 sync-before-acknowledgement; SQLite 3.49.1 used WAL and
`synchronous=FULL`; PostgreSQL 18.6 enabled `fsync`, synchronous commit and
full-page writes; MongoDB 8.0.34 used journaled acknowledgements with its
journal interval tuned to 1 ms. Prototype and SQLite used Rust loopback
adapters, while PostgreSQL and MongoDB used Python drivers and native protocols
through local container port forwarding. The mechanisms and adapters differ;
similar sync settings do not prove equivalent failure guarantees. The
[methodology](COMPARISON_METHOD.md) records setup, validation and limitations.

All 16 pre-run quiet checks passed, with no recorded cgroup throttling,
memory-limit event or direct reclaim during the measured intervals. VM steal
reached 7.14% in one SQLite mixed run. Other short development and verification
work also ran on this VM during the overall campaign. SQLite's mixed p99
ranged from 18.8 to 54.1 ms across its two runs, illustrating the variability
in the tail. The workspace overlay's backing device was not independently
verified. These are closed-loop, warm-cache measurements on an Intel Xeon
Platinum 8573C VM, and do not measure cold recovery, SQL/document queries,
replication, scale-to-zero, total memory or physical power loss. See the
separate [large-fixture restart/resource comparison](SQLITE_RESTART_RESOURCES_2026_11_06.md)
and [process-crash checks](CROSS_ENGINE_PROCESS_CRASH_2026_11_06.md).
