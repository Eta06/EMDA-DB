# Compound range comparison on the current engine

On 2026-11-05 we repeated the [compound equality-prefix and range workload](COMPOUND_RANGE_QUERY_COMPARISON_2026_11_04.md) on revision `fc88463`, using SQLite 3.49.1, PostgreSQL 18.6 and MongoDB 8.0.34. Each query selected a tenant and integer lower bound and returned the same ID-ordered decoded documents. Each run verified results and reopened or reconnected to verify all stored documents. This is one client and is not a crash or restore drill.

Each case has four fresh rounds, with ten warmups and fifty timed queries per path in each round. Times below are microseconds: the median of the four run medians, with the range of those medians in parentheses.

| Documents / matches | Prototype scan | Prototype planned | SQLite scan | SQLite index | PostgreSQL index | MongoDB index |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 / 10 | 87.30 (86.37–87.36) | 20.13 (19.59–20.25) | 40.57 (40.15–40.75) | 12.16 (12.00–12.24) | 167.51 (134.03–179.76) | 410.21 (394.50–709.11) |
| 1,000 / 10 | 831.75 (819.65–859.09) | 23.56 (23.23–25.05) | 330.54 (326.27–338.30) | 14.09 (13.87–15.97) | 184.81 (182.00–227.72) | 578.75 (300.64–743.96) |
| 1,000 / 100 | 883.46 (875.06–891.89) | 184.13 (182.31–184.87) | 404.00 (402.50–406.56) | 89.91 (88.53–90.71) | 603.70 (588.21–634.45) | 893.73 (848.19–952.15) |

SQLite's indexed median was lower in all three cases. PostgreSQL and MongoDB used Python drivers and disposable loopback containers; the prototype and SQLite used in-process Rust adapters. Their differing transport, JSON/BSON handling, validation, plans and durability settings prevent a general engine-speed ranking. Indexed plans were forced or hinted and checked, and the workload uses small hot data and read-only timed queries. It does not measure concurrent writes, cold starts, physical power failure, off-host recovery, total engine memory or project isolation. No performance claim is made for collection deletion.

Prototype writes use a synchronized witnessed log; SQLite uses WAL with `synchronous=FULL`; PostgreSQL uses `fsync`, `synchronous_commit` and `full_page_writes`; MongoDB setup writes use `w=1, j=true`. All 36 pre-run quiet checks passed; across the complete VM/cgroup process windows, steal reached 3.85% and I/O wait 7.69%, with no recorded cgroup CPU throttling or direct reclaim. These observations include setup and verification and are not per-engine resource attribution. The VM had a four-CPU quota, pinned driver and server CPU sets, and an overlay filesystem; its host, backing device and cache state were not independently controlled.

The [raw result](../benchmarks/results/2026-11-05-compound-range-cross-engine.json) retains samples, plans, binary and script hashes, service image identities, settings, telemetry and failure records. It recorded no failures and a clean checkout at `fc88463`. The release binary was built from commit `757db3b`, whose source, benchmark example and dependency manifests match `fc88463`; the difference is the merge commit. The existing [reproduction instructions](BENCHMARKS.md#compound-equality-prefix-and-range-comparison) describe the harness.
