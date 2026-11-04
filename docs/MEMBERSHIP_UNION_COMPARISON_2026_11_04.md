# Two-value document membership comparison — 2026-11-04

A single-client, warm, in-process comparison found that the prototype's new planned single-field membership union is substantially faster than its collection scan on these small selective datasets. SQLite's indexed path was faster than the prototype's indexed path in every sampled case. These results are narrow and do not establish a general database ranking.

| Documents / matches | Prototype scan | Prototype union | SQLite scan | SQLite index |
| --- | ---: | ---: | ---: | ---: |
| 100 / 10, primary | 82.85 | 18.56 | 35.35 | 14.44 |
| 1,000 / 10, primary | 794.16 | 21.51 | 286.96 | 17.33 |
| 1,000 / 100, primary | 832.37 | 168.94 | 350.06 | 97.03 |
| 100 / 10, repeat | 83.10 | 18.56 | 35.44 | 14.44 |
| 1,000 / 10, repeat | 790.23 | 21.88 | 290.07 | 17.34 |
| 1,000 / 100, repeat | 840.61 | 171.81 | 346.68 | 96.41 |

Values are microseconds, reported as the median of four run-level medians, each calculated from 50 timed queries after 10 warmups. For 1,000 / 10, the median of the run-level nearest-rank p95 samples was 30.44 µs for the prototype union and 25.57 µs for SQLite's index in the primary run; the repeat was 31.39 and 36.62 µs. These small samples and a shared VM do not establish production tail latency.

The workload asks for two populated group values using one nonunique scalar index. It compares the complete prototype scan, the planned union of two indexed probes, a SQLite forced primary-table scan and SQLite's expression index. All paths return and verify the same IDs and decoded JSON on each query, and both databases are reopened and checked after timing. The prototype performs its own document, transaction and index checks; SQLite has different validation and execution costs, so the APIs and guarantees are not identical. SQLite 3.49.1 used WAL with `synchronous=FULL`; the prototype synchronizes successful mutations. Setup, writes and reopen verification are outside operation timers. These settings are not proof of equivalent durability or physical power-loss behavior.

The [primary raw report](../benchmarks/results/2026-11-04-membership-union-primary.json) and [repeat](../benchmarks/results/2026-11-04-membership-union-repeat.json) contain every sample, source and binary hashes, actual SQLite plans and VM telemetry. SQLite selected `SEARCH docs USING INDEX docs_group` and a temporary B-tree for `ORDER BY`. The clean source revision was `e994b032ddcf95674ecadd4b43cfff8c5c466a6f`. Runs used Linux 6.18.57 on an Intel Xeon Platinum 8573C virtualized host, an overlay filesystem, a four-CPU cgroup quota, benchmark CPU 0 and driver CPU 4. The primary reported 12/12 quiet pre-run windows, the repeat 11/12, and neither reported cgroup CPU throttling during a run; these checks cannot eliminate host interference. Peak RSS combines both databases in one process and is not a per-engine memory comparison.

The comparison excludes concurrent clients, cold cache, write throughput, larger or skewed datasets, cross-process or network overhead, recovery-time distributions, physical power loss, and off-host restore. The planner remains rule-based, supports this union only for wholly indexable scalar values on a single-field index, and falls back when eligibility is not proven. See [document semantics and limits](DOCUMENT_MODEL.md).
