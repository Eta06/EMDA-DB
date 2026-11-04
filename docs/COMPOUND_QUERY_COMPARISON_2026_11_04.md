# Compound document equality comparison, 2026-11-04

The experimental planner can use a complete equality tuple on a non-partial compound index. We compared a two-field, nonunique tuple with the prototype's complete collection scan and SQLite 3.49.1 using both a forced compound expression index and a forced primary-table scan. Each query returned IDs in ascending order and decoded the returned JSON. Every timed query was checked against the same expected rows, and both databases were reopened and checked in full after each run. The `tenant` field in this workload is an ordinary document field; it is not a project-isolation test.

The figures are microseconds: the median of four per-run medians, with each run's median range in parentheses. Each run used 10 warmup and 50 measured iterations per variant, rotated their execution order, and used one client with a hot data set.

| Documents / matches | Prototype scan | Prototype planned | SQLite table scan | SQLite index |
| --- | ---: | ---: | ---: | ---: |
| 100 / 10, primary | 121.22 (119.89–123.08) | 24.05 (23.51–24.75) | 48.73 (48.58–49.03) | 9.26 (9.05–9.46) |
| 100 / 10, repeat | 120.79 (120.39–122.25) | 24.05 (23.23–24.88) | 48.74 (48.50–49.11) | 9.25 (9.11–9.43) |
| 1,000 / 10, primary | 1,240.99 (1,215.81–1,252.36) | 33.36 (32.68–34.34) | 431.74 (424.61–435.75) | 13.31 (13.13–13.55) |
| 1,000 / 10, repeat | 1,234.13 (1,186.21–1,253.49) | 32.48 (31.50–33.83) | 425.61 (418.52–432.05) | 13.11 (12.30–13.61) |
| 1,000 / 100, primary | 1,290.20 (1,259.29–1,332.19) | 219.43 (215.39–226.72) | 513.57 (492.26–514.49) | 95.11 (94.53–96.72) |
| 1,000 / 100, repeat | 1,291.19 (1,239.73–1,306.36) | 218.28 (217.06–221.36) | 504.40 (492.93–514.38) | 95.46 (94.71–95.98) |

At 1,000 documents and 10 matches, the primary planned median was about 37 times lower than the prototype's scan median; SQLite's forced indexed median was lower than the prototype's planned median. This is a narrow single-client hot-read result, not a general ranking. The prototype decodes and validates documents through its document interface; SQLite filters with `json_extract` and the benchmark decodes returned JSON. Both produce the same results for this data, but their internal work and APIs differ. No concurrent writes, cold-cache behavior, physical power failures, off-host recovery, project isolation, total memory limit or write throughput was measured.

The primary and repeat each used 12 fresh data sets, two pinned CPUs for the benchmark and driver, a four-CPU VM quota and the workspace overlay filesystem. All 24 pre-run quiet checks passed, but the primary observed up to 1.20% VM steal and 6.25% I/O wait during measured process windows; the repeat observed up to 2.70% steal and 7.14% I/O wait. No cgroup throttling or direct reclaim was recorded in those windows. These VM/cgroup-wide observations include setup and recovery verification and are not per-engine resource attribution. Cache state, the host and backing device were not independently controlled.

The [primary](../benchmarks/results/2026-11-04-compound-query-primary.json) and [repeat](../benchmarks/results/2026-11-04-compound-query-repeat.json) contain every sample, execution order, binary and script hashes, settings and telemetry. They used revision `d383505c5422eeb95b548a090a80dd8aea86fe19`; reproduce them with the [compound comparison harness](BENCHMARKS.md#compound-document-equality-query-comparison).
