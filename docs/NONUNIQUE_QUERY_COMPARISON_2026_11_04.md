# Nonunique document equality comparison, 2026-11-04

The experimental planner can use a nonunique, single-field equality index and read its full matching range in pages. We compared that path with the prototype's complete collection scan and SQLite 3.49.1 using both an expression index and a forced primary-table scan. Every timed query returned the same IDs and decoded JSON, and both databases were reopened and checked in full after each run.

The figures are microseconds: the median of four per-run medians, with each run's median range in parentheses. Each run used 10 warmup and 50 measured iterations per variant, rotated their execution order, and used one client with a hot data set.

| Documents / matches | Prototype scan | Prototype planned | SQLite table scan | SQLite index |
| --- | ---: | ---: | ---: | ---: |
| 100 / 10, primary | 103.74 (102.03–104.29) | 19.57 (19.29–19.74) | 34.67 (34.61–35.07) | 8.46 (8.36–8.53) |
| 100 / 10, repeat | 103.41 (101.85–105.47) | 19.63 (18.37–20.67) | 34.51 (34.25–34.81) | 8.37 (8.27–8.53) |
| 1,000 / 10, primary | 1,044.67 (999.20–1,106.37) | 26.63 (24.87–28.05) | 288.70 (280.94–298.28) | 11.50 (10.78–12.30) |
| 1,000 / 10, repeat | 1,015.95 (990.31–1,053.50) | 26.01 (24.55–26.81) | 287.70 (274.12–297.93) | 11.14 (10.19–11.96) |
| 1,000 / 100, primary | 1,020.90 (1,009.49–1,044.85) | 192.01 (183.76–205.13) | 354.78 (347.92–361.58) | 82.91 (82.06–84.16) |
| 1,000 / 100, repeat | 1,019.61 (1,014.67–1,046.33) | 200.27 (186.51–211.73) | 349.20 (343.46–362.32) | 82.91 (81.79–83.27) |

At 1,000 documents and 10 matches, the primary prototype planned median was about 39 times lower than its complete scan, while SQLite's indexed median was lower than the prototype's planned median. This is a narrow, single-client hot-read result, not a general ranking. The prototype decodes and validates stored documents as part of its document interface; SQLite uses `json_extract` for filtering and the benchmark decodes returned JSON. Both use bounded, equal-result queries, but their internal work and APIs differ. No concurrent writes, cold-cache behavior, physical power failures, off-host recovery, total memory limit or write throughput was measured.

The primary and repeat each used 12 fresh data sets, two pinned CPUs for the benchmark and driver, a four-CPU VM quota and the workspace overlay filesystem. All 24 pre-run quiet checks passed, but the primary observed up to 8.70% VM steal and 7.14% I/O wait during measured process windows; the repeat observed up to 1.37% steal and 7.14% I/O wait. No cgroup throttling or direct reclaim was recorded in those windows. These VM/cgroup-wide observations include setup and recovery verification and are not per-engine resource attribution. Cache state, the host and backing device were not independently controlled.

The [primary](../benchmarks/results/2026-11-04-nonunique-query-primary.json) and [repeat](../benchmarks/results/2026-11-04-nonunique-query-repeat.json) contain every sample, execution order, binary and script hashes, settings and telemetry. They used revision `372cb494e3fcd9e8e484ba4487d974be23109aa5`; reproduce them with the [comparison harness](BENCHMARKS.md#nonunique-document-equality-query-comparison).

An earlier [exploratory run](../benchmarks/results/2026-11-04-nonunique-query-exploratory-invalid-scan.json) incorrectly labeled a SQLite query as a table scan: SQLite used the expression index despite `NOT INDEXED` on this `WITHOUT ROWID` table. Its SQLite scan figures are invalid. The following [smoke run](../benchmarks/results/2026-11-04-nonunique-query-scan-plan-failure.json) detected the incorrect access path and failed. The final harness explicitly forces the primary-table access path and verifies `SCAN docs`; it also verifies that the indexed query uses `docs_group`.
