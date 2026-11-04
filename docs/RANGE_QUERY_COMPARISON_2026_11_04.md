# Document scalar range comparison, 2026-11-04

The experimental planner can use a required scalar range on a non-partial single-field index. We compared that path with the prototype's complete collection scan and SQLite 3.49.1 using both a forced numeric expression index and a forced primary-table scan. Each query used a numeric greater-or-equal predicate, returned IDs in ascending order and decoded the returned JSON. Index values were deliberately reversed relative to ID order, so the prototype restores ID order and SQLite may sort indexed results. Every timed query was checked against the same expected rows; both databases were reopened and checked in full after each run.

The figures are microseconds: the median of four per-run medians, with each run's median range in parentheses. Each run used 10 warmup and 50 measured iterations per variant, rotated their execution order, and used one client with a hot data set.

| Documents / matches | Prototype scan | Prototype planned | SQLite table scan | SQLite index |
| --- | ---: | ---: | ---: | ---: |
| 100 / 10, primary | 109.76 (109.52–110.13) | 19.14 (18.73–19.46) | 36.37 (36.16–36.51) | 12.22 (12.11–12.44) |
| 100 / 10, repeat | 111.31 (110.02–113.01) | 19.12 (18.85–19.24) | 36.49 (36.30–36.84) | 12.43 (12.25–12.59) |
| 1,000 / 10, primary | 1,078.14 (1,059.93–1,095.04) | 24.82 (24.49–25.79) | 292.91 (291.76–294.71) | 16.53 (16.30–16.69) |
| 1,000 / 10, repeat | 1,120.61 (1,072.88–1,170.65) | 27.13 (24.88–29.38) | 297.22 (292.10–306.22) | 16.79 (15.87–17.59) |
| 1,000 / 100, primary | 1,088.59 (1,078.11–1,108.02) | 177.01 (176.26–180.09) | 359.56 (355.86–385.82) | 90.22 (89.48–92.90) |
| 1,000 / 100, repeat | 1,108.05 (1,085.99–1,126.66) | 177.62 (176.41–184.16) | 364.97 (355.29–380.56) | 91.80 (89.64–93.54) |

At 1,000 documents and 10 matches, the primary planned median was about 43 times lower than the prototype's scan median; SQLite's forced indexed median was lower than the prototype's planned median. This is a narrow single-client hot-read result, not a general ranking. The prototype decodes and validates documents through its document interface; SQLite filters with `json_extract` and the benchmark decodes returned JSON. Both produce the same results for this integer-only data, but their internal work and APIs differ. The SQLite range index was explicitly selected because the unforced planner chose a table scan in the initial setup. No concurrent writes, cold-cache behavior, physical power failures, off-host recovery, total memory limit or write throughput was measured.

The primary and repeat each used 12 fresh data sets, two pinned CPUs for the benchmark and driver, a four-CPU VM quota and the workspace overlay filesystem. All 24 pre-run quiet checks passed, but the primary observed up to 4.00% VM steal and 8.00% I/O wait during measured process windows; the repeat observed up to 2.06% steal and 6.25% I/O wait. No cgroup throttling or direct reclaim was recorded in those windows. These VM/cgroup-wide observations include setup and recovery verification and are not per-engine resource attribution. Cache state, the host and backing device were not independently controlled.

The [primary](../benchmarks/results/2026-11-04-range-query-primary.json) and [repeat](../benchmarks/results/2026-11-04-range-query-repeat.json) contain every sample, execution order, binary and script hashes, settings and telemetry. They used revision `2d83bfef85af786f69a26d98c046f78a8c25abbf`; reproduce them with the [range comparison harness](BENCHMARKS.md#document-scalar-range-query-comparison).
