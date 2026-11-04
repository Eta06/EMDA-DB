# Partial-index document query comparison, 2026-11-04

We compared an equality query guarded by `active = true` using the prototype's partial index with a full prototype scan and SQLite 3.49.1 using forced partial-expression-index and primary-table-scan plans. Some inactive documents share the queried `group` value, so the guard matters. The prototype verifies its index entries and full document schema; SQLite filters with `json_extract`, and the harness decodes the returned JSON. Both produce the same rows for this data, but their APIs and internal work differ. Every timed result was checked and both databases were reopened and checked in full after every run. The `active` field is ordinary data, not an authorization or project-isolation boundary.

Figures are microseconds: the median of four run medians, followed by the range of those medians. Each run used one client, 10 warmup and 50 measured cycles per variant, with the variants rotated within each cycle.

| Documents / matches | Prototype scan | Prototype planned | SQLite table scan | SQLite index |
| --- | ---: | ---: | ---: | ---: |
| 100 / 10, primary | 113.55 (112.82–114.13) | 20.74 (20.73–20.84) | 41.63 (41.50–41.66) | 8.84 (8.80–8.88) |
| 100 / 10, repeat | 114.39 (113.57–115.19) | 20.82 (20.43–21.60) | 41.93 (41.66–42.05) | 8.94 (8.82–8.97) |
| 1,000 / 10, primary | 1,042.32 (1,033.25–1,057.89) | 78.31 (77.42–79.08) | 342.54 (334.45–345.19) | 10.68 (10.51–11.19) |
| 1,000 / 10, repeat | 1,055.37 (1,034.82–1,086.06) | 78.29 (77.31–79.43) | 342.46 (334.44–353.10) | 11.16 (10.80–11.68) |
| 1,000 / 100, primary | 1,062.36 (1,051.55–1,080.14) | 219.18 (217.87–220.76) | 426.41 (419.25–428.48) | 89.63 (89.14–90.17) |
| 1,000 / 100, repeat | 1,069.49 (1,054.74–1,080.17) | 219.23 (217.80–221.21) | 433.47 (419.23–439.51) | 90.02 (88.87–91.19) |

The prototype's partial index improved its own full-scan median, while SQLite's indexed path was faster in these cases. At 1,000 documents and 10 matches, the raw samples also show a strong execution-order effect: planned queries following the prototype scan tended to be slower than those following the SQLite indexed query. A diagnostic repetition confirmed the planned query examined only 10 candidates. The cause has not yet been established, and the interleaved comparison should not be treated as an isolated hot-index latency measurement. We retain all samples rather than discarding the slow groups.

The primary and repeat each used 12 fresh data sets on the workspace overlay, a four-CPU VM quota and pinned benchmark/driver CPUs. All 24 pre-run quiet checks passed. The primary observed up to 5.00% VM steal and 6.25% I/O wait during process windows; the repeat observed up to 1.30% steal and 6.25% I/O wait. No cgroup throttling or direct reclaim was recorded. These VM/cgroup-wide observations include setup and reopen checks and are not per-engine measurements. The host, backing device and cache state were not independently controlled. The benchmark does not measure concurrent writes, cold starts, total project memory, physical power loss, off-host recovery or a universal performance ranking.

The [primary](../benchmarks/results/2026-11-04-partial-query-primary.json) and [repeat](../benchmarks/results/2026-11-04-partial-query-repeat.json) contain raw samples, binary and script hashes, settings and telemetry. Both used revision `02742399f846f00385d351e070dd2e5492eab71f`. The repeat records the primary result as an untracked file; that file does not affect the measured program. The [comparison harness](BENCHMARKS.md#partial-document-equality-query-comparison) describes reproduction.
