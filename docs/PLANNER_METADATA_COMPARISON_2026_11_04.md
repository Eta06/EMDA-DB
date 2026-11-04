# Planner metadata reuse comparison, 2026-11-04

The planner now reuses the collection metadata it already validated in the transaction when it probes an eligible unique index. The prior code read and decoded the same metadata twice. The index still checks entries and unique mappings, and the transaction retains its existing snapshot, read-set and conflict rules. A new concurrent index-drop test checks that a transaction using an old definition conflicts rather than committing against it.

We ran a 27-run exploratory comparison, a 70-run paired comparison, and a 70-run repeat. Each run used a single client, 20 warm-up cycles and 200 measured create/read/update/equality-query/delete cycles, and verified the baseline data after reopening. Before and after binaries were rotated alongside SQLite 3.49.1 and the explicit index path; a complete prototype scan was also included where the 1,024-document limit permitted it. The baseline is `c2796d9b39468a8c16f382fbb9d1805077359b3b`; the repeat's new binary is from `22ef505a81414df6ef645def83c115cf870fe6e7`. The primary paired run used local commit `75198d183d8953791bf3bc18a0d78271c16b00d0`, whose source, examples, scripts and fuzz files match the new main-branch revision. The exploratory run used an uncommitted worktree. Binary hashes and all samples are in the raw results. All 167 runs passed validation.

Each main table row pools 1,000 equality-query samples per variant. Values are **p50 / p99 microseconds**, using nearest-rank percentiles. SQLite returns JSON text while the prototype decodes and schema-validates JSON, so they do not perform identical work.

| Baseline documents | Before, primary | After, primary | Before, repeat | After, repeat |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 10.72 / 50.28 | 9.62 / 34.00 | 10.12 / 47.94 | 10.22 / 63.93 |
| 1,000 | 15.64 / 77.22 | 14.98 / 70.25 | 16.76 / 129.98 | 15.68 / 94.94 |
| 3,000 | 25.90 / 108.98 | 24.98 / 121.56 | 26.26 / 139.09 | 24.08 / 101.61 |

The 3,000-document p99 was higher after the change in the primary run and lower in the repeat; at 100 documents the repeat had both a slightly higher median and a higher p99. The earlier exploratory sample likewise had a higher 3,000-document p99 after the change: 74.09 µs before and 103.42 µs after, with 600 samples per variant. These short samples do not establish a consistent tail improvement.

For context, at 3,000 documents the primary explicit-index and SQLite query p50/p99 were 22.09/113.66 µs and 3.71/23.09 µs; the repeat measured 21.98/110.42 µs and 3.72/14.18 µs. The complete scan was not run at that size because it would exceed the prototype's work limit. These numbers are limited to this warm, single-client fixture and are not a general ranking.

The Linux VM used an Intel Xeon Platinum 8573C, a four-CPU cgroup quota, separate pinned benchmark and driver CPUs, and the workspace overlay filesystem. All 27 exploratory and 70 primary quiet checks passed; 67 of 70 passed in the repeat. Individual windows reached 2.75%, 2.73% and 4.26% VM steal, respectively, and 13.05%, 13.40% and 13.74% I/O wait. No cgroup throttling or direct-reclaim events were recorded. These VM/cgroup-wide counters include setup and verification; the host, backing storage and cache were not controlled. Physical power loss and independent off-host recovery were not tested.

The [exploratory](../benchmarks/results/2026-11-04-planner-metadata-exploratory.json.gz), [primary](../benchmarks/results/2026-11-04-planner-metadata-paired.json.gz) and [repeat](../benchmarks/results/2026-11-04-planner-metadata-repeat.json.gz) contain raw samples, order, hashes, validation and telemetry. A separate [2,819-input differential fuzz run](../benchmarks/results/2026-11-04-planner-metadata-fuzz.json) compared planned queries with collection scans after the change. Neither the fuzz run nor the benchmarks are exhaustive correctness or latency guarantees.
