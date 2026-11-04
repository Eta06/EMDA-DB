# Rejected scan-exhaustion optimization, 2026-11-04

This optimization was **not merged**. It tried to avoid a final empty index-page request when a transaction scan could prove the requested range had ended. Tests covered byte-limited pages, staged writes and tombstones, checkpoint overlays, multi-page phantom conflicts and 4,096 generated staged-range observations across both backends. Those checks are not exhaustive. The measured performance was mixed, and the added storage-layer complexity was not justified by a repeatable improvement.

We compared the existing implementation at `372cb494e3fcd9e8e484ba4487d974be23109aa5` with an initial candidate (`509030da6fb45dae7036cedc8f1bc55b1bedcf5f`), repeated that candidate, then tried a narrower version (`617679a08053d0787ffb17a7d17aa060a854f797`) that tracked exhaustion only for nonunique planned queries. Each series used before/after/after/before order, four fresh runs per binary and case, and the [nonunique query harness](BENCHMARKS.md#nonunique-document-equality-query-comparison). The table shows the median of the four per-run medians in microseconds. The p99 columns are the median of the four per-run nearest-rank p99 values, not a production tail-latency guarantee.

| Series | Documents / matches | Before p50 | Candidate p50 | Before p99 | Candidate p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Initial | 100 / 10 | 18.31 | 18.48 | 89.35 | 89.27 |
| Initial | 1,000 / 10 | 26.92 | 27.54 | 131.72 | 143.40 |
| Initial | 1,000 / 100 | 173.21 | 167.62 | 322.95 | 414.35 |
| Repeat | 100 / 10 | 18.14 | 17.74 | 50.15 | 78.79 |
| Repeat | 1,000 / 10 | 26.65 | 32.18 | 115.19 | 195.76 |
| Repeat | 1,000 / 100 | 165.97 | 168.02 | 446.69 | 451.28 |
| Narrow | 100 / 10 | 18.19 | 17.60 | 78.98 | 50.84 |
| Narrow | 1,000 / 10 | 26.31 | 35.77 | 104.45 | 200.54 |
| Narrow | 1,000 / 100 | 167.56 | 168.04 | 367.57 | 424.20 |

The initial and repeat each passed all 24 pre-run quiet checks; the narrow series passed 21 of 24. VM steal reached 4.55%, 3.23% and 11.01%, respectively, and I/O wait reached 5.56%, 3.57% and 3.70%. No cgroup throttling or direct reclaim was recorded in those process windows. Measurements include VM interference, hot caches and one client; they do not establish the cause of each difference or a universal ranking.

Raw [initial](../benchmarks/results/2026-11-04-scan-exhaustion-first.json), [repeat](../benchmarks/results/2026-11-04-scan-exhaustion-repeat.json) and [narrow](../benchmarks/results/2026-11-04-scan-exhaustion-narrow.json) results retain all samples, run order, binary hashes and telemetry. Nested reports identify the harness checkout; their before executable was built at revision `372cb49`, as the accompanying binary hash records. The unaccepted [initial patch](../benchmarks/experiments/scan-exhaustion-first.patch.gz) and [narrow patch](../benchmarks/experiments/scan-exhaustion-narrow.patch.gz) apply separately to revision `9210090`. They are gzip-compressed unified diffs; inspect or check one with `gzip -dc PATCH | git apply --check` from that baseline. They are retained for investigation and are not part of the running engine.
