# Document query planner comparison, 2026-11-04

At revision `c2796d9b39468a8c16f382fbb9d1805077359b3b`, the opt-in planner can select a non-partial, single-field unique index when exact scalar equality is required by a filter. It reapplies the complete filter and reports the chosen path; other cases fall back to a complete collection scan. It is not a cost-based optimizer. Index integrity remains a precondition: an index entry removed by unsupported raw writes can cause an omission. See the [document model](DOCUMENT_MODEL.md) for semantics and limits.

We compared the complete-scan query, planned query, existing explicit-index query and SQLite 3.49.1 expression-index query. Each in-process run preloaded a unique-email collection, then performed 20 warm-up and 200 measured create, read, update, equality-query and delete cycles on transient documents. Five rounds rotated the execution order for each size. The transient document adds one to the baseline collection size while it is present. The prototype's full-scan limit is 1,024 documents, so the 3,000-document scan was omitted. Every run checked its query result and the complete baseline data after reopening; all 55 runs passed.

The table pools 1,000 query samples per size and variant. Values are **p50 / p99 microseconds**, calculated using nearest-rank percentiles. The prototype returns decoded, schema-validated JSON; SQLite returns JSON text. SQLite's `EXPLAIN QUERY PLAN` reported use of the expression index. The interfaces therefore do different work and these results are not a universal ranking.

| Baseline documents | Prototype full scan | Prototype planned | Prototype explicit index | SQLite |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 103.45 / 240.99 | 10.75 / 36.72 | 8.91 / 50.59 | 3.90 / 17.88 |
| 1,000 | 1,034.57 / 1,726.28 | 17.31 / 95.29 | 12.69 / 71.75 | 3.75 / 18.91 |
| 3,000 | Exceeds scan limit | 25.85 / 114.73 | 22.74 / 72.27 | 3.73 / 11.67 |

At 1,000 baseline documents, the planned median was about 60 times lower than the complete scan, but higher than both the prototype's explicit-index path and SQLite. At 3,000 documents, the planned median was 25.85 µs versus 3.73 µs for SQLite. This reflects only the measured single-client fixture and adapters.

The other operations in the cycle also varied between runs. At 1,000 documents, create p50/p99 was 483.92/1,060.55 µs for the scan variant, 488.76/1,042.75 µs planned, 478.49/992.18 µs explicit index and 290.76/819.55 µs SQLite. Delete p50/p99 was 613.67/1,217.54, 484.61/1,108.90, 473.50/1,082.58 and 285.96/757.98 µs respectively. Those operations use the same engine code within the three prototype variants; their differences illustrate run and workload variation, not an isolated planner effect. The raw data includes all operation samples.

## Environment and scope

The prototypes used witnessed V3 stores and SQLite used WAL with `synchronous=FULL` and autocommit. The Linux VM used an Intel Xeon Platinum 8573C, a four-CPU cgroup quota and a workspace overlay filesystem; the benchmark and driver were pinned to separate CPUs. The host, filesystem backing device and cache were not independently controlled. All 55 pre-run quiet checks passed, with no recorded cgroup throttling or direct reclaim; individual measurement windows reached 9.87% VM steal and 13.91% I/O wait. These are VM/cgroup-wide measurements, include preparation and verification, and do not attribute resources to an individual engine. The dataset is small, warm, single-client and single-node; physical power-loss and independent off-host restoration were not tested.

Two [differential fuzz runs](../benchmarks/results/2026-11-04-document-query-planner-fuzz.json) compared planned queries with complete scans across both writable backends for 10,121 generated inputs. Separate tests cover indexed phantom detection and corruption behavior; these checks are not exhaustive.

[Raw results](../benchmarks/results/2026-11-04-document-query-planner.json) retain every latency sample, run order, exact binary and script hashes, operation configuration, verification result and noise telemetry. Reproduce with the release `document_bench` example and `scripts/document_scale_compare.py --compare-plans` at the stated revision.
