# Equality filter budget

`Equal` operands now share the 256-value and 4 MiB encoded-JSON admission limits with `In`, `NotIn` and array-value predicates, and their evaluated equality operations share the existing 1,000,000-comparison work limit. Each repeated operand and each boolean branch counts before a query executes, including a branch that would short-circuit. A missing path does not perform an equality comparison. `Compare` scalar operands and field paths are not charged to this counter. These are logical limits, not total process memory, CPU, project isolation or response-time limits; nested JSON equality can still be expensive.

This changes behavior: a query with many equality operands previously bypassed these combined input counters and can now return `InvalidInput`. The validation applies to complete scans, planned queries and aggregation, including filters evaluated inside transactions. Propagating the error aborts staged writes; a caller that deliberately catches an error retains the transaction's existing callback semantics. Tests cover exactly 4 MiB of escaped JSON, over-limit short-circuited branches, the 256/257-value boundary, shared comparison work, rollback and both writable backends. [Six repeated test-suite runs](../benchmarks/results/2026-11-05-equality-budget-stress.json) passed on tmpfs and the workspace overlay. The compatibility change is recorded in the changelog.

## Paired local comparison

The [tmpfs](../benchmarks/results/2026-11-05-equality-budget-tmpfs.json) and [workspace overlay](../benchmarks/results/2026-11-05-equality-budget-overlay.json) raw files each contain 30 validated single-client runs. They compare the `e56d10d` baseline with `ae045dc` using the existing [`document_scale_compare.py`](../scripts/document_scale_compare.py) driver and [`document_bench`](../examples/document_bench.rs). Both use an indexed email equality query with 10 or 500 preloaded documents, 20 warmups and 100 measured create/read/update/query/delete cycles. For each size, three rotated rounds include baseline and new planned queries, the new explicit-index and scan paths, and SQLite's indexed JSON-text query. Each run checks results and reopens its database. New prototype stores use the synchronized V3 log path; SQLite uses WAL with `synchronous=FULL` and autocommit writes. The SQLite adapter returns JSON text while this prototype decodes and validates JSON; durability settings and interfaces are not identical.

The benchmark and driver were pinned to separate available CPUs. Every run had an initially quiet observation window and none recorded cgroup CPU throttling during the run; VM steal was nonzero in 6/30 tmpfs and 21/30 workspace runs. CPU affinity and quiet windows do not eliminate host or storage interference. The measurements use warm caches on one VM and are not a throughput, concurrency, reliability or universal database ranking.

The table reports the median of the three per-process p50 and p95 values in microseconds for the equality query:

| Filesystem / documents | Baseline planned p50 / p95 | New planned p50 / p95 | SQLite p50 / p95 |
| --- | ---: | ---: | ---: |
| tmpfs / 10 | 4.077 / 5.294 | 4.127 / 5.287 | 2.189 / 2.314 |
| tmpfs / 500 | 5.845 / 12.682 | 5.750 / 11.610 | 2.356 / 2.520 |
| workspace / 10 | 9.624 / 15.405 | 10.073 / 19.926 | 3.911 / 5.908 |
| workspace / 500 | 13.597 / 24.405 | 14.578 / 28.944 | 3.693 / 5.843 |

The new admission work adds cost in some runs; a small median difference or a noisy tail does not establish a causal percentage. SQLite's indexed query was faster in these samples with the adapter differences above. The before/after binaries predate the separately merged transaction-lock change, so the combined implementation needs its own validation. Raw samples and telemetry are retained rather than hiding regressions.

Stable and Rust 1.85 debug all-target tests and clippy passed, as did the stable optimized all-target suite and 48 Python workflow/benchmark tests. Four [60-second model campaigns](../benchmarks/results/2026-11-05-equality-budget-fuzz.json) exercised 18,610 generated cases: 330 bounded-input cases including synthetic large values, 6,978 array queries, 4,170 membership queries and 7,132 compound-planner cases. They did not find a failure; these finite serial campaigns do not exhaust possible inputs, concurrency schedules or storage failures.
