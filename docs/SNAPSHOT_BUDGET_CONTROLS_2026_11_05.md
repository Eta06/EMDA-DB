# Snapshot reservation controls: local validation

The ordinary and checkpoint-backed stores expose configurable per-open aggregate snapshot reservations and a status snapshot. Limit updates and reservations use the same mutex. A reduction below any live reservation fails with `WouldBlock` without changing any of the limits; limits above the existing hard maxima fail with `InvalidInput`. Supported in-process compaction handoffs preserve the selected limits, while a fresh open restores the hard defaults.

On the ordinary store, active slots also cover transaction snapshots, standalone document point reads and the internal snapshot for online compaction. Consequently a zero active limit can reject those operations. On the checkpoint-backed store the budget covers checkpoint snapshots, not checkpoint transactions. Expired views remain reserved until reaped, cancelled or dropped. These are logical, process-local reservations, not measurements of total memory, project isolation or persistent quotas. See the [storage contract](STORAGE_TRANSACTION_CONTRACT.md).

## Local validation

The full stable all-target suite passed 822 tests on implementation revision `c9f0ad4`; a subsequently added online-zero-budget test and the other six snapshot-budget integration tests passed on stable and Rust 1.85 at `c777cc9`. Stable all-target Clippy and 48 Python workflow tests passed. The new test verifies an online compaction rejected by a zero budget leaves the source usable and no previous-store path behind, then succeeds after raising the limit.

[Repeated tests](../benchmarks/results/2026-11-05-snapshot-budget-stress.json) ran seven targeted concurrency, admission and handoff cases 20 times each on tmpfs and on the workspace overlay: 280 successful test invocations. The overlay's backing device was not independently verified. The runs exercised finite schedules on a running VM, with other validation active; they do not demonstrate power-loss behavior.

Three parallel, address-sanitized, 120-second [model campaigns](../benchmarks/results/2026-11-05-snapshot-budget-fuzz.json) on `c9f0ad4` completed 18,931 snapshot-budget, 30,169 shared-snapshot and 14,098 checkpoint-store generated executions without a finding. Their models are finite and serial; no exhaustive concurrency, corruption or power-loss guarantee follows. The new snapshot-budget target and its seeds are also included in the GitHub fuzz workflow.

## Default-budget contention comparison

The same benchmark source was built in release mode before (`f508fab`) and after (`c9f0ad4`) the change. Four alternating rounds on each filesystem used 2,000 initial keys, four writers each completing 100 conservative two-key transactions, and one concurrent key-value reader. Successful runs verified content after reopen. Each row below is the median of the four per-run summaries, rather than a pooled percentile. Reader percentiles are estimates from bounded reservoir samples. All times are microseconds; throughput is completed writer transactions per second.

| Filesystem | Revision | Writer p50 | Writer p99 | Reader p50 | Reader p99 | Writer tx/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| overlay | before | 581 | 17,764 | 0.34 | 0.70 | 1,627 |
| overlay | after | 521 | 18,838 | 0.43 | 0.69 | 1,794 |
| tmpfs | before | 291 | 15,553 | 0.42 | 0.74 | 2,538 |
| tmpfs | after | 202 | 17,166 | 0.35 | 0.70 | 3,003 |

These short, noisy results do not establish a speedup or absence of overhead. On tmpfs VM steal ranged from 1.4% to 25.2% across runs; measured writer p99 worsened in the median of runs on both filesystems, and overlay reader p50 also worsened. The test uses default budgets, one VM and a single ordinary store; it does not measure configured limits, checkpoint-store performance, total memory, another database engine or open-loop load. The [overlay](../benchmarks/results/2026-11-05-snapshot-budget-overlay.json.gz) and [tmpfs](../benchmarks/results/2026-11-05-snapshot-budget-tmpfs.json.gz) files contain individual timing samples, binary hashes, VM telemetry and failed-run records. At measurement time the working tree only contained untracked validation result files; neither benchmark binary included them.

The comparison can be repeated with separately built versions of `snapshot_contention_bench` and `scripts/snapshot_contention_compare.py`, choosing a new output path and the same `--base-keys 2000 --writers 4 --operations 100 --rounds 4` parameters. Repetition on representative deployment hardware and broader workloads remains necessary.
