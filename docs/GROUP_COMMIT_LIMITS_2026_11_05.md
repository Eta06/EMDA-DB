# Group-commit admission limits

The ordinary `Store` exposes `set_group_commit_limits(bytes, records)` and `group_commit_status()`. The defaults and hard maxima remain 64 MiB of encoded records and 1,024 records. Reservations include encoded records being prepared, queued, or synchronized. A reduction below current reservations fails with `WouldBlock` and leaves both limits unchanged. A record too large for the selected byte cap, or any record when the record cap is zero, fails with `WouldBlock` before submission. Other writers wait for capacity; if the configuration changes while they wait, they wake and recheck it. A successful mutation still returns only after synchronization and publication; an append or sync error retains the unknown-outcome and reopen rules.

The setting is process-local, applies to ordinary group-commit users including atomic and transactional commits, survives the supported ordinary-store in-process compaction handoffs, and resets after reopen. The checkpoint-backed store does not use this coordinator. Reservations do not count caller buffers, threads waiting outside this coordinator, allocation overhead, other stores, disk, or total process/project memory. These limits are not a tenant isolation boundary, persistent quota, fair scheduling guarantee or bounded-response guarantee.

Tests cover reconfiguration during preparation and actual synchronization, waiter wakeup when capacity grows or a record can no longer fit, exact encoded-byte boundaries, rejection without changing the log or leaking log/live-key quota capacity, batch and transaction atomicity, both compaction handoffs, reopen defaults, and existing group I/O fault and panic behavior. A [240-run local stress campaign](../benchmarks/results/2026-11-05-group-commit-limits-stress.json) repeated reconfiguration, wakeup, in-flight sync, failed sync, concurrent writes/recovery and compaction tests on tmpfs and the workspace overlay. Four [60-second model campaigns](../benchmarks/results/2026-11-05-group-commit-limits-fuzz.json) exercised 50,993 generated cases across group admission, log limits, transaction budgets and controlled transactions without a finding. The stable and Rust 1.85 debug all-target matrices and clippy passed, as did the stable optimized all-target matrix; the last added quota-interaction regression also passed separately on both toolchains. These serial models and finite tests do not exhaust thread schedules or physical failures.

## Before/after local comparison

The [tmpfs](../benchmarks/results/2026-11-05-group-commit-limits-tmpfs.json) and [workspace overlay](../benchmarks/results/2026-11-05-group-commit-limits-overlay.json) records each contain 36 verified runs. They compare revision `3862f05` against `8ab32fb` using [`key_byte_limit_compare.py`](../scripts/key_byte_limit_compare.py) and [`kv_bench`](../examples/kv_bench.rs). Each case uses 16 existing eight-byte keys, 64-byte values, one or four clients and 200 operations per client at 50% or 100% writes. Three rotated rounds compare the earlier and new builds with the same sufficiently large log cap; the new uncapped path is also recorded. Both versions use default group-commit limits. Acknowledged writes use the same synchronized prototype path, and each process checks sampled values after reopen. The benchmark does not measure lower group limits, inserts, overload rejection or project quotas.

Each filesystem had four initially quiet case-level windows and no recorded cgroup CPU throttling during the 36 runs. VM steal was nonzero in 3/36 tmpfs and 19/36 workspace runs. The workload and driver were pinned to separate available CPUs, but affinity cannot remove VM or storage interference. Measurements are short, warm-cache samples from one machine; per-run summaries are retained rather than individual latency samples.

The table gives the median of three per-run write p50/p99 latencies and operation throughput for the **log-capped** baseline and candidate. Times are microseconds.

| Filesystem | Clients / writes | Baseline p50 / p99 | Candidate p50 / p99 | Baseline / candidate ops/s |
| --- | --- | ---: | ---: | ---: |
| tmpfs | 1 / 50% | 3.1 / 62.4 | 3.0 / 74.6 | 294,698 / 320,290 |
| tmpfs | 1 / 100% | 3.0 / 5.6 | 3.0 / 4.8 | 231,921 / 200,750 |
| tmpfs | 4 / 50% | 35.5 / 283.1 | 15.9 / 162.4 | 145,990 / 243,433 |
| tmpfs | 4 / 100% | 21.0 / 223.5 | 16.4 / 189.8 | 115,655 / 118,649 |
| workspace | 1 / 50% | 220.9 / 557.4 | 219.4 / 560.1 | 8,520 / 8,842 |
| workspace | 1 / 100% | 234.6 / 601.5 | 231.0 / 451.3 | 3,716 / 3,926 |
| workspace | 4 / 50% | 500.7 / 922.6 | 479.0 / 1,088.9 | 14,317 / 14,699 |
| workspace | 4 / 100% | 445.5 / 1,046.3 | 445.8 / 1,043.3 | 7,601 / 7,636 |

The samples show mixed changes and substantial run-to-run variation; they do not establish a consistent speedup or absence of regression. They compare two revisions of this engine rather than SQLite or PostgreSQL, and do not validate physical power-loss safety.
