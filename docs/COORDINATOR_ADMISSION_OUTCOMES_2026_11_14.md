# Ordinary coordinator admission outcomes

Revision `b16934155d2bfe5635cfedc8010290e8b53510cf` moves the detailed
mutation/transaction submission boundary to immediately before queue insertion
under the ordinary coordinator lock. A capacity, preparation or lock failure
before that point rejects this call. Once queued, the outcome remains
conservatively unknown on failure, even if the record was never appended.
Rejection does not establish store health or resolve an earlier uncertain write.
The existing non-detailed methods and commit synchronization are unchanged.

Tests cover zero and insufficient group capacity, direct puts/deletes,
compare-exchange, increments, atomic batches, key-value and document
transactions, unchanged log bytes, index consistency and log-quota
reservation release. A controlled concurrency test blocks one synchronized
group while other requests are queued or waiting for capacity. On injected
group sync failure, queued requests return unknown and capacity waiters return
rejected; recovery checks that rejected keys stayed absent. A
[25-run optimized repetition](../benchmarks/results/2026-11-14-coordinator-outcomes-stress.log.gz)
passed. These sampled schedules do not establish physical power-loss safety.

The [development log](../benchmarks/results/2026-11-14-coordinator-outcomes-test-development.log.gz)
retains two initial failures in the test's scheduling assumptions. One assumed
a queued request could not already be in the active group; another mistook a
reserved but not yet queued record for a submitted request. The final test
waits for actual queued or active requests. An environment restart also
interrupted an initial Rust 1.85 suite after 950 passing tests; the full suite
was rerun rather than counted as complete.

Stable, Rust 1.85 and optimized stable all-target suites each passed 969 tests
with four test threads; both all-target Clippy checks and 129 Python tests
passed. The five GitHub workflows for `b169341` passed.

## Local performance check

The [paired log-cap report](../benchmarks/results/2026-11-14-coordinator-outcomes-performance.json.gz)
contains 36 rotated runs against separately built `ae092d0` and `b169341`
executables on a shared Linux VM. It exercises existing direct APIs with 1 or
4 clients and 50% or 100% writes to 16 existing keys; it does not directly
measure detailed-API overhead or admission rejection. All acknowledged writes
use the same synchronization, and sampled values were checked after reopening.
For four clients and 50% writes, the median per-run write p50 was 357.9 µs
before and 345.4 µs after; throughput was 17,117 and 20,466 operations/s.
These three runs per variant are short and variable, so they do not establish
a general speedup. Individual runs observed up to 6.76% VM steal and 16.67%
I/O wait, with no observed cgroup throttling. Telemetry covers the entire
VM/cgroup including fixture setup and the workload.
