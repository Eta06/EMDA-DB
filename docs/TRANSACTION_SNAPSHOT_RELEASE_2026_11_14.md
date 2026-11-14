# Releasing validated transaction snapshots

Revision `71ae310abcece4a56d9ebad8207be9aab4aa4510` releases an ordinary
transaction's private snapshot after its callback finishes and validation
succeeds, while the exclusive writer barrier is still held. The transaction no
longer needs that snapshot for the remaining commit. Other readers retain
their own views; if they share the current index, the existing copy-on-write
path still preserves them. The write is still synchronized before
acknowledgement. The private snapshot reservation is released before commit,
while the separate active-transaction admission remains held until completion.
The checkpoint-backed transaction path is unchanged.

## Correctness and recovery

Targeted tests check that a transaction does not force a full index copy when
no other view shares it, that an independently held snapshot remains stable,
and that a snapshot created while a transaction is blocked in synchronization
continues to see the earlier value. They also check reservation accounting,
transaction admission and recovery after the write. The existing transaction,
document/index, cancellation, injected-fault and snapshot tests also ran.

The [recovery log](../benchmarks/results/2026-11-14-transaction-snapshot-recovery.log.gz)
records 160 process-kill cases with ordinary and checkpoint writers. Each
child acknowledged three document/index/raw-key transactions before the
parent interrupted an additional transaction. Both recovery paths verified
the acknowledged state in all cases; the pending transaction was present in
123 cases. Two 4,096-history mixed concurrent-operation model runs passed on
the workspace overlay and tmpfs, respectively, each checking both recovery
paths. They cover V2/V3, both writers, transaction modes, deletions, conditional
writes and atomic batches. These finite schedules and process kills leave the
OS running; they are not physical power-loss or independent off-host tests.

Stable, Rust 1.85 and optimized stable all-target suites each passed 971 tests
with four test threads. The stable rerun used test-only follow-up `792f41b`;
the other full suites tested the same production code at `71ae310`, and 25
metadata fault tests were rerun on both toolchains after the follow-up. Both
final all-target Clippy checks, formatting and 129 Python tests passed. The
[development log](../benchmarks/results/2026-11-14-transaction-snapshot-test-development.log.gz)
retains an initial stable-suite failure: an existing metadata test tried to
reopen a private fixture while a parallel fork temporarily retained its lock.
The follow-up applies the existing bounded retry to four such fixture reopens;
other errors still surface. A restarted environment also interrupted an
initial release-binary build, which was rerun.

## Local document comparison

The [first](../benchmarks/results/2026-11-14-transaction-snapshot-performance.json.gz)
and [repeat](../benchmarks/results/2026-11-14-transaction-snapshot-performance-repeat.json.gz)
compare separately built `b169341` and `71ae310` binaries with SQLite. The
first has four rounds of 100 cycles at 100, 1,000 and 3,000 documents; the
repeat has six rounds of 150 cycles at 1,000 and 3,000 documents. All 72 runs
completed and checked recovered data. The prototype uses synchronized writes;
SQLite uses WAL/FULL. The prototype decodes JSON while the SQLite adapter
returns JSON text, so the implementations do not perform identical work.

At 3,000 documents, the medians of per-run p50 write latencies were:

| Run | Engine | Create | Replace | Delete |
| --- | --- | ---: | ---: | ---: |
| First | Before | 713.99 µs | 706.39 µs | 715.93 µs |
| First | After | 173.83 µs | 169.86 µs | 171.17 µs |
| First | SQLite | 129.43 µs | 126.48 µs | 125.94 µs |
| Repeat | Before | 685.26 µs | 677.47 µs | 684.53 µs |
| Repeat | After | 183.22 µs | 177.87 µs | 183.16 µs |
| Repeat | SQLite | 119.29 µs | 119.43 µs | 118.44 µs |

The first 100-document sample moved slightly in the other direction: median
create p50 was 196.88 µs before and 200.89 µs after. SQLite's indexed reads
were faster in these samples. The first and repeat recorded up to 4.17% and
2.82% VM steal and 13.64% and 13.93% I/O wait respectively, with no observed
cgroup throttling. These short, warm-cache runs use an overlay of unknown
backing and do not establish a general ranking or production performance.

## Local concurrent comparison

The [contention report](../benchmarks/results/2026-11-14-transaction-snapshot-contention.json.gz)
rotates four runs per revision with 3,000 base keys, four writers retrying
conservative two-key transactions and one reader. It verifies final values
after reopening. Median writer throughput was 1,539 transactions/s before and
2,746 after; median per-run writer p99 was 31.43 ms and 13.70 ms. Median
sampled reader p99 was 0.73 µs and 0.70 µs. Reader percentile samples use a
bounded reservoir. Up to 3.75% VM steal and 10% I/O wait were observed; no
cgroup throttling was observed. This is a small closed-loop workload, not an
offered-load, cross-engine, total-memory or general performance result.
