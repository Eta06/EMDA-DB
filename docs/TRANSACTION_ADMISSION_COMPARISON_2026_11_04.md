# Active transaction admission: evidence and local comparisons

Both writable stores now admit at most 64 active transactions per open instance by default. The limit can be lowered, including to zero; an over-limit call returns `WouldBlock` before its callback starts. The count includes FIFO waiters, callbacks and commits. Reconfiguration and admission share the existing transaction-budget mutex, so reducing the limit below the current count fails without changing it. The setting survives supported compaction handoffs and resets on reopen. It does not constrain direct writes, independent snapshots, other stores, caller threads, total memory or fairness.

## Validation

Both backends have targeted tests for limit boundaries, nested transactions, simultaneous callers, cancellation, callback panic, direct writes, and release of admission slots. Separate tests inject a poisoned budget lock, check that new callbacks do not run, reopen the store, and verify that an independent acknowledged write remains readable. Linux tests cover blocking compaction and changes made during online copying. The full local stable, Rust 1.85 and release matrix passed for the admission implementation; the later poison/recovery additions change tests only and passed their targeted runs.

A [four-target model campaign](../benchmarks/results/2026-11-04-active-admission-fuzz.json) exercised 61,252 generated serial cases on revision `74c4136`, including changing limits, nested transactions, cancellation, read sets and reopening. [Fifty repeated checks](../benchmarks/results/2026-11-04-active-admission-stress.json) covered concurrent admission, cancellation, panic and handoff on tmpfs and the workspace overlay. These finite tests do not exhaust scheduling, bound total memory, establish physical power-loss safety or replace an independent off-host restore.

## Cost of the limit

The [first](../benchmarks/results/2026-11-04-active-admission-primary.json.gz) and [repeat](../benchmarks/results/2026-11-04-active-admission-repeat.json.gz) compare the pre-limit revision `fe8ffa4` with the mutex-based limit `74c4136`. Each run executes two-key serializable transactions, retries conflicts, and verifies counters after reopening. No workload reaches the new admission limit. Three paired process rounds are recorded for each of two workloads, one or four clients and four transaction modes; each client completes 100 operations. Reported values below are the medians of the three process-level p50 values, in microseconds.

| Workload and mode | Before limit → with limit | Before limit → with limit, repeat |
| --- | ---: | ---: |
| One client, disjoint, conservative | 4.65 → 4.76 | 4.67 → 4.74 |
| One client, disjoint, checkpoint | 3.87 → 3.94 | 3.87 → 3.94 |
| Four clients, disjoint, conservative | 14.59 → 15.99 | 15.49 → 15.09 |
| Four clients, disjoint, checkpoint | 32.71 → 39.28 | 26.18 → 30.81 |

Four-client tails and retry counts varied widely: for the disjoint conservative case the median process p99 was 239 → 1,692 µs in the first run and 1,053 → 703 µs in the repeat. These small, noisy samples do not establish a stable tail-latency effect. All modes, workloads, raw samples and failures are recorded in the linked files.

## Atomic-counter experiment: not merged

An alternative kept the active count and its limit in one atomic word, separate from the staged-byte mutex. The initial implementation allowed a new read-only transaction after the byte-budget mutex had been poisoned; the two [injected-fault tests failed](../benchmarks/results/2026-11-04-atomic-admission-poison-failure.txt). A later revision added an observed-poison check, and the tests passed. The alternative also observes the active and byte-budget status pairs separately, and its interaction with a concurrently poisoning mutex differs from the accepted implementation. Finite tests do not prove the two failure behaviors equivalent.

The corrected candidate `0106b0d` passed the local stable, Rust 1.85 and release matrix, [59,258 model cases](../benchmarks/results/2026-11-04-atomic-admission-final-fuzz.json) and [80 repeated checks](../benchmarks/results/2026-11-04-atomic-admission-final-stress.json); an earlier candidate had its own [model](../benchmarks/results/2026-11-04-atomic-admission-initial-fuzz.json) and [stress](../benchmarks/results/2026-11-04-atomic-admission-initial-stress.json) runs. These results do not eliminate the failure-ordering concern.

The following table compares the accepted mutex implementation to the corrected atomic experiment for four clients on tmpfs. Each cell is the median of three process-level p50 values in microseconds; the [first](../benchmarks/results/2026-11-04-atomic-admission-mutex-primary.json.gz) and [repeat](../benchmarks/results/2026-11-04-atomic-admission-mutex-repeat.json.gz) retain all samples and tails.

| Workload | Mode | Mutex → atomic | Mutex → atomic, repeat |
| --- | --- | ---: | ---: |
| Disjoint | Conservative | 12.58 → 14.79 | 12.66 → 12.80 |
| Disjoint | Read set | 12.79 → 15.37 | 12.92 → 15.87 |
| Disjoint | Checkpoint | 29.07 → 38.35 | 25.41 → 18.25 |
| Disjoint | Checkpoint read set | 30.53 → 20.95 | 46.02 → 32.50 |
| Shared | Conservative | 15.09 → 12.43 | 14.90 → 12.32 |
| Shared | Read set | 12.19 → 15.12 | 12.52 → 12.53 |
| Shared | Checkpoint | 34.01 → 30.77 | 30.74 → 31.09 |
| Shared | Checkpoint read set | 16.35 → 17.49 | 14.20 → 40.22 |

Single-client p50 differences were small in these samples. A [separate comparison](../benchmarks/results/2026-11-04-atomic-admission-unbounded.json.gz) uses the pre-limit revision as baseline. An [overlay run](../benchmarks/results/2026-11-04-atomic-admission-mutex-overlay.json.gz) also produced mixed results; its disjoint checkpoint p50 was 268.32 → 347.78 µs and p99 was 13.99 → 22.40 ms. Two overlay pre-run quiet checks failed, and the backing device was not independently verified. The experiment is not merged: its performance did not improve consistently, while its failure and status semantics require further work.

The unmerged [patch](../benchmarks/experiments/atomic-admission.patch.gz) and [Git bundle](../benchmarks/experiments/atomic-admission.bundle) retain the experimental revisions, including the initial failure and later fix. The bundle requires `74c4136` as a prerequisite; inspect it with `git bundle verify` before fetching its experimental ref.

## Method and scope

Measurements ran in a VM reporting an Intel Xeon Platinum 8573C, a four-CPU cgroup quota, benchmark affinity on four allowed CPUs and a separate driver CPU. Runs alternate baseline/candidate order. The tmpfs measurements reported no cgroup throttling events, but individual whole-VM steal observations reached 33.3%; these short observations are coarse and host scheduling remains outside our control. The overlay run had two failed quiet checks. The files preserve every observation rather than removing noisy runs.

The [benchmark](../examples/transaction_bench.rs) and [comparison driver](../scripts/transaction_before_after.py) use identical work across separately built revisions; the driver SHA-256 was `f4bc3adf6326b7020fd30c14bbeceb272f9177c13748a5040e2f4768f5411b8a`. Each result records revision and binary hashes, commands, affinity, failures and raw timing samples. These are in-process warm-cache measurements, not a comparison against SQLite or PostgreSQL, a total resource quota, a production latency claim, or evidence of physical power-loss safety.
