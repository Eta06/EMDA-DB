# Pipelined commit for ordinary-store transactions

Before this change, a writing `Store` transaction took the exclusive write
barrier for validation and held it through its batch's synchronization.
Independent transactions therefore ran one sync at a time.

## Change

A writing transaction now takes **shared** barrier access, like an ordinary
put, and holds it until it is acknowledged. Transactions under live-key
limits keep the exclusive path, because those limits need stable
membership.

- **Pending registry.** The group-commit coordinator registers the keys of
  every queued record when it is queued, and removes them after its index
  update is published.
- **Validation.** It runs under the coordinator's queue mutex, immediately
  before the batch is queued. It checks the published index as before, and
  checks the transaction's observations against the pending keys. Every
  record ordered before the transaction in the log is therefore either
  published, and caught by the existing validation, or pending, and caught
  by the new check. A read-set transaction conflicts when a pending write
  touches an observed point or falls inside an observed range. A
  conservative transaction conflicts on any pending write.
- **Backpressure.** A transaction that conflicts on a pending write first
  waits, under its deadline and cancellation, until that write is
  published. A retry therefore sees it, instead of spinning while it
  synchronizes. An earlier draft without this wait exhausted the retry
  limit in `tests/long_transaction_history.rs`.
- **Exclusive writers.** Exclusive writers and maintenance still wait for
  every shared holder: compare-exchange, increment, membership changes
  under live-key limits, backups and compaction. They therefore see no
  unacknowledged writes in flight.
- **Unchanged.** Results become visible only after synchronization. A
  failed sync fails the store closed. Read-only transactions keep their
  validation without the barrier.

## Tests

`src/store/pipelined_commit_tests.rs` holds the first transaction's sync
open:

- A disjoint read-set transaction, with a point read and a range scan,
  validates and queues behind it. Nothing is visible before the sync, and
  both survive reopen.
- A transaction whose observed point or range overlaps the pending write,
  or a conservative transaction, neither queues nor returns while the write
  syncs. It then returns a conflict, and a retry sees the write.
- Increment and compare-exchange wait for the pending transaction. The
  exchange compares against the acknowledged transaction write.
- Eight clients mix disjoint and contended transfers, retrying conflicts.
  They commit all 240 transfers, and the balance total survives reopen.

Mutation checks: without the pending check, the overlap test and the
concurrent transfer test both fail; the balance invariant breaks. Without
the pipelined path, the disjoint test fails. The existing serial-history
models, long contended histories, deadline and cancellation models and
process-kill transaction histories all pass. So does the full native
suite.

## Measurement

`examples/transaction_bench.rs` ran on macOS 27.0, Apple M6, APFS, Rust
1.100.0, release build: 8 clients, 100 two-key read-modify-write
transactions each, three interleaved trials per configuration, `01b5b048`
against this change. Every write was synchronized. Ranges are across
trials.

| Mode | Workload | Revision | Transactions/s | p99 | Conflicts |
| --- | --- | --- | ---: | ---: | ---: |
| Read set | Disjoint | Before | 156 to 175 | 54.9 to 63.0 ms | 0 |
| Read set | Disjoint | After | 662 to 698 | 14.0 to 17.2 ms | 0 |
| Read set | Shared | Before | 162 to 174 | 842 to 1,132 ms | 927 to 968 |
| Read set | Shared | After | 158 to 173 | 487 to 643 ms | 4,055 to 4,240 |
| Conservative | Disjoint | Before | 159 to 171 | 712 to 921 ms | 980 to 1,046 |
| Conservative | Disjoint | After | 158 to 170 | 633 to 823 ms | 4,010 to 4,109 |
| Conservative | Shared | Before | 166 to 172 | 671 to 939 ms | 988 to 1,036 |
| Conservative | Shared | After | 163 to 168 | 523 to 752 ms | 3,306 to 4,029 |

Disjoint read-set transactions now share syncs: throughput rose about
fourfold and p99 fell about fourfold. Contended and conservative workloads,
where every transaction conflicts with its neighbors, kept the same
throughput, and p99 was lower in five of the six pairs. They report about
four times as many conflicts: a transaction now conflicts on an
overlapping in-flight write, waits for it to publish, and retries.
Previously it waited on the barrier and conflicted after the sync.

This is one host and filesystem with a synthetic workload. PostgreSQL was
not available on this host for the equivalent comparison the follow-up
item asks for.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every run's summary, the measurement script, the
workload source, both binary hashes, the host description and
`SHA256SUMS`.
