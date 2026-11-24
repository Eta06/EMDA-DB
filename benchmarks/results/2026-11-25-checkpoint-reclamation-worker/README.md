# Scheduled reclamation for online checkpoint stores

Revision `30d52d14` adds `OnlineCheckpointStore::start_reclamation_worker`.
It is an opt-in worker that runs the verified online checkpoint handoff
and reclamation on an interval or on request. It reuses the ordinary
worker's lifecycle and status types, and its loop is now shared. The loop
adds a deferred-check count, and the ordinary worker's behavior is
unchanged. The [storage contract](../../../docs/STORAGE_TRANSACTION_CONTRACT.md)
gives the policy.

It closes this sentence in the online compaction follow-up: "there is no
scheduled worker for this store type". The broader items for online
compaction with bounded recovery, safe live-log reclamation for all
snapshot lifetimes, and project-wide RAM remain open. The worker does not
bound reclamation latency, recovery, memory or pause time, and it does not
persist across reopen.

## Policy

- **Lifecycle:** explicit start and stop, one worker per handle (a second
  start returns `AlreadyExists` until the first stops), a 1 ms to 24 hour
  interval, coalesced requests, and waiting on a condition variable without
  spinning.
- **Thresholds:** a minimum log size, and growth since the boundary observed
  after the worker's own handoff.
- **Deferral:** decided while holding the handle's maintenance lease, which
  every manual compaction, cleanup and budget reconciliation also takes.
  The worker defers when a generation previously replaced through the
  handle is still held by an owned snapshot, so scheduled handoffs never
  stack retired generations. It also defers when the disk budget records
  maintenance that cannot yet be reconciled, or refuses the replacement
  reservation. Nothing is reserved or published when it defers.
- **Stopping:** it stops, recording the failure, on a leftover previous
  path, a failed or uncertain handoff, or a failed reclamation. It never
  removes a candidate, a named previous generation or a retired stage it
  did not just create.

## Tests

Eight tests in `src/checkpoint_store/online/worker/tests.rs` cover:

- A scheduled handoff under a concurrent writer with an owned snapshot,
  with and without a disk budget. The snapshot keeps its captured state,
  live and reopened data match every acknowledged write, no staging
  remains, and a budget returns to the live charge.
- Growth hysteresis with coalesced requests, a refused second worker and a
  restart after stop.
- Deferral while a replaced generation is pinned, and the handoff after
  release.
- Deferral while the disk budget cannot fit a replacement, and the handoff
  after the limit is raised.
- A stop on an interrupted candidate left at the previous path. The
  candidate's files and the budget's pending compaction are preserved, and
  a new worker refuses to start.
- The stop and deferral classification of uncertain exchanges,
  budget refusals and maintenance conflicts.
- A manual compaction paused just after its reservation became durable.
  The scheduled path, a worker check and public reconciliation all see the
  maintenance lease as busy, and the reservation is unchanged.
- A manual handoff between the worker's preflight and its compaction. The
  scheduled compaction then defers under the lease, and only one retired
  generation is held.

The last two came from an independent review. The first version reconciled
the budget and checked for pinned generations before taking the lease.
These tests cover that window. The full native macOS suite passed 1,514
tests (`full-tests.log`), and warnings-denied Clippy passed. On `30d52d14`,
[CI] passed Linux Rust 1.85, stable, optimized stable and the
memory-ceiling job. [macOS] passed both toolchains, and the [EIO],
[ENOSPC] and [cross-engine smoke] workflows passed. The worker tests passed
in all five test jobs. These tests run on a live OS with injected failures
and concurrent writers. They are not physical power-loss tests.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45507049074
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45507049185
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45507049111
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45507049122
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45507049058
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the native test log and the hosted run records and
logs.
