# Retained rotation prepared off the writer path

`ManagedRetainedStore::compact` and `AuthorizedRetainedStore::compact`
take `&mut self` for the whole rotation, so writes wait while the successor
base is written and the generation is verified. The new rotation job splits
that work into three steps:

- **`begin_rotation`** makes `compact`'s pre-allocation checks and
  verifies the commit witness. It captures the committed state as
  duplicated read-only handles and an overlay copy. Under a catalog quota
  it also reserves room for the successor.
- **`RetainedRotationJob::prepare`** borrows nothing. It verifies the
  captured suffix bytes and every base block, then writes and opens the
  successor in a private stage. It can run on another thread while the
  store keeps acknowledging writes.
- **`complete_rotation`** checks that the catalog has not rotated and that
  the bytes since the capture extend the captured suffix digest. It replays
  those writes in commit order into the successor, merged into bounded
  batches whose exact size is admitted first. It then rechecks the live
  generation's files and witness, and publishes the successor and the
  selection or authority record exactly as `compact` does.

## Tests

The tests are in `src/retained_candidate/writable/managed/tests.rs` and
`.../managed/authority/tests.rs`:

- **Concurrent writes.** A put, a delete and a batch are made while the job
  prepares on another thread. All are present after completion and after
  reopen, and the previous generation can be reclaimed.
- **Authority.** An authorized catalog does the same. Its authority record
  names the successor, and a reopen through the authority recovers
  everything.
- **Interruption.** A dropped job leaves no stage. A child process that
  prepares a job and exits without cleanup leaves a stage that
  `cleanup_abandoned_staging` removes, after which the data is intact and
  a new job completes.
- **Refusals.** A second job is refused while one is outstanding.
  Completion is refused without preparation, and after another rotation.
- **Quota.** With room one byte short of the reservation, `begin_rotation`
  is refused before staging. With room for the reservation and one append,
  a second append is refused while the job holds its room.

Mutation checks: without the catch-up, the concurrent-write and quota tests
fail. Without the reservation in append admission, the quota test fails.

## Measurement

`examples/retained_rotation_job.rs` ran on macOS 27.0, Apple M6, APFS, Rust
1.100.0, release build. Two identical catalogs hold 200,000 keys with
64-byte values and a 2,000-update suffix. One rotates with `compact`. The
other uses a job and keeps writing synchronized puts during preparation.
Every value checked was correct after reopening.

| Trial | `compact` | `begin_rotation` | `prepare` (writers continue) | Writes during prepare | `complete_rotation` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 98.3 ms | 3.2 ms | 620.2 ms | 97 | 25.2 ms |
| 2 | 98.3 ms | 3.9 ms | 558.8 ms | 91 | 23.5 ms |
| 3 | 101.8 ms | 4.2 ms | 627.6 ms | 96 | 19.6 ms |
| 4 | 111.9 ms | 1.9 ms | 511.2 ms | 107 | 8.6 ms |
| 5 | 97.6 ms | 1.6 ms | 306.9 ms | 79 | 33.2 ms |

Writers were excluded for about 10 to 37 ms per job (`begin_rotation` plus
`complete_rotation`) instead of about 100 ms for `compact`. Preparation
took longer than `compact` itself because it competed with the
synchronized writes and verified the generation separately. Writers kept
making progress throughout it.

An earlier draft replayed each caught-up record with its own sync. Its
completion took 520 ms in a single trial, longer than `compact`. Merging
the catch-up into bounded batches fixed that.

## Limits

This is one host and filesystem with a synthetic workload. Recovery at
deployment scale needs a defined deployment.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every trial's output, the workload source, the
binary hash, the host description and `SHA256SUMS`.
