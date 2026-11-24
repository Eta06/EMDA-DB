# Online checkpoint-store compaction and reclamation

Revision `6cd96388` adds `OnlineCheckpointStore`. It compacts a managed
checkpoint-backed store while callbacks continue, replays acknowledged writes
into the candidate, and publishes a verified replacement with its own
checkpoint and managed selection by atomic directory exchange. It can then
remove the previous generation. The blocking `CheckpointStore::compact_and_replace`
API is unchanged. The [storage contract](../../../docs/STORAGE_TRANSACTION_CONTRACT.md)
describes the handoff and failure classifications.

## Correctness evidence

Ten new tests in `src/checkpoint_store/online/tests.rs` cover:

- A concurrent writer and an atomic counter running through compaction,
  with an open owned snapshot that keeps its captured state. Every
  acknowledged value is present after reopening, with and without
  automatic checkpoints.
- Reclamation while a snapshot pins the retired generation. The view keeps
  reading, its reservation stays in the shared budget, and the budget is
  released on drop. A second generation compacts the first replacement.
- Injected failures at every pre-exchange point. Each is classified
  `CandidateOutcomeUnknown`, the source stays live and writable, and a
  retry succeeds after the unselected candidate is removed.
- `NotPublished` rejections for an occupied previous path and an
  exceeded copy read limit. A final-delta limit rejects before exchange.
- A parent-sync failure after exchange, which disables the handle. Both
  paths reopen with the complete state.
- Injected failures at eight retirement points, with and without
  automatic checkpoints. The handle stays usable and scoped retired
  cleanup finishes the stage.
- A retirement interrupted midway through removing the automatic
  checkpoint root. Scoped cleanup resumes it, and an unexpected entry keeps
  the stage preserved.
- Process kills at 12 handoff and retirement boundaries. Every
  acknowledged value is recovered at the original path, and cleanup and
  a later compaction succeed.

During review, the scoped-retry gap for a partly removed automatic root was
reproduced by removing only that handling. The resume test then removed no
payload (`automatic-root-gap-repro.log`). With the fix it passes.

The full native macOS suite passed **1,492 tests** with stable Rust
(`full-tests.log`). Formatting and warnings-denied Clippy passed. On the
pushed revision, [hosted CI] passed Rust 1.85, stable and optimized stable on
Linux, including the million-key memory-ceiling job. [Hosted macOS] passed
both toolchains. The new tests passed in all five test jobs. The
[filesystem EIO], [ENOSPC] and [cross-engine smoke] workflows also passed.

These tests run on a live OS with injected errors and process kills. They do
not establish physical power-loss behavior or an independent off-host
restore. Both remain deferred.

## Workload and measurements

`examples/online_checkpoint_reclaim_bench.rs` writes 200,000 8-byte keys
with 64-byte values three times and deletes one eighth. It checkpoints that
history, opens it as a managed checkpoint store and measures a 2-second
writer baseline. It then pins the current generation with an owned
snapshot and runs `compact_and_reclaim` while one thread commits
synchronized single-key puts. After reclamation it checks that the snapshot
still reads its captured state, drops the snapshot and reopens the store
exactly against a model of every acknowledged write. Free space is read
after `sync` and a 3-second settle, before compaction, while pinned, and
after release. Descriptor counts come from `/dev/fd` or `/proc/self/fd`.

Each row is the median of three runs. Writer latency is per synchronized
put. The maximum latency during compaction includes the final pause.

| Host | Compaction | Writer p99 baseline / during | Writer max during | Source → compacted log | Freed after release | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS APFS, Apple M6 | 4.01 s | 8.1 / 11.0 ms | 211 ms | 56.65 MB → 16.37 MB | 57.31 MB | 89.6 MB |
| Hosted Linux x86_64 | 0.91 s | 0.68 / 0.67 ms | 358 ms | 57.86 MB → 17.58 MB | 57.87 MB | 92.6 MB |

**The final pause is not bounded by `max_final_delta_bytes` alone.** While
callbacks are paused the handoff replays the remaining suffix. It compares
every live key and value of source and candidate, here about 176,000–189,000
entries, and reads the complete 56–58 MB source log again. It then publishes
the selection and exchanges and synchronizes the directories. The final
suffix was 200–1,900 bytes. The 203–375 ms maximum write latencies therefore
mostly reflect that store-proportional work. Phases were not timed
separately.

Old-log space was released only after the snapshot was dropped. Open
descriptors rose from 9 to 14 on macOS and from 8 to 12 on Linux while the
snapshot pinned the retired generation, then returned to the baseline.
The freed space matches the retired log length. Free space is a
filesystem-wide reading and can include unrelated activity.

Peak RSS covers the whole process, including fixture creation, the model
and the reopen verification. The Linux attempts did not record the
`RUNNER_TEMP` filesystem type. The macOS runs used one ARM64 Mac mini on
macOS 27.0 with rustc 1.100.0. The Linux runs used GitHub-hosted x86_64
Azure Linux 7.0 with rustc 1.100.0 and glibc 2.39.

The three earlier macOS runs in `runs-unsettled-free-space/` read free space
immediately after release. They showed about −12 KB, because APFS freed the
blocks asynchronously. A diagnostic run with a settle step and descriptor
counts showed the release. The final binary was rebuilt with those readings.
Its hash is in `binary.sha256`. No A/B comparison is claimed: this is a new
path without an earlier implementation.

## Remaining limitations

- Interruption tests cover the in-process hooks named above and run on a
  live OS.
- Recovery still verifies the full covered prefix on open.
- There is no scheduled worker or bounded reclamation latency for this
  store type.
- Owned snapshots can pin retired disk space without a byte limit.
- Neither memory nor disk use is bounded overall.

[hosted CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45485377395
[Hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45485377388
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45485377397
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45485377376
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45485377404
[raw archive]: raw-artifacts.tar.gz

The [raw archive] contains every local and hosted attempt, logs, commands,
binary hashes and environment details.
