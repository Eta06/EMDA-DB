# Persistent disk budget for managed checkpoint stores

Revision `5a043126` adds a persistent logical disk budget to one managed
checkpoint-backed store. `196a5130` makes a pre-existing backup-test lock
race deterministic, and `d4e92be1` bounds the concurrent writer keys in two
benchmark workloads. Neither later revision changes the engine. The
[storage contract](../../../docs/STORAGE_TRANSACTION_CONTRACT.md) defines the
accounting. This is a single-store slice of the open project-wide quota
criterion. It does not establish a project, backup or memory quota, or
filesystem-allocation enforcement.

## Accounting contract

The budget charges logical file lengths, never block allocation. Charged
bytes are the sum of:

- every regular file in the store directory, plus the selected checkpoint
  when it lies outside that directory, and one spare 4,096-byte record
  length;
- a durable reservation for the complete online replacement;
- a previous generation while it is named or held in a marked retired stage;
- retired generations that owned snapshots in this process still hold open.

The limit and any outstanding maintenance live in a checksummed
`disk.budget` record inside the store directory. Every checkpoint-store
open reloads it under the exclusive store lock. Ordinary `Store` opens and
managed rebuilds check for it under the same lock and refuse the
directory. Managed rollovers, automatic checkpoints and the blocking
handoff are refused because they would allocate selected checkpoints the
budget does not reserve.

## Tests

Ten deterministic tests in `src/checkpoint_store/online/tests/budget.rs`
cover:

- An exact limit admits one record, the next record is refused with
  `StorageFull` and the store is unchanged. A limit below the charge is
  refused. The limit and charge survive reopen, including through the plain
  checkpoint-store entry point.
- Ordinary opens, managed rebuilds, managed rollovers, enabling automatic
  checkpoints, retain-mode online compaction and the blocking handoff are
  refused for a budgeted store.
- A limit 1 KiB above the charge refuses compaction before anything is
  published.
- An append admitted during compaction is checked against the live charge
  plus the replacement reservation. One fits and the next is refused.
- The charge stays at or above the observed file bytes at hooks during
  copy, final barrier and retirement. It stays with a held snapshot and is
  released when the snapshot is dropped.
- An injected pre-exchange failure keeps the reservation on disk across
  reopen, and reconciliation releases it only after the unselected
  candidate is removed.
- A failed retirement stays charged until scoped cleanup finishes it.
- Process kills at six handoff and retirement points leave every remaining
  file charged after reopen. Cleanup and reconciliation then return to the
  live size.
- A reclaimed generation's charge ends only when its owned snapshot, the
  last owner of its generation token, is released.

The full native macOS suite passed 1,503 tests before the double-count fix
below (`full-tests.log`). After the fix, the checkpoint-store (254),
staging (51) and ordinary online-store (100) library tests and four
integration suites passed (`focused-final.log`). Formatting and
warnings-denied Clippy passed.

Hosted results for `5a043126`: [macOS][macOS 5a04], [EIO], [ENOSPC] and
the [cross-engine smoke] passed. In [CI][CI 5a04], Linux stable and the
memory-ceiling job passed, and the Linux Rust 1.85 job failed in
`tests/backup.rs` as described below. On `196a5130`, [CI][CI 196a] passed
Rust 1.85 and the stable test steps. Its stable benchmark smoke failed in
the reclaim workload, also described below. [macOS][macOS 196a] passed
both toolchains. On `d4e92be1`, [CI][CI d4e9] passed every job, including
both benchmarks on Linux.

## Measurements

`examples/online_checkpoint_budget_bench.rs` builds a 200,000-key managed
store with overwritten and deleted history and installs a budget. It
checks that a limit equal to the charge refuses both a put and a
compaction. It then compacts and reclaims under a concurrent synchronized
writer with an owned snapshot held. A sampler records the charge and the
named files the store owns. Quiescent phase readings are exact. Each value
is the median of three runs.

| Phase | macOS APFS charged | Files | Hosted Linux charged | Files |
| --- | ---: | ---: | ---: | ---: |
| After fill | 61,115,366 | 61,111,270 | | |
| Peak during compaction | 98,851,366 | 82,063,414 | 99,627,368 | 83,156,465 |
| After reclaim, snapshot held | 77,525,886 | 20,897,820 | | |
| After snapshot release | 20,901,916 | 20,897,820 | 21,695,233 | 21,691,137 |

No sample saw more named file bytes than were charged (0 of 903 macOS and
0 of 362 Linux median samples). The quiescent difference is the 4,096-byte
spare record. The peak reservation was 37.7 MB. Of that, 16 MiB is the
default catch-up allowance and 64 KiB is the metadata allowance, which this
run did not use. After reclamation, the 56.6 MB previous generation stayed
charged as pinned until the snapshot was dropped. Each reopen kept the
charge. Median peak process RSS was 89.5 MB on macOS and 89.3 MB on Linux,
including the benchmark's full model of the data. The Linux figures come
from the `d4e92be1` attempts. The macOS runs used Rust 1.100.0 on APFS.

Two earlier macOS benchmark revisions are kept as superseded runs:

- `superseded-double-count/` charged the generation being retired both as
  named files and as pinned. Its peak charge reached 134 MB against 82 MB
  of files. The engine now charges that generation once, at the larger of
  the two measures, until its names are gone.
- `superseded-sampler-race/` counted the external base checkpoint after it
  was no longer selected, and could read the selection before and after an
  exchange. That gave 5–6 samples per run up to 4.5 MB above the charge.
  The final sampler counts the external file only when it was selected both
  before its walk and after its status read.

## Investigated CI failures

**Backup test lock race.** The Linux Rust 1.85 job on `5a043126` failed
`restore_log_limit_bounds_witnessed_and_nonempty_legacy_backups` with
`NotPublished(WouldBlock)` (`job-138801653097.log`). The test took a raw
`flock` on its parent directory and only dropped the file. A child forked
by a parallel test in the same binary can hold a duplicate until it execs.

- `probe-v2-cross-thread.rs` reproduces the retained lock and `EAGAIN`, and
  shows that an explicit unlock releases it.
- An earlier same-thread probe was invalid because `spawn` waits for the
  child's exec. It is kept.
- The backup code and tests were unchanged from parent `2379c281`, and
  engine lock holders already unlock explicitly.
- 400 runs of the whole backup suite passed on parent, current and fixed
  binaries, with hashes recorded. One earlier repetition used wrong binary
  paths and is kept as invalid.
- `196a5130` adds the explicit unlock to the test. Rust 1.85 Linux then
  passed.

**Benchmark key cap.** On `196a5130` the stable Linux benchmark smoke failed
in `online-checkpoint-reclaim-1` with `checkpoint suffix key limit would be
exceeded` (`stable-196a5130-benchmark-smoke/`). Its writer used a new key
per put, so a fast runner exceeded the engine's 16,384-key suffix cap.
`d4e92be1` makes both workloads cycle 4,096 keys per writer and verify the
latest acknowledged values. The engine cap is unchanged.

## Not covered

- Filesystem block allocation, metadata overhead and device free space.
- Other processes and older binaries that ignore the record.
- External checkpoints once no longer selected. They are not removed.
- Backups and restored copies, which do not carry the record.
- Project-wide accounting across stores, and memory.
- Physical power loss and independent off-host restore, which remain
  deferred.
- The local Lima VM had 114 MB of free root disk and 230 MB of available
  memory (`lima-capacity.txt`), so Linux evidence is from hosted runners.

[CI 5a04]: https://github.com/Eta06/EMDA-DB/actions/runs/45494611379
[macOS 5a04]: https://github.com/Eta06/EMDA-DB/actions/runs/45494611462
[CI 196a]: https://github.com/Eta06/EMDA-DB/actions/runs/45499888466
[macOS 196a]: https://github.com/Eta06/EMDA-DB/actions/runs/45499888435
[CI d4e9]: https://github.com/Eta06/EMDA-DB/actions/runs/45501665100
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45494611350
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45494611629
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45494611460
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every local and hosted attempt, failed and
superseded runs, probes, repetition logs, job logs and binary hashes.
Run directories under `hosted/` are named by workflow run ID.
