# Extended synchronization EIO checks

On clean revision `bcddef4`, two local campaigns each exercised 698 error
cases: one injected a single `EIO` and the other continued returning `EIO` for
the selected syscall after the chosen ordinal. The driver first traced each
successful path, then tested every observed `fsync` and `fdatasync` ordinal
with both writable recovery paths taking a turn reopening first. The 698 cases
per campaign represent 349 observed ordinals, each tested in both recovery
orders.

| Operation | V2-source ordinals | V3-source ordinals |
| --- | ---: | ---: |
| Ordinary write / backup / compaction | 10 / 17 / 37 | 9 / 17 / 37 |
| Restore / format upgrade | 12 / 22 | 13 / 22 |
| Ordinary checkpoint creation | 17 | 17 |
| Checkpoint-backed write / backup | 3 / 10 | 2 / 10 |
| Checkpoint-backed compaction / rollover | 26 / 10 | 26 / 10 |
| New-store initialization | 11 | 11 |

The source-format label describes the existing reference fixture. New-store
initialization used the filesystem's default V3 format. Counts include store
opening and resolved ancestor-directory synchronization and depend on the
fixture path. All 1,396 injected actions returned errors, and both recovery
paths found the previously acknowledged source values. For each run, the
source log prefix, reference backup, trusted checkpoint and checkpoint digest
were preserved. Copy and initialization operations left the source unchanged.

In each campaign, 192 copies or checkpoints were visible after an error and
all validated. A checkpoint created from the original store was checked using
the separately saved original digest. For a newly compacted checkpoint, the
digest was recomputed from that output, which detects accidental corruption
but does not independently authenticate it. Twelve failed-write runs in each
campaign recovered the attempted value; an error therefore cannot be treated
as proof that the write was absent. Of 44 initialization-error runs per
campaign, 40 targets were openable or could be initialized on retry, while
four incomplete targets failed closed without rewriting existing evidence.

In the persistent-error campaign, 402 cases received more than one injected
error and 402 had top-level staging names remaining before explicit cleanup.
Recognized top-level staging payloads had valid ownership markers; after errors stopped,
cleanup removed all of the fixture's top-level staging names. This observes
the running filesystem and does not prove what survives a physical crash.

The [one-shot](../benchmarks/results/2026-11-06-sync-eio-expanded.json.gz)
and [persistent-error](../benchmarks/results/2026-11-06-sync-eio-persistent.json.gz)
records retain each injected call, result, source hashes, initialization
evidence and recovery outcome. They ran on one Linux VM and overlay filesystem
with `strace` 6.13. They do not exhaust other syscall failures, combinations of
different failing syscalls, arbitrary schedules, nested staging paths, storage
reordering, physical power loss or off-host recovery. See the [failure matrix](FOUNDATION_FAILURE_MATRIX.md#running-synchronization-eio-injection)
for the runner and its limits.
