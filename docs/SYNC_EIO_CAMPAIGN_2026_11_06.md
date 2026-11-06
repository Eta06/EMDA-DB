# Synchronization EIO campaign

On clean revision `ede2aa5`, we traced successful V2 and V3 write, backup,
restore, format-upgrade, checkpoint and separate-copy-compaction operations.
For each observed `fsync` or `fdatasync` ordinal, `strace` injected one
returned `EIO` into a fresh fixture twice: once with the ordinary store and
once with the checkpoint-backed store reopening first. The second recovery
path reopened afterward. All **460 injected runs** completed their checks.

| Operation | V2 sync ordinals | V3 sync ordinals | Injected runs, both formats and recovery orders |
| --- | ---: | ---: | ---: |
| Write | 10 | 9 | 38 |
| Backup | 17 | 17 | 68 |
| Restore | 12 | 13 | 50 |
| Format upgrade | 22 | 22 | 88 |
| Checkpoint | 17 | 17 | 68 |
| Separate-copy compaction | 37 | 37 | 148 |

The counts include synchronization calls during opening and resolved
directory ancestry as well as the operation itself; they are specific to the
fixture path and environment. Each injected action returned an error. After
reopening, both recovery paths found the two previously acknowledged values.
The source log prefix, reference backup, trusted checkpoint and its digest
remained intact. Copy operations also left the complete source log and
witness unchanged. All 120 copies or checkpoints visible after an error
validated successfully; checkpoint validation used the separately saved
trusted digest.

Six of the failed-write runs recovered the attempted value. The others did
not. Both are permitted unknown outcomes after a write error; callers must
not assume that an error means the write was absent. The probe's negative
controls reject missing acknowledged values, a partial backup or checkpoint,
and a reported successful copy with no destination.

The [raw campaign](../benchmarks/results/2026-11-06-sync-eio-campaign.json.gz)
records the baseline counts, each injected syscall and target descriptor,
reported outcome, source/backup/checkpoint hashes and recovery result. It ran
with `strace` 6.13 on a Linux VM using an overlay filesystem. These are
returned syscall errors on a running OS. They do not cover every syscall,
calls reached only on failure paths, arbitrary concurrent schedules, storage
reordering, physical power loss or off-host restore. A valid checksum is not
authentication. See the [failure matrix](FOUNDATION_FAILURE_MATRIX.md#running-synchronization-eio-injection)
for reproduction and the separately deferred validation.
