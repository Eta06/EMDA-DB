# Checkpoint compaction log budgets

Revision `c8d1e9ba` adds opt-in logical source read and replacement log
limits to `CheckpointStore::compact_to_with_limits_detailed` and the Linux
`compact_and_replace_with_limits_detailed`. The source budget charges the
log length before the full history verification, then charges each live
value before copying it. The replacement log budget charges the new header
and complete encoded records. An exact fit succeeds. A budget rejection
starts no publication and can follow work in a private stage.

Checkpoint files, witness and identity probes, target readback and reopen,
and physical retries have separate costs. The target budget covers the log
file. The target checkpoint and witness have separate sizes.

## What the regressions verify

- For a source log of length **L** and **V** live value bytes, the copy
  succeeds with a source ceiling of **L + V**. A one-byte-short source
  ceiling rejects before publication. A ceiling below **L** also rejects.
- The replacement log accepts exactly its header and encoded live records.
  A ceiling one byte short and a ceiling below the header size both reject.
- The source log, checkpoint, and witness bytes survive rejection. The
  fixture checks that no staging entries remain. The exact copy reopens
  with a batch deletion, an empty value, and a 4 MiB value correctly applied.
  Corruption in a superseded record still fails under an admitted budget.
- The Linux handoff test rejects without changing the managed selection or
  creating the sibling. The exact-budget exchange retains the old log and
  witness and the replacement accepts a subsequent write.

## Cross-platform verification

Before the Linux rerun, `9cba4be8` corrected a Clippy warning in the
Linux-only test. The storage code is identical to `c8d1e9ba`. The
initial Linux CI stopped at lint and the first hosted macOS run was
superseded. Both runs are preserved.

The native macOS Rust 1.85 suite passed 1,361 tests across 112 nonempty
binaries. [Hosted macOS] passed both toolchains. [Hosted Linux] passed Rust
1.85, stable, and the stable optimized suite. [Filesystem EIO], [ENOSPC],
and [cross-engine smoke] passed.

The [raw archive] records 57 benchmark attempts across both revisions.
The rerun records 45 CI smoke attempts, five cross-engine comparisons,
and the million-key Linux recovery workload. All 53 phases of that
workload verified, including 25 under a 48 MiB address-space ceiling.
The initial run records five comparison attempts and its own
million-key recovery attempt. Logs, verification scripts, and a SHA-256 manifest
are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45441083543
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45441083739
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45441083908
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45441084005
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45441083999
[raw archive]: raw-artifacts.tar.gz
