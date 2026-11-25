# Bounded recovery after online checkpoint handoffs

Revision `1d4f8ae1` checks bounded reopen of the selected generation after
online checkpoint compaction and reclamation. It also bounds the
verification that finishes an interrupted retirement. The combined scan
budget counts logical checkpoint and log bytes read during open. It is not
a bound on physical I/O, retries, elapsed time, memory or project quotas.

## Limit coverage

Recovery uses the existing
`CheckpointStore::open_managed_with_total_recovery_scan_limit`, followed by
`OnlineCheckpointStore::from_store`. A clean open scans the selected
checkpoint file and the complete witnessed log once. Tests in
`src/checkpoint_store/online/tests/recovery_budget.rs` cover:

- **Clean handoff.** A clean handoff with acknowledged writes made during
  it and a snapshot captured before it, with and without a disk budget. The
  selection names the compacted replacement. One byte below the budget is
  refused with `NotAdopted(InvalidInput)`, before selection synchronization,
  stage cleanup or tail adoption, and every file in the parent directory is
  byte-identical afterwards. The exact budget recovers the complete
  expected key/value set and leaves the disk-budget charge unchanged.
- **Interrupted handoff.** Process kills at `InitialCopy` and
  `BeforeExchange` (an unselected candidate remains), `AfterExchange` (the
  previous generation remains named), and `AfterMove` and `AfterRecords` (a
  retired stage remains). The same exact and one-short checks apply. The
  bounded reopen keeps the pending compaction or retirement charged.
  Cleanup, reconciliation, a new compaction and a second bounded reopen
  then succeed.
- **Scoped cleanup.** The only new code is
  `cleanup_retired_staging_with_scan_limit`. Scoped cleanup had re-verified
  the current generation's checkpoint and full log with no ceiling. The new
  ceiling is admitted inside the checkpoint index validation. It uses the
  open view's pinned checkpoint descriptor after reading only the header,
  and the log length after the witness for that length is verified under
  the writer gate. It applies only when a stage is about to be removed. One
  byte short returns `NotStarted(InvalidInput)` with every file and the
  disk-budget charge unchanged, and the exact ceiling removes the stage.
  Substituting a larger file at the selected checkpoint's path, as a new
  inode, does not change the admission.
- **Appends after open.** Acknowledged appends made after the replacement
  was opened count once. The first version charged them twice because the
  witnessed boundary captured at open was older. `suffix-gap-before-fix.log`
  records that failure, and the fix passes the freshly witness-verified
  boundary on the bounded path only. With no stage to remove, a zero
  ceiling reads nothing.

## Verification

The four recovery tests, the 269 checkpoint-store library tests and five
checkpoint integration suites passed locally on macOS APFS with Rust
1.100.0. Formatting and warnings-denied Clippy passed. On `1d4f8ae1`,
[CI] passed Linux Rust 1.85, stable, optimized stable and the
memory-ceiling job. [macOS] passed both toolchains, and the [EIO],
[ENOSPC] and [cross-engine smoke] workflows passed. The four recovery tests
passed in all five test jobs. The process kills run on a live OS; physical
power loss remains deferred.

## Residual gaps

- Recovery still scans the complete covered log, so the budget grows with
  the store.
- In this revision the online handoff's catch-up replay, final comparison
  and final source-log confirmation have no read ceiling.
- Total process memory and project-wide accounting remain open.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45575536735
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45575536867
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45575536746
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45575536722
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45575536696
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the focused test logs, the pre-fix failure, the
committed diff and the hosted run records and logs.
