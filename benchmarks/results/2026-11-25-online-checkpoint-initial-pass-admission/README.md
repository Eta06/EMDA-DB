# Initial-pass admission for online checkpoint compaction

Revision `8ff55f5b` fixes the order of the initial-copy source read
ceiling, `copy_limits.max_source_log_read_bytes`, in online checkpoint
compaction. The ceiling charges one pass over the captured log prefix. It
was charged only after that pass had verified the whole prefix and, on a
disk-budgeted store, after the replacement reservation had been written.
An under-budget attempt therefore read the entire source and could write a
reservation before refusing. The charge now comes right after the boundary
is captured, before the prefix scan and before any reservation. Copied live
values are still charged against the remainder. Catch-up replay, the final
comparison and the final source confirmation are not charged by this
ceiling.

The charged bytes match the verification: one pass over the log up to the
captured boundary. Checkpoint index reads are a separate, documented cost.

## Tests

Two tests in `src/checkpoint_store/online/tests/copy_budget.rs` run on a
disk-budgeted store:

- **Refusal before corruption.** A payload byte in the middle of the source
  log is corrupted, beyond the header, anchor and witness probes. A ceiling
  one byte below the captured log length is refused with
  `NotPublished(InvalidInput)`. The source log, witness, budget record and
  sibling namespace are byte-identical afterwards, and the budget charge is
  unchanged. An admitted attempt then reports the corruption as
  `InvalidData`.
- **Exact and one short.** The exact ceiling, the captured log length plus
  every copied live value, succeeds and reopens with the complete key/value
  set. One byte short is refused before publication with the same state
  unchanged.

On the previous order the first test failed: the under-budget attempt read
the whole prefix and returned `record checksum mismatch`
(`before-fix.log`). Both tests pass with the fix (`after-fix.log`). All 37
online checkpoint tests and two integration suites passed locally.
Warnings-denied Clippy passed. On `8ff55f5b`, [CI] passed Linux Rust 1.85,
stable, optimized stable and the memory-ceiling job. [macOS] passed both
toolchains, and the [EIO], [ENOSPC] and [cross-engine smoke] workflows
passed. Both tests passed in all five test jobs.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45577085385
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45577085226
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45577085319
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45577085258
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45577085372
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the before-fix and after-fix logs, the focused test
logs and the hosted run records and logs.
