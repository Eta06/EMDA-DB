# Source read ceiling for each online checkpoint handoff

Revision `6f353e3b` adds
`OnlineCheckpointStore::set_maintenance_source_read_limit`. It is a
per-handle inclusive ceiling on the logical source reads of each online
checkpoint handoff, including scheduled worker handoffs. It is the
checkpoint counterpart of the ordinary `OnlineStore` setting. The default
is unlimited, no public struct changed, and the setting resets on reopen.

## Charged reads

Each read is charged before it happens:

- one pass over the captured log prefix;
- each copied live value, inside the existing copy loop;
- each catch-up and final replay range, and each value reread from it;
- each source value in the final comparison;
- the complete final source confirmation, reserved before the final replay.

These are not charged:

- checkpoint index reads;
- candidate reads;
- witness and identity checks;
- physical retries.

The initial-copy ceiling `copy_limits.max_source_log_read_bytes` and the
recovery ceiling `cleanup_retired_staging_with_scan_limit` keep their own
metrics. The source read budget moved to a shared internal module and is
passed directly. Ordinary copies pass an unlimited budget, so their hot
path makes no dynamic call. The ordinary online copy module remains
private.

## Tests

Three tests in `src/checkpoint_store/online/tests/maintenance_reads.rs`
use a disk-budgeted store:

- **Before publication.** One byte short of the initial pass, and one byte
  short of the initial pass plus every copied value, each return
  `NotPublished`. The source log, witness, budget record, budget charge and
  sibling namespace are unchanged, and clearing the limit restores the
  default.
- **Concurrent handoff.** Acknowledged writes land during catch-up and just
  before the final barrier, so both replay call sites read. Success reports
  nonzero concurrent and final replay bytes. The exact total is computed
  independently from the log lengths and value sizes. It succeeds and
  reopens with every acknowledged value. One byte short is refused under
  the final barrier as `CandidateOutcomeUnknown`: the source stays live with
  every write, the candidate and its reservation remain, and
  reconciliation and a new compaction succeed.
- **Worker.** A scheduled handoff on the ceiling stops the worker with a
  `Compaction` failure and leaves the data intact.

As a mutation check, the final replay was given an unlimited budget. The
one-short run then succeeded and the test failed
(`mutation-final-replay-unbudgeted.log`). The full native macOS suite
passed 1,531 tests (`full-tests.log`), and warnings-denied Clippy passed.
On `6f353e3b`, [CI] passed Linux Rust 1.85, stable, optimized stable and
the memory-ceiling job. [macOS] passed both toolchains, and the [EIO],
[ENOSPC] and [cross-engine smoke] workflows passed. The three tests passed
in all five test jobs.

## Still open

Recovery and the final source confirmation still scan the whole covered
log, so these ceilings grow with the store. Total process memory and
project-wide quotas remain open.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45579839001
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45579838988
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45579839299
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45579839410
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45579838973
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the focused and full test logs, the mutation-check
log, the committed diff and the hosted run records and logs.
