# Offline managed-checkpoint rebuild outcomes

The opt-in `rebuild_managed_selection_detailed` and
`rebuild_managed_selection_with_limits_detailed` APIs return a
`ManagedRebuildFailure` when a rebuild fails. They classify the current call's
checkpoint and selection publication attempts:

| Failure | Meaning |
| --- | --- |
| `NotPublished` | This call began neither checkpoint nor selection publication. |
| `CheckpointOutcomeUnknown` | The checkpoint may have been published; selection publication did not begin. |
| `SelectionOutcomeUnknown` | Selection publication may have occurred; inspect and reconcile the visible selection. |

The classifications are conservative: for example, a pre-existing destination
is reported as checkpoint-uncertain once this call attempts non-overwriting
publication. Existing files or temporary staging artifacts can remain in any
case. A failed call returns no new checkpoint digest. The methods release the
store lock before returning, so callers must coordinate other writers before
inspecting or reopening either path. Panics do not return a classification.
The error does not certify source health, cleanup, or a backup.

Full-log recovery happens before checkpoint publication and can itself
synchronize and adopt a complete, previously unacknowledged tail. Consequently,
`NotPublished` does not mean the source is unchanged. A focused test constructs
that situation with V2 and V3 witnesses, then rejects malformed selection
metadata before either new publication begins. Existing non-detailed methods
retain their return types and behavior.

## Evidence

- On implementation revision `92b99e5`, stable debug, Rust 1.85 debug and
  stable release each passed 1,015 tests across 142 binaries; each ignored the
  guarded tmpfs ENOSPC case. Stable and minimum-version Clippy passed with
  warnings denied. The [evidence manifest](../benchmarks/results/2026-11-14-managed-rebuild-outcomes-evidence.json)
  links checksummed logs. Candidate `e5e30c3` adds only the suffix-aware fault
  probe, driver, verifier tests and workflow entries; its probe tests passed on
  both stable Rust and Rust 1.85, and 147 Python tests passed.
- Focused V2 and default-format tests cover missing destinations, locks and
  limits, existing files, checkpoint-sync and checkpoint-verification errors,
  failures before and after selection rename, previous and new selection
  visibility, full-log and checkpoint recovery, and restoration of a local
  backup. Some errors after an attempt are intentionally classified as unknown
  even when this particular test observes the old selection.
- The [fault archive](../benchmarks/results/2026-11-14-managed-rebuild-outcomes-faults.tar.gz)
  contains 72 independently verified reports covering 4,240 injected `EIO`
  and `EAGAIN` errors. They cover both formats, both recovery orders, a first
  selection and an existing selection, with and without an acknowledged
  suffix after the earlier checkpoint. The suffix cases bind the resulting
  selection to an independently prepared expected checkpoint digest and check
  the previous checkpoint and reference backup. The recorded outcomes were
  2,496 not-published errors, 912 checkpoint-uncertain errors, 640
  selection-uncertain errors and 192 successful operations after a handled
  error. The campaigns inject individual returned errors into observed sync,
  data, read, xattr-read, metadata, path-read, open, namespace and lock calls;
  they do not exhaust failure-only call paths or combinations of failures.
- A 121-second AddressSanitizer managed-checkpoint model completed 3,843
  executions on `92b99e5`, followed by replay of all 383 saved corpus files.
  The [archive](../benchmarks/results/2026-11-14-managed-rebuild-outcomes-model.tar.gz)
  includes the inputs and logs. It mixes detailed and original rebuilds,
  logical limits, rejected destinations, existing files and ordinary mutations.
  This is a finite serial model, not a proof of concurrent behavior.
- The existing [guarded ENOSPC regression](../benchmarks/results/2026-11-14-managed-rebuild-outcomes-enospc.json)
  passed in an isolated 8 MiB, 64-inode Docker tmpfs on `e5e30c3`. It checks
  V2/V3 writes and copy/retry behavior; it does not specifically inject ENOSPC
  into the new detailed rebuild method or emulate a physical disk failure.
- A [process-termination campaign](../benchmarks/results/2026-11-14-managed-rebuild-crashes.json)
  on `39279c1` killed 600 paused rebuild processes across tmpfs and workspace
  overlay, with V2/V3, an existing or absent selection, and three publication
  points. Every case checked cleanup exclusion while active, the resulting
  selection, unchanged source and witness evidence, both writable recovery
  paths, the new checkpoint against a separately prepared digest, and a local
  backup restore. Stable, Rust 1.85 and optimized focused tests passed. These
  terminations occur on a running OS and do not simulate physical power loss.

To verify a retained fault report, extract the archive and run
`python3 scripts/verify_one_shot_fs_results.py <extracted-report>`. The model
corpus can be replayed with
`cargo +nightly-2026-11-03 fuzz run managed_checkpoint <corpus-path> -- -runs=384 -max_len=128 -timeout=15`.

These checks do not establish physical power-loss safety, independent off-host
restoration, total memory bounds, tenant isolation or production readiness.
