# Fail-closed disk-budget record publication

Revision `a15a4c98` classifies a managed checkpoint disk-budget record
replacement by where it failed. Before the rename, the previous record is
still visible and the call is an ordinary refusal. At or after the rename,
the visible record is uncertain:

- a first install fences the whole handle, including any failure to build
  the accounting after publication;
- a limit change or a compaction reservation fences the budget.

Appends, compaction, limit changes, reconciliation and budget status are
then refused until reopen, which loads the visible record. A limit below
the current charge is still an ordinary `WouldBlock` refusal with nothing
published. Failed reconciliation or removal keep an in-memory charge that
is never smaller than either visible record's, as documented.

Four tests in `src/checkpoint_store/online/tests/budget.rs` inject a
directory sync failure after the rename:

- an uncertain first install;
- an uncertain lowered limit, where the old, larger limit must not keep
  admitting writes;
- an uncertain compaction reservation, where the visible `Pending::Compacting`
  stays charged after reopen until reconciled;
- an ordinary limit below the charge.

Each checks the visible record, refusal on the open handle, and after
reopen the visible limit at its exact boundary. The 278 checkpoint-store
library tests, four checkpoint integration suites and the full native macOS
suite (1,535 tests, `full-tests.log`) passed, with warnings-denied Clippy.
On `a15a4c98`, [CI], [macOS], [EIO], [ENOSPC] and [cross-engine smoke]
passed, and the four tests passed in all five test jobs.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45583614857
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45583614949
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45583615010
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45583614791
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45583614945
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the local test logs and the hosted run records and
logs.
