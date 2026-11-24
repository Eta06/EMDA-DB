# Automatic checkpoint rollover outcomes

Revision `21f251a5` tracks the automatic checkpoint slot publication and
managed selection rename separately. `ManagedRolloverFailure` identifies
which publication may have begun. A slot failure leaves the previous
selection active. Once a selection rename has been attempted, a failure
makes the handle fail closed and requires reconciliation on reopen.

Detailed mutations and transactions rejected during automatic rollover keep
the existing I/O kind and text, with the classified cause in the error
source chain. Any raw OS code on the underlying I/O error remains on that
nested error. These rejections occur before the user append. The worker exposes
the classified cause through `last_rollover_failure` and
`stop_with_rollover_failure`. Its text status and pinned-slot deferrals
remain available.

## Fault coverage

- The rollover fixture distinguishes errors before the slot rename, after
  slot publication, before the selection rename, and after the selection
  rename. It checks both on-disk paths, store health and acknowledged data
  after reopening.
- The worker loop preserves slot and selection sync failures separately.
  Reopen recovers acknowledged bytes in both cases, and the final stop
  result returns the same classified cause.
- The public detailed mutation and transaction tests reject a triggering
  write before append and expose the cause. The previous revision, built
  with the new test, lacks that cause. An initial local test caught a text
  compatibility regression. The shipped wrapper preserves the text.

## Cross-platform verification

Native macOS Rust 1.85 passed 58 focused tests. [Hosted macOS] passed the
full suites on both toolchains. [Hosted Linux] passed Rust 1.85, stable,
and the stable optimized suite. [Filesystem EIO], [ENOSPC], and
[cross-engine smoke] passed.

The [raw archive] preserves all 51 benchmark attempts, including five
cross-engine comparisons. The hosted million-key recovery workload verified
53 phases, with 25 under a 48 MiB address-space ceiling. Native and hosted
logs, verification scripts, and a SHA-256 manifest are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45453068398
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45453068553
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45453069100
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45453068592
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45453068473
[raw archive]: raw-artifacts.tar.gz
