# Store-lock ownership verification

Engine revision `cd75fdf89a64d1dde527e52276520020ac2b9f39` shares the flock
descriptor across in-process owners and releases it on the final owner close.
The process lifecycle tests at `fdce95812e7e779e07ead9f58c8f9265ab417181`
check the updated contract. The engine source is identical at both revisions.

| Platform | Library suite at 16 threads | Integration targets |
| --- | --- | --- |
| Linux ARM64 VM, Rust 1.85.0 | Five consecutive runs with 633 passes each | 113 targets, 563 passes, one ignored ENOSPC test |
| Apple silicon macOS, Rust 1.100.0 | 471 passes | 113 targets, 497 passes |

Integration totals use the most recent result for each target. The initial
Linux run omitted four tracked fuzz seed files from the test copy, so one
batch could not compile. Both platforms initially ran two lifecycle tests
which still expected close-only release. The follow-up runs supply the seeds
and execute the corrected tests. The Linux run uses offline builds with test
debug info and incremental compilation disabled, ten test targets per batch,
and 16 test threads. Mac Clippy with warnings denied and formatting checks
passed.

Tests check live-owner exclusion, including independently executed contenders,
then reopen while children still have inherited descriptors. A bare flock
descriptor supplies the close-on-exec control. The fork regressions also
cover shared checkpoint readers, writable checkpoint owners, bounded rebuild
outcomes and retired-stage cleanup retry. The Linux descriptor-exhaustion
test verifies that a failed open releases ownership and can be retried after
freeing descriptors.

These checks exercise a live OS. Physical power-loss and independent off-host
restore remain deferred. A killed acquiring process cannot explicitly
unlock. A child retaining its descriptor can still delay reopening until
exec or exit in that case.
