# Integration handoff — 2026-11-16

The local source-of-truth branch is `integration/steps-1-3-20261116`, based on
`a2f6192`. Do not push or merge into main without the owner's approval.
The owner supplied the local Git author for integration commits.

Before creating this branch, all 11 existing worktrees were inventoried and
their changed and untracked files preserved in
`/Users/emir/EMDA-workspace-preservation-20261116.tar.gz` (SHA-256
`e13275676e9c0a818d158ebf7cefe11f41ebe810048ea1c77c8d35d6efee9223`).
The archive's 178 file entries were verified by reading them back. It includes
binary and staged diffs plus status for each worktree. A separate verified Git
bundle, `/Users/emir/EMDA-git-refs-20261116.bundle` (SHA-256
`1b6e4687842a9932d7d51fd9ac13d5328ff60d68824e2b4f9565600f46a27dc`),
preserves 187 refs, including T3 checkpoints. Neither artifact is an off-host
backup or restore drill.

After further integration-checkout changes, a second local archive,
`/Users/emir/EMDA-integration-20261117.tar.gz`, was verified against 143 file
hashes (archive SHA-256
`2c5cbf1c6662fbfbb86ed74d3d2ebfd84717f946ca8fbea3d0cc17abf9e75bbc`).
It predates subsequent edits to this handoff and any later work.

A later local archive, `/Users/emir/EMDA-integration-20261117-tail-and-oracle.tar.gz`
(SHA-256 `5c6386e840561bab8d877cf7a7a4b0596a11995f0d3cc3e1e04ae342e4e57665`),
contains the then-current changed and untracked files, Git status and binary
diffs. Its 154 archived file hashes were read back and verified. It predates
this paragraph and any later changes; it is not an off-host backup.

There were 159 T3 checkpoint refs. Their per-ref comparison against the
integration checkout is in `/Users/emir/EMDA-checkpoint-inventory-20261116.tsv`;
per-path differing blobs are in `/Users/emir/EMDA-checkpoint-differences-20261116.tsv`.
There are 58 distinct differing paths, of which four seed paths are absent by
name but present with identical bytes under `fuzz/large-record-recovery-seeds`.
Differences include earlier snapshots and are not automatically safe to drop.
An independent audit of five differing worktrees found earlier or narrower
versions of changes in the integration checkout. One intentionally omitted
API difference is the older public export of the internal recovery-buffer
constant; it remains preserved in its original worktree and archive.

The retention and document-fault worktrees have identical changed files to
the integration checkout. Other worktrees have differing versions. The
`t3/recovery-resource-control` worktree has a distinct lock-release patch;
prior review found unresolved ownership and error-classification concerns, so
it is preserved and not integrated wholesale. The early replay, document,
fault and fuzz worktrees contain earlier or differing files; reconciliation is
ongoing. No worktrees have been deleted or reset.

The interrupted bounded-rebuild fuzz oracle has been extended to check a
generator-derived historical key-count peak, selected checkpoint path and
digest, and the V3 `commit.state` anchor. A Linux instrumented seed smoke run
passed, followed by 89,989 runs in 301 seconds without a reported finding;
the [log and corpus](../benchmarks/results/2026-11-16-linux-arm64-bounded-oracle-v2-fuzz.tar.gz)
are retained. The tested target SHA-256 was
`216a100d2218fd68f04324e8a1d07a5cd1b13bf358bee2374070b64c1c83501a`.
The earlier fuzz campaigns used the previous oracle. A further independent
generator model now checks final live-value offsets, lengths and checksums from
DB01/DB02 record sizes. Independent review found no arithmetic defect. An
address-sanitized Linux run completed 27,647 executions in 61 seconds without
a reported finding; the [log and corpus](../benchmarks/results/2026-11-17-linux-arm64-bounded-offset-oracle-fuzz.tar.gz)
are retained (target SHA-256
`792a6d259461418ed7847594542497d7333acedeef7f7b674f5f74b650b98217`).
The generator still has small keys, values and batches and does not check
intermediate locations later overwritten. A later seed checks a two-run merge
at 323 and 324 bytes; it passed as a fixed seed on macOS and in the Linux
address-sanitized target ([log](../benchmarks/results/2026-11-17-linux-arm64-merge-peak-seed.log.gz)).
That boundary probe was not part of the cited fuzz run.

The bounded rebuild remains experimental. A post-verification spill corruption
bug was reproduced and addressed using per-entry checks; targeted and broad
tests are recorded in `LINUX_ARM64_VALIDATION_2026_11_15.md`.
In-memory suffix emission now consumes its sorted map and moves keys instead
of retaining and cloning previously emitted entries. On macOS Rust 1.85, 16
rebuild/spill tests and the seeded rebuild model passed. This reduces transient
retention only; it does not bound total memory, recovery time, or log growth.
The Linux
parallel rebuild test has also failed with `NotPublished(WouldBlock)` before
an injected hook; the lock holder was not captured, and the historical failure
remains unexplained. Do not weaken assertions or silently retry it away.
Two subsequent Linux 16-thread library controls under `strace` passed, first
532 and then 533 tests. Their traces are retained under `benchmarks/results`;
the second used test-only lock-holder diagnostics but did not trigger them.
These passing controls do not identify the earlier holder.
A further six traced 16-thread Rust 1.85 library rounds each passed all 534
tests; the [per-round logs and traces](../benchmarks/results/2026-11-17-linux-arm64-lock-diagnostic-six-rounds.tar.gz)
are retained. The campaign would have stopped at the first failure, but none
occurred. It does not explain the earlier failed run. The Linux diagnostic
also identified the expected process in a separately held-lock fixture while
returning the original error; it does not retry failed acquisitions.
An untraced 16-thread campaign then stopped on its first round with three
failures: one fixture reopen returned `EAGAIN`, one witness-substitution hook
was not reached, and another test observed an unchanged witness. The
[round log](../benchmarks/results/2026-11-17-linux-arm64-lock-untraced-failure.tar.gz)
is retained. At the time those exact paths did not yet report lock holders;
diagnostics were extended there for a separate follow-up campaign, with the
original assertions and errors retained.
That follow-up also stopped on its first round: 529 passed and five failed,
all reporting `EAGAIN` at fixture opens. For one, diagnostics recorded the
lock path, device and inode, but `/proc/locks` contained no matching entry by
the time it was read. The [complete log](../benchmarks/results/2026-11-17-linux-arm64-lock-untraced-diag-v2.tar.gz)
is retained. This does not identify the holder or establish whether it had
already exited; test-only diagnostics remain enabled without retries.
A later kernel file-lock trace reproduced the original classification-test
failure (`NotPublished(EAGAIN)` before the injected checkpoint hook) and
matched the test's lock device and inode to the kernel event. The tracepoint
reported a null blocker, so it still did not identify a holder. The [test and
trace logs](../benchmarks/results/2026-11-17-linux-arm64-lock-bpf-failure.tar.gz)
are retained; kernel tracing is diagnostic evidence, not a fix.
A subsequent [kernel lock and fork history](../benchmarks/results/2026-11-17-linux-arm64-lock-bpf-history-failure.tar.gz)
captured an inherited-descriptor mechanism for one new reproduction. For
device 38, inode 15133272, a test thread in process 375807 acquired an
exclusive lock with owner `0xffff0000c286b180`. Another thread in that
process forked child 376327 while the lock was held. The rebuild's new lock
attempt on that inode returned `EAGAIN`; immediately afterward the child
executed and the kernel recorded release of the same lock owner from that
child. This explains that reproduced classification-test failure and supports
the inherited-descriptor hypothesis. It does not prove the cause of the older
archived failure or justify changing the documented long-lived lock contract.
A narrow test-fixture change explicitly unlocks its temporary backup store only
after backup completion, before drop; it leaves the rebuild source open and
classification assertions direct. A duplicate-descriptor regression and the
classification test passed on macOS and Linux. The first untraced 16-thread
round after this change still failed a different witness-adoption test; its
[log](../benchmarks/results/2026-11-17-linux-arm64-fixture-release-failure.tar.gz)
is preserved. At that point the assertion did not show the rebuild result;
diagnostics were corrected afterward. No general parallel-suite pass is claimed.
A further untraced round stopped with two different fixture-open `EAGAIN`
failures; one diagnostic captured its inode but no `/proc/locks` holder. Its
[log](../benchmarks/results/2026-11-17-linux-arm64-fixture-release-diag-v2.tar.gz)
is retained. The single fixture cleanup does not solve all cross-test lock
inheritance, and other failures must not be classified without evidence.
A later 16-thread run with lock, fork, exec and exit tracing passed all 539
library tests; its [trace](../benchmarks/results/2026-11-17-linux-arm64-lock-followup-bpf.tar.gz)
did not reproduce either failing fixture or identify its holder.
An isolated Linux regression now forks from another thread while the actual
checkpoint fixture owns its lock, confirms the child holds that exact inode
through `/proc`, and observes `WouldBlock` until the child exits. Its [test and
Clippy log](../benchmarks/results/2026-11-17-linux-arm64-fixture-fork-regression.log.gz)
is retained. It demonstrates the mechanism deterministically; it does not
identify the holders in the other archived failures.
Fixture-owned checkpoint and managed-rebuild setup now explicitly unlock only
after successful fixture work; error and panic paths do not unlock early. The
drop-only controls remain. A controlled spill fixture test verifies both the
blocked drop-only case and successful reopen after fixture-only unlock while
the inheriting child remains alive. Focused tests passed, but strict 16-thread
runs still failed: [one run](../benchmarks/results/2026-11-17-linux-arm64-fixture-release-parallel-failure.log.gz)
hit the spill fixture before its release was added, and a
[later run](../benchmarks/results/2026-11-17-linux-arm64-fixture-release-parallel-failure2.log.gz)
hit `EAGAIN` in bounded rebuild and shared snapshot tests. Their holders are
not identified. No general parallel-suite resolution is claimed.
An additional isolated regression exercises the initial store drop/reopen
boundaries of the spill and crash fixtures. It checks the exact inherited inode
and a direct blocked reopen, then a successful reopen after the child exits.
The managed variant runs its rebuild afterward; it does not fork while that
rebuild itself holds the lock. The historical failure holders remain unknown.
A separate isolated regression now forks while managed rebuild holds its source
lock. It verifies the child's inherited inode, observes direct reopen blocked
after rebuild returns, and reopens successfully after child exit with selection
and source intact. This establishes that mechanism for the controlled case,
not the holders in the archived failure.
Ordinary and checkpoint opens now adopt only the exact log end validated by
replay. This is defense against mutation by a writer ignoring the advisory
lock, which remains outside the supported concurrency contract. Two targeted
V2 interleaving tests passed on macOS and Linux; Linux also passed the existing
V2/V3 large-batch tail adoption and rejection test and 12 durable-witness
tests. The [Linux test log](../benchmarks/results/2026-11-17-linux-arm64-tail-adoption.log.gz)
is retained. Additional V3 growth interleavings on both open paths passed on
Linux; their [test log](../benchmarks/results/2026-11-17-linux-arm64-tail-adoption-v3.log.gz)
is retained. These tests do not simulate physical power loss.
The [Linux serial library run](../benchmarks/results/2026-11-17-linux-arm64-tail-adoption-lib-serial.log.gz)
passed 537 tests; Rust 1.85 Clippy passed on macOS. Serial execution does not
resolve the parallel lock failures. An independent code review found no
correctness defect under the supported advisory-lock model; adoption-specific
sync and witness failure injection remains a useful coverage gap.
Physical power-loss testing and the independent off-host restore drill remain
explicitly deferred, not passed.

An ordinary-store non-authoritative retained-base exporter now verifies the
full witnessed history under the writer barrier and streams into a staged,
no-clobber file with caller resource limits. The caller must keep its
destination outside the source and aliases; the API cannot enforce this.
Source/directory stability against non-cooperating mutation is assumed. The
file is not recoverable authority and does not permit reclamation. Eight
exporter and eight format tests passed on Linux and macOS; Linux Clippy passed.
The first Linux run exposed a V3-specific fault-fixture assumption; its failure
log is retained alongside the passing corrected run. Direct witness fault
injection during export remains open.

After syncing and hash-checking 331 tracked source, test, example and Cargo
files, a Linux ARM64 Rust 1.85 `--all-targets` run passed, including 577 library
tests; one test was ignored. The [full log](../benchmarks/results/2026-11-17-linux-arm64-post-export-all-targets.log.gz)
and [source manifest](../benchmarks/results/2026-11-17-linux-arm64-post-export-source.sha256)
are retained. The earlier [mixed-source failure](../benchmarks/results/2026-11-17-linux-arm64-mixed-snapshot-failure.log.gz)
and [stale-binary failure](../benchmarks/results/2026-11-17-linux-arm64-stale-binary-failure.log.gz)
are also retained: the latter occurred after copying files with old mtimes;
updating validation-checkout source mtimes forced recompilation. This passing
run does not prove the intermittent parallel lock failures are resolved.
