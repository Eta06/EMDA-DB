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
Controlled tests now also cover the exact initial bounded-rebuild and failed
accounting-store lifetimes, preserving their recovery assertions. Fixture-only
release occurs after the relevant store work and is verified against a live
forked child; drop-only variants remain. A subsequent strict 16-thread run
still failed three rebuild tests (two `EAGAIN`, one selection hook not reached)
with 588 passed; its [log](../benchmarks/results/2026-11-17-linux-arm64-exact-release-parallel-failure.log.gz)
is retained. The missing-hook assertion now reports its early error. These
results do not establish the failing holders or resolve parallel reliability.
After releasing three additional completed fixture stores, another strict
16-thread run passed 591 tests and failed one. The improved assertion recorded
the second bounded rebuild returning `NotPublished(EAGAIN)` before its selection
hook after a prior uncertain checkpoint publication. The
[failure log](../benchmarks/results/2026-11-17-linux-arm64-bounded-release-parallel-failure.log.gz)
is retained. The holder remains unknown; no uncertain-path unlock was added.
The second-call assertion now also records lock inode diagnostics on an early
`WouldBlock`. A subsequent [kernel-traced 16-thread run](../benchmarks/results/2026-11-17-linux-arm64-bounded-uncertain-bpf.tar.gz)
passed 592 library tests and therefore did not capture that failure or its
holder. This pass does not resolve the earlier untraced failure.
A Linux-only test hook now captures the exact bounded-rebuild lock descriptor.
Controlled fork cases cover success, log-limit rejection, checkpoint and
selection uncertainty, and panic: reopening remains blocked while the child
retains the inherited lock. In the success-only explicit-release control,
reopening succeeds while the child remains alive. No production unlock was
added, and this evidence does not identify the archived holders.
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

Subsequent local commits add generation-qualified ordinary and checkpoint
read-set observations, conservative transaction identity checks, captured-log
binding for ordinary snapshots, transactions and direct reads, and captured
log use for initial online copying. These are prerequisites only: no live log
switch or reclamation is supported. The non-authoritative EMDARB02 export and
single-key bound lookup do not establish witness provenance or authority.
The exporter requires callers to keep destinations outside the source and its
aliases. A proposed persistent source-directory descriptor was not integrated:
review found it could leave a fresh lock-only store after an added failure and
did not prevent exchange-away-and-back during pathname-based log opening.
Descriptor-relative lock, log and witness opens and a separately specified
recoverable initialization protocol remain open.

Document work since the prior handoff includes bounded `ArrayReverse`, OR
presence inference for eligible indexes, and targeted rollback/phantom tests.
The well-formed replaced migration/build cursor trust gap remains open; an
extra cursor in the same writable metadata would not authenticate progress.

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

At integration commit `3dac783`, a fresh Linux ARM64 Rust 1.85 build passed
all 592 library tests with 16 threads and kernel lock/fork tracing enabled.
The [trace and test evidence](../benchmarks/results/2026-11-18-linux-arm64-integration-head-lock-trace.tar.gz)
has SHA-256 `81d85060b7fc129826dcebeca0c8fc42abae6cc9769ad1c800dfd9c92f1878af`.
This run did not reproduce a lock failure. A preserved earlier trace linked
one failed acquisition to a fork-inherited descriptor; other archived failures
still lack holder identification, so parallel reliability remains open.
The same commit added `ArrayKeepLast`; both array test suites passed on macOS,
including an exact shared work-budget boundary on both writable backends.

Current-reader `EMDALOG4` and truncated-marker guards now cover ordinary and
checkpoint opens, backup, and restore. All three tests passed on macOS and in
a fresh Linux ARM64 Rust 1.85 checkout at `9750892`. The Linux test also uses
the default store format, but did not assert which format the filesystem chose.
These tests check preserved bytes; they do not exercise older binaries or prove
that no write syscall was issued. A proposed external anti-rollback authority
design remains in an isolated worktree and is not an adopted deployment
requirement. Recovery cutover and retained-data reclamation remain unimplemented.

The crash-boundary fixture now explicitly releases its initial store lock after
its checkpoint completes, before reopening or managed rebuild. Its isolated
Linux fork control checks the exact inherited inode, both blocked drop-only and
successful explicit-release cases, including managed setup. Targeted fork and
crash-boundary tests passed in a fresh Linux ARM64 Rust 1.85 source copy. A
16-thread library run failed one witness-substitution test with EAGAIN (592
passed); its holder was not identified. A separate diagnostic control replaced
the two library retry helpers with single attempts in the disposable copy and
skipped the retry-helper unit test: 536 passed and 56 failed, many with EAGAIN.
That modified-source control is not a clean suite pass and does not establish
individual holders. Logs and exit statuses are preserved in
[the evidence archive](../benchmarks/results/2026-11-18-linux-arm64-crash-fixture-lock-control.tar.gz)
(SHA-256 `55d9bfbfba340f598d0cb5b056a47575c48b9eb2c6600c084b99df66e1bfaf3e`).
No production lock behavior or error assertions were changed.

The failed witness-substitution case was its final V2 bounded rebuild after
restoring the witness; the recorded EAGAIN did not identify which lock or
holder was involved. A fresh 16-thread traced run at `d112d94` passed all 593
library tests and did not reproduce it. The [research trace](../benchmarks/results/2026-11-18-linux-arm64-witness-research-trace.tar.gz)
has SHA-256 `0b5ce7890e539b952800de90e3d7f8b124744f05a299afaa2614b67c82c5fa55`.
The intermittent lock issue remains unresolved.

A test-build diagnostic now reports the operation, path, descriptor device and
inode, and original error for bounded rebuild store-lock acquisition and staging
parent shared/exclusive acquisition. Controlled Linux ARM64 contention verified
all three reports; the focused rebuild module passed 12 tests. The
[logs and flock trace](../benchmarks/results/2026-11-18-linux-lock-diagnostics-evidence.tar.gz)
include a 16-thread control on a disposable copy with the two test retry helpers
replaced by single attempts (Rust 1.100). It failed: 544 passed and 51 failed.
The intended skip filter did not match the retry-helper unit test, so that
expected failure is included. One bounded log-limit test failed at its later
`CheckpointStore::open_managed` fixture reopen, outside the instrumented bounded
acquisition; its EAGAIN is not discriminated by this instrumentation. Other
failures and trace are retained without attributing holders or a root cause.
The control is not a normal-source suite result. Production lock lifetime and
errors are unchanged; physical power loss and off-host restore remain deferred.

Test-only diagnostics also cover ordinary open, checkpoint source open and
retired-store cleanup. Controlled Linux tests passed; a normal-source traced
16-thread Linux run passed 597 tests without reproducing the intermittent
EAGAIN. An earlier trace ran out of disk space. Both are in the [evidence
archive](../benchmarks/results/2026-11-18-managed-open-lock-diagnostics-evidence.tar.gz),
SHA-256 `5d7b55cc5e234fe0b78d426f5f00646620f26d74cd9ac1b61934a5434776b516`.
A macOS test initially failed because `/var` and `/private/var` are aliases;
the assertion now canonicalizes the observed path, and all four focused lock
diagnostic tests pass there. On last inspection the Linux VM had 227 MiB free;
preserve existing data and evidence when planning further runs.

An experimental candidate builder now exports a point-in-time retained base
into a no-clobber directory with an `EMDALOG4` marker and a header-bound
inspection witness. The verifier requires an independently retained binding;
uncertain publication returns that binding. Candidate output limits cover the
base, header and witness, not filesystem allocation or staging overhead. The
caller must keep the destination outside the source and keep directories
trusted and stable. A current Store open rejects a copied candidate; a synced
empty lock file keeps it inspectable after that rejected open. This is not a
recovery format, an authority, a compatibility promise, or log reclamation.
Retained-related tests passed on macOS; Linux V3 and process-kill tests for the
candidate itself remain outstanding. All-target Clippy passed on macOS.

The candidate also supports bounded, detached suffix inspection using an
independently retained candidate binding, suffix length and SHA-256 digest.
It validates DB01 and DB02 records before returning a private replayed live
set. Its work counter bounds entry visits and vector shifts; suffix bytes bound
hashing and checksumming. Neither counter is a total CPU or memory bound.
This does not bind a durable witness or implement store recovery or cutover.

The ordinary Store can now export a non-authoritative source-bound suffix.
Under the writer barrier it verifies the witnessed source and exact boundary,
compares the candidate's prefix values and historical peaks with source replay,
replays the bounded suffix and compares the final live state before staged
publication. The independent binding is returned on uncertain publication.
Thirty retained-export tests and all-target Clippy passed on macOS. This export
does not establish rollback protection, writable retained recovery, bounded
total process memory, cutover, or reclamation. The existing trusted stable
directory and caller-selected external destination assumptions remain.

An explicit offline restore now validates a bound candidate and suffix and
creates a separate, quota-limited ordinary Store through no-clobber staging.
It reopens and compares the staged store before publication. The restored
store has a new identity and history; prior historical peaks and witness are
not preserved. Input and output limits are distinct, and the replayed state
and output index coexist in memory. Thirty-one retained-export tests and
warnings-denied all-target Clippy passed on macOS. Restore-specific running-OS
process-kill tests now cover pre-operation boundaries and after rename before
parent sync; they do not simulate kills inside operations or partial I/O, prove
parent-sync durability, or establish Linux V3 behavior. This is not a live
cutover or reclamation.

Subsequent work includes source-bound suffix export, offline restore, running-OS
restore kill tests, and incremental replay totals; see the local Git history for
their focused tests. A V2 fault test now covers schema publication alongside a
document replacement, raw key and partial indexes after zero-byte append and
post-sync failures. Linux exchange reconciliation remains unimplemented; after
an uncertain exchange, both paths may be writable and a sidecar alone cannot
fence older binaries. The Linux-only exchange regression now verifies that
both locks stay held through a failed parent sync. It also verifies that the
two paths can reopen directly afterward and the retained copy becomes stale
once writes resume. The exact test and the 17-test exchange selection passed
on Linux ARM64 with Rust 1.85.0 against `a6b5eef` plus the recorded test patch.
See the [raw test log](../benchmarks/results/2026-11-19-linux-arm64-exchange-fault/exact-test.log)
and [source patch](../benchmarks/results/2026-11-19-linux-arm64-exchange-fault/source.patch).

The Nov 19 capacity check found 69 GiB free on the Mac and 226 MiB on the
validation VM root, with 717 MiB free on the VM's separate tmpfs. Use an isolated,
size-conscious build for targeted Linux validation. The main checkout was at
`2756483`, with 168 registered worktrees including the main checkout. Preserve
and verify source changes and evidence before removing any old worktrees or VM
artifacts.

### Maintainer status, November 19

Step 2, durable storage and core operations, is the main implementation focus.
Steps 1 and 3 continue in parallel. The estimates below are maintainer judgment
against the [roadmap exit criteria](ROADMAP.md), with roughly 29% overall if
the eight steps are weighted equally.

| Step | Estimate | Evidence and remaining work |
| --- | ---: | --- |
| 1. Correctness and foundations | 88% | Witness recovery and fault coverage have expanded. Intermittent Linux lock failures remain open. Physical power-loss testing and independent off-host restore are deferred. |
| 2. Durable storage and core | 60% | Bounded offline rebuild and retained export, replay and new-store restore are implemented. Live reclamation, durable cutover reconciliation and end-to-end memory and recovery bounds remain open. |
| 3. Document data and queries | 56% | CRUD, schemas, indexed queries and controlled maintenance and fault tests are in place. Richer query planning and broader workloads and fault evidence remain open. |
| 4. Relational and SQL | 0% | SQL interface and execution remain pending. |
| 5. Secure service and project isolation | 3% | Embedded admission controls and design groundwork exist. The secure service and project-wide isolation remain pending. |
| 6. Performance and scalability | 18% | Benchmarks and resource evidence cover selected local workloads. Broader workload and scalability evidence remains pending. |
| 7. Replication and operations | 5% | Local backup, restore and format tooling exist. Replication and failover remain pending. |
| 8. Broader features and tooling | 3% | Feature inventory and design groundwork exist. Most implementation remains pending. |

The long-lived local branches are `main`, `development` (the former
integration branch), and `features` (created from the development baseline).
Keep delegated worktrees short-lived and preserve unique changes and evidence
before removing them. A local archive outside the checkout records the original
branch and worktree inventory, complete Git bundle and checkout snapshots.

The local cleanup retired 170 temporary checkouts and their obsolete branch
refs. The primary checkout is on `development`. The archive contains a Git
bundle independently restored and checked against all 830 original refs, plus
98 verified snapshots of checkouts with local or ignored files. Remote refs
were outside this cleanup.

A traced Linux ARM64 library run using Rust 1.85.0 and 16 test threads at
`2efbbc8` finished with 626 passes and four failures. For each failure, the
trace links the contended lock to an unrelated child that inherited the
descriptor at fork and released it at exec. The affected paths were the
witness-substitution fixture, the detailed rebuild outcome fixture, the
detailed rebuild crash child, and the private staged-store reopen during
retained restore. The exchange tests passed in the same run. This establishes
the holder for these four failures. The lock failures remain open.

The binary SHA-256 was
`37a96613708df402c6350c06895d3f719ce04eff131ca94c9b157f4ac459a7a`.
The VM had 226 MiB free on its root filesystem and 568 MiB free on its tmpfs
after the run. See the [test log](../benchmarks/results/2026-11-19-linux-arm64-strict-lock/test.log)
and [compressed kernel trace](../benchmarks/results/2026-11-19-linux-arm64-strict-lock/bpf.log.gz).

The shared-lock change at `cd75fdf` releases the advisory lock when the last
owner in the acquiring process closes, even if a forked child still holds
its descriptor before exec. Checkpoint handoffs share one lock descriptor.
The process lifecycle tests at `fdce958` preserve the separate close-on-exec
control and verify exclusion while an owner is live. After updating those
tests, all 113 integration targets completed on both local platforms in
split runs. Linux passed 563 tests with one ignored ENOSPC test, and macOS
passed 497 tests. Five 16-thread Linux library runs each passed all 633
tests. The macOS run passed all 471 library tests. See the
[validation logs](../benchmarks/results/2026-11-19-shared-store-lock/README.md).

The compatibility probe also passed on binaries built from the historical
`main` revision `324a082` on both platforms. It covers the proposed
byte-zero retained log marker in selected opens, backup and restore. It
does not establish compatibility of an actual retained format or cutover.
See the [historical marker probe](../benchmarks/results/2026-11-19-main-log-marker-probe/README.md).

The retained candidate can now be read through a sparse, authenticated base
directory. The indexed detached suffix view merges ordered DB01 and DB02
operations, including tombstones, with point and page reads. It holds its
bounded suffix bytes privately and keeps only the touched keys and value
offsets in the overlay. A verified page reads from the captured base
descriptor without materializing the base live set. Both local platforms
passed all five new integration tests. The
[validation and paired measurement](../benchmarks/results/2026-11-19-retained-index-overlay/README.md)
record lower process RSS with slower complete scans in one small synthetic
workload. An authoritative witness, writable suffix recovery, durable cutover
and reclamation still require implementation.

A writable retained generation now persists its recovery budgets and
independently witnesses the exact committed suffix prefix. The staged
creation path initializes from an authenticated candidate and can import a
detached suffix. Both local platforms passed the full library suite and the
indexed and format integration tests. The process-kill tests cover stops
after log sync and after witness sync. A historical binary from `324a082`
rejected an actual new generation with a committed suffix on both
platforms. See the [validation report](../benchmarks/results/2026-11-19-writable-retained/README.md).
Step 2 still requires durable active-generation selection, coordinated
retirement and resource evidence for the complete workflow.

The next Step 2 slice adds durable active selection for managed retained
catalogs. The catalog lock spans cutover and open synchronizes the selected
mapping before admitting writes. Rebase verifies the active data and carries
the historical count and key-byte peaks forward. Explicit reclamation checks
the selector and removes only a verified inactive generation owned by the
catalog. These catalogs use a separate open API. Ordinary store directories
still require a migration and retirement protocol.

At `005c1b6` the 16-thread library suite passed 494 tests on macOS and 656 on
Linux ARM64. Both also passed nine indexed and format integration tests.
On the sampled 14,044-key live set, reclamation reduced catalog file length
from 1,580,757 to 604,672 bytes compared with the initial import.
The [validation and phase measurements](../benchmarks/results/2026-11-19-managed-retained/README.md)
include the test logs, resource limits and reproducible probe.

Managed retained catalogs can now resize their limits by publishing a
verified successor. This recovers a live state that needs a larger base
even when its suffix has no remaining write capacity. Open enforces
caller-supplied recovery ceilings and uses the selected generation's
witness-bound limits. The same ceiling can cover either side of an
uncertain resize. The [resize validation](../benchmarks/results/2026-11-19-retained-resize/README.md)
also refreshes document serial histories and kill-race evidence on both
writable backends and both local platforms.

Managed retained catalogs support opt-in automatic compaction and
reclamation. A handle opened with this policy clears abandoned catalog
stages and retires verified inactive generations. Before append it
rotates on suffix pressure when the triggering mutation fits an empty
suffix. Recovery tests kill the process at the selector and retirement
boundaries. The [automatic reclamation report](../benchmarks/results/2026-11-19-retained-auto-reclaim/README.md)
records the cross-platform results, reduced logical file lengths in one
fixture, and substantial write-path latency when rotation coincides with
other I/O.

The ordinary-store import at `6c7aa50` consumes a witnessed source handle
and keeps the source locked while publishing a verified managed catalog
at a separate path. The source retains its data and can reopen normally.
The [ordinary import report](../benchmarks/results/2026-11-19-retained-ordinary-import/README.md)
records the process-kill and uncertain publication tests on both platforms,
plus matching scans and source checksums in the paired probe. Moving
writers to the new path still requires coordination. Existing-path
cutover and its independent authority and old-reader policy remain open.

Managed retained catalogs can now back up the selected generation and
restore into an independent managed catalog under a separately retained
binding. The [backup and restore report](../benchmarks/results/2026-11-19-managed-retained-backup/README.md)
records source reclamation, corruption and interrupted-publication tests.
It also records a local tmpfs-to-ext4 backup and tmpfs restore with
matching ordered state across 14,044 entries. Independent off-host restore
and physical power-loss testing remain deferred.

The document API now accepts an exclusively borrowed managed retained
catalog. It stages document, index and raw-key changes in one bounded
batch and merges staged mutations into indexed reads. The
[document validation report](../benchmarks/results/2026-11-19-managed-retained-documents/README.md)
records indexed queries, rollback and control checks, and a verified
restore after automatic reclamation on both local platforms.

The retained document history checker now exercises modelled transfers,
indexed reads, unique claims, busy admission and cancellation through
automatic rotation. In the [cross-platform campaign](../benchmarks/results/2026-11-19-retained-document-histories/README.md),
768 histories and 224 retained process-kill races passed on macOS, Linux
tmpfs and Linux ext4. The process-kill cases recovered all acknowledged
transactions and agreed across documents, indexes and raw-key receipts.

The authority prototype prepares a separately fenced catalog and checks a
caller-supplied linearisable CAS provider before acknowledging a bounded
append. Reopen resolves a pending intent from exact local fingerprints.
The [authority report](../benchmarks/results/2026-11-19-retained-authority/README.md)
records the provider fault matrix, competing copies, rollback rejection
and the preceding engine's format rejection. A deployed independent
provider, authority-selected rotation and existing-path cutover remain
open.

Authorized rotation now synchronizes the successor before selecting it
with the external authority. Reopen finds only the matching generation
and suffix. Explicit reclamation advances the authority revision before
deleting the inactive slot. The [rotation validation report](../benchmarks/results/2026-11-19-retained-authority-rotation/README.md)
records both uncertainty outcomes and the local Linux process-kill probe.
Original-path cutover, authority deployment and the deferred durability
drills remain open.

The file-backed authority provides a checksummed revision record and
durable CAS for cooperating processes on one host. The
[provider validation report](../benchmarks/results/2026-11-19-file-retained-authority/README.md)
records two-process races, process stops around replacement, and a Linux
cross-filesystem exercise using the bundled provider. The deployment
must protect authority storage independently of catalog rollback.
Original-path cutover and the deferred durability drills remain open.

Original-path cutover now consumes an exclusive witnessed store and
installs a durable fence before the retained catalog can admit writes.
The old log and witness remain archived in the prepared catalog. The
[cutover report](../benchmarks/results/2026-11-19-original-path-retained-cutover/README.md)
records process kills around fence replacement, the macOS and Linux ARM64
library runs, and five rejected open paths in one older engine revision.
Deployment still needs a supported-reader inventory, fencing for old
backups and independent copies, and protected authority storage.

Authority-checked catalogs now expose the document transaction API, including
at the fenced original path. Each write submits the document, index and raw
key changes through one authority intent and one atomic append. Read-only
transactions revalidate authority before returning. The
[document validation report](../benchmarks/results/2026-11-19-authorized-retained-documents/README.md)
records the cross-platform cutover and recovery tests. Automatic rotation
under independent authority, broader recovery and resource evidence, and
the remaining query and planner work are still open.

Authority-checked retained stores now offer opt-in automatic rotation and
reclamation. The same path handles byte-key batches and document
transactions. The
[rotation and reclamation report](../benchmarks/results/2026-11-19-authority-auto-reclaim/README.md)
records the verified selection and retirement behavior across authority
errors and process kills. Remaining Step 2 work includes measuring recovery
and peak resources across generations and establishing deployment policy for
legacy archives, supported readers and independently protected authority.

The [paired retained recovery campaign](../benchmarks/results/2026-11-19-authorized-recovery-resources/README.md)
verified both backends after eight fresh-process reopens of 131,072-key
fixtures on macOS APFS and Linux ext4. With a larger benchmark work budget
the retained catalog rotated five times instead of 42. It used less
reopen RSS in every sampled profile and took longer to reopen. The
report records the cost of synchronous rotation, the retained legacy
archive and observed staging peaks. End-to-end resource bounds and
deployment-scale cold-cache validation remain open.

Retained base verification and indexed reads now buffer file I/O within
the existing byte budgets. Rebase writes buffer privately before the
existing flush, sync and validation sequence. On the same warmed fixture,
the [buffered I/O report](../benchmarks/results/2026-11-19-retained-buffered-io/README.md)
records median reopen times of 71 ms on macOS and 61 ms on Linux, down
from 195 ms and 150 ms. The Linux trace counted the same synchronization
calls. Full-base verification and the separately documented resource and
deployment gates still apply.

Required document disjunctions now use a bounded index union when every
branch independently qualifies. Branches share candidate, byte and probe
limits. The executor charges duplicate visits, returns each ID once and
applies the full filter. The
[disjunction validation report](../benchmarks/results/2026-11-19-document-disjunction/README.md)
records both document suites, empty-branch conflict checks, staged retained
queries and a paired local comparison against EMDA's scan and SQLite.
Broader planner choices, document fault campaigns and deployment-scale
comparisons remain open.

Guarded document maintenance now authenticates incremental index and
schema job definitions and cursors before a bounded step can publish.
Callers retain a separate key per job. The
[guarded maintenance report](../benchmarks/results/2026-11-19-guarded-maintenance/README.md)
records substituted-cursor rejection, recovery with a deleted cursor
document, retained rotation, and rejection by the previous engine while
a guarded job is pending. Same-job replay and deployment key management
remain separate verification gates.

Original-path retained cutover now drains buffered base output before
sync and staged validation. The [paired cutover report](../benchmarks/results/2026-11-19-buffered-retained-export/README.md)
records APFS and ext4 timings, identical Linux fsync counts, and V2/V3
kernel file-size failures that preserve the ordinary store and permit
retry. The original archive is retained, and reopen still verifies the
full admitted base.

Managed and authority-backed retained rotation now stream the rebase
without allocating scan pages. The [rotation report](../benchmarks/results/2026-11-19-streamed-retained-rebase/README.md)
records exact successor-budget checks, corruption detection for a
shadowed value, and about 24 MiB less peak process RSS on the sampled
24 MiB live set. The active and successor files still overlap on disk,
and open still authenticates the complete admitted base.

Writable retained generations now replay the suffix one physical
record at a time and authenticate reads from disk against per-value
digests. Backup and rebase stream from the file. The
[disk-backed suffix report](../benchmarks/results/2026-11-19-disk-backed-retained-suffix/README.md)
records rehashed-batch rejection, Linux and macOS regressions, and a
48 MiB reduction in sampled peak RSS. Open latency increased on that
fixture. Detached imports still buffer their input, and the catalog
still retains the old ordinary log at an original-path cutover.

Writable retained import and managed backup restore now stream the
detached suffix into a private generation and replay it at the exact
trusted boundary. The [streamed restore report](../benchmarks/results/2026-11-19-streamed-retained-restore/README.md)
records the byte and operation limits, six new Linux kernel write
failure cases, and lower sampled peak RSS for a 48 MiB suffix.
Detached materialized inspection and legacy archive retention remain
separate resource concerns.

Original-path catalogs can explicitly retire their ordinary archive
after verifying and synchronizing a retained backup of the current
authority-selected state. A caller must retain the pre-cutover archive
fingerprints independently. The [retirement regression](../benchmarks/results/2026-11-19-cutover-archive-retirement/README.md)
covers archive substitution, stale backup rejection and reopening
after an injected partial unlink. The original-path fence and witness
remain in place. Separate old copies and interrupted preparations
still require their own retention policy.

Complete abandoned original-path cutover preparations now have
an explicit cleanup path. Before fencing, cleanup checks source
hard links under the ordinary lock and requires an absent authority
record. A fenced catalog checks current authority and accepts the
original archive fingerprints even after its own archive is retired.
The [cleanup regression](../benchmarks/results/2026-11-19-abandoned-cutover-cleanup/README.md)
covers process interruption, protected source and selected catalogs,
and repeat cleanup. Whole-catalog disk admission remains open.
