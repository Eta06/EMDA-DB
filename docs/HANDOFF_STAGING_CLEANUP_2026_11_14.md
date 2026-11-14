# Staging cleanup handoff — 2026-11-14

This is an unmerged work-in-progress branch. Do not merge it into `main` until
the failure below has been investigated and the branch has passed the relevant
checks. Its base is `324a082db7622acf1b6aabd7b55ff0e364488c07`; the cleanup
implementation is `6f2df6f5cd4cb048eea91b50ad0d9c256b6dd25c`, followed by
`c72440d421dd5611850c2cdc8463f82fc2633cd3` with additional diagnostics and
fault-driver options. The latter has not received a complete engine test or
fault campaign.

## Unresolved failure

An all-target stable debug run of `6f2df6f` failed once in
`checkpoint_store::tests::rebuild_outcomes::detailed_rebuild_classifies_publication_boundaries_and_preserves_recovery`.
The assertion at `src/checkpoint_store/tests/rebuild_outcomes.rs:104` expected
`SelectionOutcomeUnknown`, but the old assertion did not record the actual
result or case. The process ran under heavy concurrent build and fuzz load.
That observation does not identify the cause, prove a lock race, or rule out a
production bug. The other 490 library tests in that run passed; remaining test
targets were not run after the failure.

The Rust 1.85 and optimized stable all-target suites at `6f2df6f` each passed
1,024 tests with one explicitly ignored disposable-filesystem test. The isolated
rebuild test passed again after `c72440d` added the failing case and actual
result to its assertion message. The diagnostic test change does not alter the
engine's production behavior. Reproduction or a passing retry alone does not
resolve the original failure.

Investigate with the improved assertion on an appropriately provisioned
machine; examine the returned error and the exact fixture before deciding on a
fix. A transient lock or a forked child inheriting a descriptor is a possible
hypothesis, not an established explanation. Do not blindly retry every
`WouldBlock` or weaken the expected failure and recovery assertions. Retain
evidence of any newly identified race and cover its actual cause.

## What is on this branch

The additive `cleanup_abandoned_staging_detailed` API classifies errors as
`NotStarted` before this call starts a recognized generic stage removal, or
`OutcomeUnknown` after removal has been attempted. These are operation-boundary
classifications, not assurances about durability, prior artifacts, secure
erasure, or the validity of a backup. A reported unknown outcome requires
inspection before resumption. Existing cleanup APIs remain available; selective
retired-stage cleanup is a separate operation. See the branch's code and
storage contract for exact semantics.

At `6f2df6f`, the local EIO/EAGAIN campaigns recorded 70 actual returned
injections across selected syscall groups: 30 `NotStarted`, 38
`OutcomeUnknown`, and two successfully handled cases. Some reported groups had
no eligible calls, and these counts cover neither every path nor physical power
loss. The staging-cleanup model fuzzer ran 93,079 executions; replay of the
saved corpus completed successfully. The subsequent `c72440d` extends driver
and verifier options to EINTR, ENOSPC and EDQUOT, and adds EAGAIN/ENOSPC to
the proposed CI matrix. These new combinations still need real fault-campaign
verification. The branch's 150 Python tests, workflow YAML parse, formatting and
the isolated rebuild test passed after that change.

Raw local test logs, the fuzz corpus and the 18 EIO/EAGAIN reports are preserved
in [the evidence archive](../benchmarks/results/2026-11-14-staging-cleanup-wip-evidence.tar.gz).
The report revision and binary hashes identify the tested code; the logs and
reports contain local temporary paths. The archive is evidence from this
machine, not a request for the user to run another benchmark.

## Resume on the new computer

1. Fetch this branch, check `git status`, read this document and the existing
   [foundation follow-up](FOUNDATION_FOLLOW_UP.md) and
   [document follow-up](DOCUMENT_FOLLOW_UP.md). Keep `main` separate until the
   branch is ready. First diagnose the reported test failure.
2. On a supported Linux environment with stable Rust, Rust 1.85, Python and
   `strace`, rerun `cargo +stable test --locked --all-targets`,
   `cargo +1.85.0 test --locked --all-targets`, and
   `cargo +stable test --release --locked --all-targets`. Run
   `cargo +stable clippy --locked --all-targets -- -D warnings`,
   `cargo +stable fmt --all -- --check`, and
   `python3 -m unittest discover -s scripts/tests`. Run the relevant GitHub
   workflows, including native macOS checks, before considering a merge.
3. Re-run and independently verify targeted staging cleanup campaigns and the
   fuzzer using the commands in the workflow and existing campaign scripts;
   exercise and verify the newly added errno options. Preserve failures and
   distinguish synthetic returned errors from a real full filesystem. Verify
   the guarded ENOSPC case on an isolated, disposable full filesystem.
4. Continue roadmap steps 1–3, including bounded memory/recovery and broader
   resource control, document semantics, and their correctness and performance
   evidence. Record progress and remaining work honestly. Physical power-loss
   and independent off-host restore tests are deferred by the user for later
   sessions; do not claim they have passed. Do not wait for user-provided quiet
   machine measurements while independent development can continue.

Use a fast local SSD and a quiet, well-provisioned Linux host for more reliable
comparisons; run native macOS tests separately. Docker on macOS runs Linux in a
VM, so document that environment when comparing timings. Benchmarks must match
workload and durability settings, report noise, and never trade away durability,
isolation or recovery guarantees for a faster result.
