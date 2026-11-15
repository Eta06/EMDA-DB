# Session handoff — 2026-11-15

This is a working handoff for the next coding agent, prepared at the owner's
request. It summarizes decisions and evidence available in the previous cloud
conversation; it is **not** the full chat transcript, a persistent copy of an
assistant's internal state, or proof that every historical detail was captured.
If the owner supplies a transcript, read it as additional context without
committing private account exports or credentials to the repository. If this
document conflicts with a newer owner instruction or the actual repository and
test evidence, use the newer information and explain the discrepancy.

## First response and working style

Before changing code, read this file and the linked primary documents. Verify
the current branch, `git status`, remote heads, and relevant CI results; other
sessions may have continued since this handoff. Give the owner a concise initial
update in your own words covering: the product and current scope; durability,
isolation and recovery requirements; the unresolved rebuild failure; what
tests actually passed; deferred physical tests; and your immediate next work.
Say what is unknown instead of claiming complete understanding. Then continue
independent work autonomously—do not stop and wait for acknowledgement or
external benchmark results.

The owner prefers direct progress, useful updates, and clear evidence over
repeated permission questions. Preserve their changes and work with the
repository's instructions. Make small, reviewable changes with meaningful
tests, good file structure, clear commits, a current changelog/README, and CI
appropriate to the change. They have authorized commits and pushes in this
project; first inspect branch state and remote heads, never overwrite others'
work, and keep unvalidated changes off `main`. The owner prefers commits to use
their identity where properly configured; inspect the existing Git setup and do
not invent or impersonate an identity.

At each substantive stopping point, report approximate progress for steps 1–8
and overall, alongside concrete completed and remaining work. Estimates are
subjective and can change: they are not measured completion, a guarantee of
release readiness, or a replacement for exit criteria.

## Product direction

- The present repository, `Eta06/EMDA-DB`, is an **unreleased Rust database
  engine prototype**. Its name is a placeholder and can change. The engine is
  intended to be open source under Apache 2.0; a separately operated managed
  cloud and related services can be commercial. See [NOTICE](../NOTICE),
  [README](../README.md), and [versioning](VERSIONING.md).
- The owner wants a fast, resource-efficient, reliable general-purpose engine
  and ultimately a multi-model system: key-value, documents and SQL sharing a
  coherent storage/transaction foundation. The existing document interface is
  experimental; SQL, MongoDB compatibility and PostgreSQL compatibility are
  **not implemented or promised**. Check
  [competitor inventory](COMPETITOR_FEATURES.md) and
  [benchmark methodology](BENCHMARKS.md) when planning features and comparisons.
- The owner wants strong project isolation and scale-to-zero for a future
  managed cloud. Shared infrastructure, elastic usage, project-level quotas and
  inactive projects consuming minimal compute are goals, not current features
  or established guarantees. Security boundaries, noisy neighbors, durability,
  wake-up and operational behavior must be designed and tested explicitly;
  avoid absolute claims such as “perfect isolation.”
- Longer-term platform ideas include project-owned end-user authentication
  (email/password, optional social sign-ins and possibly Kour accounts),
  databases, developer AI services and optional BYOK. Phone authentication is
  not planned for the initial scope. Other ideas included an editable exported
  single-app Flutter runtime and managed source control with optional GitHub
  integration. These are product directions, not present features or permission
  to divert the current database work. The owner explicitly shifted focus to
  the platform and database rather than developing the earlier Kour app now.
- The owner wants the engine to be faster than alternatives where possible.
  Use profiling and measurements, compare equivalent work and durability,
  include latency distributions and resource usage, and record noise and
  regressions. Do not claim universal superiority from a benchmark. Rust is
  the current engine language; SIMD or assembly may be investigated for a
  measured hotspot only with portable fallbacks and equivalent correctness.

## Non-negotiable engineering contract

Read [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md) before
editing persistence, transactions, recovery or performance paths. **No
optimization may violate the durability, isolation or recovery guarantees in
force.** Preserve acknowledged-write, unknown-outcome, atomicity, snapshot,
ordering, filesystem and format assumptions as documented; define the
contract before expanding it. SQL, documents and key-value interfaces must not
silently develop incompatible transaction assumptions. Detecting corruption is
not the same as recovering it, and process termination is not physical power
loss. Do not relax synchronization or retry uncertain outcomes just to improve
benchmark numbers.

## Roadmap and current priority

The owner wants steps 1–3 finished as far as independently testable, then step
4. See [roadmap](ROADMAP.md), [foundation follow-up](FOUNDATION_FOLLOW_UP.md),
and [document follow-up](DOCUMENT_FOLLOW_UP.md) for exit criteria and detailed
open items. Continue useful work while tests or external results run.

1. **Correctness and foundations:** substantial prototype and validation;
   incomplete. Broader failure and concurrency evidence and the rebuild test
   failure below remain open. Physical power-loss validation and an independent
   off-host restore drill were expressly deferred to later sessions; do not
   represent them as completed or wait for them before other work.
2. **Durable core:** substantial prototype; incomplete. Important gaps include
   bounded memory and recovery, safe log reclamation, fairness, quotas and
   broader resource controls. Existing checkpoints and compaction do not
   provide a general total-memory bound or scale-to-zero lifecycle.
3. **Document model:** experimental CRUD, bounded queries, indexes and some
   maintenance exist; richer semantics and representative correctness, fault,
   concurrency and performance evidence remain open. Do not claim MongoDB
   compatibility.
4. **SQL:** not implemented.
5. **Service, project isolation and scale-to-zero:** design goals, not an
   implemented production service.
6. **Performance and scalability:** benchmarking and optimizations exist;
   broad workload, resource and scalability goals remain open.
7. **Replication and operations:** no production distributed replication or
   failover claim; operational capabilities remain incomplete.
8. **Broader features and smarter tooling:** planned and largely unimplemented.

Previously communicated rough estimates were: step 1 85–90%, step 2 45–55%,
step 3 50–55%, step 4 0%, step 5 0–5%, step 6 about 15%, step 7 about 5%,
step 8 0–5%, overall 20–30%. They were subjective estimates without a formal
measurement model; reevaluate rather than treating them as facts or proof
that the difficult remaining work is small.

## Git state and unresolved work at handoff

The remote `main` was `324a082db7622acf1b6aabd7b55ff0e364488c07`. Unmerged
work was on `codex/wip-staging-cleanup-20261114-fe66661`, based on that main.
Before this documentation-only handoff, its tip was
`fe666615a06476a44247941c968e3513ec36239f`; this handoff is a later commit on
the same branch. Recheck both refs before any push or merge.

The WIP adds an additive detailed abandoned-stage cleanup API and fault
campaigns. Its exact semantics, scope and evidence are in
[the staging cleanup handoff](HANDOFF_STAGING_CLEANUP_2026_11_14.md). It is
**not ready to merge** solely because a test passed.

A full stable Linux run of implementation commit `6f2df6f` once failed
`detailed_rebuild_classifies_publication_boundaries_and_preserves_recovery`:
the old assertion did not capture the fixture variant or returned error. Its
cause is unknown; an inherited transient lock is only a hypothesis. Subsequent
diagnostics capture the case and actual result. Do not silently add retries,
weaken recovery assertions, or characterize the failure as merely a flaky test
without evidence. Investigate the real returned error if reproduced.

Evidence tied to exact revisions:

- At `6f2df6f`, Rust 1.85 and optimized stable Linux all-target suites each
  passed 1,024 tests with one explicitly ignored disposable-filesystem test.
  The original stable debug failure and other evidence are in
  [the archived WIP evidence](../benchmarks/results/2026-11-14-staging-cleanup-wip-evidence.tar.gz).
- At `fe66661`, a later stable Linux all-target run passed 1,024 tests with one
  ignored, including the previously failing test. Its
  [raw log](../benchmarks/results/2026-11-15-fe66661-linux-stable-tests.log.gz)
  is preserved here. The prior failure is still unexplained.
- Hosted workflows for `fe66661` completed successfully:
  [native macOS](https://github.com/Eta06/EMDA-DB/actions/runs/43672200863),
  [filesystem errors](https://github.com/Eta06/EMDA-DB/actions/runs/43672200977),
  [disposable ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/43672200860),
  and [recovery fuzzing](https://github.com/Eta06/EMDA-DB/actions/runs/43672200825).
  The full hosted Linux CI workflow was not run on that WIP revision in the
  previous session. Passing tests are evidence for the tested paths, not a
  resolution of the original failure or a physical-power-loss test.
- A separate Mac session reported no Rust toolchain and failures in local Python
  checks because of missing Linux APIs and an older Python without `tomllib`.
  Treat that as a dated environment observation; inspect the current machine
  and install/configure needed tools as appropriate. Native macOS and Linux
  capabilities differ; Docker on macOS uses a Linux VM.

After diagnosing and validating the WIP, continue the remaining independently
testable steps 1–3; do not equate this one change with completing the roadmap.

## Parallel agents and external testing

The owner explicitly authorizes use of T3 Code's multi-agent features for
independent work. Delegate in parallel when useful, for example investigation,
independent review, tests and performance profiling. Use separate worktrees or
clearly assigned files for concurrent edits; integrate and verify results
centrally, avoid conflicting pushes, and preserve existing work. GPT-6 Luna,
Cursor agents and Gemini 4 Hydrogen are examples the owner mentioned; check
actual provider/model availability, permissions and account access rather than
claiming that any named model is available. Do not fabricate agent findings.

The owner has a Mac mini for quieter, independent measurements. Do not send a
large stream of benchmark requests or block development on their results.
When an independent run is genuinely necessary, add a specific request under
[external-test-requests](../external-test-requests/README.md) with the exact
commit, commands, comparable settings, destination for results and limits.
Request 001 is already pending; retain its pinned revision. Physical power loss
and independent off-host restore are separate deferred checks.
