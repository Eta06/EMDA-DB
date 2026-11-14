# Document mutation outcomes

Revision `5e11657` adds opt-in structured outcomes for document insert,
replace, conditional update, delete and atomic bulk operations on both
writable backends. The methods use the same conservative transaction,
document/index staging and synchronization as the existing convenience
methods. `Callback` and `Rejected` mean this call's staged changes were not
submitted; they do not resolve an earlier uncertain write or independent
checkpoint maintenance. `OutcomeUnknown` must be reconciled before retry.
The existing methods and their error types remain available.

## Local correctness checks

The new integration test covers V2 and V3, ordinary and checkpoint-backed
writers, unique-index conflicts, conditional no-ops, a partially staged but
rejected bulk call, a successful atomic batch, admission rejection and
reopening. Index verification follows both rejected and successful mutations.

Four targeted test functions exercise 120 selected injected-failure cases
over the five new operations. The cases cover V2/V3, both writers, zero- or
one-byte failed appends, V2 log synchronization errors and V3 witness
read/write/synchronization errors. Each failed call returned an unknown
outcome. Across those cases, 25 recovered the prior state, 75 recovered the
complete mutation, and 20 torn records failed closed on both recovery paths
without changing log bytes. The recovered document and unique index were
checked together, with recovery order varied. These are selected synthetic
faults with the operating system running; they do not establish power-loss
safety or an exhaustive concurrency proof.

The [development test log](../benchmarks/results/2026-11-14-document-outcomes-test-development.log.gz)
retains initial failures and subsequent checks. The first integration fixture
reopened while it still owned the ordinary store lock. An initial V3 test
assumed that injecting synchronization failure into a separate log I/O wrapper
would affect V3, although V3 uses the witness handle; the final test injects
that fault at the witness instead. A further test initially expected all
witness failures to leave a complete new mutation; a witness read failure can
occur earlier, so the final check verifies either complete state and matching
indexes on both recovery paths. These were test assumptions, not evidence of a
committed engine change fixing those behaviors.

On revision `17dd628`, stable, Rust 1.85 and optimized stable all-target
suites each passed 966 tests with four test threads. Both all-target Clippy
checks, formatting and 129 Python tests passed.

## Local performance comparison

Revision `17dd628cabc7bf8f1773b968cc982d43bb5a7870` added a benchmark mode
for standard and detailed document calls from the same binary, alongside
SQLite. The [first run](../benchmarks/results/2026-11-14-document-outcomes-performance.json.gz)
had four rounds of 100 cycles for each configuration; the
[repeat](../benchmarks/results/2026-11-14-document-outcomes-performance-repeat.json.gz)
had six rounds of 200 cycles. Each used 100 and 1,000 preloaded documents,
rotated the three configurations, verified sample counts and checked data
after reopen. All 60 runs completed. Both prototype paths synchronize writes;
SQLite used WAL/FULL. The prototype decodes JSON while the SQLite adapter
returns JSON text, so these do not perform identical work.

At 1,000 documents, the median of per-run p50 write latencies was:

| Run | API | Create | Replace | Delete |
| --- | --- | ---: | ---: | ---: |
| First | Standard | 409.15 µs | 395.56 µs | 399.30 µs |
| First | Detailed | 413.59 µs | 413.35 µs | 422.37 µs |
| First | SQLite | 272.18 µs | 284.55 µs | 260.84 µs |
| Repeat | Standard | 389.68 µs | 384.33 µs | 389.49 µs |
| Repeat | Detailed | 400.24 µs | 394.70 µs | 391.70 µs |
| Repeat | SQLite | 191.50 µs | 190.18 µs | 183.91 µs |

At 100 documents the prototype's write medians were lower than SQLite's in
the first run, while the repeat was much closer. SQLite's indexed reads were
faster in both small samples. The reports include every latency sample and
noise observation. The first and repeat observed up to 5.39% and 3.45% VM
steal and 13.12% and 13.87% I/O wait respectively; no cgroup throttling was
observed. Telemetry covers the whole VM/cgroup, including the workload. The
short, warm-cache, single-client runs on an overlay of unknown backing do not
establish a reliable overhead estimate, general performance ranking, resource
bound or physical failure behavior.
