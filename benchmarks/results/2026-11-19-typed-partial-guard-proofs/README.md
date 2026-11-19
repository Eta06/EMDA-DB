# Typed equality proofs for partial indexes

The planner admits a partial index with a typed equality guard when
required predicates prove both bounds at the guard value. A nonempty
disjunction contributes a bound only if every branch proves it. Exact
typed equality and repeated instances of the same value in an `In` list
can prove both bounds. The query still applies the full filter to all
index candidates.

The regression uses a common upper bound and an `Or` whose branches
prove the lower bound through a comparison or duplicate membership.
It succeeds with a one-candidate limit on both writable backends and
matches the collection scan. Mixed branches and integer/float membership
lists exercise fallback. A transaction verifies that an empty index
probe still conflicts with a matching insert and permits a nonmatching
insert.

Running the improved typed-value test against baseline `f6f961b`
produced the expected access assertion failure. The baseline selected
the collection scan. [Baseline test](baseline-test.log).

All 33 document integration targets passed on both macOS and Linux
ARM64, with 192 tests on macOS and 195 on Linux. Linux used Rust 1.85.0.
Formatting for the main and fuzz manifests and warnings-denied Clippy
for the main targets passed on macOS.
[macOS results](macos-documents.log.gz), [Linux results](linux-documents.log.gz),
[verified source hashes](source.sha256).

```sh
cargo test --offline --locked --test 'document_*' --no-fail-fast -- --test-threads=16
```

The address-sanitized partial-index model ran 48,814 inputs in 61 seconds
on Linux ARM64 without a reported finding. The harness compares planned
results with scans across mutations and reopens on both writable backends.
The new cases exercise Boolean, null and integer guards, including mixed
integer and float source documents. Inputs were capped at 96 bytes.
[Starting seeds, generated corpus and log](linux-fuzz.tar.gz).

```sh
CARGO_BUILD_JOBS=1 cargo +nightly-2026-11-03 fuzz run --codegen-units 16 \
  document_partial_planner corpus -- -max_total_time=60 -max_len=96 -seed=42
```
