# Exact probes from closed typed bounds

Required lower and upper comparisons at the same inclusive typed scalar
supply an exact value for index planning. The planner can then use later
compound fields or select a complete tuple. The regression exercises an
exact compound probe, a membership union, a presence-qualified prefix
and a later-field range that fits a one-candidate limit.

The new regression fails at baseline `e6f4298` with a scan-limit error.
Both writable backends match the collection scan with the new plan.
The transaction regression checks conflicts for matching inserts and
permits inserts outside the selected range. The typed-bound cases cover
null, booleans, integers, and strings including an embedded NUL, along
with exclusive endpoints. [Baseline test](baseline-test.log).

All 33 document integration targets passed on macOS ARM64 (193 tests)
and Linux ARM64 (196 tests). Linux used Rust 1.85.0. Formatting for both
manifests and warnings-denied Clippy for the main targets passed on
macOS. [macOS tests](macos-documents.log.gz),
[Linux tests](linux-documents.log.gz), [Clippy](macos-clippy.log),
[source hashes](source.sha256).

```sh
cargo test --offline --locked --test 'document_*' --no-fail-fast -- --test-threads=16
```

The address-sanitized compound-range model ran 44,312 inputs in 61
seconds on Linux ARM64 with no reported finding. It compares planned
queries with scans across both writable backends. Its new modes use
closed bounds in compound ranges, exact tuples and membership unions,
and exercise an exclusive endpoint. The cap on generated inputs was 96
bytes. [Seeds, generated corpus and run log](linux-fuzz.tar.gz).

```sh
CARGO_BUILD_JOBS=1 cargo +nightly-2026-11-03 fuzz run --codegen-units 16 \
  document_compound_range corpus -- -max_total_time=60 -max_len=96 -seed=42
```
