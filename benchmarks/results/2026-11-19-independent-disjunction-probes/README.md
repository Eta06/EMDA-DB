# Compound index planning for independent literal disjunctions

After trying complete branch plans, the planner can derive finite membership
choices from required same-field disjunctions. Several independent choices
can complete a compound tuple or enable a range on a later field. A valid
product has at most 256 probes. All candidate visits retain the existing
budgets and transaction checks, and the original filter is applied in full.

On both writable backends, the partial compound range regression reaches
the two expected candidates and matches the collection scan. The same
two-candidate query fails with an operation-limit error at baseline
`a83cdec`. Cases also cover nested alternatives, duplicate and typed values,
nonindexable and mixed-field fallbacks, the exact 256-probe product, and
an oversized product that uses the existing prefix fallback. Empty probes
conflict with matching concurrent inserts and permit inserts outside the
probed tuples. [Baseline regression log](baseline-test.log).

All 33 document integration targets passed on macOS ARM64 (196 tests)
and Linux ARM64 (199 tests). Linux used Rust 1.85.0. Formatting for both
manifests and warnings-denied Clippy for the main targets passed on macOS.
[macOS results](macos-documents.log.gz), [Linux results](linux-documents.log.gz),
[Source hashes](source.sha256).

```sh
cargo test --offline --locked --test 'document_*' --no-fail-fast -- --test-threads=16
```

The address-sanitized compound planner model ran 46,819 inputs in 61 seconds
on Linux ARM64 without a reported finding. The added cases compare complete
and later-field probes with collection scans on both writable backends.
They include regular and partial indexes, duplicates, multiple scalar types
and an unindexable float that exercises fallback. The input cap was 96
bytes. [Seeds, corpus and run log](linux-fuzz.tar.gz).

```sh
CARGO_BUILD_JOBS=1 cargo +nightly-2026-11-03 fuzz run --codegen-units 16 \
  document_compound_range corpus -- -max_total_time=60 -max_len=96 -seed=42
```
