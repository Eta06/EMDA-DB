# Compound equality and membership prefix scans

The planner can scan a compound index for a contiguous prefix of required
equalities or `In` alternatives when the filter proves every omitted index
field is present and implies the partial-index guard. The full predicate
still runs on every candidate. A prefix may match several rows of a unique
index. Prefix branches share the same probe, candidate and byte limits as
other indexed branches.

The focused regressions compare the plan with collection scans on the
ordinary and checkpoint stores. They cover explicit null, missing fields,
partial indexes, repeat membership alternatives, exact candidate and
probe limits, staged changes and conflicts after an insert into an empty
prefix. A separate case checks that an existing, narrower indexed
disjunction retains priority over a broad prefix scan.

All 33 document integration targets passed on both hosts. macOS ARM64
passed 191 tests and Linux ARM64 passed 194. The Linux build used Rust
1.85.0. The commands below used the same checked source and locked dependencies.
Formatting and warnings-denied Clippy passed on macOS.

```sh
cargo test --offline --locked --test 'document_*' --no-fail-fast -- --test-threads=16
```

[macOS log](macos.log.gz), [Linux log](linux.log.gz), [source hashes](source.sha256).

The address-sanitized compound planner model ran 51,595 inputs in 76
seconds on Linux ARM64 without a reported finding. It compares plan
selection and complete query results across puts, deletes and reopens.
New modes cover equality prefixes, deduplicated membership prefixes and
a mixed disjunction that cannot prove a trailing field is present.
The four starting inputs exercise both backends with and without a
partial index. The first sanitizer compiler process was killed. The
successful build used one build job and 16 code generation units.
This campaign used inputs up to 96 bytes and serial model replay.
[Fuzz logs and corpus](linux-fuzz.tar.gz).

```sh
CARGO_BUILD_JOBS=1 cargo +nightly-2026-11-03 fuzz run --codegen-units 16 \
  document_compound_range corpus -- -max_total_time=75 -max_len=96 -seed=42
```
