# Differential checks for negated document disjunctions

The query-planner target compares complete collection scans with planned
queries after generated replacements and reopens on the ordinary and
checkpoint-backed stores. The generator varies the polarity of nested
disjunctions, uses a negated `NotIn` membership alternative, and checks the
compound plan for guarded cases. It compares rows, matching and return
counts, and sort behavior. The integration tests also cover probe and
candidate budgets and transactions that read empty indexed alternatives.

The Linux ARM64 AddressSanitizer campaign completed 1,383 executions in
66 seconds from five checked-in seeds, with no reported finding. It ran
with Rust nightly 2026-11-03 in an unoptimized build. The optimized build
was killed during compilation. The [raw sanitizer log](linux-fuzz.log.gz)
and [source hashes](source.sha256) identify this run.

```sh
run=$(mktemp -d "${TMPDIR:-/tmp}/emda-negated-query.XXXXXX")
cp fuzz/document-query-planner-seeds/* "$run/"
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo +nightly-2026-11-03 fuzz run --dev --codegen-units 2 \
  document_query_planner "$run" -- -max_total_time=65 -max_len=96 -seed=38291
```

The full macOS test suite passed 1,143 tests. The Linux ARM64 Rust 1.85
library suite passed 765 tests, and all 63 selected planner integration
tests passed on each platform. The differential model shares the filter
evaluator with the planner and samples bounded query shapes.
