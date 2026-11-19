# Differential checks for compound document disjunctions

The [query planner fuzzer](../../../fuzz/fuzz_targets/document_query_planner.rs)
compares indexed execution with a complete collection scan at the same
query limit, offset, sorting and projection. It checks the returned
documents and the match, return and sort counts. A required partial-index
guard and two independent literal disjunctions must select a six-probe
compound membership plan. The other new shapes combine an unguarded
branch or negation with the same typed literals. Generated replacements
change compound fields across integer, boolean, null and missing values
and move rows into and out of the partial index. Two retained seeds cover
the ordinary and checkpoint-backed stores and include reopen operations.

The Linux ARM64 address-sanitized campaign executed 15,910 inputs in 61
seconds and reported no finding. It used three seeds, random seed 42 and
a 96-byte input ceiling. The [Linux archive](linux-fuzz.tar.gz) contains
the expanded corpus, logs, compiler version and binary hash. macOS ran
the same three deterministic seeds with no address sanitizer. All 26
focused planner integration tests passed on each platform.
[Source manifest](source.sha256), [Mac seeds](macos-seeds.log.gz),
[Mac tests](macos-tests.log.gz), [Linux tests](linux-tests.log.gz).

```sh
run=$(mktemp -d "${TMPDIR:-/tmp}/emda-query-planner.XXXXXX")
cp fuzz/document-query-planner-seeds/* "$run/"
CARGO_BUILD_JOBS=1 cargo +nightly-2026-11-03 fuzz run --codegen-units 16 \
  document_query_planner "$run" -- -max_total_time=60 -max_len=96 -seed=42
```

These checks cover one collection and bounded query shapes. They compare
planner behavior with the scan evaluator and share the filter semantics.
They do not exhaust query equivalence, concurrent schedules or power-loss
behavior.
