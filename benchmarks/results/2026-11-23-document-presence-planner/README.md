# Index scans for document presence queries

The planner can use a bounded full-index scan when explicit presence
predicates cover every indexed field and the filter implies any partial
guard. More selective equality, membership, range, prefix and disjunction
plans keep their existing priority. The result still applies the full
filter and uses the normal candidate and byte limits.

Both writable backends compare the partial compound plan with a collection
scan, including an indexed null and documents omitted by an absent field
or the partial guard. A deterministic read-set test starts with an empty
index and checks that a concurrent replacement adding an indexed null
invalidates the reader's staged write.

The Linux ARM64 AddressSanitizer campaign completed 5,432 executions in
61 seconds without a finding. The differential target exercises guarded
and unguarded presence, missing compound fields, negation, sorted and
projected pages, and reopen. The campaign ran in the development profile
on Rust nightly 2026-11-03, from five fixed inputs. The
[Linux archive](linux-arm64-evidence.tar.gz) contains the raw log, corpus
and source hashes. The [macOS seed run](macos-seeds.log.gz) executed all
five fixed inputs.

```sh
run=$(mktemp -d "${TMPDIR:-/tmp}/emda-presence-planner.XXXXXX")
cp fuzz/document-partial-planner-seeds/* "$run/"
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo +nightly-2026-11-03 fuzz run --dev --codegen-units 2 \
  document_partial_planner "$run" -- -max_total_time=60 -max_len=96 -seed=61967
```

Linux ARM64 passed all 768 library tests. The 65 selected planner
integration tests passed on macOS and Linux after updating an existing
full-scan plan expectation to assert the new presence access path.
Formatting and warnings-denied Clippy passed on macOS.
