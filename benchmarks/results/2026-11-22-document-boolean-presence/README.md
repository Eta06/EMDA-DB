# Presence proofs through boolean document filters

At `599d8de`, partial indexes and compound index presence checks
recognize field-existence proofs through negated conjunctions,
negated disjunctions, and double negation. The planner visits the
validated filter tree without materializing a transformed expression.
Comparisons against a complete collection scan cover missing fields,
nulls, array ancestors, empty filters, and mixed branches. The 45
focused tests passed on macOS ARM64 and Linux ARM64. The full macOS
document suite passed all 212 tests. Formatting and warnings-denied
Clippy passed on macOS.

## Differential fuzz campaign

The updated partial-index model exercises ordinary and checkpoint
backends, single and compound indexes, mutations and reopen, and
the new presence-proof cases. It compares each planned result with
the complete scan and checks the expected access path. The final
instrumented build at `e2130c9` completed 3,974 runs in 91 seconds
from 336 structured seeds, with no reported finding. The seed was
4187 and the maximum input length was 96 bytes.

The first optimized sanitizer build was killed before running. The
development build ran with AddressSanitizer and found a stale oracle case
for a directly negated missing-array predicate. The engine already
recognizes that predicate as proof of field presence. The updated oracle
recognizes the plan and still requires result equality. The [raw archive](raw-results.tar.gz)
contains the initial failure, its input, the successful run, the corpus,
source hashes, and the platform and test logs.

From an offline Linux ARM64 checkout with the pinned nightly and
`cargo-fuzz` installed, reproduce with the archived corpus:

```sh
CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo +nightly-2026-11-03 fuzz run --dev --codegen-units 16 \
  document_partial_planner /path/to/corpus -- \
  -seed=4187 -max_total_time=90 -max_len=96 -timeout=15
```

This is a finite local model campaign. It covers query result agreement
and excludes physical power loss and concurrent schedules.
