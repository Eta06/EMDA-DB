# Remove covered index ranges from document disjunctions

Revision `4ad7c2cd` removes a range probe when another range on the
same index covers its entire encoded interval. Bounds are constructed
by the same helper used during execution. The planner preserves exact
probes, crossing ranges, and intervals whose endpoints need clipping.
The existing 256-probe planning cap still applies before pruning.

The complete filter remains active, so a row admitted by the covering
range can match a residual condition from a removed branch. The tests
check inclusive and exclusive bounds, rows matching only the removed
branch, a read-set conflict on a concurrent matching insert and
rollback of the reader's staged receipt. The byte-budget regression
uses crossing ranges, which continue to charge each candidate visit.

## macOS paired comparison

Separate ARM64 release builds used Rust 1.85 and the same 256-document
fixture. The 16 disjuncts had nested integer ranges and two residual
predicates. Every timed query returned the same 47 ordered documents
and matched the independent expected values. All six permutations of
the three cases were run, with five warmups and 20 timed calls per case
and round. Fixture setup, process startup and equality checks are
outside the timer. Materialization and JSON decoding are included.

| Path | EMDA probes | EMDA source visits | Samples | Median (µs) | p95 (µs) |
| --- | ---: | ---: | ---: | ---: | ---: |
| EMDA `f329395c` | 16 | 528 | 120 | 405.71 | 628.00 |
| EMDA `4ad7c2cd` | 1 | 48 | 120 | 58.08 | 79.25 |
| SQLite 3.49.1 WAL/FULL | — | — | 120 | 30.52 | 46.62 |

SQLite was free to choose its plan and selected a table scan despite
the available tag index. The first archive-verification attempt assumed
it would choose that index. The verifier was corrected to check the
observed plan, and the original timing samples were retained. These
single-client warm-cache samples measure this nested-range workload.

## Verification evidence

The native macOS Rust 1.85 suite passed 1,357 tests across 112 nonempty
library and integration binaries. [Hosted macOS] passed both toolchains.
[Hosted Linux] passed Rust 1.85, stable, the optimized suite, and all
53 phases of the million-key recovery workload. The [filesystem EIO],
[ENOSPC], and [cross-engine smoke] workflows passed.

The [raw archive] contains 52 recorded benchmark attempts. It includes
45 standard smoke cases, five cross-engine cases, the Linux recovery
workload, and the paired native comparison with all 360 timing samples.
The probe, exact engine revisions, binary hashes, test logs, verification
scripts, and SHA-256 manifest are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45429677307
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45429677233
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45429677134
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45429677321
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45429677196
[raw archive]: raw-artifacts.tar.gz
