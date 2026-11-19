# Bounded document disjunction planning

Revision `d52c809` adds an indexed union for a required disjunction
when each branch has an independently safe plan. It charges every
candidate visit and the encoded document bytes against shared budgets
before deduplicating by ID. It evaluates the full filter in collection
order before sorting, pagination and projection. The source and test
hashes matched between macOS ARM64 and Linux ARM64.

## Correctness

| Platform | Library suite at 16 threads | Full document integration suite |
| --- | ---: | ---: |
| macOS ARM64 | 535 passed | 181 passed across 32 targets |
| Linux ARM64 | 697 passed | 184 passed across 32 targets |

The disjunction tests exercise a partial unique equality combined with a
range, overlapping hits, staged edits, sorted projection after deduplication,
the exact 256-probe boundary and shared 32 MiB budget, and collection-scan fallback.
Ordinary and checkpoint-backed read-set transactions reject a matching
phantom in an initially empty nested branch and accept an unrelated
insert. An authorized retained transaction also exercises a staged
indexed disjunction through the document API. Formatting and
warnings-denied Clippy passed on macOS.

The first Linux document attempt hit `ENOSPC` while writing a fixture
on tmpfs. The outcome and original log are preserved. Final Linux
document tests used ext4 with the same engine source and test targets. Tests of
the previous fallback behavior for independently indexable disjunctions
were updated to assert the new access path and retain their scan
comparisons.

## Paired local benchmark

The [benchmark](../../../examples/document_disjunction_bench.rs) uses
1,024 documents and two indexes. A partial unique equality is ORed
with an inclusive three-value range on another index. Both databases
commit the fixture in 32-document batches and reopen before measurement.
SQLite uses WAL and FULL synchronous mode for setup. The benchmark
confirms SQLite's multi-index OR plan and verifies the complete ordered
result from all three paths against an independent workload model for
each query. It rotates execution order and records 300 samples per
path after 30 warmup queries. SQLite returns the complete JSON and
the adapter decodes it. EMDA's indexed path examined 1,050 candidates
across the 300 measured queries, an average of 3.5 per query.

| Platform and run | EMDA scan p50 / p95 µs | EMDA index union p50 / p95 µs | SQLite p50 / p95 µs |
| --- | ---: | ---: | ---: |
| macOS first | 475.38 / 525.58 | 6.54 / 9.75 | 3.79 / 8.25 |
| macOS repeat | 478.12 / 671.71 | 6.62 / 11.00 | 3.96 / 9.12 |
| Linux first | 440.42 / 508.63 | 5.88 / 7.42 | 3.79 / 6.21 |
| Linux repeat | 434.38 / 652.29 | 5.83 / 8.83 | 3.67 / 5.79 |

Build with `cargo build --release --locked --example
document_disjunction_bench`. Run the resulting binary with an existing
directory for its temporary fixtures. The [environment record](environment.txt)
contains compiler and binary hashes. Compressed JSON files contain
all samples and the SQLite 3.49.1 plan. This comparison covers one
single-client warm-cache workload. It leaves concurrency, other query
shapes, comparable MongoDB measurements and production-scale
performance open.
