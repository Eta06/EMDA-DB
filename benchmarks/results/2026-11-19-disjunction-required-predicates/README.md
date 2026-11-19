# Shared predicates in document disjunction plans

Engine revision `cd821c9529bf2054333c23bb4940bcb28aa98ed7` plans each
indexed branch with its surrounding required predicates. A shared guard can
qualify a partial index. Shared equalities and presence proofs can complete
a compound prefix and range. Alternatives retain their own scope.

On both ordinary and checkpoint stores, the focused tests compare planned
and scanned results, exercise nested disjunctions, and verify fallback when a
guard applies to only one alternative. A writer that enters an empty partial
index probe causes a read-set conflict and rolls back the reader's raw-key
write. An insert outside that guard permits the transaction to finish. The
same compound query on `8c46781` exhausted the scan budget. The regression
log shows the exact failure before the fix.

The release benchmark created 1,024 records with two indexes. It checked
each result against an independent expected set and rotated between a full
engine scan, the planned engine query, and SQLite. Each shared-guard run used
30 warmups and 300 measured queries on macOS ARM64. The planned path visited
1,050 candidates per run, or 3.5 per query. Median times are in microseconds.

| Run | Engine scan | Engine index union | SQLite |
| --- | ---: | ---: | ---: |
| 1 | 464.50 | 7.67 | 150.00 |
| 2 | 464.71 | 7.88 | 150.17 |
| 3 | 461.58 | 7.92 | 148.62 |

SQLite chose `SCAN docs` for the shared-guard SQL and `MULTI-INDEX OR` for
the separate branch-local guard workload. The raw JSON preserves query
plans, samples and tail latencies. These timings cover one warm-cache
single-client fixture and the recorded adapter behavior. The reproduced
commands are:

```sh
cargo build --offline --locked --release --example document_disjunction_bench
target/release/examples/document_disjunction_bench /tmp shared-guard
```

The library suites used 16 threads. Integration targets covered disjunction,
compound ranges, membership filters and indexes, maintenance interleaving,
and retained-document transactions. Rust 1.85.0 built the Linux suite. macOS
used Rust 1.100.0 and passed formatting and warnings-denied Clippy.
[Source manifest](source.sha256), [toolchain details](environment.log).

| Platform | Library tests | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 554 passed | 43 passed |
| Linux ARM64 | 716 passed | 43 passed |

The initial Linux build exhausted `/tmp`. The successful run used the same
source and moved the existing test build cache to `/dev/shm`. The library
and integration logs preserve individual results.

The Linux ARM64 query model ran with address sanitizer from an empty corpus.
It generated 40,719 inputs in 61 seconds without a reported finding. It
varies ordinary and checkpoint backends, replacements, reopening, sorting,
pagination, and shared versus branch-local guards. It checks both the chosen
access path and the result against a scan. The campaign exercises serial
sequences. The deterministic tests exercise the concurrent schedules.
The [fuzz archive](linux-fuzz.tar.gz) includes the instrumented run log and
generated corpus. The command used `cargo +nightly-2026-11-03 fuzz run
document_query_planner` with `-max_total_time=60 -max_len=96 -seed=42`.
