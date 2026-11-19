# Compound ranges with membership prefixes

Engine revision `3172230eb4b84a786942e01c21ee426482d67d94` plans a
compound range over each distinct prefix supplied by equality and `In`.
The index must cover every possible match. The plan requires the partial
guard where applicable and proof that every indexed field after the range
is present. The union shares a 256-probe cap with other disjuncts.

The new regression passes with a three-candidate limit on ordinary and
checkpoint stores. On `96e43ee`, the same query hit the bounded scan's
operation limit before the access assertion. The historical probe replaced
the test's new enum variants with `CollectionScan` to compile against that
revision. The test checks
unique-index ranges with multiple hits, typed null, duplicate membership
values, missing fields, partial guards, and staged document changes.
Separate cases exercise the exact probe cap and a concurrent insert into
an empty later probe, including rollback of the reader's raw write.

The macOS release benchmark verifies every result against an independent
expected set and checks both engines after reopening. These runs used
1,024 documents, two membership values, eight matching rows, 10 warmups,
and 300 measured cycles. The planned path examined eight candidates. Both
engines used a compound index for their indexed variant. Median values
below are in microseconds.

| Run | Engine scan | Engine range union | SQLite scan | SQLite index |
| --- | ---: | ---: | ---: | ---: |
| 1 | 502.42 | 11.12 | 147.81 | 5.58 |
| 2 | 503.83 | 10.33 | 149.10 | 6.19 |
| 3 | 501.81 | 11.21 | 149.62 | 5.71 |

This is a single-client, warm-cache fixture with recorded adapter and
index choices. The raw JSON preserves all timings and query plans. An
equality-prefix run also verified the benchmark's existing mode.

```sh
cargo build --offline --locked --release --example compound_range_query_bench
target/release/examples/compound_range_query_bench /tmp 1024 8 300 membership
```

The library suites ran at 16 threads. The six integration targets covered
compound ranges, disjunction, membership filters and indexes, maintenance
interleavings, and retained document transactions. Source SHA-256 manifests
matched across the arm64 hosts. Formatting and warnings-denied Clippy
passed on macOS. [Source manifest](source.sha256), [toolchains](environment.log).

| Platform | Library tests | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 554 passed | 46 passed |
| Linux ARM64 | 716 passed | 46 passed |

The Linux address-sanitized `document_compound_range` model completed
59,346 executions in 76 seconds without a reported finding. The four
starting seeds exercise ordinary and checkpoint backends with and without
partial indexes. The model compares the planner with scans after varied
replacements, deletions and reopens, covering typed range bounds and
fallback when a prefix or trailing field lacks proof. This is a serial
campaign. The [archive](linux-fuzz.tar.gz) preserves the log, original
seeds and resulting corpus. Reproduce with `cargo +nightly-2026-11-03 fuzz
run document_compound_range` and `-max_total_time=75 -max_len=96 -seed=42`.
