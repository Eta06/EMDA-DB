# Membership-filtered grouped statistics on Linux ARM64

This release-build comparison uses Rust 1.85.0 and one client. The
query selects two of eight groups and computes count, sum, minimum
and maximum for two signed integer fields. It verifies every timed
result against SQLite with a prepared indexed GROUP BY over
normalized columns.

| Documents | Matches | Previous EMDA index | Updated EMDA index | Updated EMDA scan | SQLite indexed, updated run |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 16 | 65.2 µs | 28.0 µs | 56.2 µs | 9.3 µs |
| 256 | 64 | 175.8 µs | 102.5 µs | 221.5 µs | 28.3 µs |

Values are medians of five round medians. Each round records 50 samples
after five warmups. The two builds ran in sequence in one VM. The
64-document scan and SQLite controls also varied between runs. The
results describe this workload and do not establish a general ranking.

The [archive](raw-attempts.tar.gz) contains all 20 benchmark runs,
including commands, output, samples and noise counters. It also
contains the build and test logs. The benchmark harness comes from
`2d683f6` in both builds. For the prior engine, the three changed
document modules are from `20124c8`. Source and binary checksums appear
in the build logs. The initial verification after swapping archived
source files failed the seven-unit regression in the reused target
directory. After refreshing the three modules' modification times,
the rebuild passed all 35 selected Linux tests and the benchmark ran.

```sh
cargo +1.85.0 build --locked --release --example aggregate_stats_bench
python3 scripts/aggregate_stats_compare.py \
  --directory /tmp --binary target/release/examples/aggregate_stats_bench \
  --output results.json --documents 64,256 --rounds 5 --cycles 50 --membership
```
