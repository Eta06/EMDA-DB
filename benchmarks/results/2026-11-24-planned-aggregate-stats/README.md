# Selective grouped integer statistics on Linux ARM64

EMDA revision `8259dfe`, built in release mode with Rust 1.85.0.
A single client computes count, sum, minimum and maximum for two integer
fields in one of eight groups. EMDA runs the same filter via a complete
scan and a nonunique equality index. SQLite uses normalized columns and
a prepared indexed GROUP BY. Every timed result is checked against the
expected groups, including each field's count, sum and extrema.

| Documents | Matches | EMDA scan | EMDA indexed | SQLite indexed |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 8 | 88.8 µs | 20.8 µs | 7.3 µs |
| 256 | 32 | 246.4 µs | 53.1 µs | 15.4 µs |

Each value is the median of five round medians, with 50 samples per variant
per round. The command excludes fixture creation, reopening and five warmup
cycles. This VM result covers one workload and one data layout. It makes no
write-throughput or general database ranking claim.

The [verified archive](raw-attempts.tar.gz) contains all 10 selective
attempts and both 16-document full-scan smoke attempts with commands,
samples, provenance, noise counters and raw process output. The guest
used a source-only export of the committed implementation. The driver
records the checkout as dirty because unrelated tracked files were absent
from that export. The archive also includes the Linux test and build log.

```sh
cargo +1.85.0 build --locked --release --example aggregate_stats_bench
python3 scripts/aggregate_stats_compare.py \
  --directory /tmp --binary target/release/examples/aggregate_stats_bench \
  --output results.json --documents 64,256 --rounds 5 --cycles 50 --selective
```
