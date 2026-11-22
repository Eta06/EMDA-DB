# Bounded checkpoint rebuild with small spill chunks

Replay merges runs covering the same number of source chunks. The
existing fan-in still bounds the inputs to one merge. Runs stay in
chronological order and the last run containing a key supplies its
location or tombstone. All closed runs, temporary merge output, and
offset sidecars count toward the spill ceiling. This schedule can keep
more closed runs while rewriting the largest runs less frequently.

## Paired macOS ARM64 results

Both Rust 1.85 release builds rebuilt the same witnessed V2 logs using
eight keys per chunk, fan-in two, and a 64 MiB spill limit. The keys are
32 bytes, are inserted in ascending order, and have 64-byte values.
Each timed sample ran in a fresh process and included checkpoint and
selection publication. Three pairs alternated execution order. Every
built checkpoint matched the ordinary full-log reference byte for
byte. The source log and witness hashes matched before and after each
set of three pairs.

| Keys | Median rebuild time before / after | Peak RSS before / after |
| ---: | ---: | ---: |
| 2,048 | 193.48 / 121.79 ms | 2.67 / 2.69 MiB |
| 4,096 | 523.01 / 216.19 ms | 2.66 / 2.70 MiB |
| 8,192 | 1,622.36 / 385.08 ms | 2.66 / 2.69 MiB |

Peak RSS is the highest process high-water mark among the three runs.
These samples use warm files on a single host. They characterize the
frequent-merge case with distinct sorted keys. Other workloads can
perform extra lookups across more retained runs. The samples do not
measure cold start, filesystem allocation peaks, or a total memory or
recovery time bound.

## Paired Linux ARM64 results

The same probe used Rust 1.85 and three pairs per size in a Linux ARM64
VM on tmpfs. These fixtures used V3 metadata witnesses. Every candidate
and baseline checkpoint matched the ordinary reference byte for byte.
The source log, anchor file, and witness xattr matched before and after.

| Keys | Median rebuild time before / after | Peak RSS before / after |
| ---: | ---: | ---: |
| 2,048 | 76.57 / 12.36 ms | 2.15 / 2.15 MiB |
| 4,096 | 289.52 / 25.64 ms | 2.15 / 2.15 MiB |
| 8,192 | 1,144.73 / 54.11 ms | 2.15 / 2.15 MiB |

The Linux filesystem and witness format differ from those of the Mac
run. Use the before/after comparison within each environment.

## Reproduction

The [raw evidence](raw-results.tar.gz) includes the probe, benchmark runner,
source diff against `26c5324`, toolchain and binary hashes, and timings.
Build each engine revision with `cargo +1.85.0 build --locked --release --lib`
and link the same extracted probe against each respective library:

```sh
rustc +1.85.0 --edition=2024 -O probe.rs \
  --extern database_engine=target/release/libdatabase_engine.rlib \
  -L dependency=target/release/deps -o "$run/probe"
```

Name the baseline binary `probe` and the candidate binary `tiered` in a
fresh macOS workspace. Run `sh repeat-pairs.sh "$run"`. On Linux, use
`/usr/bin/time -f 'peak_rss_kib=%M'` and a filesystem with V3 xattrs.
