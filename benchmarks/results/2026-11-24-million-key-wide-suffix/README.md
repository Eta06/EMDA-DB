# Million-key recovery with a larger suffix

This run compares baseline and larger retained suffixes on the same initial
dataset of 1,048,576 eight-byte keys and 96-byte values. Each of six epochs
updates and deletes from 4,096 keys in the baseline workload or from
32,768 keys in the wide workload. Each batch has 128 mutations.
All 72 fresh-process point checks and 72 complete ordered scans matched
the expected live state across ordinary EMDA, authority-backed retained
EMDA and SQLite WAL/FULL.

## Reopen and write resource measurements

Values below are medians of six epochs, except reopen RSS, which is the
maximum across the six fresh-process probes. Ready time includes opening
the store and checking a point read. Verified scan time includes the
complete model comparison after opening. Write time covers the whole
epoch and includes a rotation when one occurs.

| Filesystem | Workload | Engine | Ready wall ms | Ready CPU ms | Reopen peak RSS MiB | Verified scan ms | Write wall ms |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| macOS APFS | baseline | ordinary | 249.00 | 243.51 | 127.97 | 308.32 | 189.84 |
| macOS APFS | baseline | retained | 98.19 | 94.01 | 3.06 | 165.61 | 1228.90 |
| macOS APFS | baseline | sqlite | 0.66 | 0.50 | 3.16 | 162.90 | 3.57 |
| macOS APFS | wide | ordinary | 255.99 | 251.79 | 127.97 | 301.09 | 1436.77 |
| macOS APFS | wide | retained | 102.97 | 99.51 | 6.92 | 168.36 | 7404.56 |
| macOS APFS | wide | sqlite | 0.73 | 0.66 | 3.17 | 157.81 | 28.86 |
| Linux tmpfs | baseline | ordinary | 237.80 | 237.78 | 131.54 | 248.64 | 0.70 |
| Linux tmpfs | baseline | retained | 67.50 | 67.49 | 5.22 | 136.18 | 239.61 |
| Linux tmpfs | baseline | sqlite | 0.34 | 0.33 | 3.93 | 145.45 | 2.33 |
| Linux tmpfs | wide | ordinary | 253.12 | 253.11 | 132.03 | 254.05 | 5.34 |
| Linux tmpfs | wide | retained | 73.83 | 73.83 | 8.15 | 143.35 | 20.97 |
| Linux tmpfs | wide | sqlite | 0.46 | 0.46 | 4.06 | 145.40 | 18.91 |

The retained store rotated four times per platform with the baseline
policy and twice per platform with the wide policy. The largest observed
suffix increased from 626,244 to 8,602,362 bytes. The policies change the
suffix byte, operation, work and overlay limits together. These runs
measure the combined effect of those policies and the heavier workload.

On a separate traced Linux wide reopen at epoch six, reads from the base
returned 117,195,296 bytes, including the checked read after open. Reads
from both log files returned 1,940,452 bytes with a 1,938,132-byte
committed suffix. The traced base file was 117,107,008 bytes. This
confirms that full base authentication dominates bytes read in this
profile. The trace was excluded from the timed comparisons.

## Local authorized restore

Each run created a bound backup, retired the original log archive,
recorded a preparation receipt and restored to a fresh authority and
catalog. Source and restored state both matched the final model. The
wide workloads checked 1,045,597 live entries per catalog and verified
that the persisted receipt and new authority record matched byte for
byte. The ordinary wide fixture exercised V2 on macOS and V3 on Linux.

| Filesystem | Workload | Backup ms | Archive retirement ms | Restore preparation ms | Restore registration ms | Total restore ms | Lifecycle peak RSS MiB | Sampled allocation MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS APFS | baseline | 104.77 | 235.11 | 196.07 | 104.91 | 301.04 | 3.77 | 349.74 |
| macOS APFS | wide | 102.19 | 144.66 | 188.87 | 98.59 | 287.53 | 7.45 | 352.48 |
| Linux tmpfs | baseline | 60.83 | 140.85 | 74.50 | 64.38 | 138.88 | 3.77 | 349.74 |
| Linux tmpfs | wide | 60.10 | 142.65 | 81.10 | 70.28 | 151.40 | 9.29 | 352.32 |

## Reproduction and limits

The engine was built from `b9ea9ca` on ARM64 with Rust 1.85.0 and locked
dependencies. The baseline used the original harness with
`EMDA_BENCH_SUFFIX_WORK=480000000`. The wide runs used the included harness
patch and its default 12-billion-unit work limit. Use the
[phase runner](../../../examples/authorized_recovery_bench.md) with
`1048576 6 --sqlite --retire-and-restore --authorized-restore` and add
`--wide-suffix` for the larger workload. The [raw archive](raw-results.tar.gz)
contains phase logs, JSONL, binary hashes, the trace and the summarizer.
Extract it and run `python3 summarize.py .` to validate the phase counts,
verify the logged model coverage and regenerate the summary.

These are warm-cache runs on one Mac and its Linux VM. SQLite does less
integrity work at open. Linux tmpfs timings omit block-device persistence.
Sampled allocation includes visible lifecycle stages and can miss shorter
peaks. RSS excludes kernel cache. The workloads exercise clean recovery
and a local restore with separate authority. Total memory bounds, physical
power-loss behavior and independent off-host restore remain unverified.
