# Sorted retained rotation sizing

Retained rotation determines which touched keys already exist in its
verified base before calculating the successor's encoded size and replaying
historical live-key peaks. This change walks the sorted overlay keys and
visited base entries together. Each base entry and overlay key advances that
walk at most once. Suffix operations are still replayed in their original
order. The rotation preserves the frame, per-block digest, complete suffix,
and final source and successor verification passes.

The baseline is `e055b08`. Both variants use the Rust 1.85.0 release profile.
The runner clones the same witnessed seed before cutover for each variant
and shape, then alternates the variant order by epoch. Each initial store
has 524,288 keys. Six epochs apply 4,096 operations in 32 durable batches.
The clustered shape changes the first 4,096 keys. The spread shape changes
every 128th key, covering the base range. Each variant rotates in epochs 2,
3, 5, and 6 in both shapes.

| Platform | Shape | Baseline CPU ms | Sorted sizing CPU ms | Baseline wall ms | Sorted sizing wall ms | Pairs with lower CPU |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| macOS APFS | clustered | 196.33 | 186.72 | 1026.65 | 1041.44 | 3/4 |
| macOS APFS | spread | 237.04 | 232.30 | 1106.80 | 1080.18 | 4/4 |
| Linux ARM64 tmpfs | clustered | 117.52 | 116.55 | 117.52 | 116.55 | 3/4 |
| Linux ARM64 tmpfs | spread | 148.90 | 141.14 | 148.93 | 141.14 | 3/4 |

The timing covers all writes in a rotating epoch, including full verification,
rotation and authority publication. Values are medians across the four
rotating epochs per run. The spread workload used less CPU on both platforms.
The clustered Linux median differed by about 1 ms, and one pair went the
other way. These samples are warm, on one host and its VM. Linux tmpfs
timings characterize CPU and user-space work. They exclude physical device
synchronization and do not establish a general wall-time improvement.

All 48 epochs checked the first read and every live entry after reopen
against the workload model. Each pair matched its rotation count, suffix
length and live entry count. The tests cover absent keys within and outside
touched blocks, a corrupted unselected block, corrupted shadowed values,
changed base framing and ordered intermediate peaks.

## Reproduction

[Raw evidence](raw-results.tar.gz) includes the exact code diff, test and
build logs, probe, driver, and result rows. Apply `change.diff` to the
candidate checkout only. Copy `authorized_recovery_bench.rs` into `examples/`
in both checkouts and build with
`cargo +1.85.0 build --locked --release --example authorized_recovery_bench`.
Use distinct target directories or clean the package before rebuilding.
The recorded runs verified distinct binaries and matching source hashes
across platforms.

```sh
EMDA_BENCH_SUFFIX_WORK=240000000 python3 paired.py BASELINE CANDIDATE   authorized_recovery_bench.py NEW_RESULTS_DIR 524288
```

The driver creates its own synthetic fixtures under the result directory.
It removes the private per-trial copies after recording their results and
keeps the unchanged witnessed seed.
