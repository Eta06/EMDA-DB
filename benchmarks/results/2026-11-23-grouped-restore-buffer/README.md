# Grouped restore buffer measurement

The baseline is `58b0f39`. The revised writer reads each authenticated
value directly into its encoded DB01 record before queuing it for
synchronization. Ordinary compaction uses the same grouped writer.
The measurements cover detached restore.

The fixture has four 3.75 MiB values in the base and four more in the
suffix. Each restored value fits in a 4 MiB write group. Each platform
ran five alternating pairs of separate restore processes on the same
fixture. The runtime includes the entire restore call through parent
directory synchronization. Process peak RSS comes from the platform's
`time` utility. Setup, source hashing and external verification ran
outside the measured processes. The verifier reopened every restored
store, checked all 30 MiB of values, and checked that there were no extra
keys. The fixture hashes matched after every run.

| Platform | Build | Median peak RSS MiB | Median restore ms |
| --- | --- | ---: | ---: |
| macOS ARM64 APFS | before | 9.67 | 129.50 |
| macOS ARM64 APFS | after | 5.89 | 117.75 |
| Linux ARM64 ext4 | before | 10.91 | 77.45 |
| Linux ARM64 ext4 | after | 7.59 | 76.06 |

The direct buffer reduced median peak process RSS by 39% on macOS and
30% on Linux. The Linux run used an ext4 guest on the same ARM64 host.
Elapsed times are observations from these warm-cache runs.
Peak process RSS excludes kernel cache and concurrent restore activity.
The measurement covers values that fit in the grouped path.

## Reproduction

Extract the [raw archive](raw-results.tar.gz) and use the commit in
`revision.txt` in a scratch checkout. Copy `grouped_restore_memory.rs`
into `examples/`. Build with
`cargo +1.85.0 build --locked --release --example grouped_restore_memory`
and save the executable. Apply `grouped-buffer.diff`, rebuild, and save
the second executable. Run `python3 run_pairs.py BEFORE AFTER FIXTURE_PARENT`
once on each platform and redirect stdout to its JSONL file.
`python3 summarize.py MACOS_JSONL LINUX_JSONL` reproduces the table.
The archive includes source and binary hashes, environment details,
and the raw timings and verification results.
