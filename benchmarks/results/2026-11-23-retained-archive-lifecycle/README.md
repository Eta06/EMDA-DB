# Retained backup, archive retirement, and restore

Two six-epoch runs exercised the same local lifecycle after an original-path
cutover. macOS ARM64 APFS started with 1,048,576 keys. The Linux ARM64 ext4 VM
started with 131,072 keys to fit the guest disk. Both used Rust 1.85.0,
8-byte keys, 96-byte values, and 4,096 updates or deletes in 32 durable
batches per epoch. Each run verified ordinary and retained state after every
epoch and performed four automatic retained rotations.

Before cutover, the harness saved the legacy log length and SHA-256 digests of
the log and witness sidecar while holding the ordinary store lock. After the
last epoch it published a verified backup of the authority-selected generation
and used the saved fingerprints and backup binding to retire the legacy
archive. Both archive files were absent afterward. It restored the backup to a
separate directory, then reopened and compared the complete restored and
authority-selected live state with the workload model. Each comparison checked
1,048,204 entries on macOS and 130,700 on Linux.

## Measurements

| Platform | Backup ms | Retire ms | Restore ms | Lifecycle elapsed s | Peak RSS MiB | Initial / sampled peak / final allocation MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS APFS | 103.03 | 281.76 | 153.19 | 1.06 | 3.73 | 237.48 / 349.74 / 336.79 |
| Linux ext4 | 133.31 | 39.90 | 534.84 | 0.83 | 3.40 | 29.96 / 44.23 / 42.84 |

Retirement removed the 125.20 MiB archived log and its witness on macOS,
and the 15.65 MiB archived log and its witness on Linux. The final
allocation includes both the backup and restored store. Each operation
time covers the named API call. The lifecycle elapsed time and RSS also
include opening the inputs and both post-restore scans. Allocation was
sampled approximately every 25 ms across the retained source, local
authority, archive binding, backup, restored store, and visible staging
entries. The samples deduplicate hard links by device and inode and can
miss short-lived allocations. They exclude other fixtures, filesystem
metadata, and kernel caches.

These are warm-cache measurements at different scales and on different
filesystems. A preliminary macOS run overlapping other local I/O took
8.75 seconds for the lifecycle. Use the timings as observations of the
tested runs. The authority and backup remained on the same machine.
Physical power-loss validation and independent off-host restore remain deferred.

## Reproduction

Follow the [phase runner](../../../examples/authorized_recovery_bench.md)
with `EMDA_BENCH_SUFFIX_WORK=240000000`, six epochs, and
`--retire-and-restore`. The engine revision was `e4ef4e9`.
The [raw archive](raw-results.tar.gz) includes phase logs and JSONL metrics,
platform and binary hashes, archive inspection results, the harness diff, and
the preliminary macOS lifecycle log. Its SHA-256 is
`8f5610d988b86f347f9ac0392a1cf4e48badced856980103ea758e65230cc5b0`.
