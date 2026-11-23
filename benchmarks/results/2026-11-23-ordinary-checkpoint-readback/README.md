# Ordinary checkpoint readback without a duplicate index

The ordinary writer verifies the witnessed log against its live index
before staging a checkpoint. Readback now compares every decoded key and
value location with the next live index entry. It still validates the
entire staged checkpoint against the writer's digest, then verifies the
covered log, length, and witness before publication. The replay that
precedes staging still reconstructs a live index to verify the history
and calculate the historical peak.

## Paired publication measurements

Both Rust 1.85.0 release builds used the same fixtures within each group.
Each fixture started with 262,144 keys and 8-byte values, followed by
1,024 overwrites and 1,024 inserts in two durable batches. The probe timed
`Store::checkpoint_to` with the store already open. After each trial it
compared the output digest and every output byte with a reference from
the baseline engine. The source log, witness sidecar, and V3 witness
xattr where applicable matched before and after each group.

Each group ran eleven pairs in fresh processes, alternating run order.
The table reports the per-build medians. CPU and wall time each fell in
all 44 pairs. Allocation counters were disabled during the timed trials.
A separate instrumented trial per build and group counted allocation
requests and bytes requested, including reallocations. These totals
measure allocator traffic over the operation. They do not measure peak
resident memory or establish a whole-store memory bound.

| Platform | Key bytes | CPU ms before / after | Wall ms before / after | Allocations before / after | Requested MiB before / after |
| --- | ---: | ---: | ---: | ---: | ---: |
| macOS ARM64 APFS, V2 | 8 | 106.07 / 77.97 | 119.14 / 89.55 | 615,348 / 308,321 | 64.87 / 39.53 |
| macOS ARM64 APFS, V2 | 128 | 167.64 / 136.18 | 185.77 / 152.05 | 615,348 / 308,321 | 125.22 / 69.77 |
| Linux ARM64 ext4 VM, V3 | 8 | 103.05 / 77.26 | 107.89 / 80.72 | 615,347 / 308,320 | 64.87 / 39.53 |
| Linux ARM64 ext4 VM, V3 | 128 | 169.13 / 132.37 | 180.42 / 143.80 | 615,347 / 308,320 | 125.22 / 69.77 |

These warm-cache runs measured explicit checkpoint publication on one host
and its VM. Reopening from a checkpoint and measuring peak process memory
are separate tests.

## Verification and reproduction

The checkpoint, rollover, and bounded managed rebuild integration suites
passed 28 tests on macOS and 29 on Linux. The six direct checkpoint library
tests passed on macOS, including altered source bytes and interrupted
publication. Formatting and warnings-denied Clippy passed.

The [raw archive](raw-results.tar.gz) includes the probe, Python runner,
both runs, source and binary hashes, and test logs. To reproduce, place
`checkpoint_index_probe.rs` under `examples/` in a scratch export of
`67dce15`. Build with
`cargo +1.85.0 build --locked --release --example checkpoint_index_probe`,
save the resulting binary as `baseline`, apply `checkpoint.diff`, and
build and save `candidate` with the same command. Run
`python3 repeat-pairs.py /path/to/baseline /path/to/candidate /path/to/fixtures`
using an existing fixture parent with room for the log and two checkpoint
files. The archive SHA-256 is
`3e43bf4cd9b508b0d1ac5b3630a2218843a1aa2678246252141c6ecd06c3c6cf`.
