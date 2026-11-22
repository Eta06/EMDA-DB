# Shared checkpoint snapshot state

Snapshots of the same verified checkpoint share its open file and immutable
block directory. The captured suffix overlay is still copied. A rollover
replaces the store's checkpoint view, while old snapshots retain the same
file and directory they captured. Snapshot admission continues charging the
full logical block count for each snapshot.

## Linux ARM64 resource check

The probe opened an existing checkpoint-backed store, held 16 simultaneous
snapshots, checked sampled values in every view, and released them. It
counted allocations and reallocations during snapshot construction and
additional open file descriptors with all 16 views live. Each measurement
ran in a fresh process. Five pairs per fixture alternated which version ran
first. The fixtures used an empty suffix, mixed eight- through 256-byte
keys, and a Linux tmpfs. Both binaries used Rust 1.85 in the nonoptimized
profile. The baseline is `a095856`.

| Checkpoint entries | Baseline allocation calls | Current allocation calls | Logical blocks reserved |
| ---: | ---: | ---: | ---: |
| 4,096 | 288 | 16 | 256 |
| 65,536 | 4,016 | 16 | 3,984 |

The baseline opened 16 additional descriptors in each sample. The shared
path opened zero. The counters and descriptor inventory returned to their
pre-snapshot state after drop. The reported allocation counts exclude
opening the store, the result vector allocated before sampling, snapshot
reads, and allocator byte overhead. An overlay adds work that this fixture
does not measure.

A Linux regression separately filled every spare descriptor slot before
taking a snapshot of an already-open store. The baseline returned
`EMFILE` and the shared path captured and read the snapshot. The snapshot
and automatic rollover suites also exercise reads across replacement and
slot unlinking.

The [evidence archive](linux-evidence.tar.gz) contains the probe, runner,
build and test logs, binary hashes, and measurements. To reproduce, extract
the probe to `examples/checkpoint_snapshot_resource_probe.rs` in each
revision and build with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0
cargo +1.85.0 build --locked --example checkpoint_snapshot_resource_probe`.
Place the binaries in a fresh Linux workspace as `baseline` and
`candidate`, then run the extracted `measure.py WORKSPACE`.

On macOS, 41 snapshot-focused library tests and 21 integration tests
passed, with warnings-denied Clippy. On Linux ARM64, 767 library tests
and 23 targeted integration tests passed, including the final targeted
run of the expanded descriptor-exhaustion test.
