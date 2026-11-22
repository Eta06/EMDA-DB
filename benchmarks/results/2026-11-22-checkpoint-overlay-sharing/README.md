# Shared checkpoint snapshot overlays

Capturing a checkpoint snapshot now retains its current suffix overlay.
Snapshots of the same state share that overlay along with the verified
base file and directory. The first write to a shared overlay detaches it
under the state write lock before submitting records. Deadline and
cancellation checks still run before append. The writer applies mutations
after the normal witness synchronization. Each snapshot continues
reserving its full logical key, key-byte and block counts.

## Linux ARM64 resource comparison

The probe opens a checkpoint-backed store with a 256-key base and holds
16 snapshots. It then replaces the same existing suffix key twice,
checking that the live view returns the latest value and all snapshots
still return the captured value. It also checks that the reservations
return to zero after releasing the snapshots. Five alternating-order
pairs used copies of the same fixture on Linux tmpfs. Both binaries
used the nonoptimized Rust 1.85 profile. The baseline is `de4f39c`.

| Overlay keys | Baseline capture allocations | Shared capture allocations | Baseline first write allocations | Shared first write allocations |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 1,200 | 16 | 5 | 80 |
| 4,096 | 76,448 | 16 | 5 | 4,783 |

The second write used five allocation calls in both versions. At 4,096
keys the observed median total capture time fell from 3.61 ms to
5 microseconds. The median first write, including synchronization, rose
from 23 to 242 microseconds. The median second write in each version
was about 7 microseconds. These are warm tmpfs samples, not a throughput
or production tail-latency comparison. Capturing a new snapshot between
each mutation can move one overlay copy into each writer's critical
section. The overlay remains subject to the existing suffix limits.

The [evidence archive](linux-evidence.tar.gz) contains the probe, runner,
binary hashes, raw results, and Linux tests. Reproduce by extracting the
probe into `examples/snapshot_overlay_resource_probe.rs` in each revision
and building with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0
cargo +1.85.0 build --locked --example snapshot_overlay_resource_probe`.
Copy the binaries to a fresh Linux directory as `baseline` and
`candidate`, and run the extracted `measure.py WORKSPACE`.

On macOS, 196 checkpoint-focused library tests and 12 integration tests
passed, with warnings-denied Clippy. On Linux ARM64, all 768 library tests
and 14 targeted integration tests passed.
