# Streaming checkpoint publication with one payload digest per pass

Streaming checkpoint creation previously computed an entry-only digest on
both write and readback. Each pass already computes the complete checkpoint
payload digest, covering the header and every entry. Readback compares that
digest against the writer's digest and validates the source log against the
bound digest before tail adoption or publication. The change keeps the
checkpoint format and those verification steps.

## Paired publication measurements

Both Rust 1.85.0 release builds used identical fixtures within each group.
Each fixture started with 262,144 keys and 8-byte values. The suffix held
1,024 overwrites and 1,024 inserts in two durable batches. The probe timed
`CheckpointStore::checkpoint_to` after open, including staged readback,
durability sync, and publication. It verified every output byte against an
ordinary full-log reference checkpoint outside the timed interval. The
source log, sidecar, and V3 xattr where present matched before and after.

Each group ran eleven pairs in fresh processes, alternating the run order.
The table reports per-build medians and the median of the eleven paired CPU
reductions. CPU time fell in all 44 pairs in the primary runs.

| Platform | Key bytes | CPU ms before / after | Median paired CPU reduction | Wall ms before / after |
| --- | ---: | ---: | ---: | ---: |
| macOS ARM64 APFS, V2 | 8 | 33.07 / 29.74 | 9.92% | 44.25 / 40.18 |
| macOS ARM64 APFS, V2 | 128 | 129.45 / 112.23 | 13.31% | 141.54 / 124.10 |
| Linux ARM64 ext4 VM, V3 | 8 | 34.23 / 30.69 | 11.30% | 61.01 / 52.18 |
| Linux ARM64 ext4 VM, V3 | 128 | 197.89 / 176.10 | 8.40% | 499.12 / 467.91 |

The 8-byte-key checkpoint is 6,842,468 bytes and the 128-byte-key checkpoint
is 38,422,628 bytes. These are warm-cache runs on one host and its VM.
The primary macOS run repeated an earlier run that overlapped the
correctness suite. The earlier run is retained and shows substantially
higher and more variable wall times. These measurements characterize
explicit publication with the tested suffix. They leave managed handoff
pauses and recovery time to be measured separately.

## Verification and reproduction

The checkpoint, rollover, and bounded managed rebuild integration suites
passed 28 tests on macOS and 29 on Linux. The macOS checkpoint-filtered
library suite passed 204 tests, including interrupted publication, damaged
spill, file and block admission, and changed source bytes. Formatting and
warnings-denied Clippy passed.

The [raw archive](raw-results.tar.gz) includes the probe, Python runner,
both macOS runs, Linux run, hashes, and test logs. To reproduce, place
`checkpoint_digest_probe.rs` under `examples/` in a scratch export of
`7b2784e`. Build with
`cargo +1.85.0 build --locked --release --example checkpoint_digest_probe`,
save the resulting binary as `baseline`, apply `checkpoint.diff`, and
build and save `candidate` with the same command. Run
`python3 repeat-pairs.py /path/to/baseline /path/to/candidate /path/to/fixtures`
using an existing fixture parent with room for the log and three checkpoint
files. The archive SHA-256 is
`4939fbad3ca49381dea497f973bb6bcc0220bec5c190ee36068802f88ad2134b`.
