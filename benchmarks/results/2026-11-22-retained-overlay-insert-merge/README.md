# Retained overlay insertion batches

When enough new sorted keys fall within the existing overlay,
`5e6c3db` places the complete admitted batch in a reverse merge.
The write reserves vector growth before append and publishes the new
overlay after syncing the log and witness. The existing charge for
every ordered operation continues to govern admission.

## Paired durability benchmark

Baseline `934f126` and candidate `5e6c3db` built with separate target
directories and the same example. Every run seeded an empty retained
base with the specified overlay size. The measured batch inserted
1,024 keys before or after the existing range or replaced the first
1,024 keys in the range. Timing includes encoding, durable append and
overlay update. Setup, close, replay, and a full verification against
the expected model were outside the timer. All 96 stores reopened and
matched their complete expected key/value state.

Wall and process CPU times are medians in milliseconds from six pairs
per platform and workload, with the two binaries alternating order.

| Platform | Existing keys | Batch | Baseline wall | Baseline CPU | Merge wall | Merge CPU |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| macOS APFS | 8,192 | Insert before | 15.828 | 9.157 | 6.006 | 0.293 |
| macOS APFS | 65,536 | Insert before | 98.183 | 92.421 | 7.949 | 2.197 |
| macOS APFS | 65,536 | Insert after | 6.939 | 1.242 | 7.776 | 1.244 |
| macOS APFS | 65,536 | Replace | 7.223 | 1.405 | 6.524 | 1.470 |
| Linux ext4 | 8,192 | Insert before | 11.555 | 11.103 | 0.840 | 0.358 |
| Linux ext4 | 65,536 | Insert before | 80.248 | 79.865 | 1.057 | 0.610 |
| Linux ext4 | 65,536 | Insert after | 0.676 | 0.240 | 0.760 | 0.290 |
| Linux ext4 | 65,536 | Replace | 0.585 | 0.278 | 0.701 | 0.349 |

The largest front insert saved more than 79 ms of median CPU on both
platforms. The append and replace controls had small absolute
differences and use the direct update path. This intentionally stresses
a placement where the old algorithm repeatedly moved existing entries.
It does not measure rotation, concurrent throughput or replay speed.
The [SQLite matrix](../2026-11-22-recovery-sqlite-matrix/README.md)
provides the broader cross-engine comparison.

The [archive](raw-results.tar.gz) includes the example, driver, binary
hashes and full phase logs. To reproduce, copy the archived example into
`examples/retained_overlay_merge_probe.rs` at each revision. Build it
with `cargo build --locked --release --example retained_overlay_merge_probe`
using a different target directory for each revision. Name the binaries
`baseline` and `candidate` in one run directory. Pass that directory
and an existing writable fixture parent to the archived `run-paired.sh`.
