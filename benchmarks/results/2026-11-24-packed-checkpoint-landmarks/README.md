# Checkpoint directory allocation with packed landmarks

A Linux ARM64 tmpfs run paired Rust 1.85.0 debug binaries on the
same three checkpoint fixtures. Each open used exact block and file
limits. The probe checked the complete fixture dimensions, sampled
point lookups and a bounded scan. All 18 opens passed. Each pair
ran three times with alternating order, and each repetition returned
the same allocation counts. Source file hashes matched before and
after the comparisons.

| Keys × key bytes | Blocks | Allocation and resize requests, before / after | Requested bytes, before / after | Live requested bytes, before / after |
| --- | ---: | ---: | ---: | ---: |
| 145 × 1,000 | 5 | 26 / 23 | 9,900 / 9,476 | 1,036 / 668 |
| 200,000 × 8 | 159 | 979 / 39 | 137,356 / 107,140 | 65,380 / 45,740 |
| 20,000 × 1,000 | 625 | 653 / 37 | 209,904 / 144,392 | 103,532 / 52,300 |

The probe counts successful Rust allocator requests during open.
The live column records the net change in requested heap bytes
while the opened view is held, before lookup and scan verification.
Allocator overhead, temporary stack buffers, the OS page cache,
and physical residency have separate costs. The comparison
measures allocation, without a timing or total-memory claim.

The [raw archive](raw-results.tar.gz) includes the baseline revision,
candidate diff, counters, probe, binary hashes, fixture hashes,
build and test output, and a summary verifier. Build the probe as
the Rust example `packed_checkpoint_probe`. Seed each fixture with
`setup <fixture> <keys> <key-bytes>` before measuring both builds
with `open <fixture> <keys> <key-bytes>`.
