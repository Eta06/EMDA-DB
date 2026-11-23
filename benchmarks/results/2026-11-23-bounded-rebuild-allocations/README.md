# Allocation counts for bounded full-log checkpoint rebuild

The paired test used Apple M6, macOS 27.0 (26A425), APFS and Rust 1.85.0.
The baseline is `22a0bb71bf7a21334cdabaeecc33974be023c0b6`. The candidate
replaces only `src/checkpoint_store/rebuild_spill.rs`, SHA-256
`ab56523423862ba97b8e2224c528753a5991acf840344edd7467dedb048c34d3`.
The release binaries were built in separate target directories.

Each run created a fresh V2 store with 12,288 ordered operations in batches
of 256. Three permutations visited 4,096 distinct eight-byte keys. The
third pass deleted every seventh key. Puts used 64-byte values. The
rebuild used 64-key chunks, four-way merges and a 4 MiB spill limit.
Setup and final comparison were outside the measured phase. All twelve
checks compared the generated checkpoint byte-for-byte with the ordinary
checkpoint, checked the source log hash, and verified every key through
the reopened managed store.

| Rebuild measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Rust allocation and reallocation calls, each run | 795,885 | 134,107 |
| Cumulative requested bytes, each run | 16,284,521 | 9,978,585 |
| Elapsed milliseconds, median (range) | 943.96 (922.72–987.68) | 923.35 (909.25–991.51) |

The allocator counted requests from this process between entry to the
bounded rebuild and its return. Cumulative requested bytes include
temporary buffers and reallocations. They do not measure peak RSS, peak
heap use, stack space, or total recovery memory. Timing ranges overlap
and each allocation counter adds measurement overhead.

[Raw samples](macos-samples.log) preserve the alternating run order.
The baseline and candidate executable SHA-256 hashes are
`fdbc73afc9e3bb28bef6051fd6d6cf82d95437c7c06eedc3d8b424efb84fd2b7` and
`5f7ccb38d757b172f40b336221db59d67b8a5a32a10b17211f7ebe9530aff91e`.
A pilot used a shared Cargo target and produced identical binaries.
Only the separate-target results are used here.

To reproduce, place [the same probe](measure_spill_allocations.rs) in the
`examples` directory of both source copies. Build each with
`cargo +1.85.0 build --locked --release --example measure_spill_allocations`
and a separate target directory. Run each executable with the path of an
existing directory on the measurement filesystem as its only argument.

On macOS, 17 spill tests, 11 selected bounded library tests, and all
10 bounded managed rebuild integration tests passed. On Linux ARM64
tmpfs, all 11 bounded managed rebuild integration tests passed, including
V3 metadata-witness and torn-tail cases. Rust 1.85.0 was used on both
hosts.
