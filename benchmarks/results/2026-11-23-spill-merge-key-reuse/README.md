# Key-buffer reuse in spill merges

Six paired full-log bounded rebuilds on Apple Silicon macOS 27.0 APFS
compared revision `0edc187` with the replacement
`src/checkpoint_store/rebuild_spill.rs` (SHA-256
`cc1b41c0d4c979c1ff5b3414487f7b9f0617f51a8bc757092d9aa895cc796a6e`).
The release binaries used Rust 1.85.0 and separate target directories.
The [same fixture and allocator probe](../2026-11-23-bounded-rebuild-allocations/README.md)
used 12,288 operations across 4,096 keys, 64-key chunks, and four-way
merges. The run order alternated. Each rebuild produced a byte-identical
checkpoint compared with ordinary checkpoint creation, preserved the
source log hash, and returned the expected value for every key after
managed reopen.

| Rebuild measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Allocation and reallocation calls per run | 134,107 | 36,441 |
| Cumulative requested bytes per run | 9,978,585 | 9,197,257 |
| Elapsed ms, median (range) | 923.55 (912.12–946.76) | 911.41 (899.19–1,008.36) |

These are allocator requests during rebuild, excluding setup and post-run
verification. Peak memory and physical I/O were outside the measurement.
Timing ranges overlap and the allocation counters contribute overhead.

[Raw samples](macos-samples.log) record all twelve runs. The baseline and
candidate executable SHA-256 hashes are
`0beaead9d49810b54ff350cb05532056dd0001095e0e1904c88637edaf8e718b` and
`7a68e20830c027387de0d6a8342f2e3e0cd8452b20013cb1ec1d370431685027`.

The 17 spill tests and 10 bounded-rebuild integration tests passed on
macOS. All 11 bounded-rebuild integration tests passed on Linux ARM64
tmpfs, including the V3 torn-tail case.
