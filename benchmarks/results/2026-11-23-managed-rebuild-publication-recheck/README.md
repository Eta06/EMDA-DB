# Streaming the managed rebuild publication recheck

The rebuilt checkpoint is reopened by name and checked before updating the
managed selection. This pass streams the published checkpoint, counts blocks
against the same limit as a view, and verifies the covered log, exact length
and witness. It does not build a sparse directory or retain lookup landmarks.

## Allocation measurement on macOS

Apple M6, macOS 27.0 (26A425), APFS, Rust 1.85.0 release builds.
Baseline revision `2fac3bce3d18d6d0e614a82f73024ed256109028`.
The candidate replaces `src/checkpoint.rs`, `src/checkpoint_view.rs`,
and `src/checkpoint_store/rebuild.rs`. The builds use separate targets.

Each run rebuilds from the same trusted V2 checkpoint with 131,072 eight-byte
keys, eight-byte values and an empty log suffix. Both checkpoint limits are
exactly the fixture size and 105 blocks. An untimed pilot established an
existing valid managed selection. Measured destinations have equal-length,
fresh names. Each run checks that the published checkpoint is byte-identical
to the trusted input, the source log and witness are intact, and a reopened
managed store contains the expected ordered key and value for all 131,072 keys.

| Whole rebuild measurement | Baseline | Streaming recheck |
| --- | ---: | ---: |
| Rust allocation and reallocation calls | 1,349 | 717 |
| Cumulative requested bytes | 234,656 | 157,888 |
| Elapsed ms, median (range) | 45.71 (43.37–60.46) | 43.04 (41.75–45.80) |

There are four warm samples per build in alternating pair order. Setup
and verification after the rebuild are outside the measured interval.
Requested bytes are cumulative allocator traffic. They include transient
buffers and reallocations. They do not measure peak RSS or a recovery memory
cap. Elapsed-time ranges overlap.

The executable SHA-256 values are
`3d95a4fdacbcf4cb85753364fd8fff6bcd11f4751177ef2dbd039d9dbb801b43` and
`5c7ff54f0d9de1fa311c82195f326ca6e88072d87d23ec94241343787e26c67d`.
The [probe](measure_managed_recheck.rs) and [samples](macos-samples.log) are
included. To reproduce, copy the probe into `examples/` in both source
copies, then run `cargo +1.85.0 build --locked --release --example measure_managed_recheck`.
Run one binary with `prepare <fixture-dir>`, run an untimed pilot with
`pilot-run01 <fixture-dir>`, then alternate builds using unique names of the
same length for the first argument and the same fixture directory.

## Correctness checks

Focused macOS tests cover damage to the published checkpoint or covered log,
an unexpected suffix, and a changed witness after checkpoint publication.
The error retains the previous selection. After the injected change is
cleared or the complete tail is recovered, a clean retry reopens with the
expected data. The bounded rebuild, checkpoint and managed-rebuild
integration suites pass on macOS and Linux ARM64, including Linux V3 paths.
