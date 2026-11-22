# Checkpoint range scans using verified landmarks

A bounded checkpoint range scan now uses the sparse entry offsets in its
first block to find the last landmark at or below the exclusive lower
bound. It still reads and hashes every visited block before using the
offsets. The scan and its block search reuse one buffer. The response
limits, ordering, and transaction read-set coverage are preserved.

Regression tests exercise lower bounds around landmarks and block
boundaries, including absent keys, and reject corruption in bytes before
the seek position. On macOS, 70 checkpoint, read-set and document planner
tests passed. On Linux ARM64, 768 library and 59 selected integration tests
passed using Rust 1.85.

## Paired Linux ARM64 scan measurements

Both versions used the same 4,096-entry warmed checkpoint on tmpfs and
verified every returned key and value. Seven pairs alternated execution
order. Page timings exclude opening the store and include value reads
and result comparisons. The short pages used four-byte keys. The
1,024-row pages used 128-byte keys and touched five index blocks.
Both binaries used the Rust 1.85 release profile with optimization level
2. The baseline is `8f9bdab`.

| Page | Baseline time | Current time | Baseline allocations | Current allocations |
| --- | ---: | ---: | ---: | ---: |
| One row near the start of a block | 11.79 μs | 11.77 μs | 4 | 4 |
| One row near the end of a block | 16.14 μs | 11.65 μs | 4 | 4 |
| 1,024 rows over five blocks | 291.14 μs | 286.03 μs | 2,062 | 2,058 |

Times are medians of the per-run mean page time. These samples exercise
one warmed historical view at a time. They do not characterize cold
storage, write contention, or total process memory.

The [archive](linux-evidence.tar.gz) includes build logs, hashes,
individual measurements, and the probe and runner. To reproduce, place
the extracted `checkpoint_scan_probe.rs` under `examples` in each
revision and build with `CARGO_PROFILE_RELEASE_OPT_LEVEL=2
CARGO_INCREMENTAL=0 cargo +1.85.0 build --release --locked --example
checkpoint_scan_probe`. Copy the binaries to a Linux workspace as
`baseline` and `candidate`, then run `paired.py WORKSPACE OUTPUT_JSON`.
