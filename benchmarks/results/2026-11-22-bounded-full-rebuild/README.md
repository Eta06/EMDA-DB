# Bounded full-log checkpoint rebuild

On macOS ARM64, ordinary and bounded full-log rebuilds ran in fresh
processes against the same witnessed log with 65,536 live keys and
a 7,674,396-byte log. The fixture writes 32-byte keys and 64-byte values
in 256-child batches. The bounded rebuild uses 2,048 keys per spill chunk,
16 merge inputs at most, a 64 MiB spill ceiling, and exact historical
key-count and key-byte ceilings. Both paths rebuilt and selected the
checkpoint on every run. All three pairs produced byte-identical files.

| Method | Median elapsed time | Median peak process RSS |
| --- | ---: | ---: |
| Ordinary full-log rebuild | 69.23 ms | 22.03 MiB |
| Bounded full-log rebuild | 600.19 ms | 2.73 MiB |

The elapsed time includes checkpoint and selection publication. RSS
covers each process from launch. The filesystem cache was warm. These
three pairs characterize this workload, with no universal latency or
total-memory bound. The bounded path trades spill I/O and merging for
lower memory when a caller has no usable base checkpoint. Spill admission
covers temporary run files and sidecars. Checkpoint output, filesystem
allocation overhead, and other process allocations are separate.

The [raw archive](raw-results.tar.gz) records the baseline `6a4a882`,
the source diff, harness, runner, file hashes and raw timing logs. To
repeat, apply the diff to that baseline, copy `probe.rs` temporarily
to `examples/__emda_full_rebuild_probe.rs`, and run
`cargo build --release --locked --example __emda_full_rebuild_probe`.
Copy the binary to a fresh run directory at `probe`. Run
`probe setup <run-directory>/store` and
`sh paired.sh <run-directory>` from the archive.

The focused macOS rebuild suites passed 33 tests. Rust 1.85 on Linux
passed 763 library tests and nine bounded rebuild integration tests.
The tests cover a missing selected checkpoint, exact limits and
rejections before tail adoption, same-length changed bytes, a torn tail,
and a successful retry through normal managed reopen.
