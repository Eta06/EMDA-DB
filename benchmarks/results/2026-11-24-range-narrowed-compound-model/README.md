# Range-narrowed compound query model

The [raw archive](raw-artifacts.tar.gz) preserves 128 generated histories,
the model patch on `0057f3d`, toolchain information, the executable hash,
and the build and run logs. All 135 file hashes verified after extraction.

Seed 9147 supplied 18 mutation or reopen steps per history. All **2,304**
planned queries matched complete collection scans in order, rows, and
match counts. The selected plan used four exact probes over the complete
three-field tuple. The histories cover both writable backends, full and
partial indexes, signed endpoints, mixed membership types, pagination,
projection, and repeated clean reopen.

This was a fixed-corpus macOS ARM64 run using a Rust 1.85.0 release build.
The binary ran every supplied input once. The run used neither mutation
fuzzing nor a sanitizer. Reproduce it from the extracted corpus with
`document_compound_range -runs=1 corpus/case-*`.
