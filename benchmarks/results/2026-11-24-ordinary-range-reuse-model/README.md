# Ordinary read-set range reuse verification

The [raw archive](raw-artifacts.tar.gz) records `008e9e5` on macOS ARM64,
including the generated inputs, toolchain, binary hash and complete logs.
All 198 file hashes verified after extraction. Warnings-denied Clippy and
all 37 focused tests passed.

The existing independent read-set model passed 192 generated histories
and nine repository seeds. The generated histories used seed 8123, 64
operation bytes per history and all four read-set admission modes. Each
history compares point and range results, checks commit or conflict
against the modeled observations, and verifies every key after reopen.
These are fixed inputs exercised with Rust 1.85.0 release builds.

Build the model with `cargo +1.85.0 build --locked --release --manifest-path
fuzz/Cargo.toml --bin transaction_read_set`. Run the binary with
`-runs=1 corpus/case-* fuzz/transaction-read-set-seeds/*` using the extracted
`corpus` directory and the repository seeds at the recorded revision.
