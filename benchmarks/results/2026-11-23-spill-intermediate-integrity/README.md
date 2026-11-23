# Bounded rebuild with damaged spill intermediates

Both bounded rebuild APIs rejected damage at two spill boundaries on macOS
ARM64 APFS with a V2 witness and on Linux ARM64 ext4 with V2 and V3 witnesses.
The test forces single-key chunks and a fan-in of two. It changes the first
offset after the sidecar is flushed or flips a byte in a merged run's digest
after that run is verified. This exercised four cases on macOS and eight on
Linux.

Each case required `NotPublished(InvalidData)` with the expected spill error.
It compared the log, witness, anchor or sidecar, trusted base, and existing
selection byte for byte with their original contents. It checked that no
destination was created and no spill stages remained. A clean retry adopted
the pending tail, produced the same checkpoint bytes and digest as ordinary
full-log replay, and selected the rebuilt checkpoint.

All 20 spill-related tests passed on each platform. The broader checkpoint
rebuild module passed 16 tests on macOS and 19 on Linux at four test threads.
These checks cover two selected intermediate corruption states on a running OS.

## Reproduction

Use Rust 1.85.0 and a writable temporary directory on a filesystem that
supports user xattrs to exercise V3 on Linux.

```sh
cargo +1.85.0 test --locked --lib corrupt_spill_intermediates -- --nocapture --test-threads=1
cargo +1.85.0 test --locked --lib spill -- --test-threads=1
cargo +1.85.0 test --locked --lib checkpoint_store::rebuild::tests -- --test-threads=4
```

The Linux VM build used the toolchain's LLD in single-threaded mode after
GNU ld builds exhausted available guest memory. The [raw results](raw-results.tar.gz)
record both platforms, the hashes of the tested source files, and test output.
The archive SHA-256 is
`761b9c20eba12271be920efd259773bfb903569e3ee83cb0ace40bfb32529374`.
