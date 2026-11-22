# Buffered spill I/O for bounded checkpoint rebuilds

A macOS process sample from the 65,536-key full rebuild showed time
in short positional reads during merge and emission and in per-entry
offset-sidecar writes. The change buffers sequential payload and offset
reads and batches sidecar writes. Random lookup continues to read the
exact entry span. Sequential reads still validate each entry's span
and checksum, and a digest and trailer recheck precedes acceptance of
a complete pass. This recheck catches damage to bytes already prefetched.

On macOS ARM64, six alternating pairs ran the bounded rebuild against
the same 7,674,396-byte witnessed log. All pairs produced byte-identical
checkpoints. Each run started a separate process. The same limits apply
to chunk entries, temporary file bytes, and merge descriptors.

| Build | Median elapsed time | Median peak process RSS |
| --- | ---: | ---: |
| Unbuffered spill | 624.19 ms | 2.75 MiB |
| Buffered spill | 134.89 ms | 3.11 MiB |

Elapsed time includes checkpoint and selection publication. RSS
includes process startup and every spill buffer. The operating system
cache was warm, with other host work active. Each sequential merge
input uses up to two 8 KiB buffers. The fixture and measurement do not
establish worst-case latency or a total-memory ceiling.

The [raw archive](raw-results.tar.gz) records baseline `641ad86`,
the candidate diff, binary and fixture hashes, raw timings, and the
process sample. Create the fixture using the
[full rebuild probe](../2026-11-22-bounded-full-rebuild/README.md).
Build the archived `probe.rs` at the baseline and after applying
`buffered-spill.diff`. Place the binaries at `baseline` and `candidate`
in a fresh directory and run
`sh paired.sh <run-directory> <store-directory>`.

## Fault validation

The macOS rebuild suites passed 34 tests. Rust 1.85 on Linux passed 764
library and nine integration tests. The corruption regression modifies
a prefetched entry or offset and checks that the buffered pass fails at
its final verification, even on another attempt to finish it.

On Linux ARM64 tmpfs, the existing
[spill fault runner](../2026-11-19-bounded-spill-io-faults/README.md)
was run against the buffered implementation with
`--syscall pread64 --syscall write` for both error codes. The fixtures
cover V2 and V3, each with an acknowledged suffix and an unwitnessed
complete suffix. All 1,308 EIO cases rejected publication and passed a
clean retry. All 1,308 EINTR cases completed with verified output. The
runner checks the log, witness, selection, and checkpoint bytes.
The [fault archive](linux-faults.tar.gz) includes the control traces,
individual injected syscalls, observed outcomes, and campaign logs.
This campaign exercises a single returned syscall error per case.
