# Source reads before full-log rebuild adopts a tail

Full-log rebuild streams replayed locations to the private checkpoint. The
staged checkpoint verifier compares the source log with the replay digest
before the old witness can advance. This check also catches a same-length
replacement of a complete tail. The earlier standalone digest check is
redundant on this path. Trusted-checkpoint rebuild retains its separate
prefix and suffix check before adopting a tail.

## Linux ARM64 measurements and returned errors

The [evidence archive](linux-evidence.tar.gz) contains the probe and runners,
Rust 1.85 build logs, hashes, source-read traces and each fault result. Two
builds used the same source and harness except for the change in
`src/checkpoint_store/rebuild.rs`, with `e18467f` as the baseline. The
controls used a Linux ARM64 tmpfs fixture with V2 and V3 witnesses and either
an acknowledged suffix or a complete unwitnessed tail. Both checkpoint and
selection publication were included in the source-read trace. All 16
controls verified every live key and value through both writable paths and
compared the checkpoint byte-for-byte with ordinary full-log recovery.

For a complete unwitnessed tail, the observed logical source bytes were
the same for V2 and V3:

| Source log bytes | Baseline positional read bytes | Current positional read bytes |
| ---: | ---: | ---: |
| 337 | 1,564 | 1,227 |
| 8,388,967 | 33,556,084 | 25,167,117 |

The larger fixture adds an 8 MiB value that a later put supersedes. The
saved bytes equal one complete source log. Acknowledged cases had the
same read totals on both builds. These are syscall byte counts from the
running OS. Device I/O and elapsed time were outside the measurement.

The [existing fault runner](../2026-11-19-checkpoint-replay-binding/replay_io_campaign.py)
injected an error into each of the 12 successful source positional reads
before checkpoint rename on the small fixture. It used a fresh store for
each injection and verified the exact injected ordinal, preserved log,
previous selection and base, cleanup and recovery. All 48 `EIO` cases
returned a pre-publication failure. In 44 cases the witness retained its old
value. In the other four cases the failure occurred in the final reread of
the log header after the witness write, and the witness retained the
complete advanced boundary. Every clean retry produced the reference
checkpoint. All 48 `EINTR` cases resumed and published the reference.
Ordering traces independently identify the witness-write boundary. These
are synthetic returned errno values on a running filesystem.

The focused macOS rebuild checks passed 27 unit and eight integration tests,
and warnings-denied Clippy passed. On Linux ARM64, all 765 library tests and
nine bounded rebuild integration tests passed with Rust 1.85.

To reproduce, extract the archive and place
`source/examples/full_rebuild_read_probe.rs` in `examples` on each revision.
Build with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +1.85.0 build
--locked --example full_rebuild_read_probe`. Copy the binaries to a fresh
Linux workspace as `baseline` and `candidate`. Run the extracted
`compare.py WORKSPACE NEW_COMPARISON_DIRECTORY`. Then run the existing fault
runner with `WORKSPACE/candidate WORKSPACE WORKSPACE/EIO --error EIO` and
`WORKSPACE/candidate WORKSPACE WORKSPACE/EINTR --error EINTR --require-retry
pread64`. Run the extracted `check_adoption.py WORKSPACE` to verify the
witness classifications against a dedicated ordering trace.
