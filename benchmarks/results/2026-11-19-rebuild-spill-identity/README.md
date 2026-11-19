# Private rebuild spill identity

A spill run now retains the digest calculated by its writer in the session's
run metadata. Every cursor open checks the on-disk trailer against that
digest. Merge and emission also hash the full run. Entry checksums incorporate
the private run path, which includes a fresh staging directory name and the
run sequence number. This version is private to the temporary spill.

The engine regression overwrites a finished spill on the same inode after
the emission cursor opens. The replacement comes from a different session
with the same run sequence, entry count and file length. The replacement
points to valid source-log values, with the locations for keys `b` and `c`
swapped. Baseline `f8597e0`, with diagnostic output added to the test,
published and reopened the swapped values. The fixed rebuild rejects the
replacement and publishes neither checkpoint nor selection. The already
validated tail is witnessed, consistent with the existing ordering. A clean
retry recovers `b = one` and `c = two` from the unchanged log.

The lower-level regression also substitutes a valid run before point lookup
and before full cursor open. The retained digest rejects both attempts.
These checks exercise valid run substitution within disposable fixtures.
The storage contract still requires trusted staging directories.
[Baseline reproduction](baseline-test.log).

## Cross-platform validation

The full library suites ran at 16 threads. The integration runs include
managed rebuild, the large suffix case and the full-log differential model.

| Platform | Library tests | Rebuild integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 574 passed | 9 passed |
| Linux ARM64, Rust 1.85.0 | 736 passed | 10 passed |

Formatting and warnings-denied Clippy passed on macOS. The initial Linux
compiler process was killed under memory pressure. The retry passed after
previous benchmark fixtures were archived and removed from the VM tmpfs.
[Initial build log](linux-library-build-killed.log.gz),
[source manifest](source.sha256), [macOS library](macos-library.log.gz),
[macOS rebuilds](macos-integration.log.gz), [Linux library](linux-library.log.gz),
[Linux rebuilds](linux-integration.log.gz).

The address-sanitized bounded rebuild model ran 22,358 inputs in 61
seconds on Linux ARM64 with no reported finding. It uses the tracked
`fuzz/bounded-rebuild-seeds` and compares rebuild with the full-log
reference. Inputs were capped at 48 bytes. [Corpus and run log](linux-fuzz.tar.gz).

```sh
CARGO_BUILD_JOBS=1 cargo +nightly-2026-11-03 fuzz run --codegen-units 16 \
  bounded_rebuild corpus -- -max_total_time=60 -max_len=48 -seed=42
```
