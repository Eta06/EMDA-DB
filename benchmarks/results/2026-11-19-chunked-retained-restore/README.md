# Fixed-buffer large values in retained restore

Values with encoded records over 4 MiB stream into the private log through
a 1 MiB buffer. The writer checks the exact declared length, computes the
ordinary DB01 checksum incrementally, and checks the authenticated value
digest before writing the checksum trailer and synchronizing the boundary.
Smaller records retain grouped synchronization. The staged log is still
reopened and verified against the expected sequence digest before publication.

The fault test corrupts or truncates the end of a large value after queuing an
ordinary small record. It observes partial bytes in the private log, verifies
that the durable witness stops at the small record, and checks stage cleanup.
The retained integration suite also restores maximum-size values and verifies
the reopened state.

The 16-thread library suite passed 563 tests on macOS ARM64 with Rust
1.100.0 and 725 on Linux ARM64 with Rust 1.85.0. Linux passed on both ext4
and tmpfs. Each suite ran from a fixed copy of its test executable. Six
retained integration tests passed separately on each platform. Formatting
and warnings-denied Clippy passed on macOS.
[macOS suite](macos-library.log.gz), [Linux ext4 suite](linux-library.log.gz),
[Linux tmpfs suite](linux-tmpfs-library.log.gz), [source hashes](source.sha256).

## macOS ARM64 large-value sample

The [existing driver](../2026-11-19-retained-restore-locations/restore_memory.rs)
restored four 16 MiB base values and four 16 MiB suffix values. Three
alternating pairs used the same 128 MiB live set and separate release binaries.
The baseline was `9d077db`. Every output reopened and matched the expected
values. The DB01 record bytes after the identity header had the same hash
across all six outputs. The input hashes matched before and after.

| Large-value writer | Median restore time | Median peak RSS |
| --- | ---: | ---: |
| Complete value and encoded record | 0.88 s | 34.19 MiB |
| Fixed 1 MiB buffer | 1.46 s | 3.19 MiB |

An earlier 64 KiB trial had a 7.11 s median and 2.22 MiB peak RSS. The final
buffer reduces append calls and retains a fixed memory cost for large
values. The benchmark includes replay, synchronized writes, verification and
publication, with independently checked output outside the timed process.
The warm local samples include concurrent validation activity. Smaller
records, suffix cardinality, concurrent restores and allocator overhead
have separate memory costs. [Raw timings, hashes and checks](macos-samples.tar.gz).

Reproduce by following the [driver procedure](../2026-11-19-retained-restore-locations/README.md#reproduction)
with separate Cargo target directories. Run restore with `/usr/bin/time -l`
and compare the log record region after the 28-byte header in addition to
reopening each resulting store.
