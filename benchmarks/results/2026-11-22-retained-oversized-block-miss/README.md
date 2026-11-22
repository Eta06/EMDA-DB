# Missing retained key after an oversized value

The retained-base directory packs whole entries into blocks with a 128 KiB
target. Any block larger than that target contains just one entry. Its verified
anchor therefore proves a point miss when that entry's key differs from the
requested key. The lookup still checks the captured file's length. Point hits
and range scans still authenticate the blocks they read. The base directory
is built from the complete verified file at open.

The regression damages an oversized block after opening its descriptor. A
missing key after its anchor and before the next anchor returns absent. Reading
the oversized key or scanning the block still fails on the bad digest.

## Linux ARM64 ext4

Both Rust 1.85.0 builds use the same probe in the
[trace archive](linux-pread-traces.tar.gz). The baseline is `efddd81`. The
verified fixture has a 16 MiB value at `b` between small entries at `a` and `c`.
The miss requests `bb`. The hit reads and compares every byte at `b`.

| Point lookup | Baseline positional bytes | Current positional bytes |
| --- | ---: | ---: |
| Miss after `b` | 16,777,225 | 0 |
| Hit at `b` | 16,777,225 | 16,777,225 |

Counts are successful `pread64` bytes on the captured base descriptor. The
initial buffered base verification uses sequential reads and is outside these
counts. The probe measures logical reads. It does not measure elapsed time,
cache state or physical storage I/O.

Probe SHA-256 values are `d4923c1db6e2011bc091dccd8976ef5b34da5ef86cdef3c7dcc4ba4f41d90d01`
for the baseline and `f537a3e04e43b70e8d22af05c044446c06c6d19d2c39ec96e302c7c0d106c66b`
for the current build.

Both platforms passed the ten retained-index and document integration tests.
The Linux ARM64 library suite passed 751 tests at 16 threads. Formatting and
warnings-denied Clippy passed on macOS.
