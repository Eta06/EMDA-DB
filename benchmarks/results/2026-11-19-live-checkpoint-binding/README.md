# Bind live checkpoints to verified log replay

The ordinary checkpoint writer now carries a digest from the same
sequential replay that reconstructs and verifies the live index. The
checkpoint-backed writer continues hashing from the trusted base prefix
with the exact suffix bytes used to verify the bounded overlay and
compute its historical peak. Manual, managed and automatic rollover use
this path. Streamed compaction also supplies a digest of the new log from
its record verification. The production streaming writer requires a
verified log digest and compares it with the source during staging.
It checks the covered length and witness before publishing.

The regression adapter replaces a valid record after supplying its original
bytes to the parser, with the same length and a valid checksum. In the
baseline, ordinary creation and a checkpoint-store copy both publish
despite that substitution. The fix returns an integrity error before
publication. The live regression exercises a separate checkpoint copy,
unmanaged rollover, managed rollover and automatic rollover. The original
selection and overlay remain active, and restoring the original bytes
permits clean reopening. [Failing baseline](macos-before-fix.log.gz).

The store lock still requires cooperating writers and stable external
paths. Source verification and publication do not constitute an atomic
transaction against another process changing the files directly.

## Linux log-read measurements

The two integration tests ran in a Linux ARM64 VM on tmpfs using Rust
1.85.0 and the same workload on both builds. The baseline is `f4711a4`.
Counts include fixture setup, test execution and reopen. They sum
successful `pread64` results on descriptors for `records.log` only.
Both traced runs of each test passed.

| Fixture | Baseline logical log bytes | Fixed logical log bytes |
| --- | ---: | ---: |
| Ordinary checkpoint with an atomic-batch historical peak | 855 | 769 |
| Live rollover over multiple index blocks with shared key prefixes | 1,765,240 | 1,326,760 |

The separate full-log hash pass is eliminated for ordinary checkpoint
creation and live checkpoint rollover. Verification of the staged output
still reads the complete covered log. The trace measures logical read
volume on these two workloads. It does not give device I/O or elapsed
time bounds. [Read totals](linux-log-reads.txt),
[raw traces, test outputs and binary hashes](linux-traces.tar.gz).

## Cross-platform validation

Both platforms ran their library suites and seven focused integration
targets at 16 threads. The targets cover checkpoint rollover, streamed
compaction, historical views and offline rebuild with differential replay.

| Platform | Library tests | Integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 579 passed | 42 passed |
| Linux ARM64 | 741 passed | 43 passed |

Formatting and warnings-denied all-target Clippy passed on macOS.
[Source manifest](linux-source.sha256), [macOS tests](macos-tests.log.gz),
[Linux library](linux-library.log.gz), [Linux integration](linux-integration.log.gz).

Physical power-loss testing and independent off-host restore remain
deferred.
