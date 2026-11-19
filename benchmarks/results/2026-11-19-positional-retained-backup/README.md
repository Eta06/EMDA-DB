# Concurrent retained backups and positional reads

Managed backup copies the captured base and exact witnessed suffix directly
from the open generation. It validates the synced stage against the
captured binding. It checks source descriptor identity, the log boundary,
the witness, and the catalog selection before publishing. Full source
verification used by retirement and compaction also uses positional reads.

A regression synchronizes six backup threads on one shared handle and
restores every result, comparing complete key/value state with the captured
generation. On baseline `ebf22fa`, concurrent verification failed with
`UnexpectedEof` because it reused the same file cursor. The updated path
passed. Additional tests reject a rechecksummed source base, a replacement
inode carrying identical bytes, and a corrupted witnessed suffix before
publication. [Baseline test log](baseline-test.log).

## Linux ARM64 read trace

Rust 1.85.0 release builds opened the same catalog on VM tmpfs and published
backups to separate paths. The fixture has a 28 MiB base and a 4 MiB
suffix. The measurement counts successful read and copy syscalls from the
source generation during open and backup. The log column includes fixed
header reads.

| Build | Source base bytes | Source log bytes |
| --- | ---: | ---: |
| Baseline `ebf22fa` | 88,081,005 | 12,584,103 |
| Positional backup | 58,720,670 | 8,389,722 |

The trace shows one fewer read of the base and one fewer read of the
suffix. Every backup file matched by SHA-256, the source file hashes
matched before and after each backup, and both restores matched all
seven expected keys and values. [Trace totals and hashes](summary.json),
[raw traces and logs](linux-backup-traces.tar.gz).

To reproduce, use the `setup` phase of the earlier
[rotation workload](../2026-11-19-streamed-retained-rebase/rotate.rs).
Build [backup.rs](backup.rs) as a release example at both revisions and
run it with the managed catalog path and distinct backup and restore
paths. Collect the trace with `strace -y -s 0 -e
trace=read,pread64,readv,preadv,preadv2,copy_file_range,sendfile,splice`.

## Regression validation

The full library suites ran at 16 threads and passed on macOS ARM64
(573 tests) and Linux ARM64 with Rust 1.85.0 (735 tests). Both platforms
also passed all 21 selected integration tests covering retained indexed
reads, retained documents, future-format guards and atomic batches.
Formatting and warnings-denied Clippy passed on macOS.
[Source manifest](source.sha256), [macOS library](macos-library.log.gz),
[macOS integration](macos-integration.log.gz), [Linux library](linux-library.log.gz),
[Linux integration](linux-integration.log.gz).
