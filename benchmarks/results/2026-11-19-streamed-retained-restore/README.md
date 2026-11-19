# Bounded writable retained import and restore

Revision `33cc032` opens an admitted detached suffix once and copies
at most 1 MiB at a time into a private generation. The copy must match
the exact endpoint and independently held digest. The new log and its
witness are synchronized and the generation is replayed under the
configured limits. Initialization checks the replayed endpoint and
digest before publication and rejects a complete extra tail as well
as an incomplete one. The on-disk format stays the same.

## Resource measurement on macOS APFS

The [workload](restore_rss.rs) uses the 48 MiB suffix from the
[disk-backed recovery fixture](../2026-11-19-disk-backed-retained-suffix/README.md).
One bound backup supplied all four alternating pairs of fresh-process
restores. Each restored catalog was reopened in a separate process and
checked against the same three-key model, including all live value
bytes. Backup file hashes matched before and after. The timed interval
covers restore through durable publication. Peak RSS covers the
restore process and excludes the later verification process.

| Release build | Median restore time | Median peak process RSS |
| --- | ---: | ---: |
| Full suffix buffer, `7d41226` | 615.7 ms | 49.91 MiB |
| Streamed copy, `33cc032` | 575.2 ms | 3.09 MiB |

Earlier campaigns using 64 KiB chunks measured about 3.4 to 3.9 s per
restore on the same host while library tests were exercising storage.
The 1 MiB experiment measured 711 ms in the comparison campaign and
kept a similar RSS to the 64 KiB build. The final four streamed runs
ranged from 383 to 703 ms. The experiment supports the lower memory
use on this fixture. The timing variation precludes a general speed
claim. It does not measure total process memory for arbitrary import
limits or establish a whole-catalog disk quota.

To reproduce, build the supplied `restore_rss.rs` as a release example
at both revisions using separate Cargo target directories. Create
the 48 MiB fixture with `suffix_rss setup` from the linked report.
Run `restore_rss backup <fixture>/managed <new-input-directory>`
once. Alternate `/usr/bin/time -l restore_rss restore <input> <new-output>`
between both binaries. Verify each output with `suffix_rss reopen`.
The exact source manifest, executable hashes, alternate-buffer results
and trial logs are retained in this directory.

## Correctness and failure results

The new replay test covers an exact byte and operation cap, a repeated
multi-chunk batch key, a tombstone and a present empty value. Restore
rejects a batch with a bad CRC even when its detached digest matches
the supplied binding. Descriptor-level tests cover a suffix shortened,
extended or substituted after admission.

Six Linux kernel file-size cases exercise partial staged log writes
at 1,024, 2,048 and 3,072 bytes, with both returned `EFBIG` and process
termination by `SIGXFSZ`. Each leaves the destination absent and
preserves the backup, trusted binding, and source generation. Cleanup
and a retry from the same backup restore the source state. The whole
kernel campaign includes the earlier 48 cases for other copy paths.

macOS ARM64 passed 540 library tests with the initial buffer and all
72 retained tests with the final buffer. Its selected integration
binaries passed 14 tests, and formatting and warnings-denied Clippy
passed on the final source. Physical power-loss testing and independent
off-host restore remain deferred.

Linux ARM64 passed 702 library tests and 18 selected integration
tests on the final source, including all 54 kernel file-size failure
cases. The library suites used 16 threads and the checked source
manifest matched between hosts.
