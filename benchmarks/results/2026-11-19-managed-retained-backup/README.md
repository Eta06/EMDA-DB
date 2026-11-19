# Managed retained backup and restore validation

Production code revision `199f5ff`. The final tested Linux sources and
Cargo inputs match the [SHA-256 manifest](source.sha256). The macOS full
library run used the same production code before removal of one redundant
borrow in a test assertion. The focused macOS backup run and Clippy tested
that final adjustment. See the [environment record](environment.txt).

| Validation | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite, 16 threads | 508 | 670 |
| Indexed-candidate and format-guard integration tests | 9 | 9 |
| Post-adjustment focused backup tests | 25 | Included in library suite |

New tests restore a snapshot after the source compacts and reclaims its
generation. They reject an older binding, corrupted base and suffix
files, changed witnessed source bytes and an insufficient recovery limit.
They validate the visible backup after an uncertain parent sync and
after a child process dies at that boundary. The latter check uses a
binding independently retained from an identical previously acknowledged
snapshot. It exercises termination of a process on a running OS.

Logs are available for [macOS library](macos-lib.log.gz),
[integration](macos-integration.log.gz), [focused backup](macos-focused.log.gz),
[Clippy](macos-clippy.log.gz), [Linux library](linux-lib.log.gz) and
[Linux integration](linux-integration.log.gz).

## Local cross-filesystem drill

The [probe archive](probe.tar.gz) contains the Rust source, timed commands
and output. A managed catalog on Linux tmpfs imported the
[synthetic fixture](../2026-11-19-retained-index-overlay/synthetic-input.tar.gz).
It backed up to an ext4 filesystem on the same VM, then restored to a
new tmpfs catalog using the independently retained binding. The backed-up
base was 704,699 bytes and the suffix was 875,422 bytes. The original and
restored catalogs each returned 14,044 entries with ordered checksum
`1c6afbee4f80ff7a`. A release-mode backup took 0.14 seconds with 6,232 KiB
peak process RSS, and restore took 0.02 seconds with 6,032 KiB.
These single runs exclude compilation and the separately checked scans.

To reproduce, extract the probe and fixture into temporary directories.
Set the probe dependency path to a checkout of `199f5ff` and build with
`cargo build --release --locked --offline`. Use `create FIXTURE COORDINATION/catalog`,
then `backup COORDINATION NEW_BACKUP` with the backup destination on ext4.
Keep the binding files produced under `COORDINATION` separate from the backup.
Point `COORDINATION/backup` to the backup, run `restore COORDINATION NEW_CATALOG`,
and compare the `scan COORDINATION CATALOG` and restored scan outputs.
Physical power-loss testing and independent off-host restore remain deferred.
