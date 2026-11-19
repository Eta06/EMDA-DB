# Disk-backed writable retained suffix

Revision `d120876` replays writable suffixes with one reusable record
buffer and a bounded read buffer. The open handle keeps touched keys,
tombstones, and the file offsets, lengths and SHA-256 digests of live
suffix values. A read authenticates the selected value bytes. Backup
streams the suffix and verifies the staged digest. Rotation verifies
the bytes used to reconstruct historical peaks. The persisted format,
witness fields and synchronization protocol are unchanged.

## macOS ARM64 APFS result

The [workload](suffix_rss.rs) appends 48 one-MiB values over two hot
keys, with repeated keys inside DB02 batches and a base tombstone. The
resulting generation log is 50,333,979 bytes. Both builds opened the same
catalog from fresh processes in four alternating pairs. Each run checked
all three live keys and every selected byte through scans and point reads.
File hashes matched before and after the run.

| Release build | Median open time | Median peak process RSS |
| --- | ---: | ---: |
| Resident suffix, `e6816f6` | 122.5 ms | 53.16 MiB |
| Disk-backed suffix, `d120876` | 194.4 ms | 5.16 MiB |

Open timing ends when the handle returns. Peak RSS covers the whole
process, including verification and read results. This is a warm,
same-host experiment. Per-value authentication adds hashing on replay,
and suffix reads now use disk I/O. The measurement does not establish
a process-wide memory or latency ceiling. A single physical record can
be just over 16 MiB, and sparse indexes, overlay metadata, appends and
result pages have separate costs. Detached suffix imports still stage
the whole input before creating the generation.

To reproduce, place `suffix_rss.rs` in `examples/` at each revision
and build with `cargo build --release --locked --example suffix_rss`
using different target directories. Use the baseline binary to run
`suffix_rss setup <new-directory>`. Run
`/usr/bin/time -l suffix_rss reopen <new-directory>/managed` with
each build in alternating order. The benchmark source comparison
matched the final production code. The only source difference was
a test-only Rust path qualification. Executable hashes, raw timing
logs and catalog hashes are retained beside this report.

## Recovery and corruption verification

The new regression verifies a multi-chunk batch with a repeated key
after append and reopen. A substitution with freshly computed CRCs
fails point and page authentication on the open handle and fails the
witness check on reopen. A short physical header, torn record, split
witness boundary, missing committed data and substituted data all
leave the witness unchanged.

- macOS ARM64 passed 538 library tests at 16 threads, then all 70
  retained tests on the final source after adding the short-header
  case. The 14 selected integration tests covered document mutations,
  retained document transactions, future format rejection and indexed
  retained reads. Formatting and warnings-denied Clippy passed on
  the final source.
- Linux ARM64 passed 700 library tests at 16 threads on the final
  source and 16 selected integration tests, including the 48-case
  kernel file-size campaign. The checked source hashes matched
  between hosts. An earlier relink ran out of tmpfs space while
  the old test process was running. Six re-exec tests failed to
  find their executable. Both logs are retained. The final build and
  test run were sequential and passed without test retries.

Physical power-loss validation and independent off-host restore remain
deferred.
