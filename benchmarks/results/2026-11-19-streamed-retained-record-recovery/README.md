# Streaming retained records during recovery

Engine revision `debfaf8c31fff001ca1fdb00b484b77d60bb4b3f` replays
writable retained suffixes through a 64 KiB buffer. Ordinary and retained
recovery share the streaming DB02 decoder. A batch keeps at most 1,024
child keys and their value locations. An over-budget batch skips child
allocation and decoding while recovery checks the outer checksum and the
acknowledged digest. Ordered replay and per-value SHA-256 authentication
remain in place. The on-disk format and witness protocol are unchanged.

The new tests exercise failure precedence and ensure a malformed or
over-budget batch cannot expose a child. They also verify correct value
offsets after crossing the read-buffer boundary and check that the rebase
history scan detects a rechecksummed superseded value.

## macOS ARM64 reopen workload

The same catalog held a generation log of 81,791,131 bytes, including a
16 MiB put and a near-16 MiB batch that were later superseded. Each release
binary reopened the catalog in four fresh processes, alternating which
version ran first. Each run verified three live keys and two 1 MiB values.
All fixture file hashes matched before and after the experiment.

| Release build | Median peak process RSS | Median open time |
| --- | ---: | ---: |
| Previous record-sized buffer, `ac2ef72` | 18.05 MiB | 260.92 ms |
| Streaming record decoder, `debfaf8` | 5.16 MiB | 261.48 ms |

Peak RSS includes open and result verification. Open time measures only
the call returning the handle. These samples cover one warm-cache host
and workload. Base verification, overlay metadata, result pages and
other allocations still contribute to process memory and startup time.
The [fixture program](suffix_rss.rs), binary hashes, raw samples and
fixture hashes are retained here.

To reproduce, place `suffix_rss.rs` in `examples/` at each revision and
build with `cargo build --offline --locked --release --example suffix_rss`
in separate checkout and target directories. Run the prior binary with
`setup <new-directory>` once, then use `/usr/bin/time -l` with each binary
and `reopen <new-directory>/managed` in alternating order.

## Cross-platform validation

The same source and test inputs compiled on both ARM64 hosts. Library
suites ran at 16 threads. The selected integration targets covered
atomic batches, indexed retained reads, future format guards and retained
document transactions. Formatting and warnings-denied Clippy passed on
macOS. [Source manifest](source.sha256), [toolchain details](environment.log).

| Platform | Library tests | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 556 passed | 20 passed |
| Linux ARM64 | 718 passed | 20 passed |

Linux also passed the six-test kernel file-size suite, which exercises 54
child-process failure cases. The address-sanitized `large_record_recovery`
model exercised the shared decoder through ordinary-store reopen. It
completed 1,204 executions in 61 seconds from 13 existing seeds. The model
checks legacy, V2 and platform-default witnessed records, truncation, and
corruption with recomputed outer batch CRCs. The [archive](linux-fuzz.tar.gz)
contains the log and resulting corpus. The command used
`cargo +nightly-2026-11-03 fuzz run large_record_recovery` with
`-max_total_time=60 -max_len=16 -seed=42`.

Physical power loss and independent off-host restore remain deferred.
