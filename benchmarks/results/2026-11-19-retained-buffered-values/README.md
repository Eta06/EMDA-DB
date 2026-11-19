# Buffered retained-base value verification

The `d7d2484` build authenticates retained values directly from admitted
input-buffer slices. Eager open still verifies the complete base and computes
the same file and block digests. Indexed reads still authenticate each touched
block before returning results.

## Validation

The new buffered value test checks an interrupted read, a truncated value
and corruption in the last value byte. The indexed suite checks the same
authenticated block through point and range operations. Formatting and
warnings-denied Clippy passed on macOS.

The first default-suite runs passed the 16-thread library tests on both
platforms and stopped at a stale compound-membership assertion. One branch
of an `Or` can now use a compound index when an enclosing predicate supplies
the other field. Commit `441e34c` checks that plan against a scan and preserves
fallback coverage for an unindexable alternative. All 10 tests in that
integration target then passed on both platforms. The initial suite logs
are retained.

The Rust runs used macOS ARM64 with Rust 1.100.0 and Linux ARM64 with
Rust 1.85.0. The Linux filesystem was ext4. The source manifest records
matching build and test inputs on both hosts after the planner-test
and history-test corrections.

The second default-suite run used `--no-fail-fast` and finished all
117 integration targets on each platform. On Linux it exposed one
schedule-dependent assertion in the document history model. A round
made no effective write and therefore did not rotate. Commit
`66722a5` gives each round a distinct acknowledged epoch before the
concurrent operations and checks the epoch after reopen. The fixed
history target passed with its default 48 rounds and with
`EMDA_RETAINED_DOCUMENT_MODEL_HISTORIES=512` on both platforms.

| Platform | Library tests passed | Integration tests passed | Ignored |
| --- | ---: | ---: | ---: |
| macOS ARM64 | 559 | 519 | 0 |
| Linux ARM64 | 721 | 587 | 1 |

The integration totals use the separately rerun history target for
the test-only change. All other Rust results come from the second
default-suite run. The ignored Linux ENOSPC test needs a disposable
filesystem. Physical power loss and independent off-host restore
remain deferred.

Linux also passed all 150 Python CI tooling tests. They require a Git
checkout and include Linux-only benchmark tools. A source-only export
lacks the revision needed by five of those tests. The macOS release-note
validator passed all eight tests under Python 3.15.2. The macOS system
Python 3.9 run of the entire tooling suite encountered missing
`tomllib` and Linux `os` APIs.

## Reopen measurement

The recorded run used macOS ARM64, Rust 1.100.0 and one persisted catalog with
eight 16 MiB values. The encoded base was 134,217,944 bytes. Each invocation ran
in a fresh process with a warm filesystem cache. The timer covers
`ManagedRetainedStore::open`. After timing, the driver checked every byte of
every value. File hashes matched before and after the measurement.

| Build | Median | Observed range | Samples |
| --- | ---: | ---: | ---: |
| `c2f1928` | 419.46 ms | 418.27–436.55 ms | 5 |
| `d7d2484` | 417.61 ms | 416.12–457.38 ms | 5 |

The observed ranges overlap. The change removes a 64 KiB value-copy scratch
buffer from the buffered verification path. These samples establish neither a
repeatable latency gain nor a process peak-memory measurement.

## Reproduction

Export each revision into its own scratch checkout. Copy `base_open.rs` into
each checkout's `examples` directory and run
`cargo build --locked --release --example base_open` in each checkout. Invoke
the older build's driver with `setup /path/to/new-fixture`. Run
`reopen /path/to/new-fixture/catalog` five times per build in alternating
order. Hash the fixture files before and after the run. The raw timing samples
report nanoseconds. The binary hashes identify the exact local binaries used.
