# Automatic retained reclamation validation

Tested code revision `7a2d619`. The tested sources and Cargo inputs matched
the [SHA-256 manifest](source.sha256) on macOS and Linux ARM64. See the
[environment record](environment.txt).

| Validation | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite, 16 threads | 499 | 661 |
| Indexed-candidate and format-guard integration tests | 9 | 9 |

The tests cover successive reuse of both generation slots, rejection of
a mutation that exceeds an empty suffix, rotation at each type of suffix
capacity, and preservation of the log, witness and selector when rebase
rejects the triggering mutation. The process-kill test interrupts cutover
before and after the selection rename and interrupts reclamation after
moving the inactive generation. Automatic reopen verifies the selected
generation, cleans remaining stages or slots, and preserves acknowledged
values. These process stops leave the operating system running. Physical
power-loss and independent off-host restore remain deferred.

Logs are available for [macOS library](macos-lib.log.gz),
[macOS integration](macos-integration.log.gz), [Clippy](macos-clippy.log.gz),
[Linux library](linux-lib.log.gz) and [Linux integration](linux-integration.log.gz).
The test commands used `cargo test --locked --lib -- --test-threads=16` and
`cargo test --locked --test retained_indexed_candidate --test future_log_format_guard`.
Linux used Rust 1.85.0 and offline cached dependencies with two build jobs.
macOS formatting and warnings-denied Clippy also passed.

## Write latency and retained bytes

The [probe archive](probe.tar.gz) contains the Rust source, lockfile, and
raw output. Both modes independently import the same
[synthetic fixture](../2026-11-19-retained-index-overlay/synthetic-input.tar.gz)
and then write eight distinct keys with 900-byte values. The automatic
handle uses a 4,096-byte threshold, causing two rotations. The manual
handle does no compaction. Release-mode timings exclude fixture import
and compilation. A separate ordered scan of each catalog returned 14,052
entries and checksum `90819a91b9329886` on both platforms.

Logical file lengths after eight writes were **612,044 bytes with automatic
reclamation** versus **1,588,181 bytes without maintenance**, a 61.5%
reduction in this fixture. The measurement excludes filesystem overhead and
transient staging. Each reported write includes synchronous maintenance if
triggered. The Linux VM ran on tmpfs and macOS on the host data volume.
The timings describe each local setup with no cold-cache controls.

| Run | Two rotating writes | Other writes with automatic maintenance enabled | Writes with manual mode |
| --- | ---: | ---: | ---: |
| macOS after the fault suite | 302 and 286 ms | 4.9–6.9 ms | 4.9–7.9 ms |
| macOS alongside the fault suite | 59.5 and 19.1 s | 14.6–23.8 ms | 14.9–58.6 ms |
| Linux VM on tmpfs | 147 and 144 ms | 0.011–0.013 ms | 0.011–0.122 ms |

The concurrent macOS run overlapped the I/O fault tests. The repeat began
after the suite and targeted tests exited. These sparse observations show
the cost of doing reclamation in the write path under the sampled conditions.
They do not establish a latency bound or support a filesystem performance
ranking.

To repeat, extract the probe and fixture into temporary directories. Set
the dependency path in `source/Cargo.toml` to a checkout of `7a2d619` and
build with `cargo build --release --locked --offline`. Use
`managed-retained-probe create FIXTURE NEW_CATALOG` separately for each
mode. Run `managed-retained-probe manual FIXTURE MANUAL_CATALOG` and
`managed-retained-probe auto FIXTURE AUTO_CATALOG`, then run `scan` on each
and compare outputs.
