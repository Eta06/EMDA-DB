# Witnessed ordinary-store import validation

Tested code revision `6c7aa50`. macOS and Linux ARM64 used the same
[Rust source, integration and Cargo inputs](source.sha256). Platform and
compiler details are in the [environment record](environment.txt).

| Check | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite, 16 threads | 503 | 665 |
| Indexed-candidate and format-guard integration tests | 9 | 9 |

The ordinary import tests verify ordered batch peaks, source file
preservation, rejection of path mismatch and insufficient replay budget,
and detection of corrupt superseded history. Another test fails the
destination parent sync and checks that a second source open remains
excluded at that boundary. A process-kill test stops after publication and
before that sync, then verifies the visible destination and the original
source. This is evidence from process termination on a running OS.
Physical power-loss testing and independent off-host restore remain deferred.

The commands were `cargo test --locked --lib -- --test-threads=16` and
`cargo test --locked --test retained_indexed_candidate --test future_log_format_guard`.
Linux used Rust 1.85.0, cached offline dependencies and two build jobs.
Mac also passed formatting and Clippy with all targets and warnings denied.

Raw [macOS library](macos-lib.log.gz), [macOS integration](macos-integration.log.gz),
[Clippy](macos-clippy.log.gz), [Linux library](linux-lib.log.gz) and
[Linux integration](linux-integration.log.gz) logs are preserved here.

## Paired import probe

Both modes start from identical copies of the witnessed V2 ordinary source
in the [preserved phase fixture](../2026-11-18-macos-retained-phase-run.tar.gz).
The source log is 1,793,010 bytes. Both managed copies reopened and returned
14,044 entries with ordered checksum `1c6afbee4f80ff7a` on both platforms.
Source file hashes matched both before and after the two imports. The direct
path omitted the separately published candidate, which retained 604,180
logical file bytes in the two-stage workflow. These totals do not measure
transient staging, allocated blocks or filesystem metadata.

| Local run | Direct import elapsed | Candidate and import elapsed | Direct peak RSS | Candidate and import peak RSS |
| --- | ---: | ---: | ---: | ---: |
| macOS data volume | 0.32 s | 0.16 s | 6,995,968 B | 6,979,584 B |
| Linux ARM64 tmpfs | 0.04 s | 0.04 s | 6,608 KiB | 7,740 KiB |

Each import timing includes ordinary open, verification and publication.
Compilation and the subsequent scan run separately. The direct mode ran
first without a cache reset. These are single local measurements with
per-process RSS. They do not establish a speedup or an allocation bound.
Ordinary open and source replay still hold memory proportional to the live
and replayed keys within applicable admission limits.

The [probe archive](probe.tar.gz) contains build inputs, source checksums
and raw logs. To reproduce, extract the source directory from the phase
fixture into a private directory, point the probe Cargo.toml dependency
at a checkout of `6c7aa50`, and build with
`cargo build --release --locked --offline`. Run the probe with
`direct SOURCE NEW_CATALOG` and `candidate SOURCE OTHER_NEW_CATALOG`, then
run `scan SOURCE CATALOG` for each catalog.
