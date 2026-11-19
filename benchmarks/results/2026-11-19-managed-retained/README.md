# Managed retained generation validation

The source was commit `005c1b6` on arm64 macOS and Linux ARM64. The checked
source files and test inputs matched the [SHA-256 manifest](source.sha256)
on both systems. See the [environment details](environment.txt).

| Check | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite at 16 threads | 494 passed | 656 passed |
| Indexed candidate and format guard integration tests | 9 passed | 9 passed |

The Mac also passed formatting and all-target Clippy with warnings denied.
The [macOS library log](macos-lib.log.gz), [macOS integration log](macos-integration.log.gz),
[Clippy log](macos-clippy.log.gz), [Linux library log](linux-lib.log.gz) and
[Linux integration log](linux-integration.log.gz) are preserved here. Library
tests include process termination before and after the selection rename and
injected selection sync errors. These tests observe recovery with the OS
running. Physical power-loss and independent off-host restore remain deferred.

## Phase measurements

The [probe archive](probe.tar.gz) contains the Rust source, lockfile and raw
output from separate release-mode processes under macOS `/usr/bin/time -l`.
The input is the existing [synthetic fixture](../2026-11-19-retained-index-overlay/synthetic-input.tar.gz)
with 16,385 base keys and 16,385 suffix operations. Timings exclude compilation.
Open and scan includes hashing every returned entry in order. The three scans
reported 14,044 entries and the same checksum, `1c6afbee4f80ff7a`.

| Phase | Elapsed (s) | Peak process RSS (bytes) | Catalog file bytes at end |
| --- | ---: | ---: | ---: |
| Import | 0.67 | 4,390,912 | 1,580,757 |
| Open and scan original | 0.10 | 4,554,752 | 1,580,757 |
| Open and compact | 18.85 | 4,685,824 | 2,185,117 |
| Open and scan successor | 0.09 | 2,342,912 | 2,185,117 |
| Open and reclaim inactive slot | 0.16 | 2,113,536 | 604,672 |
| Open and scan after reclamation | 0.09 | 2,342,912 | 604,672 |

Reclamation removed 976,085 logical file bytes relative to the imported
catalog, a 61.7% reduction. File lengths exclude allocation metadata and
transient staging. RSS is a per-process measurement. One run on this input
with no cache reset provides neither a scaling result nor a latency
distribution.

To repeat, extract the probe and fixture into temporary directories. Set the
probe dependency path in its Cargo.toml to the checkout of `005c1b6`. Build
using `cargo build --release --locked --offline`. Run the binary with the
phase, fixture directory containing the binding files, and a fresh catalog
path. Execute `create`, `scan`, `compact`, `scan`, `reclaim`, and `scan`
in that order. Record each with `/usr/bin/time -l` on macOS and sum regular
file lengths within the catalog after each phase.
