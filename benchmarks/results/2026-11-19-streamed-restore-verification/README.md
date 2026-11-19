# Streaming verification of a staged retained restore

New-history restore reopens the private output read-only and checks the
attached witness and exact expected log length before and after the scan.
The regular recovery parser checks every record checksum. It hashes each
value with SHA-256 in the same pass and checks the ordered keys, lengths
and digests against the authenticated replay entries. Verification uses
the 64 KiB recovery buffer and no second live index.

The corruption test covers damaged input values, a corrupt output
checksum, a changed output key or value with recomputed checksums, and an
unexpected byte added to the output. Each rejected restore removes the
private stage. The unchanged case publishes and reopens with the expected
values. The process-kill tests exercise the existing publication checks.

The 16-thread library suites passed 562 tests on macOS ARM64 with Rust
1.100.0 and 724 on Linux ARM64 ext4 with Rust 1.85.0. Each ran a fixed copy
of the test executable. Six retained integration tests passed separately on
each platform. Formatting and warnings-denied Clippy passed on macOS.
[macOS library](macos-library.log.gz), [Linux library](linux-library.log.gz),
[macOS integration](macos-integration.log.gz),
[Linux integration](linux-integration.log.gz), [source hashes](source.sha256).

## Linux ARM64 many-key sample

The fixture has 400,000 initial keys and 1,024 suffix operations, leaving
399,853 live keys. Each value is 18 bytes. The runs used the same warm
fixture on VM tmpfs and fresh processes. The timed scope includes replay,
writing the private output, verifying it and publishing it. The
baseline was `cb50db6` with the grouped writer and ordinary-store
verification reopen. Both versions emitted a V3 output and a separate
process checked every result against the independent model. Source,
candidate, suffix and binding file hashes matched before and after.

| Verification method | Median elapsed | Median peak RSS |
| --- | ---: | ---: |
| Ordinary-store reopen | 0.47 s | 95.38 MiB |
| Streaming witness and record verification | 0.26 s | 46.65 MiB |

These are three alternating pairs, with concurrent VM validation and no
physical storage latency in the fixture. Replay still keeps live keys
and authenticated value locations in memory. The sampled RSS does not
establish a total-memory bound or deployment-wide performance.

The [fixture patch](batched-fixture.patch) changes setup in the
[phase driver](../../../examples/retained_phase_bench.md). Apply it with
`git apply --unidiff-zero` in each scratch checkout. Build and copy
both executables with separate Cargo target directories, `--release
--locked --offline` and Rust 1.85.0. The first attempt reused the prior
executable and its timings were excluded after checking the hashes. Use
`KEYS=400000 SUFFIX=1024 SEED=42` for setup, then run build, mutate and export.
Time restore with `/usr/bin/time -f 'elapsed_seconds=%e max_rss_kib=%M'`,
verify the output outside the timed process and move it aside before each
subsequent restore. [Build, binary hashes and sample logs](linux-samples.tar.gz).
