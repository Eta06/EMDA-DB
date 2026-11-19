# File-backed detached suffix inspection

`IndexedSuffix::open_file_backed` uses the writable store's fixed-buffer
replay to verify a detached suffix and build its bounded overlay. The
view retains one extra file descriptor relative to the existing
buffered mode. It checks the suffix length and authenticates each
returned suffix value with the digest captured during replay. The
opened files must remain immutable while the view is live.

## Correctness

The tests use the same source export and ordinary-store state for both
view modes. They cover exact byte, operation, work and overlay limits,
empty input and binding mismatch, ordered batch edits, tombstones,
page byte limits, pinned-file reads after path replacement, and changed
suffix values and lengths.

On macOS ARM64 with Rust 1.100.0, all 559 library tests and eight
selected integration tests passed at 16 threads. Formatting and
warnings-denied Clippy also passed. On Linux ARM64 with Rust 1.85.0,
all eight selected integration tests passed. The VM killed the
separate library test compilation with SIGKILL. These Linux results
cover `retained_indexed_candidate` and `document_retained`.
The retained logs include both Linux attempts. The source manifest
matched across platforms.

## Peak process RSS

The fixture contains a small retained base and a 134,217,933-byte
suffix with eight 16 MiB values and one short value. Independent
verification runs retrieved every value through each mode and checked
all bytes. The timed processes each open the view and read only the
small base and suffix values. The timer in the driver measures the
open. macOS `/usr/bin/time -l` records peak RSS for the entire process.

| Mode | Median open | Median peak RSS | Samples |
| --- | ---: | ---: | ---: |
| Buffered | 431.76 ms | 130.14 MiB | 5 |
| File-backed | 424.19 ms | 2.16 MiB | 5 |

The sample used five alternating pairs of fresh processes on one warm
Apple M6 host. The file-backed view reduced sampled peak RSS by
127.98 MiB. This measures one process with a nine-key suffix.
Filesystem cache, large returned values, larger indexes and overlays,
and simultaneous handles can add memory costs. The run measures
open and validates two small reads. It does not measure large-value
read latency or establish a system-wide memory bound. Fixture hashes
matched before and after sampling.

## Reproduction

Check out `567c205` in a scratch directory and copy `suffix_open.rs`
into `examples`. Build with
`cargo build --release --locked --example suffix_open`.
Invoke the resulting binary with `setup /path/to/new-fixture`. Run
`verify-buffered` and `verify-file` with that fixture path to check the
data. Then use `/usr/bin/time -l` to run the binary with `buffered`
and `file` in alternating fresh processes. Record the fixture hashes
around the measurement and preserve the raw samples.
