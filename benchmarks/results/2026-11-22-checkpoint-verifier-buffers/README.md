# Positional checkpoint verification and key buffer reuse

The checkpoint verifier borrows the open file with a private positional
read cursor. Two fixed key buffers let it compare each key to its predecessor
without allocating keys solely for verification. It still verifies ordered
keys, value locations, file length and the trusted checkpoint digest.

The regression interrupts a verification to verify the same handle again,
then finishes the outer scan. Alternating one-byte and 1,024-byte keys
span multiple buffered reads. With the previous parser the inner scan
moved the shared descriptor to EOF and the outer scan returned
`UnexpectedEof`. The new parser verifies both scans and leaves the
descriptor's original seek position unchanged.

## Linux ARM64 allocation counts

Five alternating-order pairs at each key count opened a historical
checkpoint view in a fresh process on tmpfs. The probe counts allocation
and reallocation calls from the start of open through successful view
creation. The builds used Rust 1.85 and the nonoptimized profile. Both
check the row count and sample values, and the runner checks fixture file
hashes afterward. The baseline is `2446d9c`.

| Checkpoint entries | Baseline allocation calls | Current allocation calls |
| ---: | ---: | ---: |
| 4,096 | 4,176 | 80 |
| 65,536 | 66,550 | 1,014 |

Paired syscall traces confirmed the same index read-byte totals. The
baseline duplicated and sought the checkpoint descriptor once, and the
current verifier did neither. The open times in the raw logs include
allocation counting and the warm-cache log-prefix verification. The
allocation counts do not measure peak memory or establish a latency gain.

The [evidence archive](linux-evidence.tar.gz) contains the probe, runner,
binary hashes, baseline regression, traces and individual samples. To
reproduce, extract the probe to `examples/checkpoint_alloc_probe.rs`
in each revision and build with `CARGO_PROFILE_DEV_DEBUG=0
CARGO_INCREMENTAL=0 cargo +1.85.0 build --locked --example
checkpoint_alloc_probe`. Place the two binaries in a fresh Linux directory
as `baseline` and `candidate`, then run the extracted `measure.py
WORKSPACE NEW_OUTPUT_DIRECTORY`.

On macOS, 194 focused library tests and 23 integration tests passed.
Formatting and warnings-denied Clippy passed. On Linux ARM64, 766 library
tests, nine bounded rebuild integration tests and two descriptor-exhaustion
tests passed.
