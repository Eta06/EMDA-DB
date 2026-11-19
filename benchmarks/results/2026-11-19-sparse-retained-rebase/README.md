# Retained rotation sizing with sparse suffix changes

The sizing pass verifies the original frame and reads each base block
that could contain a suffix-touched key once. It computes encoded output
size from the authenticated original total, the lengths being replaced or
removed, and the final suffix values. Replaying every witnessed suffix
operation preserves historical peaks, including intermediate batch states.

The private merge still authenticates all base blocks and reads the retained
values. Rotation checks the synchronized successor and rechecks the whole
source before selection. The regression changes an untouched block, confirms
that sizing succeeds, and checks that rotation rejects the corruption before
publishing a successor. After repairing the byte, it verifies the exact
size, ordered contents and historical peaks after rotation and reopen.

## Linux ARM64 read trace

The [rotation workload](../2026-11-19-streamed-retained-rebase/rotate.rs)
opens and rotates an identical copy of a 28 MiB base with a 4 MiB suffix.
The sizing pass reads three candidate blocks for a delete, replacement and
insert. The other four blocks are authenticated during the merge and
the complete recheck.

| Release build | Source base bytes read | Source log bytes read |
| --- | ---: | ---: |
| Baseline `449080c` | 117,441,196 | 16,778,658 |
| Sparse sizing | 100,663,944 | 16,778,658 |

Both successors occupied 25,166,042 bytes, reopened with seven keys and
matched after removing the fresh base ID and its dependent digest. The
original generation file hashes matched the fixture after both rotations.

The trace counts bytes returned by successful read calls on the old
generation during open and rotation on VM tmpfs. These counts measure
logical reads for this fixture. Sizing can still read the full base when
touched keys cover all blocks. [Read totals](summary.json),
[raw traces, build and verification logs](linux-rotation-traces.tar.gz).

To reproduce, build the linked workload as a release example for the
baseline and the sparse build with `cargo +1.85.0 build --offline --locked
--release --example rotate`. Use the baseline binary's `setup` phase and
copy its managed catalog for each run. Trace the `rotate` phase with
`strace -y -s 0 -e trace=read,pread64,readv,preadv,preadv2`. Sum successful
return values for descriptors labeled `/generation-a/base>` or
`/generation-a/records.log>`. Then run each binary's `verify` phase and
compare pre-rotation and post-rotation source hashes.

## Regression validation

The complete library suites ran at 16 threads from matching sources.
The four selected integration targets exercise retained indexed reads,
document transactions, old-format guards, and atomic batches.

| Platform | Library tests | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 564 passed | 21 passed |
| Linux ARM64, Rust 1.85.0 | 726 passed | 21 passed |

Formatting and warnings-denied Clippy passed on macOS. [Source hashes](source.sha256),
[macOS library](macos-library.log.gz), [macOS integration](macos-integration.log.gz),
[Linux library](linux-library.log.gz), [Linux integration](linux-integration.log.gz).
