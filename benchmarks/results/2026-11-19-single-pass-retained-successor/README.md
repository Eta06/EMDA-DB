# Single-pass retained successor verification

The rebase writer binds the synchronized successor bytes using the digest
it computed while writing. Successor initialization authenticates those bytes
against the binding in the same full pass that builds the sparse index.
Before the candidate leaves staging, rotation still rechecks the complete
source base and witnessed suffix.

The regression changes a staged value and recomputes the file digest. The
file passes standalone format validation. Initialization rejects it against
the writer's binding, and the source selection and live state remain intact.
After that rejection, a normal compaction publishes a successor that reopens
with the expected state.

## Linux ARM64 read trace

Release builds from baseline `19b0615` and the updated source opened and
rotated copies of the same managed catalog on VM tmpfs. The fixture has a
28 MiB base and a 4 MiB suffix. The staged successor was 25,166,042 bytes.

| Build | Staged successor bytes read | Source base bytes read | Source log bytes read |
| --- | ---: | ---: | ---: |
| Baseline | 50,332,084 | 100,663,944 | 16,778,658 |
| Single pass | 25,166,042 | 100,663,944 | 16,778,658 |

Both published successors reopened with all seven expected keys and values.
Their base files matched after removing the fresh base ID and dependent
digest. Original generation file hashes matched the input fixture after
each rotation. The trace measures logical bytes returned by read syscalls
during open and rotation on this fixture.
[Read totals](summary.json), [raw traces, build and verification logs](linux-rotation-traces.tar.gz).

To reproduce, build the same [rotation example](../2026-11-19-streamed-retained-rebase/rotate.rs)
with Rust 1.85.0 at both revisions. Create one fixture with `rotate setup`
and copy its managed catalog for each run. Trace `rotate rotate` with
`strace -y -s 0 -e trace=read,pread64,readv,preadv,preadv2`. Sum bytes for
the source `generation-a` base and log and the `base` inside the private
directory stage. Run `rotate verify` on both published catalogs.

## Regression validation

The full library suites ran at 16 test threads. The selected integration
targets cover retained indexed reads and documents, future-format guards
and atomic batches.

| Platform | Library tests | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 567 passed | 21 passed |
| Linux ARM64, Rust 1.85.0 | 729 passed | 21 passed |

Formatting and warnings-denied Clippy passed on macOS. [Source hashes](source.sha256),
[macOS library](macos-library.log.gz), [macOS integration](macos-integration.log.gz),
[Linux library](linux-library.log.gz), [Linux integration](linux-integration.log.gz).
