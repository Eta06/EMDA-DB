# Bind bounded checkpoint rebuild to the bytes replayed

The regression replaces one complete unacknowledged record with a valid
same-length record after suffix validation. The old path adopts the tail,
writes an index for the old key, and publishes a checkpoint whose digest
covers the new key. On the fixed path the witness, base checkpoint and
selection retain their previous state. After restoring the original log
bytes, a clean retry adopts its complete tail and the resulting managed
store returns the expected key. A second test changes the source after
the full log digest has been bound, during checkpoint staging. Staged
verification rejects that change before publishing.
[Failing baseline](macos-before-fix.log.gz).

Bounded rebuild now hashes the exact suffix bytes consumed by the record
parser, including batch framing, tombstones and superseded records. It
compares that digest and the caller-trusted base prefix against the bytes
read just before adoption or checkpoint creation. The same reads form
the output checkpoint's log digest. Reopening the staged and published
checkpoint still verifies that digest. This keeps the existing witness,
stage synchronization, non-overwrite publication and selection protocol.
The store lock still assumes cooperating writers and stable external
paths. The checks do not provide atomicity with uncooperating filesystem
changes during the final validation, sync and publication operations.

## Linux controls and faults

The [probe fixture](../2026-11-19-bounded-spill-io-faults/README.md) exercises
acknowledged and complete unacknowledged V2 and V3 logs on Linux ARM64
tmpfs. The baseline is `640f52b`. Each build used Rust 1.85.0 and the
same probe source. All eight baseline and candidate controls published and
matched full-log replay for every key, value and checkpoint byte.
Each trace returned 1,804 logical log bytes before the change and 1,724
after it, saving one 80-byte covered-prefix read. This small fixture
checks pass count. It provides no estimate of elapsed time at scale.

Each candidate control had 17 successful pre-publication positional log
reads. The [fault runner](replay_io_campaign.py) injected one fault at
each position on a fresh fixture. It checked source bytes, the prior
selection on rejection, the witness and cleanup. Every clean retry and
every uninterrupted publication matched the full-log reference.

| Fault | Cases | Published on first attempt | Rejected before publication |
| --- | ---: | ---: | ---: |
| EIO | 68 | 0 | 68 |
| EINTR | 68 | 68 | 0 |

Sixteen EIO cases failed after adoption. The witness advanced to exactly
the fully validated tail in those cases. The outcome is consistent with
the existing distinction between adoption and checkpoint publication.
[Controls, fault records and binary hashes](linux-controls-and-faults.tar.gz).

## Cross-platform suites

Both library suites and nine integration targets ran at 16 threads.
The targets cover bounded rebuild above the recovered overlay ceiling,
a full-log differential model, other checkpoint creation and rollover
paths, historical views, backup and replay bounds.

| Platform | Library tests | Integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 577 passed | 65 passed |
| Linux ARM64 | 739 passed | 68 passed |

Formatting and warnings-denied all-target Clippy passed on macOS.
[Source manifest](source.sha256), [macOS library and rebuild](macos-tests.log.gz),
[macOS checkpoint and backup](macos-checkpoint-integration.log.gz),
[Linux library tests](linux-library.log.gz),
[Linux rebuild tests](linux-integration.log.gz),
[Linux checkpoint and backup](linux-checkpoint-integration.log.gz).

This work covers offline bounded rebuild. Current recovery still hashes
the covered log prefix. Physical power-loss testing and independent
off-host restore remain deferred.
