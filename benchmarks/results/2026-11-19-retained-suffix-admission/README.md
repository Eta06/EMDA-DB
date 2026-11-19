# Retained suffix operation admission

File-backed replay checks a single put's remaining operation budget before
creating its per-value index digest. Recovery still reads the complete
record, checks its CRC, and checks the suffix digest at the committed
boundary. That ordering preserves integrity error precedence when replay
rejects a record.

The focused test uses a put spanning five recovery buffers and a digest
implementation that fails if called. It verifies that over-budget records
stay out of the index, a changed value still fails the checksum, and a
changed witnessed record fails the committed suffix digest. At the exact
operation limit, the admitted put produces the expected value offset,
length and SHA-256 digest. The writable recovery test rejects a large
unacknowledged put without changing the log or witness. It then reopens the
original committed prefix and reads the acknowledged value.

The byte limit continues to bound the record and suffix integrity checks.
The change removes the additional per-value hash for rejected puts.

The same focused test, applied to baseline `0962091`, fails because replay
initializes the value digest for the rejected put. [Baseline log](baseline-test.log).

The full library suite passed at 16 threads on macOS ARM64 (570 tests) and
in three consecutive runs on Linux ARM64 with Rust 1.85.0 (732 tests each).
All 21 selected integration tests passed on both platforms, covering
retained indexed reads, retained documents, future-format guards and
atomic batches. Formatting and warnings-denied Clippy passed on macOS.
[Source hashes](source.sha256), [macOS library](macos-library.log.gz),
[macOS integration](macos-integration.log.gz), [Linux library](linux-library.log.gz),
[Linux repeat 2](linux-library-2.log.gz), [Linux repeat 3](linux-library-3.log.gz),
[Linux integration](linux-integration.log.gz).
