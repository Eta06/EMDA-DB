# Authority-backed document commit interruption

The regression runs a document transaction in a child process on an authorized
retained catalog with the file authority in a separate local directory. The
transaction replaces one document, inserts another, updates the unique rank
and nonunique tag indexes, and writes a raw receipt in one batch. The parent
kills the child at a named boundary before the transaction returns.

| Kill boundary | Recovery on the running OS |
| --- | --- |
| Before installing the authority intent | The prior authority and prior document state remain visible. |
| After the intent is synchronized, before append | Reopen clears the intent and preserves the prior document state. |
| After the complete append, before log sync | Reopen verifies and adopts the exact proposed suffix. |
| After log sync, before witness update | Reopen updates the witness and finalizes the proposed suffix. |
| After witness sync, before final authority CAS | Reopen finalizes the witnessed suffix. |
| After final authority CAS | Reopen accepts the already finalized suffix. |

A seventh case starts at the complete-append boundary and truncates
one byte from the log after the process exits. Reopen rejects the torn
record without modifying the log, witness or authority. Restoring the
exact proposed tail permits recovery. Every recovered case checks both
indexes, the receipt, and survival of a subsequent acknowledged write
across another reopen.

A complete pre-sync append can still be visible after a process kill
because the OS stays up. The simulated tear covers one incomplete
suffix and does not model all possible persistence orderings. Physical
power loss and independent off-host restore remain deferred.

Reproduce the focused check with:

```sh
cargo test --offline --locked --lib process_kills_recover_document_indexes_and_raw_receipt_together
```

The full library suite passed on macOS ARM64 (566 tests) and Linux ARM64
with Rust 1.85.0 (728 tests in each of three consecutive runs). The
library runs used 16 test threads. The three retained document integration
tests passed on each platform. Formatting and warnings-denied Clippy passed
on macOS. [Source hashes](source.sha256), [macOS library results](macos-library.log.gz),
[macOS document results](macos-document-retained.log.gz),
[Linux results](linux-results.tar.gz).
