# Document transactions on managed retained catalogs

Tested revision `dcdbd20`. The checked Rust source, tests and Cargo inputs
matched on macOS and Linux ARM64. See the [SHA-256 manifest](source.sha256)
and [platform and compiler record](environment.txt).

| Validation | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite, 16 threads | 509 | 671 |
| Targeted document and retained-index integration tests | 16 | 16 |

The new scenario imports an ordinary indexed collection, then uses the document
interface to update the retained catalog. Indexed transaction reads check
that staged inserts and replacements are visible and staged deletions hide
earlier rows. The test triggers generation reclamation, takes a bound backup,
and verifies the resulting documents, index integrity and raw receipt after
restore. Control tests check deadline expiry before callback, cancellation
with staged changes, rejection of reentrant admission, and rollback after a
callback handles a unique-index error. A separate tested write rejects
admission after reclamation and verifies through reopen that the triggering
batch was never submitted.

The integration targets were `document_retained`, `document_transaction_model`,
`document_maintenance_interleaving`, and `retained_indexed_candidate`.
The existing model and maintenance interleavings cover the ordinary and
checkpoint-backed document paths. Linux used Rust 1.85.0, cached offline
dependencies and two build jobs. macOS formatting and all-target Clippy
with warnings denied passed.

Logs are available for [macOS library](macos-lib.log.gz),
[macOS integration](macos-integration.log.gz), [Clippy](macos-clippy.log.gz),
[Linux library](linux-lib.log.gz) and [Linux integration](linux-integration.log.gz).
Physical power-loss testing and independent off-host restore remain deferred.
