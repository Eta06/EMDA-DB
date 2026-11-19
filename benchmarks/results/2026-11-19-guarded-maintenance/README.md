# Guarded document maintenance progress

Revision `861f732` adds opt-in guarded index builds and schema migrations.
Each bounded step verifies the job definition and cursor with an
independently retained random key before it can scan or publish. Its
HMAC-SHA256 tag commits atomically with the updated cursor and index or
schema work. The [API contract](../../../docs/DOCUMENT_MODEL.md#guarded-progress-for-index-builds-and-schema-migrations)
describes key custody and recovery. [RustCrypto HMAC 0.12.1](https://docs.rs/hmac/0.12.1/hmac/)

| Platform | Library suite at 16 threads | Document integration |
| --- | ---: | ---: |
| macOS ARM64 | 535 passed | 184 passed across all 33 targets |
| Linux ARM64 | 697 passed | 37 passed across six targeted suites |

Source hashes match on both platforms. Linux document fixtures used
ext4. Formatting and warnings-denied Clippy passed on macOS.

The regressions exercise both ordinary and checkpoint stores with typed,
well-formed cursor replacements persisted through the raw storage API.
They reject the replacement after reopen, keep the index or pending
schema unpublished and discard a staged transaction receipt. They cover
missing tags, definition changes, wrong keys, progress copied from a
cancelled job, job cancellation and rollback of a staged advance, and resuming
after deletion of the cursor document. The authorized retained test
resumes a guarded build across rotation and reopen.

The [previous-reader probe](previous-reader-probe.tar.gz) uses revision
`1a06249` on macOS. It opens the underlying store, accepts the legacy
collection metadata and rejects advancement of both guarded job types.
The file paths and content hashes match before and after its attempts.
The archived probe contains the driver sources, build logs and hashes.

The keyed APIs authenticate the job parameters and cursor. They
require a different independently kept key for every job. Replays of
valid states within one job and unrelated changes to the reserved
namespace remain outside the protection. Physical power-loss and
independent off-host restore remain deferred.
