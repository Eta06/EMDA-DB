# Writable retained-generation validation

Engine revision `4ce2535` creates a separate writable retained generation
from an independently verified base and an optional detached suffix. Source
hashes matched on both machines.

| Platform | Library suite at 16 threads | Indexed and format integration tests |
| --- | ---: | ---: |
| Apple silicon macOS, Rust 1.100.0 | 482 passed | 9 passed |
| Linux ARM64 VM, Rust 1.85.0 | 644 passed | 9 passed |

The library coverage includes exact admission boundaries, batch work limits
across repeated keys and tombstones, complete-tail adoption, torn records,
torn witnesses, missing acknowledged bytes, equal-size substitution with a
valid record checksum, and swapped witnesses between two generations.
A failed commit poisons the handle. The tests kill a writer after log sync
and after witness sync, and verify recovery of the complete atomic batch
and a prior acknowledged value. Injected publication and reopen sync errors
check that an uncertain creation is inspectable and a rejected open leaves
the unacknowledged tail unwitnessed. Formatting and all-target Clippy with
warnings denied also passed on macOS.

The historical test builds `main` at `324a082` on both hosts. It rejects
the bundled real writable-generation fixture with a committed suffix on the
ordinary, checkpoint and recovered-view open paths. It also rejects a
checksum-valid backup constructed from the new log and verifies the original
generation files are unchanged. To repeat, extract `synthetic-fixture.tar.gz`,
copy `historical-guard.rs` into the historical source tree as
`tests/writable_retained_guard.rs`, and set
`EMDA_WRITABLE_RETAINED_FIXTURE` to the extracted `live` directory before
running `cargo test --locked --test writable_retained_guard`.
`build-fixture.rs` records how the synthetic data was created.

These are running-OS checks on the selected platforms and historical
revision. Physical power loss and independent off-host restore remain
deferred. Durable selection of an active generation and reclamation of
previous history are still open.
