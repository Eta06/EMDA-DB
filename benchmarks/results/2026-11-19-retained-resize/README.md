# Retained generation resizing and document recovery

Tested code revision `93961ee`. The tested Rust source, integration tests and
Cargo inputs matched the [SHA-256 manifest](source.sha256) on macOS and Linux
ARM64. Compiler and OS details are in [the environment record](environment.txt).

The resize tests fill the suffix operation budget while the live set exceeds
the base entry limit. They verify that compaction with the old limits leaves
the selector untouched, resizing retains the committed value, and a recovery
ceiling below the successor budget rejects reopen. A separate test injects a
selection-sync error and checks recovery under each of the two complete
selector records. A distinct-value header fixture checks all 11 limits,
including the platform-sized index and overlay counts.

| Validation | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite, 16 threads | 496 | 658 |
| Indexed-candidate and format-guard tests | 9 | 9 |
| Document serial histories across both backends | 256 | 1,024 |
| Document process-kill cases across both backends | 64 | 256 |

The model checks serializability and compares the recovered document, index
and raw-key state against the state before close. The kill test checks recovery
of three acknowledged transactions and atomicity of the fourth. macOS used its
normal temporary directory on the data volume and Linux used `/tmp` on tmpfs.
These are finite schedules and stops on a running OS. Physical power-loss
testing and independent off-host restore remain deferred.

Logs are available for [macOS library](macos-lib.log.gz),
[integration](macos-integration.log.gz), [Clippy](macos-clippy.log.gz),
[document model](macos-document-model.log.gz) and
[document kills](macos-document-kill.log.gz), and for
[Linux library](linux-lib.log.gz), [integration](linux-integration.log.gz),
[document model](linux-document-model.log.gz) and
[document kills](linux-document-kill.log.gz).

The library command was `cargo test --locked --lib -- --test-threads=16`.
The focused integration targets were `retained_indexed_candidate` and
`future_log_format_guard`. Document commands were
`cargo test --locked --test document_transaction_model -- --nocapture` with
`EMDA_DOCUMENT_MODEL_HISTORIES` set to 128 on Mac and 512 on Linux, and
`cargo test --locked --test document_kill_races -- --nocapture --test-threads=1`
with `EMDA_DOCUMENT_KILL_REPETITIONS` set to 8 and 32 respectively.
Linux used Rust 1.85.0, offline cached dependencies and two build jobs.
