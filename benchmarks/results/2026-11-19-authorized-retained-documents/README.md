# Document transactions after authority-backed cutover

Source revision `95dbc46`. Source hashes matched across macOS ARM64 and the Linux ARM64 validation guest. See [environment.txt](environment.txt) for the toolchains.

| Platform | Library suite at 16 threads | Document integration |
| --- | ---: | ---: |
| macOS ARM64 | 530 passed | 176 passed across all 31 `document_*` targets |
| Linux ARM64 | 692 passed | 8 passed across `document_retained` and `document_maintenance_interleaving` |

Formatting and warnings-denied Clippy passed on macOS. The Linux runs used Rust 1.85 with locked dependencies and offline Cargo.

The cutover integration test imports a document collection and its unique index at the original path. It checks staged indexed reads, cancellation before intent submission, one durable authority intent for the successful transaction, and the reopened document, index and raw-key state after generation rotation and reclamation.

The authority fault matrix injects failures on both sides of intent and finalization CAS updates. Reopen recovers the document, index entry and raw-key receipt together or leaves all three absent. An independent authority revision change during a read-only callback causes the read to fail. The fault matrix exercises provider errors in process. Physical power-loss and independent off-host restore remain deferred.

The authorized catalog serializes document transactions. Rotation and reclamation remain explicit in this mode. The test results do not establish total process memory bounds or deployment-scale contention.
