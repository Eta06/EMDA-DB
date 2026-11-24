# Checkpoint recovery scan admission checks

The [raw archive](raw-artifacts.tar.gz) preserves the three successful
hosted cross-engine smoke attempts at `a9672c2`, plus the local tests
and API documentation build. All 12 file hashes verified after extraction.

On macOS ARM64 with Rust 1.85.0, 171 checkpoint tests passed, as did
warnings-denied Clippy and rustdoc. The recovery regression checks each
explicit and managed open with a witnessed suffix followed by a pending
atomic batch. A short budget rejects before replay, an admitted damaged
tail still fails closed, and exact admission permits adoption. The same
fixture then reopens at the lower acknowledged-log budget.

[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45349825327) ·
[Storage contract](../../../docs/STORAGE_TRANSACTION_CONTRACT.md)
