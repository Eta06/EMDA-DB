# Writable checkpoint recovery header admission

On macOS ARM64 with Rust 1.85.0, the strengthened regression failed on
`170f8a9` because recovery inspected a damaged checkpoint trailer before
rejecting the log scan budget. The fixed open rejects from the header,
then verifies the checkpoint and rechecks the budget for an admitted
call. The test covers explicit and managed opens, checks witness and
selection preservation, then permits exact-budget tail adoption once
the verified checkpoint and log bytes are restored.

All 46 focused checkpoint tests passed, along with warnings-denied
Clippy and formatting. The [raw archive](raw-artifacts.tar.gz)
preserves the regression, tests and source diff. This checks admission
and error ordering. No timing comparison was performed.
