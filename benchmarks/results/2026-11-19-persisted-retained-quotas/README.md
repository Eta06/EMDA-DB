# Persistent retained catalog quotas

Engine revision `f2f8586099ab97d9514e8bea0ce6870e2b6d2fc1` adds opt-in
persistent logical file-byte and directory-entry limits to managed and
authority-backed retained catalogs. A successful rotation selects the new
header and policy together. Reopen enforces that policy before suffix replay
or adoption. Staging must fit both the current and the proposed limits.

- The managed regression injects a failure syncing the new selection and
  checks the reopened policy. A plain rotation preserves that policy. A
  larger per-open limit cannot allow an oversize physical tail to advance
  the witness. Insufficient rotation headroom leaves the old slot selected.
- The original-path regression checks that the external authority binds the
  quota-bearing header. After reopening, a regular file outside the selected
  catalog subdirectory still consumes budget. A rejected append leaves the
  authority unchanged.
- On macOS, a separately built reader from revision `9db2d34` returned
  `InvalidData` for both catalog types while both slots still existed. The
  current reader returned the expected value before and after the old-reader
  probe. File names and SHA-256 hashes matched before and after the probe.
  This exercises the two historical open entry points on one revision.
  [Probe source](compatibility-probe.rs), [raw output](compatibility.log).

The ARM64 hosts used the same 313 source and test inputs, checked by SHA-256.
Run the library suite with `cargo test --locked --lib -- --test-threads=16`.
The selected integration targets were `retained_indexed_candidate`,
`future_log_format_guard` and `document_retained`. The Linux VM used Rust
1.85.0, `--offline -j 2`, and disabled incremental compilation and test
debug info. See [the source manifest](source.sha256) and
[host details](environment.log).

| Platform | Library at 16 threads | Selected integration |
| --- | ---: | ---: |
| macOS ARM64 | 549 passed | 12 passed |
| Linux ARM64 | 711 passed | 12 passed |

Formatting and warnings-denied Clippy passed on macOS. The compressed
logs retain the per-test results.

The policy counts hard links per entry and includes retained archives and
stages within its root. Initial publication and independent restores have
separate admission policies. Actual filesystem allocation, other roots,
external authority and backup storage, and total memory remain outside this
ceiling. Physical power loss and independent off-host restore remain deferred.
