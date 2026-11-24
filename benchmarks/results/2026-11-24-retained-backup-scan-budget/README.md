# Retained backup, restore and registration scan ceilings

Revision `dc380065` adds optional ceilings for retained backup and restore.
The methods charge the full base and suffix once for copying and once for
staged verification or replay. Managed restore and receipt-backed
authorized preparation also accept an independent persistent catalog
quota. Prepared authority registration bounds the normal recovery
verification, including a second suffix pass if the local witness
requires confirmation.

The managed regression checks refusal one byte short, rejects corrupted
payloads under an admitted budget, checks overflow and an exact budget,
and verifies that restored data and the persisted storage quota match.
The authorized regression checks receipt ordering and registration under
a rolled-back local witness. One byte short preserves the witness and
the empty authority. An exact retry verifies the suffix, adopts the
witness, and registers the independently bound endpoint.

The ceilings account for logical payload reads. Physical I/O retries,
metadata and framing probes, and writes have separate costs. Physical
power-loss testing and independent off-host restore remain deferred.

Native macOS Rust 1.85.0 passed 1,340 tests across 112 nonempty binaries.
Formatting, warnings-denied Clippy and rustdoc passed. Hosted [macOS]
and [Linux] passed both toolchains, including stable optimized Linux.
The [filesystem fault] and [ENOSPC] workflows passed. The
[raw archive](raw-artifacts.tar.gz) includes all 45 standard and five
[cross-engine smoke] attempts, the million-key recovery workload,
and the complete checksummed logs.

[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45395863571
[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45395863483
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45395863477
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45395863586
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45395863471
