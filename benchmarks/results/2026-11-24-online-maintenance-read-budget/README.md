# Online maintenance source-log read admission

Source revision `9506f52ab6af0d72aee5ce4c1f9dcfd0361509e2` adds
`OnlineStore::set_maintenance_source_read_limit`. The inclusive optional
budget counts source-log replay and digest confirmation plus source values
read for copy, replay and comparison. Scoped retired cleanup charges its
current-log verification before starting removal. Identity and witness
checks, target I/O and physical retries are outside this logical budget.

The Linux compaction regression appends during catch-up and immediately
before the final barrier. The exact read budget admits the exchange and
reclamation. One byte less returns `NotPublished(InvalidInput)`. The
captured snapshot still reads its original value in both cases, and
reopening the source recovers the last acknowledged write. The scoped
cleanup regression preserves the retired log at one byte below the cost
of verification and permits removal at the exact boundary.

Native macOS Rust 1.85.0 passed 1,324 tests across 112 nonempty binaries.
Formatting, warnings-denied Clippy and warnings-denied rustdoc passed.
Both hosted [macOS] and [Linux] suites passed on stable and Rust 1.85.0.
The new Linux regressions passed on both toolchains and in the optimized
stable suite. The [filesystem fault] and [ENOSPC] jobs also passed.

The [raw archive](raw-artifacts.tar.gz) preserves all 45 standard smoke
attempts, all five [cross-engine smoke] attempts, and the four-writer
checkpoint and retained recovery workload. All 51 phases of that last
workload passed, including 24 capped at 48 MiB of process address space.
The archive includes command output, artifact manifests, job results
and checksums. The throughput effect of configuring the new cap remains
unmeasured.

Physical power-loss testing and independent off-host restore remain deferred.

[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45372841380
[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45372841309
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45372841347
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45372841466
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45372841286
