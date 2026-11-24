# Automatic retained maintenance scan budget

Revision `0c82852` adds `enable_auto_reclaim_with_scan_limit` for managed
and authority-backed retained handles. Enabling the policy admits pending
retirement scans under the requested ceiling. Each subsequent mutation
receives a fresh ceiling for all rotation and retirement verification in
that call. The charges use the existing explicit scan budgets and cover
the successor readback and verification before retiring a previous slot.
Initial recovery, the requested append, generic stage cleanup, metadata
and physical I/O retries have separate costs.

The managed regressions check rotation with a one-byte-short combined
budget, exact admission and reset across repeated mutations, and an
earlier retirement consuming part of an otherwise sufficient rotation
budget. A rejected mutation leaves the verified state available. An
earlier cleanup or a selected successor can persist. The authority-backed
regression confirms the same one-byte-short case does not submit a
retirement fence or a pending append intent. Retrying with a fresh budget
finishes cleanup and commits the mutation. A clean reopen verifies the
result on both paths.

These tests exercise logical admission and publication ordering. They
do not measure physical reads or set a total CPU, memory or disk bound.
Physical power-loss testing and independent off-host restore remain
deferred.

Native macOS Rust 1.85.0 passed 1,335 tests across 112 nonempty binaries.
Formatting, warnings-denied Clippy and rustdoc passed. Hosted [macOS]
and [Linux] passed on both toolchains, including the stable optimized
Linux job. The [filesystem fault] and [ENOSPC] workflows passed.

The [raw archive](raw-artifacts.tar.gz) includes all 45 Linux benchmark
smoke attempts and all five [cross-engine smoke] attempts. It also
includes the 51 phases of the four-writer recovery workload. All phases
passed, including 24 under a 48 MiB address-space ceiling. The archive
includes the test logs, run revisions, and a checksum manifest.

[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45390622510
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45390622461
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45390622268
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45390622259
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45390622515
