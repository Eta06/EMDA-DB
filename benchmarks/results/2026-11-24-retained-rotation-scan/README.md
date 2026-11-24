# Retained rotation scan admission

Revision `4cafe6d` adds `compact_with_scan_limit` on managed and
authority-backed retained stores. The per-call logical ceiling includes
the selected base blocks read for sizing, the complete base body streamed
during the merge, the source suffix scans and live suffix value reads, the
final source-base verification, and the completed successor base readback.
The ceiling uses the current persisted generation limits. Metadata, framing
and EOF probes outside those passes, physical retries and file writes have
separate costs.

The managed test uses ordered batch puts and a tombstone against a base
block. A zero cap and a cap one byte short reject before publishing a
successor, preserving the source files and selection. The test also
distinguishes rejection before reading a touched block from detection of
the damaged block after admission. An exact budget succeeds after
restoring the original block, and reopening returns the same live entries.
The authority-backed test uses two successive puts at one key. It checks
that both suffix records count in replay and confirmation while the
merged copy reads the latest value once. The short budget leaves the
authority unchanged, and an exact retry selects the validated successor.

The new budget applies to explicit rotations. Automatic maintenance
keeps its existing behavior. Physical power-loss testing and independent
off-host restore remain deferred.

Native macOS Rust 1.85.0 passed 1,332 tests across 112 nonempty binaries.
Formatting, warnings-denied Clippy, and rustdoc passed. Hosted [Linux]
passed on both toolchains, including the stable optimized build. The
[filesystem fault] and [ENOSPC] workflows passed. Hosted [macOS] passed
on both toolchains.

The [raw archive](raw-artifacts.tar.gz) includes the logs, all 45 standard
benchmark smoke attempts, all five [cross-engine smoke] attempts, and
all 51 phases of the four-writer recovery workload. The 24 phases run
under a 48 MiB address-space ceiling passed. All benchmark attempts
exited successfully. The rotation tests check logical admission. This
run did not measure the rotation's physical I/O.

[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45387848239
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45387848504
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45387848308
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45387848206
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45387848289
