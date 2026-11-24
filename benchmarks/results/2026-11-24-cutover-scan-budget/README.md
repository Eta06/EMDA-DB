# Cutover recovery and archive retirement scan ceilings

Revision `825e79fc` bounds uncertain original-path cutover recovery and
archive retirement before reading verification payloads. Retirement
charges the original root witness, each archive file still present, and
both active and backup base and suffix payloads. A failed unlink can be
retried with a ceiling sized for the remaining archive files. The
verification and catalog synchronization still run if both archive names
are absent.

A macOS ARM64 run with Rust 1.85.0 started from 1,048,576 keys and used
four writers for one epoch. The harness exercised bound backup, retire,
restore preparation, and authority registration. All 17 phases passed.
The source and restored scans each checked all 1,045,598 final entries.
The last worker retired a 131,285,020-byte log and used a 424,886,372-byte
logical retirement ceiling. Its peak process RSS was 11,108,352 bytes.
Backup took 128.00 ms, archive retirement 170.65 ms, and restore through
authority registration 305.34 ms in this run. The macOS process ran
uncapped. The measured phases used fresh processes on one host with its
usual filesystem cache.

The [raw archive](macos-artifacts.tar.gz) records the tested source patch,
attempt, binary hash, phase logs, measurements, and verification scripts.
The [benchmark guide](../../../examples/authorized_recovery_bench.md)
describes the hosted Linux version with a 48 MiB process ceiling.

## Hosted Linux and native macOS validation

The [Linux memory job] at `c5740b46` passed all 53 phases with
1,048,576 starting keys, four writers and four epochs. The 25 capped
phases include the final lifecycle worker. Under the 48 MiB process
address-space ceiling and the 424,886,372-byte logical archive scan
ceiling, that worker retired the archive and verified all 1,045,597
live entries in both the source and restored catalogs. Its peak RSS was
13,512,704 bytes. The hosted final worker measured 203.08 ms for backup,
372.32 ms for archive retirement, and 571.20 ms for restore through
authority registration. These figures describe that fixture and runner.

Native macOS Rust 1.85.0 passed 1,341 tests across 112 nonempty binaries.
Hosted [macOS] passed both toolchains. The [Linux memory archive] contains
the exact job and phase logs, metrics, CI verifier, and local macOS test
logs. Fixture setup and cutover have separate process memory costs.
Filesystem cache and concurrent processes are outside the worker ceiling.

[Linux memory job]: https://github.com/Eta06/EMDA-DB/actions/runs/45398885440/job/138465314000
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45398885435
[Linux memory archive]: linux-memory-artifacts.tar.gz
