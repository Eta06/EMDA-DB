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
