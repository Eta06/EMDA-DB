# Interrupted synchronization followed by a retry

An interruption can be reported even though a synchronization already took
effect. The [test-only interposer](../scripts/sync_interrupt_inject.c) first
issues a real `fsync` or `fdatasync` on one exact disposable file and, only
if that call succeeds, reports `EINTR` to the caller. It then either performs
the retry or returns synthetic `EIO` without issuing that second syscall. The
[driver](../scripts/sync_interrupt_campaign.py) requires both calls in the
trace and verifies the operation's result, handle behavior, reference backup
and recovery on both writable paths.

On clean revision `43d3bc66bb7046119599ac71a99e76b4f142669c`, separate
release builds using Rust 1.100.0 and Rust 1.85.0 each passed 48 cases on a
Linux workspace overlay. They covered V2 log and witness `fdatasync`, V3 log
`fsync`, ordinary and checkpoint-backed writes and atomic batches, and both
recovery orders. For each compiler:

- In 24 cases the retry succeeded, the operation returned success and both
  paths recovered the complete write or batch.
- In 24 cases the retry returned EIO. The operation returned an error and
  its existing handle rejected subsequent reads and writes. Both recovery
  paths then found the complete attempted write or batch, which remains an
  **unknown outcome** to the caller rather than an acknowledged operation.

The [stable](../benchmarks/results/2026-11-14-sync-interrupt-stable.json.gz)
and [minimum-version](../benchmarks/results/2026-11-14-sync-interrupt-msrv.json.gz)
reports contain the recorded calls, results, source and binary hashes and
fixture summaries. The builds used separate target directories. The
[independent verifier](../scripts/verify_sync_interrupt_results.py) checks the
recorded matrix and outcomes; the probe checked actual contents before the
disposable fixtures were removed. The hosted filesystem workflow repeats the
same check with its current stable toolchain.

The [hosted Linux run](https://github.com/Eta06/EMDA-DB/actions/runs/43528263333)
passed all 48 cases on ext4 for clean revision
`71200cb5095ca9a87edbb9ff38d9f4e037fa9f43`. The
[retained report](../benchmarks/results/2026-11-14-sync-interrupt-hosted.json.gz)
also passed the independent verifier.

Unexpected failures retain the synthetic fixture and report its path; the
hosted workflow archives retained fixtures, including the V3 witness xattr.
Successful cases remove their disposable fixtures.

This is a synthetic interruption and error on a running OS, not a delivered
signal, physical power-loss test, off-host restore or proof of device
persistence. It samples only the specified sync points and does not cover
every error sequence or concurrent schedule. The overlay's backing device
was not independently verified.
