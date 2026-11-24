# Linux file-backed authority replacement checks

At `5a5019e`, the successful compare-and-swap took two synchronization
calls after replacement writing began. The Linux trace records one
file sync, the rename, and one directory sync. The authority open
before the compare-and-swap performs its own synchronization.

Injected EIO during replacement write, file sync, or rename kept the
old record visible and removed the unpublished replacement. Injected
directory-sync failure left the replacement visible and returned EIO.
A fresh provider open validated and synchronized that record. Clean retry or reconciliation confirmed the
expected next revision in each case.

The [raw archive](raw-artifacts.tar.gz) retains every trace, the
rest of the returned-error suite, the disposable ENOSPC result, and
all three hosted cross-engine benchmark attempts. Linux returned-error
and process-kill tests exercise the running OS.

[Filesystem EIO](https://github.com/Eta06/EMDA-DB/actions/runs/45355035630) ·
[Disposable ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45355035808) ·
[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45355035632) ·
[Paired macOS results](../2026-11-24-file-authority-replacement-macos/README.md)
