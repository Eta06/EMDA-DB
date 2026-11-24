# Full platform checks after planned index streaming

The `dd61154` source passed hosted stable and Rust 1.85.0 checks
on macOS and Linux. Stable Linux also passed its optimized suite
and all 45 standard benchmark smoke cases. The local macOS minimum
toolchain run passed 1,314 tests across 111 nonempty test binaries.
Linux EIO fault validation passed.

The [raw archive](raw-artifacts.tar.gz) preserves every standard
benchmark attempt and its output, Linux filesystem fault reports,
the full local test log and hosted job metadata. The
[concurrent recovery archive](../2026-11-24-four-writer-recovery-linux/README.md)
preserves the memory-ceiling workload and three additional comparison
attempts from this source revision, plus its ENOSPC result.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45352395209) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45352395291) ·
[Filesystem EIO](https://github.com/Eta06/EMDA-DB/actions/runs/45352395127)
