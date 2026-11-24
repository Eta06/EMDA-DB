# Platform checks after checkpoint header admission

The `8fed2d4` source passed hosted stable and Rust 1.85.0 checks
on macOS and Linux. Stable Linux passed its optimized suite and all
45 standard benchmark smoke cases. The four-writer recovery job passed
all 51 phases, including 24 with a 48 MiB process address-space cap.

The [raw archive](raw-artifacts.tar.gz) preserves every standard
benchmark attempt, the complete writable recovery workload, hosted
job reports, and the local macOS log for 231 focused checkpoint unit
tests. The [separate Linux fault archive](../2026-11-24-checkpoint-header-admission-linux/README.md)
preserves EIO, disposable ENOSPC and cross-engine comparison results.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45353497385) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45353497402)
