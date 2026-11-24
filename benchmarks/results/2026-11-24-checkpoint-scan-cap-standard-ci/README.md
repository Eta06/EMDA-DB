# Full platform validation after checkpoint scan admission

Rust 1.85.0 and stable passed on Linux and macOS at `a9672c2`. Stable Linux
also passed the optimized suite and all 45 standard benchmark smoke cases.
The expanded writable recovery gate completed all 24 capped worker phases.

The [raw archive](raw-artifacts.tar.gz) preserves all 46 Linux attempt
records, complete logs, phase measurements and both platform job reports.
All 170 file hashes verified after extraction.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45349825388) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45349825260) ·
[Linux recovery syscall evidence](../2026-11-24-checkpoint-recovery-scan-syscalls/README.md)
