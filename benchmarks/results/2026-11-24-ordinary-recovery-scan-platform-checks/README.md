# Platform checks for ordinary recovery scan admission

Source revision `42a9fec` passed hosted stable and Rust 1.85.0 suites on
macOS and Linux. Stable Linux also passed the optimized suite and all
45 standard benchmark smoke attempts. The native macOS Rust 1.85.0
suite passed 1,322 tests across 112 nonempty test binaries. The native
run used `b76f6b0`, which differs in source only by one Linux-only
test fixture call.

The corrected Linux four-writer memory gate passed all 51 phases with
full model verification, including 24 checkpoint and authorized
retained phases under a 48 MiB process address-space cap. Peak RSS for
those phases was 7.11 MiB for checkpoint and 7.50 MiB for authorized
retained. Setup and cutover ran outside the process cap.

The initial hosted Linux run at `b76f6b0` failed to compile one
V3-only test fixture. Its memory gate had already passed. The initial
macOS run was superseded by the corrected revision. The
[raw archive](raw-artifacts.tar.gz) preserves these outcomes, both
memory workloads, all 45 standard attempts, and the native logs.
The [Linux fault and comparison archive](../2026-11-24-ordinary-recovery-scan-linux/README.md)
contains both EIO and ENOSPC reports and all six cross-engine attempts.

[Corrected Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45360181134) ·
[Corrected macOS checks](https://github.com/Eta06/EMDA-DB/actions/runs/45360180644) ·
[Initial Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45359818421)
