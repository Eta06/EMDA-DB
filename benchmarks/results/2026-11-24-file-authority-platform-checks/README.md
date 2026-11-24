# Platform checks after file-backed authority replacement

The `5a5019e` source passed hosted stable and Rust 1.85.0 checks
on macOS and Linux. Stable Linux passed the optimized suite and
all 45 standard benchmark smoke cases.

The Linux four-writer recovery gate verified the complete model
after each restart. All 51 phases passed, including 24 checkpoint
and retained phases under the 48 MiB process address-space cap.
Peak process RSS for checkpoint and retained was 7.06 and 7.54 MiB,
respectively, across mutation, reopen and verification phases.

The [raw archive](raw-artifacts.tar.gz) preserves the 45 standard
attempts, the four-writer workload and hosted job reports. The
[Linux fault archive](../2026-11-24-file-authority-replacement-linux/README.md)
retains all three additional cross-engine attempts and fault reports.
The [paired macOS measurements](../2026-11-24-file-authority-replacement-macos/README.md)
used separate builds and comparable workloads on one host.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45355035654) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45355035617)
