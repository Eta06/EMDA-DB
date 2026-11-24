# Platform checks for authorized retained reclamation

Source revision `21928f0` passed hosted stable and Rust 1.85.0 tests
on macOS and Linux. Stable Linux passed the optimized suite and all 45
standard benchmark smoke attempts.

The four-writer Linux recovery workload started with 262,144 keys and
applied 32,768 operations per epoch over four epochs. All 51 phases
passed, including 24 checkpoint and authorized retained phases under
the 48 MiB process address-space cap. Peak RSS across those capped
phases was 7.08 MiB for checkpoint and 7.69 MiB for authorized retained.
Setup and cutover ran outside the address-space cap.

The [raw archive](raw-artifacts.tar.gz) contains every standard attempt,
the memory-gate phases and the hosted job reports. The companion
[Linux fault archive](../2026-11-24-authorized-reclamation-linux-faults/README.md)
contains all three additional cross-engine attempts and the EIO and
ENOSPC results.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45358112035) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45358112012)
