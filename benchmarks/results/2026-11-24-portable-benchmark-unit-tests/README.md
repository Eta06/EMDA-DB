# Cross-platform benchmark unit tests

Revision `50f6b9b` makes mocked CPU affinity, host telemetry and cache advice
fixtures portable. The real V3 metadata identity fixture uses Linux user
xattrs. Linux ARM64 passed all 158 Python tests. macOS ARM64 passed 157
and skipped the xattr fixture. The actual benchmark drivers retain their
platform requirements.

[Hosted Linux CI] passed both Rust toolchains, the optimized stable suite
and the 48 MiB recovery workload. All 45 standard benchmark smoke attempts
and all five [cross-engine smoke] attempts exited successfully. The
[raw archive](raw-artifacts.tar.gz) includes every attempt, the native Python
logs, all 51 recovery phases, and the checksum manifest. The smoke cases
verify the configured workloads.

[Hosted Linux CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45391644654
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45391644520
