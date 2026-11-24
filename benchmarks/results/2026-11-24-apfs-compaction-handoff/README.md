# Verified compaction and reclamation on macOS APFS

Revision `90e10dc3` enables blocking ordinary and checkpoint-store
compaction handoff, online ordinary-store handoff, and retired-store
reclamation on macOS APFS. The resolved parent must be APFS, and the
verified candidate must be a sibling. The engine uses atomic `RENAME_SWAP`
on macOS and `RENAME_EXCHANGE` on Linux. It verifies the two switched
lock identities and holds both locks through directory synchronization.
Retirement moves the old directory with an exclusive rename into a private
stage before unlinking its contents. [Apple's APFS system calls][apfs]
describe the native swap primitive.

## Correctness evidence

Native macOS Rust 1.85 passed all 100 focused tests with four threads and
again with 16 threads. The runs cover
non-APFS refusal, blocking and online exchanges, checkpoint selection and
resource limits, lock exclusion through an uncertain parent sync, surviving
snapshots and transactions, interrupted stage cleanup, and process kills
around handoff and retirement. Lint and Rustdoc passed with warnings denied.
The raw logs retain the initial Linux-gated fixture build error and the
first suite's canonical `/private/var` assertion failure. The final suite
uses canonical paths in the assertion and Linux-only V3 fixtures remain
on Linux.

The first hosted macOS suite also exposed integration tests still treating
macOS as unsupported. Revision `c7432a1a` enables the positive handoff
regressions on APFS. Its native run passed 73 tests across 11 public API
suites. [Final hosted macOS] passed 1,489 tests on each toolchain.
[Final Linux CI] passed Rust 1.85, stable, the stable optimized suite,
and the million-key recovery resource checks. Its results and the original [Linux CI], [macOS failure], [filesystem EIO], [ENOSPC], and
[cross-engine smoke] logs are included in the [raw archive]. These
checks use the documented trusted-directory and cooperating-writer
assumptions. Physical power-loss and independent off-host restore remain
deferred.

## Local APFS maintenance measurements

A fresh optimized Rust 1.85 binary ran three alternating pairs on macOS
ARM64 with V2 witnesses. Each trial began with 16,000 keys and ended with
14,000 live keys. The benchmark verified the expected live values, reopened
the store, and checked staging cleanup. Elapsed time covers compaction and
any immediate retirement, excluding fixture preparation and reopen checks.

| Mode | Min | Median | Max | Log bytes after |
| --- | ---: | ---: | ---: | ---: |
| Keep the previous store | 1.438 s | 1.492 s | 1.523 s | 3,599,102 |
| Reclaim the previous store | 1.504 s | 1.506 s | 1.549 s | 1,307,722 |

Six additional process measurements using `/usr/bin/time -lp` verified
the same workload. Median CPU time was 0.17 seconds in each mode, and
median peak RSS was 8,634,368 bytes with retention and 8,617,984 bytes
with reclamation. These process totals include fixture setup and reopen
verification. They use the same fresh binary at the test-only revision.

Each trial began with a 2,291,380-byte source log. The after column counts
encoded log lengths. The six verified trial logs and the six earlier
attempts that ran a stale shared-target executable are retained. Binary
hashes, commands, output, exit codes, and environment details are in the
raw archive. An intervening launcher invocation failed before starting
any benchmark because the output parent directory was missing. The
hosted competitor and memory-cap runs preserve their
separate attempts and results in the same archive.

[apfs]: https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/APFS_Guide/ToolsandAPIs/ToolsandAPIs.html
[Linux CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45457955134
[macOS failure]: https://github.com/Eta06/EMDA-DB/actions/runs/45457954848
[Final Linux CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45459248570
[Final hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45459248078
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45457954804
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45457955232
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45457954943
[raw archive]: raw-artifacts.tar.gz
