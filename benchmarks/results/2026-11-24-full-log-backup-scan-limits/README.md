# Full-log backup and restore scan ceilings

Revision `093cb234` adds independently composable output-size and
logical read ceilings. Ordinary and checkpoint-backed backups reserve
two passes over the source log. Restore reserves two passes over the
backup payload and one over the staged log. Limits apply before payload
reads or staging. The existing publication classifications apply.

An empty legacy backup has no payload to read. A restore charges the
generated 28-byte header for staged readback, and verifies that the
header matches exactly before installing its witness. The fault test
substitutes a different valid header after sync and confirms rejection
without publication. Other tests cover one-under and exact limits,
admitted record corruption, no-clobber publication, and unknown outcomes
after injected parent-directory synchronization failures.

## Linux recovery and comparisons

All 53 phases of the million-key workflow passed. Both kernel
address-space limits were 48 MiB in all 25 capped phases. The retained
lifecycle peaked at 11,284,480 bytes of process RSS and verified
1,045,597 live entries in each of the source and restored catalogs.

| Engine | Mutate | Reopen to first verified read | Full verification |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 113.26 | 781.22 | 860.10 |
| EMDA checkpoint | 1457.39 | 191.79 | 907.34 |
| EMDA retained | 512.38 | 229.22 | 388.47 |
| SQLite WAL/FULL | 159.77 | 1.12 | 431.74 |

Values are median milliseconds over four epochs, each with 32,768
operations and four writers, on one hosted Linux runner. Reopen
performs engine-specific integrity work. The ordinary and SQLite
controls run without the harness-imposed address-space ceiling. The
workload uses the runner's usual filesystem cache.

## Native full-log workload

A separate macOS ARM64 optimized run used 32 keys, each overwritten
after writing a 4 MiB value. The log contained 134,219,868 bytes. Each
operation rejected a budget one byte below its requirement and succeeded
at the exact ceiling. Both backup files were byte-identical. The restored
log matched the original and all 32 live entries reopened correctly.
The source log and witness hashes were unchanged.

| Operation | Logical scan ceiling (bytes) | Elapsed (ms) | Process peak RSS (bytes) |
| --- | ---: | ---: | ---: |
| Ordinary backup | 268,439,736 | 120.99 | 2,211,840 |
| Checkpoint-backed backup | 268,439,736 | 125.99 | 2,244,608 |
| Restore | 402,659,604 | 180.70 | 2,211,840 |

Each measurement is a single pass through the workload in a native
process. The elapsed column times the admitted operation. RSS covers the
whole process, including source open or restored-store verification.
The workload used small live values, large historical values, and the
machine's usual filesystem cache.

## Verification evidence

Native macOS Rust 1.85 passed 1,349 tests across 112 nonempty binaries.
[Hosted macOS] passed both toolchains. [Hosted Linux] passed Rust 1.85
and stable, including the optimized suite. The [filesystem EIO],
[ENOSPC] and [cross-engine smoke] workflows passed.

The [raw archive] preserves all 54 recorded attempts. It contains 45
standard smoke cases, five cross-engine cases, the 53-phase Linux
recovery run, and three native full-log runs. The archive also contains
the native probe source, its build log and hashes, workflow logs, the
verification scripts, and a SHA-256 manifest.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45411215808
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45411216031
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45411215998
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45411215759
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45411216072
[raw archive]: raw-artifacts.tar.gz
