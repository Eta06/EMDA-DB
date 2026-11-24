# Recovery after interrupted retirement and document commit

Revision `5d43a1f6` exercises two consecutive process interruptions
through the real retained write and retirement paths. Production
behavior is unchanged. An authority-backed catalog starts with one
document and two indexes. It rotates to a new generation, then the
test kills the retiring child at one of two namespace boundaries.
A second child opens the catalog without automatic reclamation and
submits an atomic document batch. The test kills that child after the
suffix sync and before the local witness advances.

| First kill point | Visible remaining retirement work | Exact startup scan budget |
| --- | --- | ---: |
| Owner marker published, before moving the inactive slot | Resume the marked stage and retire the inactive slot | 4,585 bytes |
| Inactive slot moved, before its staged directory sync | Resume the marked retirement only | 3,298 bytes |

For each case, a budget one byte short leaves every catalog path and
regular-file byte unchanged, with the independent authority still
pending. An admitted same-length base corruption with intact framing
also fails while preserving that state. After restoring the original
base, the exact budget resolves the pending commit, validates the
correct authority endpoint, and completes retirement. Both documents,
the unique and nonunique indexes, and the raw receipt recover together.
A subsequent acknowledged write survives a clean reopen.

These are process-kill and file-corruption checks on disposable
fixtures. They exercise OS-visible state and logical scan limits.
Physical power loss and independent off-host restore are deferred.

## Million-key Linux regression run

All 53 phases completed with four writers and 32,768 operations per
epoch across the four engines. The 25 capped phases reported both
48 MiB kernel limits. The retained lifecycle verified 1,045,597 live
entries in each catalog and peaked at 11,255,808 bytes of process RSS.

| Engine | Mutate | Reopen to first verified read | Full verification |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 105.59 | 785.08 | 859.88 |
| EMDA checkpoint | 1,410.04 | 187.43 | 907.02 |
| EMDA retained | 482.63 | 217.57 | 388.91 |
| SQLite WAL/FULL | 156.78 | 1.06 | 420.75 |

Values are median milliseconds across four epochs on one hosted Linux
runner. Reopen includes the engine's own integrity checks. Ordinary
and SQLite ran without the harness-imposed address-space limit. This
is a regression run for a test-only change under the runner's normal
cache and scheduling conditions.

## Verification evidence

The six selected retained crash tests passed on native macOS with
Rust 1.85. The native full library and integration run passed 1,356
tests across 112 nonempty binaries. That run started before the
test-hook type alias added for Clippy. The focused run and hosted
suites used the final hook declaration.

[Hosted macOS] passed both toolchains. [Hosted Linux] passed Rust 1.85
and stable, including the optimized suite. The [filesystem EIO],
[ENOSPC], and [cross-engine smoke] workflows passed.

The [raw archive] preserves 51 benchmark attempts, including 45
standard smoke cases, five cross-engine cases, and the Linux 53-phase
recovery workload. It includes native and hosted test logs, initial
test-harness and lint errors, verification scripts, and a SHA-256
manifest.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45420106234
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45420106145
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45420106109
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45420106256
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45420106067
[raw archive]: raw-artifacts.tar.gz
