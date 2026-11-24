# Retained authority recovery and local synchronization

Revision `d4656457` finalizes a pending authority intent using the
successful log and witness synchronization performed during the same
open when it adopts a complete tail. The adoption result is private
and returned only after both local syncs succeed. If the witness was
already visible at open, recovery still confirms the log and witness
before attempting the independent authority compare-and-swap.

## Direct comparison of synchronization work

The native macOS ARM64 probes used Rust 1.85 and isolated builds of
`fc84c9bd` and the changed recovery path. Each probe runs a filtered
engine test. Result markers surround the same public open call in
both versions. Test instrumentation counts completed file sync calls
along that open path and checks their order.

| Recovery branch | Prior log then witness cycles | New log then witness cycles |
| --- | ---: | ---: |
| Complete tail beyond the witness, adopted during open | 2 | 1 |
| Proposed endpoint already visible in the witness | 1 | 1 |

Both cases recovered the selected value and cleared the pending
authority intent. The counts cover the two pinned local files.
Directory and authority synchronization are separate. This
instrumented comparison measures calls and order without timing them.
The local Linux ARM64 VM could not compile the tracing build under
its available memory. The compiler error is preserved. Hosted Linux
runs exercise correctness on the final revision.

A separate instrumented build returns an error immediately before
each adoption sync in turn. Both cases leave the authority record
pending and preserve the appended log bytes. An error before log
sync leaves the old witness intact. An error before witness sync
can leave the newly written witness visible. The final production
tests separately check that recovery still refuses to finalize an
already-visible witness when confirmation fails. The injected
errors model returned I/O errors at those points.

## Comparative Linux recovery workload

The million-key regression ran all 53 phases. In the 25 capped
phases, the checkpoint and retained workloads used matching
48 MiB kernel soft and hard address-space limits. The final retained
lifecycle verified 1,045,597 live entries in each of the current and
restored catalogs and peaked at 13,684,736 bytes of process RSS.

| Engine | Mutate | Reopen to first verified read | Full verification |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 84.11 | 557.67 | 835.19 |
| EMDA checkpoint | 1,429.36 | 163.58 | 855.64 |
| EMDA retained | 453.67 | 192.23 | 329.19 |
| SQLite WAL/FULL | 130.96 | 0.98 | 336.40 |

Values are median milliseconds across four epochs on a hosted Linux
runner. Each mutation phase has four writers and 32,768 operations.
Ordinary and SQLite ran without the harness-imposed memory limit.
Each open performs its engine's own recovery checks. This workload
reopens after clean acknowledged writes and does not isolate the
pending-intent change measured by the native probes.

## Verification evidence

The native macOS Rust 1.85 library and integration suite passed
1,356 tests across 112 nonempty binaries. [Hosted macOS] passed both
toolchains. [Hosted Linux] passed Rust 1.85 and stable, including the
optimized suite. [Filesystem EIO], [ENOSPC], and [cross-engine smoke]
passed.

The [raw archive] preserves 51 benchmark attempts, comprising 45
standard smoke cases, five cross-engine cases, and the Linux recovery
workload. It also records the four instrumented sync-count probes,
two error probes, binary hashes, reproduction scripts, the incomplete
Linux trace build, and the validation logs.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45423396134
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45423396565
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45423396251
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45423396227
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45423396277
[raw archive]: raw-artifacts.tar.gz
