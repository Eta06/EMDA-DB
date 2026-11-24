# Explicit checkpoint scan admission

Revision `7f6e9c6a` adds per-call limits for independent checkpoint
copies on both writable stores and both types of manual rollover on
`CheckpointStore`. Creation charges two full logical source-log passes.
Rollover charges a third pass to open the new view. Failure to reserve
the full amount returns the corresponding `NotPublished(InvalidInput)`
before replay or staging. The caller still retains the returned trusted
digest separately.

Tests cover exact and one-byte-short ceilings, snapshot and selection
preservation, verified reopen, an obsolete record changed after replay,
and a failed parent sync after an admitted publication. The same checked
arithmetic now serves automatic rollover. The ceiling covers source-log
payload passes. Metadata and framing probes, checkpoint-file reads,
physical retries, and writes have separate costs.

## Linux recovery and comparisons

All 53 phases of the million-key workflow passed. Both kernel
address-space limits were 48 MiB in all 25 capped phases. The final
retained lifecycle peaked at 11,403,264 bytes of process RSS and
verified all 1,045,597 live entries in each of the original and restored
catalogs.

| Engine | Mutate | Reopen to first verified read | Full verification |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 116.33 | 782.20 | 864.62 |
| EMDA checkpoint | 1411.57 | 187.76 | 929.28 |
| EMDA retained | 499.59 | 216.29 | 388.55 |
| SQLite WAL/FULL | 158.75 | 1.05 | 432.45 |

Values are median milliseconds over four epochs, each with 32,768
operations and four writers, on one hosted Linux runner. Reopen
performs engine-specific integrity work. The ordinary and SQLite
controls run without the harness-imposed address-space ceiling. The
workload uses the runner's usual filesystem cache.

## Verification evidence

Native macOS Rust 1.85 passed 1,348 tests across 112 nonempty binaries.
[Hosted macOS] passed both toolchains. [Hosted Linux] passed Rust 1.85
and stable, including the optimized suite and benchmark smoke cases.
The [filesystem EIO], [ENOSPC], and [cross-engine smoke] workflows passed.

The [raw archive] preserves all 51 benchmark attempts, including 45
standard smoke cases, five cross-engine cases, and one 53-phase
million-key recovery run. It also contains the native and hosted test
logs, workload verification scripts, and a SHA-256 manifest.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45408602349
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45408602327
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45408602344
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45408602444
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45408602359
[raw archive]: raw-artifacts.tar.gz
