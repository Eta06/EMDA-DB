# Format upgrade read admission and staged verification

Revision `276d98d0` checks the generated log header and hashes exactly
the staged record bytes consumed by the parser. It performs one staged
record pass. The prior path hashed and parsed the staged records in
separate passes. Witness installation and non-overwriting publication
still follow verification.

`FormatUpgradeLimits` admits both the output length and logical read
cost before reading source records or creating a stage. The scan
charges copied source records plus the staged log including its header.
The tests cover exact and one-under limits for legacy and witnessed
logs, same-length replacement with valid staged bytes, and an uncertain
directory sync after publication. An instrumented reader checks that a
multi-buffer staged payload is read once. Header probes have separate
costs.

## Linux recovery and comparisons

All 53 phases passed. The kernel soft and hard limits matched 48 MiB
in all 25 capped phases. The final retained lifecycle peaked at
11,251,712 bytes of process RSS and verified 1,045,597 live entries
in each of the source and restored catalogs.

| Engine | Mutate | Reopen to first verified read | Full verification |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 53.28 | 409.20 | 360.52 |
| EMDA checkpoint | 1052.00 | 157.58 | 374.68 |
| EMDA retained | 218.84 | 185.26 | 268.77 |
| SQLite WAL/FULL | 69.15 | 0.76 | 281.44 |

Values are median milliseconds over four epochs, each with 32,768
operations and four writers, on one hosted Linux runner. Reopen
performs engine-specific integrity work. The ordinary and SQLite
controls run without the harness-imposed address-space ceiling.
The workflow uses the runner's usual filesystem cache.

## Paired native upgrade measurement

A macOS ARM64 Rust 1.85 optimized probe upgraded the same
134,219,868-byte witnessed log using `fda7ac56` and `276d98d0`.
The fixture overwrote 32 historical 4 MiB values with small live values.
The six pairs alternated execution order, and timers covered the
upgrade call after opening the source.

| Staged verification | Median elapsed (ms) | Range (ms) | Median process peak RSS (bytes) |
| --- | ---: | ---: | ---: |
| Separate digest and parser passes | 133.680 | 130.776–141.478 | 2,113,536 |
| One staged record pass | 128.072 | 125.488–150.170 | 2,129,920 |

The median fell 4.19% on this warm-cache fixture. Four pairs were
quicker and two were slower. All 12 outputs had the source record
digest and reopened with the expected 32 live values. Source log and
witness hashes held throughout. RSS covers each entire worker, including
both opens and post-upgrade verification. The archive records an
initial reuse of an old release artifact. The new binary was rebuilt
in an isolated target directory before timing.

## Verification evidence

Native macOS Rust 1.85 passed 1,352 tests across 112 nonempty binaries.
[Hosted macOS] passed both toolchains. [Hosted Linux] passed Rust 1.85
and stable, including the optimized suite. The [filesystem EIO],
[ENOSPC] and [cross-engine smoke] workflows passed.

The [raw archive] preserves all 63 benchmark attempts. It contains 45
standard smoke cases, five cross-engine cases, the 53-phase Linux
recovery run, and 12 native upgrade runs. The probe sources, build logs,
binary hashes, workflow logs, verification scripts, and a SHA-256
manifest are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45414383473
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45414383096
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45414383195
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45414383267
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45414383634
[raw archive]: raw-artifacts.tar.gz
