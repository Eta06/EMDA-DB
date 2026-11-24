# Borrowed retained document snapshots

Revision `cc51f2e4` exposes a read-only document view over a managed
retained selection or an authority-checked retained selection. The
borrow keeps the selected generation and suffix overlay in place and
excludes writes and maintenance on that handle. Authorized capture
checks the independent authority. Subsequent reads use that captured
state until the borrow ends.

The integration tests cover base and suffix values, selection
after rotation, indexed aggregation, ordered pagination, cumulative read
admission, managed-selector corruption, authority advancement between
captures, and reopen. A unique index page tests five logical read units
at exact admission and rejection at four. The fifth unit checks its
unique-owner entry.

## Linux recovery and comparison

At 1,048,576 starting keys, four writers and 32,768 operations per epoch,
all 53 phases passed. Both soft and hard kernel limits were 48 MiB
in each of the 25 capped phases. The retained lifecycle peaked at
11,014,144 bytes of process RSS. Full ordered verification covered
1,045,597 live entries in each of the source and restored catalogs.

Median elapsed milliseconds across the four epochs on this runner:

| Engine | Mutation | Reopen to first verified read | Full verification |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 103.63 | 775.91 | 873.59 |
| EMDA checkpoint | 1412.80 | 187.29 | 918.42 |
| EMDA retained | 458.15 | 218.56 | 385.91 |
| SQLite WAL/FULL | 145.90 | 1.03 | 421.83 |

The fixture uses synthetic data and the runner's usual filesystem cache.
SQLite and ordinary-store controls run without the harness address-space
ceiling. Opening performs engine-specific integrity work. These
measurements exercise the recovery workflow alongside the document
change.

## Verification evidence

Native macOS Rust 1.85 passed 1,346 tests across 112 nonempty binaries.
[Hosted macOS] passed both toolchains. [Hosted Linux] passed Rust 1.85
and stable, including the optimized stable suite. The [filesystem EIO],
[ENOSPC], and [cross-engine smoke] workflows passed.

The [raw archive] holds all 51 recorded benchmark attempts, including
45 standard smoke cases, five cross-engine cases, and the 53-phase
Linux recovery run. It includes machine-readable metrics, workflow and
native logs, verification scripts, and a SHA-256 manifest.

[Hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45406191014
[Hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45406190982
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45406190961
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45406191065
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45406190935
[raw archive]: raw-artifacts.tar.gz
