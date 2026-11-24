# Automatic checkpoint and recovery resource validation

Automatic checkpoint rollover now admits its three logical log passes
before any slot cleanup or append. The new ceiling remains on the
consumed handle through a compaction handoff and resets on reopen.
Document snapshot aggregates share their wrapper's cumulative logical
read budget across three captured view types.

## Linux worker limits

Each capped benchmark process reads back both kernel address-space
limits. The Linux workload starts with 1,048,576 keys and applies 32,768
operations per engine and epoch with four writers. It covers four epochs,
verified reopen and complete ordered verification, and the retained
backup, archive retirement and authorized restore. Both kernel limits
must be 50,331,648 bytes in each of the 25 capped phases.

The first two attempts both passed the memory workload, including
complete verification of the 1,045,597 live source entries and the same
number of restored entries. Stable Clippy failed separately on fixture
enum sizes at those revisions. The evidence retains both lint failures,
the cancelled macOS jobs, all recorded benchmark attempts, and the
successful Linux 1.85, ENOSPC and EIO results where those ran.

The scope is the measured workers' virtual address space. Fixture setup,
initial cutover, SQLite and ordinary store measurements run uncapped.
Filesystem caches and aggregate resource use across processes are outside
this ceiling.

## Four-epoch Linux comparison

The following are medians across the four epochs on one hosted runner at
`c87d3946`. Times are milliseconds. SQLite used WAL with
`synchronous=FULL`. Reopen-to-ready includes a verified first read, and
each engine retains its own open-time integrity work. Complete verification
compares the ordered live state with the workload model.

| Engine | 32,768 operations | Reopen to first verified read | Verify all entries |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 111.09 | 771.73 | 849.84 |
| EMDA checkpoint | 1412.05 | 186.69 | 907.92 |
| EMDA retained | 484.90 | 223.44 | 390.95 |
| SQLite WAL/FULL | 155.93 | 1.04 | 422.20 |

Peak process RSS across the capped phases was 7,622,656 bytes for the
checkpoint workers and 13,950,976 bytes for the retained workers. The
final retained lifecycle checked all 1,045,597 live entries in both
catalogs. The three runs used shared hosted runners, synthetic data, and
the runners' usual caches. Their results cover these operations and
configurations.

## Verification evidence

On `c87d3946`, [hosted Linux] passed Rust 1.85 and stable, including the
optimized stable suite. [Hosted macOS] passed both toolchains. Native
macOS Rust 1.85 passed 1,344 tests in 112 nonempty binaries. Separate
[filesystem EIO], [ENOSPC], and [cross-engine smoke] workflows passed.

The [raw archive] retains 58 benchmark attempts across the three
revisions. It contains 45 standard smoke attempts, 10 cross-engine
attempts, and three million-key memory attempts with 53 phase results
each. Workflow logs, local test logs, verification scripts, and a
SHA-256 file manifest are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45403030512
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45403030568
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45403030415
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45403030528
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45403030400
[raw archive]: raw-artifacts.tar.gz
