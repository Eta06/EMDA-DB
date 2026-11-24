# Private compaction synchronization

`233d4571` stages ordinary and checkpoint-backed compacted copies and
offline retained restores in bounded write groups. The private log keeps
its initial witness through data copying and advances it once for the
completed output. Existing V2 and V3 sync order, complete output
verification, and no-clobber publication still apply. Larger records are
streamed through bounded buffers. `07bb7cb4` applies the same path to the
initial snapshot of online compaction and reopens the private store at the
copied length before applying catch-up writes. Catch-up still uses the
ordinary witnessed write path before final verification and handoff.

## Paired macOS APFS results

Fresh Rust 1.85 optimized builds used baseline `438a4e27` and the two
revisions above. Each workload used three alternating pairs on one ARM64
host. The first two rows time `Store::compact_to`. The other rows time
online compaction and optional retirement. The elapsed time excludes
fixture initialization and the benchmark's post-operation reopen checks.

| Workload | Keys | Baseline median | New median | Speedup |
| --- | ---: | ---: | ---: | ---: |
| Ordinary compaction | 16,384 | 1.451 s | 78.1 ms | 18.6× |
| Ordinary compaction | 65,536 | 5.297 s | 128.0 ms | 41.4× |
| Online compaction, retain old copy | 16,000 | 1.447 s | 90.3 ms | 16.0× |
| Online compaction and retirement | 16,000 | 1.550 s | 121.1 ms | 12.8× |

Ordinary compaction produced identical encoded log lengths at both sizes
and checked sampled values after reopen. The online runs checked all
14,000 live values and stage cleanup. The online snapshot uses ordinary
records, reducing the compacted log from 1,307,722 to 1,302,028 bytes in
these runs. These workloads exercised V2 on one host without competing
writers. `/usr/bin/time -lp` recorded CPU and peak RSS for the entire
process, including setup and reopen. The native verifier prints those
medians. Peak RSS remained close to baseline for these fixtures.

The benchmark wrapper records the working Git revision. The baseline
executable was built from a separate source export of the SHA recorded in
`baseline-revision.txt`. The binary hashes, command lines, exit statuses,
and original outputs of all 24 timed runs are in the [raw archive].

## SQLite maintenance reference

SQLite 3.54.0 ran the same 8-byte keys, 64-byte values, 512-operation
transactions, overwrites, and deletes with WAL and `synchronous=FULL`.
The timer covers source and output integrity checks, the complete sorted
row comparisons, `VACUUM INTO`, an explicit output file sync, no-clobber
publication by hard link, and parent-directory sync. The three runs at
each size checked every live value again after reopening the published
file. See SQLite's [VACUUM INTO] and [synchronous] documentation.

| Keys | SQLite median | Output database bytes |
| ---: | ---: | ---: |
| 16,384 | 13.0 ms | 1,163,264 |
| 65,536 | 47.9 ms | 4,620,288 |

SQLite rebuilds pages from the current B-tree and WAL state. EMDA-DB
also authenticates obsolete append-log records during source verification.
The raw archive preserves all six reference attempts, their Python
harness, resource readings, and exit statuses. The results describe the
operations in these local fixtures.

## Failure and compatibility coverage

The 72 native copy, handoff, and retained restore tests include an injected
short append in the second write group and an injected final sync failure
after all 130 records have been appended. The streamed-value test observes
the initial witness after a bad large value and verifies that the partial
stage cannot be finalized. The 80 online unit tests and 20 public API tests
cover exact snapshot read admission, concurrent commits, interrupted
handoff and retirement, and an unchanged source on rejected publication.
Lint and Rustdoc completed with warnings denied. The initial test build
failure is retained in the logs.

[Initial Linux CI], [initial filesystem EIO], [initial ENOSPC], and
[initial cross-engine smoke] passed. Stable passed in the [initial macOS run].
The follow-up push superseded the still-running Rust 1.85 macOS job, which
is retained as cancelled. The native Rust 1.85 copy suite had passed.

For `07bb7cb4`, [final Linux CI] passed Rust 1.85, stable, and the
million-key recovery memory-ceiling job. [Final hosted macOS] passed on
both toolchains, and the [final filesystem EIO], [final ENOSPC], and
[final cross-engine smoke] checks passed. These checks use the documented
trusted-directory and cooperating-writer assumptions. Physical power-loss
and independent off-host restore remain deferred.

[raw archive]: raw-artifacts.tar.gz
[Initial Linux CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45462231979
[initial filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45462232162
[initial ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45462231920
[initial cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45462231895
[initial macOS run]: https://github.com/Eta06/EMDA-DB/actions/runs/45462231994
[final Linux CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45463344017
[Final hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45463344013
[final filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45463344354
[final ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45463344325
[final cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45463344004

[VACUUM INTO]: https://sqlite.org/lang_vacuum.html
[synchronous]: https://sqlite.org/pragma.html#pragma_synchronous
