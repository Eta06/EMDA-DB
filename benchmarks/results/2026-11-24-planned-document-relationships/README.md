# Planned source selection for document relationships

Revision `e3802c9d` adds opt-in planned source queries for lookups by
string ID and by a complete unique tuple. Both sides read the same
transaction or captured snapshot. The result identifies the access
path and counts actual source visits. If no index qualifies, the
planner uses a complete source scan. The existing scan methods
continue to use that path.

The tests cover a source query that fits a two-candidate limit when
indexed, projection rejection, exact and exceeded related-document
budgets with repeated references, an insert into a missing target
tuple, and an insert into the source index range. Both concurrent
conflicts roll back the transaction's staged receipt. Snapshot tests
check captured relations through target changes on ordinary and
checkpoint stores and exercise the managed and authority-backed
retained snapshot paths.

## Native macOS relationship comparison

A Rust 1.85 ARM64 release probe populated 1,024 source documents and 16
targets. The source filter matched 16 rows. One target was missing.
The probe compared the existing EMDA scan, the new planned source path,
and SQLite 3.49.1 with an indexed JSON left join, WAL and
`synchronous=FULL`. SQLite used expression indexes for both sides.
Every execution produced the expected ordered documents and decoded
related values.

| Path | Measured calls | Median (µs) | p95 (µs) |
| --- | ---: | ---: | ---: |
| EMDA source collection scan | 180 | 468.88 | 547.08 |
| EMDA planned source lookup | 180 | 25.79 | 28.62 |
| SQLite indexed JSON join | 180 | 9.50 | 16.00 |

The planned EMDA lookup examined 16 source candidates. The collection
scan examined all 1,024. Warmups preceded nine rounds with rotated
backend order. Timings include materialization and JSON decoding.
Fixture creation and result verification are outside the timer.
These single-client warm-cache samples measure the selective fixture.
Logical source visits are separate from target reads and physical I/O.

## Cross-platform verification

The native Rust 1.85 macOS library and integration suite passed 1,356
tests across 112 nonempty binaries. [Hosted macOS] passed Rust 1.85 and
stable. [Hosted Linux] passed both toolchains, the optimized suite, and
the 53-phase million-key recovery run. The [filesystem EIO], [ENOSPC],
and [cross-engine smoke] workflows passed.

The [raw archive] holds 52 benchmark attempts. It includes 45 standard
smoke cases, five cross-engine cases, the Linux recovery run, and the
native lookup comparison with all 540 timing samples. The native probe
source and binary hash, validation logs, the initial fixture error, the
verification scripts, and a SHA-256 manifest are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45426743240
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45426742933
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45426743185
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45426742909
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45426743037
[raw archive]: raw-artifacts.tar.gz
