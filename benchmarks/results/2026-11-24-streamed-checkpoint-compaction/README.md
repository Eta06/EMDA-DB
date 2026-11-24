# Streaming large values during checkpoint compaction

Source revision `d7a342e` shares the bounded group writer for ordinary
values and streams records over 4 MiB through a 1 MiB value buffer.
The staged record scan checks record integrity, hashes each value and
binds the complete log prefix for the new checkpoint. It compares the
ordered key, length and value digest stream with the source copy.
The checkpoint keeps the existing format.

The compaction regression preserves a maximum-sized 16 MiB value and
an empty suffix value, and checks that a deleted key stays absent.
A second regression changes staged data,
including a case with a matching recomputed record checksum. The live
source remains readable and the changed copy fails before publication.
The same writer supports separate copies and Linux checkpoint handoffs.

## Paired macOS measurements

The two release binaries used the same Rust 1.85.0 toolchain and locked
dependency versions. The baseline was `e987b4b`, and the candidate was
`d7a342e`. Runs used one ARM64 macOS host on APFS. Each workload used
six pairs of fresh processes with alternating order and the same source
files. Elapsed time is for the compaction call, including verification,
synchronization and directory publication. Peak RSS covers the process
from startup and includes opening the source checkpoint. Result verification
runs in another process. File cache state was shared across each series.

| Workload | Revision | Median compaction | Range | Median peak RSS |
| --- | --- | ---: | ---: | ---: |
| 4,096 values of 64 bytes | Baseline | 443.3 ms | 426.1–452.5 ms | 2.30 MiB |
| 4,096 values of 64 bytes | Candidate | 437.9 ms | 432.0–449.5 ms | 2.28 MiB |
| 126 small values plus 5 and 16 MiB values | Baseline | 113.5 ms | 107.5–143.8 ms | 34.14 MiB |
| 126 small values plus 5 and 16 MiB values | Candidate | 108.5 ms | 104.1–115.1 ms | 3.16 MiB |

The mixed fixture's median RSS fell by 90.8%. The latency ranges
overlap. All 24 output stores reopened through their independently
recorded checkpoint digest, and every value matched the fixture. Within
each workload, compacted record bytes matched across revisions after
excluding the log identity header. Source log, witness and checkpoint
hashes matched after each run. The first attempt stopped before timing
because its Python version lacked `hashlib.file_digest`. Its original
script and failure output are preserved alongside the successful runs.
These measurements cover the named workloads on one host.

## Correctness validation

Native macOS Rust 1.85.0 passed all 1,325 tests across 112 nonempty
binaries. Formatting, warnings-denied Clippy and warnings-denied
rustdoc passed. Hosted [macOS] and [Linux] passed on both toolchains.
The new regressions also passed in the optimized Linux suite.
[Filesystem fault] and [ENOSPC] workflows passed.

The [raw archive](raw-artifacts.tar.gz) includes the benchmark probe
and runner, 24 local timed attempts, all 45 standard Linux smoke
attempts, all five [cross-engine smoke] attempts, and the four-writer
recovery workload. That workload passed all 51 phases, including 24 at
a 48 MiB process address-space cap. The archive includes commands, raw
results, revision and binary hashes, and a SHA-256 manifest. Physical
power-loss testing and independent off-host restore remain deferred.

[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45378519521
[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45378519500
[Filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45378519592
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45378519626
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45378519440
