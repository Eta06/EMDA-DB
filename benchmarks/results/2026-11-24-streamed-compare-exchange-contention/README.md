# Writer contention during streamed compare-exchange

Revision `a9998110` releases the ordinary store's writer barrier during a
streamed value comparison. It checks store health and the current log
location under the barrier before applying the result. If another write
changed the location, it verifies and compares the new value before
deciding. A successful mutation holds the barrier through acknowledgement.

## Correctness evidence

The regression pauses the initial value read until a concurrent writer
finishes. It checks an unrelated write, a same-key write that invalidates
a match, and a same-key write that turns a mismatch into a match. It also
injects a returned error after a complete append on the concurrent writer.
The uncertain append disables the handle and the compare-exchange remains
unsubmitted. The fixture reopens and checks the final value and any
unrelated write. The previous revision, built with that same test,
timed out while waiting for the unrelated write during the paused read.

## Paired macOS benchmark

The native probe uses the ordinary store and compares against absence,
forcing each operation to verify the complete stored value while returning
a mismatch. An optional writer synchronizes 96 puts to a different key.
The process checks both values afterward. One comparison and one writer
thread start together for each mixed sample. Each case has six alternating
baseline and candidate pairs. The benchmark used Apple Silicon macOS and
Rust 1.85 release builds from `a4dbb61c` and `a9998110`.

| Value and workload | Compare/s before → after | Writer p95 ms before → after |
|---|---:|---:|
| 8 B, one reader | 3,867,173 → 3,878,283 | — |
| 257 B, one reader | 1,925,629 → 1,945,027 | — |
| 4 MiB, one reader | 4,341 → 4,340 | — |
| 4 MiB, one reader and writer | 1,569 → 1,506 | 7.962 → 8.027 |
| 4 MiB, four readers | 4,313 → 4,316 | — |
| 4 MiB, four readers and writer | 1,420 → 1,366 | 12.118 → 12.896 |

The follow-up uses the same 1,536 total comparisons across four readers
and tests both with and without the writer. Idle throughput was similar.
Mixed-workload comparison throughput fell by about 4%. The writer p95
rose by less than 1% with one reader and by about 6% with four readers.
The blocked-I/O regression verifies that a writer can progress during the
initial stream. These timing samples show no speedup under the tested loads.

Results describe this short local workload. The writer percentile is the
median of each sample's 95th-percentile synchronized write latency. The
archive contains raw per-write timings and the executed commands.

## Cross-platform verification

The native macOS Rust 1.85 suite passed 1,360 tests across 112 nonempty
binaries. [Hosted macOS] passed both toolchains. [Hosted Linux] passed
Rust 1.85, stable, and the stable optimized suite. The [filesystem EIO],
[ENOSPC], and [cross-engine smoke] runs passed.

All 72 local paired attempts and 51 hosted benchmark attempts succeeded.
The [raw archive] preserves the native and hosted logs, individual
benchmark attempts, verification scripts, and a SHA-256 manifest.
The hosted cases include five cross-engine comparisons and a million-key
recovery run with 53 phases, 25 under a 48 MiB address-space ceiling.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45437630092
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45437629972
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45437630009
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45437629980
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45437629994
[raw archive]: raw-artifacts.tar.gz
