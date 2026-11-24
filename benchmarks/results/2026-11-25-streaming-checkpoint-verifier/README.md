# Streaming final comparison for online checkpoint compaction

Revision `5c4041c3` replaces the online checkpoint handoff's final
source/replacement comparison. The previous code compared paired scan pages
of up to 32 MiB each. The new code walks both ordered live sets in windows
of at most 1,024 keys and 256 KiB of key bytes. It streams each value pair
through two 64 KiB buffers and verifies both checksums to the end of every
value. The final writer barrier and the source-log confirmation are
unchanged. This bounds that comparison's scratch memory. It is not a bound
on elapsed or pause time, which still grows with the store, and it says
nothing about total process or project RAM.

## Correctness evidence

Three tests in `src/checkpoint_store/online/tests/verify.rs` cover:

- Identical live sets split between checkpoint and suffix overlay, across
  several count windows (2,600 short keys) and byte windows (600 keys of
  1,000 bytes), in both directions.
- Key, count and value differences, including a key missing in a later
  window, a missing last key, an extra key inside a window and after the
  end, a same-length value change, a longer value and empty stores, each
  reported as a mismatch in both directions.
- 300 KiB values: a difference in only the last chunk is a mismatch. A
  difference in the first chunk followed by corruption 200 KiB later in the
  same value is reported as a checksum failure, with the corruption on
  either side.

All 23 online checkpoint tests passed locally, and the full native macOS
suite passed 1,506 tests (`full-tests.log`). Warnings-denied Clippy passed.
On `5c4041c3`, [CI] passed Linux Rust 1.85, stable, optimized stable and
the memory-ceiling job. [macOS] passed both toolchains, and the [EIO],
[ENOSPC] and [cross-engine smoke] workflows passed. The three verifier
tests passed in all five test jobs.

## Peak RSS A/B

`examples/online_checkpoint_verify_rss.rs` stores 64 values of 1 MiB
twice, so one overwritten copy of each remains in the log. It checkpoints,
opens the store through `OnlineCheckpointStore` and checks every value.
`setup` stops there. `compact` also runs `compact_and_reclaim` and checks
every value after reopening. Values are regenerated rather than modelled,
so `compact` minus `setup` isolates the handoff's added peak. That includes
its copy, catch-up and final comparison.

The baseline binary was built from `196a5130` with only this example added
(`baseline-revision.txt`, `baseline-tracked-changes.txt`). The candidate was
built from the working tree that became `5c4041c3`. Its engine diff matches
the committed one exactly (`candidate-source.diff`). Both binaries are
hashed in `binary.sha256`. Three alternating rounds ran on an Apple M6 Mac
mini with macOS APFS and Rust 1.100.0. Each mode ran in a fresh process
under `/usr/bin/time -lp`.

| Binary | Setup peak RSS (median) | Compact peak RSS (median) | Compact − setup (median, range) |
| --- | ---: | ---: | ---: |
| Baseline `196a5130` | 4,751,360 | 70,090,752 | 65,355,776 (65,323,008–65,388,544) |
| Streaming `5c4041c3` | 4,702,208 | 5,931,008 | 1,228,800 (1,212,416–1,245,184) |

The handoff's added peak fell from about 65.4 MB to about 1.2 MB in this
workload. That matches two 32 MiB scan pages being replaced by bounded
windows and two 64 KiB buffers. These are single-host whole-process
readings, not a bound on other allocations.

## Remaining larger allocations

From the code constants:

- The ordinary `Store` live index is the largest remaining writable and
  recovery allocation. It holds every live key and location in memory on
  open and recovery unless `max_live_keys` is set.
- Within the checkpoint online handoff, each generation's suffix overlay can
  reach 16,384 keys and 4 MiB of key bytes. During compaction the source
  and replacement each have one.
- The checkpoint block directory can reach 131,072 entries.
- A catch-up replay group can reach 4 MiB plus one oversized value.

Bounded recovery and project-wide RAM remain open.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45503223185
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45503221792
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45503221616
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45503221637
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45503221820
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every A/B attempt, build logs, binary hashes, the
candidate source diff, the full test log and the hosted run logs.
