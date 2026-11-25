# Phase-isolated online checkpoint handoff resources

Revision `5bf83c3b` adds `prepare` and `handoff` modes to
`examples/online_checkpoint_verify_rss.rs`. The existing `setup` and
`compact` modes are kept. Runtime storage behavior is unchanged.

`prepare` runs in its own uncapped process. It writes 64 values of 1 MiB
twice, so one overwritten copy of each remains in the log, checkpoints the
store and selects it through a managed rollover.

`handoff` runs in a separate fresh process. It:

1. reopens the managed store;
2. captures an owned snapshot;
3. commits eight small acknowledged writes;
4. runs `compact_and_reclaim`;
5. checks that the snapshot still reads the captured large value and none
   of the small writes;
6. drops everything and reopens;
7. verifies the store in one ordered scan, one entry per page, against a
   streamed expected sequence: the eight small values, then every
   regenerated 1 MiB value. The sequence must be exhausted exactly.

The small writes happen between the snapshot and the handoff, not during
it. Each phase is timed. A fresh process does not imply a cold OS file
cache.

## Results

Each process's peak RSS comes from `/usr/bin/time`. Each handoff process
used a fresh fixture.

| Host | Handoff peak RSS | Wall | Reopen | Handoff | Verify | Log bytes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| macOS APFS, Apple M6, median of 3 | 5,816,320 | 0.46 s | 51 ms | 322 ms | 41 ms | 134,221,852 → 67,111,292 |
| Hosted Linux x86_64 under a 48 MiB address-space limit, 1 trial | 6,078,464 | 0.63 s | 84 ms | 460 ms | 84 ms | 134,221,852 → 67,111,292 |

Every run verified all 72 live entries. On macOS the separate `prepare`
process peaked at 4.73 MB and took 1.35 s. Free disk space was about 15.3 GB
before each trial and about 15.27 GB after.

The Linux trial ran in the memory-ceiling job of [CI] on `5bf83c3b`. The
fixture was prepared without a limit. The handoff ran under `prlimit --as`
with 48 MiB, and the process reported `rlimit_as_kib=49152`. The job
checked the exit status, the complete verification, the compaction and
that peak RSS stayed within the limit. This is an address-space ceiling on
one process. It is not a total-memory, project or machine quota.

`superseded-double-read/` holds three earlier macOS runs from a reviewed
draft. That draft read every value with point gets and then again in
8-entry scan pages of 1 MiB values. Its handoff peak was about 11.0 MB. The
final version verifies in one ordered pass.

## Contrast with the whole-process smoke workloads

Hosted benchmark smoke on `8ff55f5b` ([CI 8ff5]) ran the 200,000-key online
reclaim and disk-budget workloads, three trials each, with every reopen
verified. Their median whole-process RSS was 87.535 MiB and 85.191 MiB.
Those figures include fixture seeding and the workloads' complete
reference models, so they do not measure the handoff alone. The figures
above do.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45581840048
[CI 8ff5]: https://github.com/Eta06/EMDA-DB/actions/runs/45577085385
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every trial's output and time report, disk
readings, the binary hash and source, the superseded runs, and the hosted
prepare and capped handoff attempts with the run log.
