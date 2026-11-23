# Point reads during automatic checkpoint rollover

This comparison measures the reader availability change in `b5a3a90` against
`6c9c866`. Both builds use the default Rust 1.85 release profile. Tests ran on
native macOS ARM64 APFS with a V2 witness and a Linux ARM64 ext4 VM with a
V3 witness. Each platform has 11 pairs per mode in fresh processes, with mode
and revision order alternating between pairs.

Each trial opens a private copy of a witnessed store with a selected managed
checkpoint covering 262,144 initial 32-byte keys and 8-byte values. Its
suffix contains two 1,024-operation batches that overwrite and add keys.
One reader polls an updated key with a 1 ms pause between calls. The
on-demand mode times a put that crosses the configured suffix key limit and
includes the resulting append and sync. The worker mode times an explicit
worker request until observed completion, polling worker status every 250 µs.
Both modes start with empty automatic slots.

## Results

Worst read is the longest measured point read per trial. Parentheses give
the minimum and maximum trial values around each reported median. Process
CPU includes all participating threads.

| Platform | Mode | Variant | Median worst read, ms (range) | Median operation, ms (range) | Median process CPU, ms | Median reads |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| macOS | on-demand | baseline | 97.34 (93.41–103.28) | 104.66 (99.53–111.51) | 67.38 | 6 |
| macOS | on-demand | candidate | 8.71 (7.73–10.88) | 106.66 (101.64–109.54) | 68.19 | 66 |
| macOS | worker | baseline | 99.41 (90.89–105.36) | 101.06 (92.19–107.12) | 68.61 | 3 |
| macOS | worker | candidate | 8.69 (7.65–10.41) | 97.93 (96.17–103.94) | 68.28 | 65 |
| Linux | on-demand | baseline | 65.08 (62.14–69.91) | 71.80 (67.84–76.36) | 60.59 | 6 |
| Linux | on-demand | candidate | 0.64 (0.03–1.94) | 71.99 (68.04–92.91) | 61.66 | 58 |
| Linux | worker | baseline | 65.39 (62.80–87.31) | 66.93 (63.84–88.50) | 60.89 | 3 |
| Linux | worker | candidate | 0.62 (0.04–1.51) | 66.66 (64.06–81.74) | 60.89 | 57 |

The candidate had a shorter worst read in all 44 pairs across both modes and
platforms. Median elapsed time was within 3.2 ms of its paired baseline
median in all four cases. Readers still wait for selection publication.
The experiment measures one reader and one first-slot rollover at a time
on fresh copies. The copy and open take place outside the timer and can warm
the filesystem cache. Long readers, competing writers, repeated slot reuse,
and memory usage are outside the measurement.

## Output verification

All 88 trials checked the committed reader value, compared the published
checkpoint byte for byte with a full-log checkpoint, and verified the
selection, value and suffix overlay after reopening. The worker left the log
and witness fingerprints unchanged. On-demand rollover preserved the full
original log prefix and reopened with only the acknowledged triggering write
in the new overlay. The untouched template store matched its initial log,
witness and selection fingerprints after all trials. These are successful
publication and reopen checks on the running OS.

The exact candidate passed the hosted [macOS correctness checks](https://github.com/Eta06/EMDA-DB/actions/runs/45166428276),
[Linux stable and Rust 1.85 checks](https://github.com/Eta06/EMDA-DB/actions/runs/45166428294),
and [Linux filesystem EIO checks](https://github.com/Eta06/EMDA-DB/actions/runs/45166428345).
The existing [manual rollover study](../2026-11-23-manual-rollover-readers/README.md)
measures a separate path.

## Reproduction

[Raw evidence](raw-results.tar.gz) includes revisions, probe, fixture runner,
summary script, build logs, source and library hashes, platform records and
per-trial JSON lines. Extract it and prepare disposable source copies at the
recorded revisions. Add `automatic_rollover_probe.rs` to each copy's
`examples/` directory, then build with
`cargo +1.85.0 build --locked --release --example automatic_rollover_probe`.
Use distinct target directories or run
`cargo +1.85.0 clean --release -p database-engine` before switching revisions.
The recorded runs rebuilt the library on both revisions. Pilot timings were
discarded after shared Cargo artifacts reused the baseline library.

Run `python3 repeat-pairs.py BASELINE_BIN CANDIDATE_BIN FIXTURE_PARENT`
once per platform and redirect output to the corresponding JSONL file.
`FIXTURE_PARENT` must exist on the filesystem to measure. The script creates
and removes its private fixtures. Run
`python3 summarize.py macos.jsonl linux.jsonl` to reproduce the table.
