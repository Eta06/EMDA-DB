# Read latency during manual checkpoint rollover

Manual checkpoint copy and rollover now hold the writer gate during
preparation and let readers use the committed checkpoint and overlay. The
final manual swap takes the state write lock, including persistent selection
publication in managed mode. The writer gate stays held through the operation.

## Paired measurements

A Rust 1.85 release probe compared revision `9af4b6a` with the accompanying
patch on native macOS ARM64 APFS (V2) and a Linux ARM64 ext4 VM (V3).
Each trial performed the first manual managed rollover from an explicit base
with 262,144 initial 32-byte keys and 8-byte values, then two witnessed
1,024-operation batches that overwrote and added keys. A concurrent reader
polled the same committed overlay key with a 1 ms pause between calls.
Each platform ran 11 pairs in fresh processes, alternating variant order.

| Platform | Variant | Median worst read, ms (range) | Median rollover, ms | Median process CPU, ms | Median read samples |
| --- | --- | ---: | ---: | ---: | ---: |
| macOS | Baseline | 80.69 (78.69–82.86) | 82.17 | 62.33 | 2 |
| macOS | Shared preparation | 8.76 (7.81–10.74) | 82.18 | 62.49 | 50 |
| Linux | Baseline | 67.10 (63.43–75.51) | 68.56 | 63.39 | 2 |
| Linux | Shared preparation | 0.43 (0.02–0.71) | 68.63 | 63.71 | 55 |

Worst read is the longest point read in each trial. It fell in all 11 pairs on each platform. Every trial checked the read value, verified
byte-for-byte equality and digest equality with a full-log reference checkpoint,
and reopened the managed selection with an empty overlay. Source log and
witness fingerprints matched before and after the trials on each platform.
Process CPU includes both the rollover and reader threads. Scheduling, cache
state and filesystem sync latency affect the measurements. The probe covers
one point reader and first managed publication. Existing managed selections,
automatic rollover, background workers and concurrent writer latency are
outside the timed workload.

## Correctness checks

The channel-controlled tests cover independent copies and both manual rollover
modes. During checkpoint sync, readers see the committed state and writers wait.
The managed selection hook confirms read exclusion at final publication.
A separate injected-panic test verifies that the instance fails closed until
reopen and that acknowledged data remains readable after recovery.

On macOS, 10 rollover, 16 selection and 10 integration tests passed.
On Linux, 10 rollover, 17 selection and 29 automatic-rollover tests passed.
macOS Rust 1.85 all-target Clippy passed with warnings denied.

## Reproduction

[Raw evidence](raw-results.tar.gz) includes the baseline revision, exact source
patch, probe, runner, host details, per-trial JSON lines and check logs. Extract
it, prepare two disposable copies of the recorded revision and apply
`rollover.diff` to one. In each copy, place `rollover_reader_probe.rs` in
`examples/` and run
`cargo +1.85.0 build --locked --release --example rollover_reader_probe`.
Then run `python3 repeat-pairs.py BASELINE_BIN PATCHED_BIN FIXTURE_PARENT`.
Use a scratch parent on the filesystem to measure. The runner creates and
removes its own fixture directory.
