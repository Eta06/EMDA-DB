# Committed reads during managed selection sync

The baseline is `49c1c0b`. The candidate publishes and synchronizes the
selection under the writer gate while point reads and snapshots can use
the previous committed view. The in-memory switch still follows a
successful sync. A returned selection error or a panic requires reopening.

Both builds use Rust 1.85.0 in release mode. The macOS workload uses ARM64
APFS with a V2 witness. The Linux workload uses ARM64 ext4 with a V3 witness.
Each platform runs seven baseline/candidate pairs for every mode, alternating
the order. The fixture starts with 131,072 32-byte keys, 8-byte values, and
a suffix containing 2,048 updates and inserts. One reader checks a known
committed value with a 1 ms pause between calls.

Manual mode measures an explicit managed handoff. On-demand mode measures
one put that forces a handoff. Worker mode measures one requested handoff
until observed completion. Sustained mode fills the overlay with two batches
between trigger writes. Alternating trigger keys force twelve consecutive
rollovers in each trial, reusing both automatic slots.

| Platform | Mode | Baseline read, ms | Candidate read, ms | Baseline elapsed, ms | Candidate elapsed, ms | Pairs with shorter reads |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| macOS | manual | 9.333 | 0.024 | 60.22 | 61.22 | 7/7 |
| macOS | on-demand | 9.499 | 0.026 | 92.77 | 80.67 | 7/7 |
| macOS | worker | 14.561 | 0.020 | 87.99 | 84.11 | 7/7 |
| macOS | sustained | 20.583 | 0.178 | 78.37 | 76.42 | 7/7 |
| Linux | manual | 6.388 | 0.018 | 53.02 | 47.06 | 7/7 |
| Linux | on-demand | 0.662 | 0.025 | 41.19 | 36.79 | 7/7 |
| Linux | worker | 0.392 | 0.015 | 34.21 | 35.16 | 6/7 |
| Linux | sustained | 1.188 | 0.232 | 37.87 | 37.13 | 7/7 |

For reads, each trial contributes its worst observed call and the table shows
the median of those maxima. The elapsed column shows median operation time.
For sustained runs it shows the median of each trial's mean triggering-put
latency, including the rollover and synced append. The median worst
triggering put was 98.00 / 82.88 ms on macOS and 43.11 / 45.04 ms on Linux
for baseline / candidate. Writes still wait for the full durable handoff.

All 112 trials checked their expected values and reopened the selected
checkpoint. The single-handoff trials also checked output bytes against an
ordinary full-log checkpoint. The 28 sustained trials completed 336 rollovers
and checked every rewritten key on managed reopen and ordinary full-log
replay. Fingerprints verified preservation of the original log prefix and
template. The probes measure one concurrent reader. Writer competition
beyond the triggering puts and longer-running load are outside the sample.

The macOS selection and automatic unit tests cover reads during stage
publication and synchronization, writer exclusion, snapshots, and failure
and panic recovery. The focused automatic, managed rollover and snapshot
integration suites passed on macOS and Linux. The tests exercise a running OS.

## Reproduction

Extract [the evidence archive](raw-results.tar.gz). It contains the two
probes, fixture drivers, exact source diff, environment, build hashes, raw
trial rows, test logs and summary. Apply `candidate.diff` to one disposable
checkout of the baseline. Copy the two probes to `examples/` in both
checkouts and run

```sh
cargo +1.85.0 build --locked --release --example rollover_reader_probe --example automatic_rollover_probe
```

Use distinct target directories. Run the two drivers with the matching
absolute baseline and candidate executable paths and a private writable
fixture parent on the tested filesystem.

```sh
python3 manual/repeat-pairs.py BASELINE_MANUAL CANDIDATE_MANUAL FIXTURE_PARENT
python3 automatic/repeat-pairs.py BASELINE_AUTO CANDIDATE_AUTO FIXTURE_PARENT
```

The initial sustained pilot reused the same trigger key, which stopped
forcing a rollover after the first round. Those partial rows are excluded.
