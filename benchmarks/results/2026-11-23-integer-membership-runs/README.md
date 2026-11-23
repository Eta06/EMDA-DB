# Consecutive integer membership probes

The [initial comparison](../2026-11-23-document-range-membership/README.md)
found additional probe work for a dense membership list that returned every
document in a compound range. This follow-up compares the exact-probe planner
in `4564a47` with the accompanying patch, and retains indexed SQLite 3.49.1
as a comparator. The patch combines consecutive signed integer values into
inclusive ranges on the same field. Singleton groups still use equality.
Each gap stays outside the probes.

## Paired measurements

The same 768-document workload ran on native macOS ARM64 APFS and a Linux
ARM64 ext4 VM with Rust 1.85 in release mode. Nine paired fresh-process rounds
on each host alternate the planner order and rotate the case order. Each
round interleaves EMDA-DB and SQLite, discards 15 warmups, and records 100
timed queries per engine and case. Both engines reopen the committed fixture
for each round. SQLite uses a checked compound expression index, cached
statements, WAL and `synchronous=FULL`. Timing covers query execution and
complete JSON result materialization. Each of the 21,600 timed queries
returns the expected ordered rows, with comparison outside the timer.

The sparse case continues to use eight equality probes. The dense case moves
from 192 equality probes to 64 ranges of three adjacent scores. Both planners
visit only the 192 matching documents in that case. The range-only control
uses a single ordinary range. Values below are medians of the nine round
medians, with their minimum and maximum in parentheses. SQLite uses the
candidate-round measurements.

| Platform | Case | Matches | EMDA-DB exact probes, µs | EMDA-DB grouped probes, µs | SQLite index, µs |
| --- | --- | ---: | ---: | ---: | ---: |
| macOS | sparse | 8 | 13.71 (12.79–15.58) | 13.58 (13.04–15.10) | 5.96 (5.67–6.17) |
| macOS | dense | 192 | 328.31 (320.60–337.65) | 277.38 (268.08–287.62) | 95.60 (93.33–97.31) |
| macOS | range-only | 192 | 169.77 (161.00–176.96) | 168.96 (161.42–176.40) | 67.12 (65.19–69.38) |
| Linux | sparse | 8 | 12.12 (11.67–13.23) | 12.23 (11.94–13.12) | 5.81 (5.67–6.00) |
| Linux | dense | 192 | 294.21 (289.29–316.08) | 251.98 (242.35–256.79) | 81.67 (79.62–82.85) |
| Linux | range-only | 192 | 162.98 (161.12–166.75) | 165.54 (163.17–167.94) | 58.29 (56.46–59.52) |

Dense query medians declined by 15.5% on macOS and 14.4% on Linux. The
sparse and range-only round-median intervals overlap. SQLite remains faster
in this workload. The timing covers one warm client per process. Concurrent
traffic, cold starts, and separately measured memory and CPU remain outside
this comparison.

## Correctness and reproduction

The integration tests exercise signed endpoints, gaps, mixed types, partial
compound indexes, exact candidate limits and read-set conflicts. Indexed
disjunctions retain exact deduplication for shared values across branches.
All 55 focused tests passed on each platform. Rust 1.85 Clippy passed on
macOS with warnings denied.

The [raw evidence](raw-results.tar.gz) includes all timings and plans,
both binary hashes and source hashes, the exact patch, the probe and the
runner. Build the probe in two disposable copies of the recorded baseline,
applying `final.diff` to one. Copy `bounded_membership_probe.rs` into
`examples/` and run
`cargo +1.85.0 build --locked --release --example bounded_membership_probe`.
Use separate Cargo target directories or clean `database-engine` before
switching revisions. The recorded comparison rebuilt both libraries.

Run `python3 repeat-pairs.py BASELINE_BIN CANDIDATE_BIN FIXTURE_PARENT` on
each platform to produce its JSONL. The runner creates and removes its own
fixture within the supplied directory. Run
`python3 summarize.py macos.jsonl linux.jsonl` to regenerate the table.
