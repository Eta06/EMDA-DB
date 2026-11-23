# Conjunctive indexed membership comparison

This paired benchmark compares the EMDA-DB planner at `06695af` and
`2eecf48` with bundled SQLite 3.49.1. It uses native macOS ARM64 APFS
and a Linux ARM64 ext4 guest on the same Mac. Both engines run through
in-process Rust adapters built in release mode with Rust 1.85.0.

The fixture contains 768 JSON documents and a compound index on tenant
and score. SQLite indexes equivalent JSON expressions. Every query selects
one tenant and a typed integer range.
The first list in each two-list case contains 96 scores. Eight scores overlap
in the sparse case and all 96 overlap in the full-overlap case. The single-list
control contains eight scores.

Both engines reopen the committed fixture in a fresh process for each of
nine paired rounds per platform. The runner alternates revision order
and rotates the three query cases. Each case alternates engine order,
runs 15 warmups and retains 100 timings per engine. SQLite uses WAL,
`synchronous=FULL`, a cached prepared statement, and a checked compound
index plan. Both engines return complete ID-ordered JSON documents.
Every query result is compared with the generated model outside timing.
Each process also verifies EMDA-DB's collection scan and expected index
candidate counts. Fixture creation, durable commits, reopen and plan
checks are outside the timer.

## Results

Times are medians of the nine process medians in microseconds. Parentheses
show their minimum and maximum. SQLite values come from the candidate
rounds. Both revisions' individual SQLite timings are retained.

| Platform | Case | EMDA-DB candidates | EMDA-DB before (µs) | EMDA-DB after (µs) | SQLite index (µs) |
| --- | --- | ---: | ---: | ---: | ---: |
| macOS | sparse-overlap | 96 → 8 | 182.21 (149.38–197.92) | 33.98 (26.04–49.04) | 37.23 (31.15–48.83) |
| macOS | full-overlap | 96 → 96 | 186.58 (152.17–194.04) | 212.33 (191.10–230.71) | 75.96 (67.56–85.10) |
| macOS | single-list | 8 → 8 | 15.83 (10.79–17.94) | 16.60 (12.52–17.54) | 7.42 (6.08–8.65) |
| Linux ARM64 | sparse-overlap | 96 → 8 | 115.31 (112.56–120.54) | 20.67 (20.12–22.38) | 26.46 (26.08–28.29) |
| Linux ARM64 | full-overlap | 96 → 96 | 123.56 (122.77–129.71) | 137.65 (135.33–142.96) | 48.31 (47.25–49.46) |
| Linux ARM64 | single-list | 8 → 8 | 10.08 (9.33–10.33) | 10.00 (9.67–11.85) | 5.42 (5.29–6.46) |

Sparse overlap reduced EMDA-DB candidate visits from 96 to eight and
decreased median latency by 81% on macOS and 82% on Linux. The new
planner was faster than the prior one in all nine pairs on each host.
Its process median was below SQLite's in seven of nine macOS candidate
rounds and all nine Linux candidate rounds.

Full overlap visited 96 candidates in both versions and raised the
EMDA-DB median by about 14% on macOS and 11% on Linux. All nine pairs
on each host moved in that direction. The single-list ranges overlap
on both platforms. SQLite led the full-overlap and single-list cases.

The sample covers one warm client and one dataset. It leaves concurrent
load, cold cache, isolated CPU and memory, and other index layouts
unmeasured. The Mac and its guest share hardware. An initial two-job
Linux build was killed before sampling. The successful Linux builds
used one job and the same release profile for both revisions.

## Reproduction

The [raw archive](raw-results.tar.gz) contains 21,600 timings, query
plans, source and binary SHA-256 hashes, build logs, platform details,
the probe, runner, and summary script. Extract it into a scratch
directory. Copy `bounded_membership_probe.rs` into `examples/` in two
disposable checkouts at the revisions in `revisions.txt`. Use separate
target directories and build both with
`cargo +1.85.0 build --locked --release --example bounded_membership_probe`.

On each platform run
`python3 repeat-pairs.py BASELINE_BIN CANDIDATE_BIN FIXTURE_PARENT`
and redirect stdout to its JSONL file. The fixture parent must already
exist. The runner creates and removes its own temporary fixture.
Regenerate the table with `python3 summarize.py macos.jsonl linux.jsonl`.
