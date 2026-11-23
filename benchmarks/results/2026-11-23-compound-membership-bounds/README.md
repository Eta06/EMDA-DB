# Scalar bounds on compound membership probes

This benchmark compares EMDA-DB at `dbec043` and `ec03d83` with indexed
SQLite 3.49.1. It runs on native macOS ARM64 APFS and Linux ARM64 ext4 in
a guest on the same Mac. Both engines use in-process Rust adapters built in
release mode with Rust 1.85.0.

The fixture has 768 indexed JSON documents across two tenants and an
integer score per document. Every query selects both tenants through a
membership predicate and scores in `[128, 256)`. The sparse list includes
eight scores in the range and 128 outside it. The dense list contains all
128 scores in the range plus eight outside. A third query omits the score
list. Both score lists start with 136 values, which would require more
than 256 complete tuples before applying the scalar bounds.

The two revisions reopen the same committed fixture in fresh processes
across nine paired rounds. The runner alternates the revision order,
rotates the cases, and interleaves both engines after 15 warmups.
It retains 100 query timings per engine and case in each process.
SQLite uses WAL, `synchronous=FULL`, a cached statement and an equivalent
checked expression index. Both paths fetch and decode the complete
ID-ordered JSON result. Each query is compared to the generated model
outside the timer, and each process verifies the collection scan and
the expected indexed access path and candidate count.

## Results

Times are medians of nine process medians, in microseconds. Parentheses
show the minimum and maximum process medians. SQLite results come from
the candidate rounds. Both revisions' SQLite samples are in the archive.

| Platform | Case | EMDA-DB probes | EMDA-DB candidates | Before (µs) | After (µs) | SQLite index (µs) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| macOS | sparse | 2 → 16 | 128 → 8 | 147.21 (145.96–155.08) | 19.33 (18.69–21.23) | 43.58 (42.25–44.04) |
| macOS | dense | 2 → 256 | 128 → 128 | 139.63 (137.75–146.65) | 242.85 (239.65–250.69) | 74.60 (73.46–80.92) |
| macOS | range-only | 2 → 2 | 128 → 128 | 113.46 (111.73–118.23) | 113.12 (111.50–120.85) | 43.52 (42.83–45.38) |
| Linux ARM64 | sparse | 2 → 16 | 128 → 8 | 140.12 (136.60–150.06) | 19.06 (17.38–21.56) | 42.60 (41.83–57.48) |
| Linux ARM64 | dense | 2 → 256 | 128 → 128 | 140.52 (137.40–144.81) | 251.98 (231.33–308.50) | 74.12 (71.00–96.36) |
| Linux ARM64 | range-only | 2 → 2 | 128 → 128 | 116.13 (113.54–121.69) | 117.00 (115.23–125.25) | 43.54 (41.38–45.65) |

Before scalar-bound narrowing, both lists exceeded the tuple cap and used
two range probes. Applying the bounds helped sparse queries, where only
eight candidates remained. For the dense list, enumerating all 256 exact
tuples raised the median by about 74% on macOS and 79% on Linux. The
range-only case had overlapping process-median ranges on both platforms.

## Coalescing the dense interval

A follow-up uses one inclusive score range for each tenant prefix when
the bounded list is a single contiguous integer interval. It preserves
the candidate set and leaves separate exact probes for the tested sparse
list. The runner paired the expanded and grouped implementations in a
fresh campaign using the same settings and fixture shape.

| Platform | Case | Probes | Candidates | Expanded (µs) | Grouped (µs) | SQLite index (µs) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| macOS | sparse | 16 → 16 | 8 | 19.46 (18.62–21.44) | 19.65 (19.40–59.27) | 44.17 (42.83–114.00) |
| macOS | dense | 256 → 2 | 128 | 243.96 (237.85–249.19) | 141.06 (136.83–203.58) | 75.46 (73.56–109.56) |
| macOS | range-only | 2 → 2 | 128 | 112.98 (111.75–121.06) | 113.02 (112.02–117.06) | 44.15 (43.54–45.31) |
| Linux ARM64 | sparse | 16 → 16 | 8 | 18.08 (17.17–19.38) | 18.08 (17.29–19.42) | 43.42 (42.62–46.56) |
| Linux ARM64 | dense | 256 → 2 | 128 | 231.65 (227.35–249.42) | 143.29 (141.27–184.56) | 72.15 (71.46–85.83) |
| Linux ARM64 | range-only | 2 → 2 | 128 | 117.65 (114.75–123.48) | 120.12 (116.33–124.67) | 42.48 (41.25–46.83) |

Dense medians fell by 42% on macOS and 38% on Linux in the follow-up.
SQLite still led the dense and range-only cases. The grouped
implementation's sparse median remained below SQLite's on both hosts.
One macOS sparse round was slower for both engines. The two campaigns
were measured separately. They cover warm single-client queries on one
dataset and do not measure concurrent traffic, per-engine CPU or memory.

## Reproduction

The [raw archive](raw-results.tar.gz) contains 43,200 timings, access
plans, source and binary hashes, build logs, the source patch for grouping,
and the probes and paired runners. In disposable checkouts at the two
revisions in `revisions.txt`, copy `bounded_membership_probe.rs` to
`examples/` and build with
`cargo +1.85.0 build --locked --release --example bounded_membership_probe`.
For the grouped variant apply `optimized.diff` to the second revision
and copy `grouped-membership-probe.rs` as
`examples/bounded_membership_probe.rs`. Use separate targets.

Run `repeat-pairs.py BASELINE_BIN EXPANDED_BIN FIXTURE_PARENT` for the
first table and `repeat-paired-optimization.py EXPANDED_BIN GROUPED_BIN FIXTURE_PARENT`
for the second, using Python 3 and redirecting stdout to platform-specific
JSONL files. Regenerate the tables with `summarize.py` and
`summarize-grouped.py`, each taking macOS and Linux JSONL paths.
