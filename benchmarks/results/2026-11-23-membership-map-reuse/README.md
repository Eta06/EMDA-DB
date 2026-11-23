# Membership probe map reuse

This follow-up to the [conjunctive membership comparison](../2026-11-23-conjunctive-membership/README.md)
measures one planner change. The two versions select the same index probes
and visit the same documents. The updated code builds the ordered map once,
then marks matching values in that map using a reusable encoding buffer.
A partially processed, non-indexable list never prunes candidates.
Disjoint lists keep a usable bounded access path.

Both ARM64 hosts used the same 768-document fixture and three queries as
the earlier comparison. Rust 1.85.0 release builds ran in fresh processes
on macOS APFS and Linux ext4. Each revision ran nine times, alternating
order, with an equivalent SQLite 3.49.1 expression-index query. Each
process used 15 warmups and 100 samples per engine and case. The harness
checked the selected access paths, exact candidate counts and every ordered
result. Timings include full JSON materialization and exclude validation,
fixture creation and reopening. SQLite used WAL and `synchronous=FULL`.

## Results

Times are medians of nine process medians in microseconds. Parentheses
show the minimum and maximum process medians. SQLite values come from
the updated binary's processes.

| Platform | Case | EMDA-DB candidates | Before (µs) | After (µs) | SQLite index (µs) |
| --- | --- | ---: | ---: | ---: | ---: |
| macOS | sparse-overlap | 8 → 8 | 24.46 (24.00–26.94) | 21.90 (20.85–22.67) | 28.04 (27.46–28.40) |
| macOS | full-overlap | 96 → 96 | 151.25 (148.52–152.17) | 137.10 (136.31–138.25) | 51.83 (51.25–52.88) |
| macOS | single-list | 8 → 8 | 12.44 (10.75–13.69) | 11.08 (10.75–12.02) | 5.38 (5.27–5.50) |
| Linux ARM64 | sparse-overlap | 8 → 8 | 20.50 (20.00–20.88) | 18.71 (17.98–20.33) | 26.54 (26.00–26.94) |
| Linux ARM64 | full-overlap | 96 → 96 | 136.98 (135.08–140.10) | 132.98 (131.79–139.71) | 48.15 (47.29–49.77) |
| Linux ARM64 | single-list | 8 → 8 | 9.96 (9.81–10.46) | 10.33 (9.88–10.79) | 5.38 (5.29–5.54) |

Sparse-overlap medians fell on both hosts, with the updated planner faster
in all nine pairs per platform. Its median was below indexed SQLite's in
this workload. For full overlap, medians fell by 9.4% on macOS and 2.9% on
Linux. All nine macOS pairs and six of nine Linux pairs favored map reuse.
The single-list control moved in opposite directions on the two hosts,
with overlap in the observed ranges. SQLite led full overlap and the
single-list control.

These comparisons measure the complete warm query, including filter
evaluation and result materialization. They cover one client, one dataset
and two environments on the same physical Mac. CPU and memory were
outside the measurements.

## Reproduction

The [raw archive](raw-results.tar.gz) contains 21,600 timings, access
paths, binary and source hashes, the exact planner diff and the probe
and runner. Start from the revision listed in `source-base.txt` in two
disposable copies. Apply `optimized.diff` to one. Copy
`bounded_membership_probe.rs` into `examples/` in both copies and build
the binaries with `cargo +1.85.0 build --locked --release --example bounded_membership_probe`.
Use separate target directories. The baseline binaries from the earlier
report have identical planner, probe and lockfile hashes and were reused
for this run.

Run `python3 repeat-pairs.py BASELINE_BIN OPTIMIZED_BIN FIXTURE_PARENT`
on each platform. Redirect stdout to `macos.jsonl` or `linux.jsonl`.
Regenerate the table with `python3 summarize.py macos.jsonl linux.jsonl`.
