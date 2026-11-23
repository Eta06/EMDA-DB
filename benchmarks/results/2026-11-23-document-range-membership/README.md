# Document membership within indexed scalar ranges

This paired comparison measures the planner change in `d9a5e32` against
`621c55e` and an equivalent indexed SQLite 3.49.1 query on native macOS ARM64
APFS and a Linux ARM64 ext4 VM. Both embedded engines run in one Rust 1.85
release probe per trial.

## Workload

The fixture contains 768 small JSON documents, two tenants and a
`(tenant, score)` compound index. IDs are ordered in the opposite direction
from score. Each query selects the target tenant and scores in `[128, 384)`.
The sparse case supplies ten membership literals, including two outside the
range, and matches eight documents. The dense list contains all 192 scores
present for that tenant in the range. The range-only control has no list.
SQLite uses the equivalent JSON expression index, and its plan is checked
at every open. Both paths return complete ID-ordered JSON documents.

Each platform runs nine pairs in fresh processes, alternating revision order
and rotating the order of cases. A run interleaves both engines after 15
warmups, with 100 timed queries per engine per case. Timing covers query
execution and result materialization, including JSON decoding. SQLite uses
its statement cache. The fixture is committed before measurement and both
engines reopen for each trial. SQLite uses WAL and `synchronous=FULL`.
Fixture creation, query plan assertions and result comparisons are outside
the timed samples. Every query checks the complete ordered result against the
generated model, and each trial also compares the prototype's full scan with
that model.

## Results

Values are medians of the nine trial medians in microseconds. Parentheses
give the minimum and maximum trial medians. SQLite is shown from the runs
with the candidate; SQLite timings from the baseline runs are also retained.

| Platform | Case | Matches | EMDA-DB candidates | EMDA-DB before, µs | EMDA-DB after, µs | SQLite index, µs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| macOS | sparse | 8 | 192 → 8 | 160.40 (156.15–164.88) | 13.96 (12.96–37.71) | 6.25 (5.83–16.27) |
| macOS | dense | 192 | 192 → 192 | 202.23 (196.77–206.87) | 321.83 (312.31–327.38) | 92.60 (92.06–94.38) |
| macOS | range-only | 192 | 192 → 192 | 161.98 (160.23–168.42) | 162.94 (161.15–170.25) | 65.83 (63.17–66.29) |
| Linux | sparse | 8 | 192 → 8 | 151.42 (149.17–156.50) | 12.00 (11.52–12.90) | 5.75 (5.62–5.83) |
| Linux | dense | 192 | 192 → 192 | 191.94 (187.67–193.83) | 291.69 (286.06–313.08) | 79.73 (78.56–83.60) |
| Linux | range-only | 192 | 192 → 192 | 163.90 (160.00–166.52) | 163.73 (160.96–166.29) | 57.88 (56.33–61.38) |

Sparse membership saves candidate visits and query time on both hosts. The
dense case visits the same 192 documents on both revisions and takes longer
with the candidate's 192 equality probes. The range-only medians differ by
under 1 µs on each host. SQLite recorded the lowest median for all three
cases on both hosts. These results apply to the tested warm, single-client
workload and its in-process adapters. They leave concurrent traffic,
other selectivities and distributions, cold starts, memory, and per-engine
CPU unmeasured.

## Reproduction

[Raw evidence](raw-results.tar.gz) contains 21,600 timings, SQLite plans,
the exact source revisions, the probe and runner, build logs and provenance
hashes. In each disposable revision checkout, copy
`bounded_membership_probe.rs` to `examples/` and build it with
`cargo +1.85.0 build --locked --release --example bounded_membership_probe`.
Use separate target directories or run
`cargo +1.85.0 clean --release -p database-engine` before switching revisions.
The recorded runs rebuilt both libraries.

Run `python3 repeat-pairs.py BASELINE_BIN CANDIDATE_BIN FIXTURE_PARENT` on
each host to produce its JSONL file. The runner creates a private fixture
under the supplied existing parent and removes it when done. Run
`python3 summarize.py macos.jsonl linux.jsonl` to regenerate the table.
