# Compound membership without an explicit range

This paired run isolates the compound grouping change from
`f0141db` to `6d65726`. The same `In` predicates bind two tenant
values and either 16 or 136 distinct integer scores, with no additional
range predicate. The benchmark reopens one fixture of 768 documents per
platform and checks the complete ordered results against a deterministic
oracle on every timed call.

- **Within cap** uses 16 spaced scores. Both revisions scan 16 candidates
  using 32 exact index probes.
- **Over cap, grouped** uses 136 scores in two runs. The 272 exact tuples
  do not fit. Four grouped probes examine exactly 136 candidates in the
  new version. The prior version scans the entire collection.
- **Over cap, sparse** uses 136 spaced scores. Both versions scan the
  collection.

Bundled SQLite 3.49.1 uses an index on the same tenant and score JSON
expressions, with WAL and `synchronous=FULL`. The harness checks SQLite's
index selection and each EMDA-DB access path before timing. Timers include
retrieval, JSON decoding and collection of the results. Fixture setup,
reopen, plan checks and oracle comparisons are outside the timed interval.

Each platform used nine paired rounds with alternating revision order.
Each process alternated engine order for 15 warmup cycles and 100 timed
cycles per case. The table reports the median of process medians. The
SQLite column uses the same rounds as the after revision. Builds used
Rust 1.85.0 and the release profile with 256 code-generation units.
Fixtures ran on macOS APFS and Linux ext4 in an ARM64 VM on the same host.

| Platform | Case | EMDA before µs | EMDA after µs | SQLite µs | Candidates before → after |
| --- | --- | ---: | ---: | ---: | ---: |
| macOS ARM64 | within-cap | 33.54 | 32.67 | 11.62 | 16 → 16 |
| macOS ARM64 | over-cap-grouped | 596.54 | 161.60 | 83.31 | 768 → 136 |
| macOS ARM64 | over-cap-sparse | 600.83 | 600.42 | 93.73 | 768 → 768 |
| Linux ARM64 | within-cap | 28.50 | 28.96 | 11.06 | 16 → 16 |
| Linux ARM64 | over-cap-grouped | 514.73 | 155.88 | 74.79 | 768 → 136 |
| Linux ARM64 | over-cap-sparse | 528.38 | 522.54 | 73.79 | 768 → 768 |

In the grouped case, EMDA-DB's median was 73% lower on macOS and 70%
lower on Linux. Indexed SQLite still had the lower latency on both
platforms. The within-cap and sparse cases kept their original plans.

These results measure a warm, single-client read workload. They do not
measure concurrent writers, cold opens, sustained resource use or
performance on other query families.

## Reproduction

Extract [raw-results.tar.gz](raw-results.tar.gz). In a disposable checkout
at the first revision in `revisions.txt`, copy `compound_no_range_probe.rs`
to `examples/`. Build it with:

```sh
CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256 \
  cargo +1.85.0 build --offline --locked --release -j 1 --example compound_no_range_probe
```

Save the first executable. Apply `planner.diff` or use the second revision,
then repeat the build and save the second executable. Run
`python3 repeat-pairs.py BEFORE AFTER FIXTURE_PARENT`, redirecting stdout
to a JSONL file. Use a fresh directory on the filesystem under test.
Run `python3 summarize.py MACOS_JSONL LINUX_JSONL` to regenerate the
table. The archive includes the harness, 21,600 timed calls, access plans,
build logs, environment and executable hashes.
