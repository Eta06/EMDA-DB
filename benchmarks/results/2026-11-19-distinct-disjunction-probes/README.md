# Distinct probes in indexed document disjunctions

Each disjunction probe is identified by its index, equality values, and
lower and upper bounds including inclusivity. Identical probes run once
in first-use order. The planner admits up to 256 distinct probes, rolls
back the probes from an unsuccessful alternative, and still counts each
visit from distinct overlapping ranges. The selected probes and the
complete residual filter run in the same transaction.

Focused cases cover combined membership probes, the exact 256-probe
boundary, rollback when that cap is exceeded, shared candidate and byte
limits, and concurrent insertion into a deduplicated empty probe. Tests
compare result ordering and values with the collection scan. The 33
document integration targets passed on both platforms with 192 tests on
macOS ARM64 and 195 on Linux ARM64. Linux used Rust 1.85.0. Formatting and
warnings-denied Clippy passed on macOS. [macOS tests](macos-documents.log.gz),
[Linux tests](linux-documents.log.gz), [source hashes](source.sha256).

## Paired macOS samples

The patched disjunction driver used 1,024 documents, 30 warmup queries and
300 measured queries per run. Each query repeated the same partial unique
equality branch and scalar-range branch 12 times. Each result matched the
collection scan, a separate fixture model and the equivalent SQLite
WAL/FULL query. The baseline was `8ff9c66`. Three alternating pairs used
different release executables and freshly populated fixtures.

| Implementation | Probes per query | Candidate visits per run | Median run p50 | Median run p95 |
| --- | ---: | ---: | ---: | ---: |
| Before deduplication | 24 | 12,600 | 51.33 µs | 63.67 µs |
| Distinct probes | 2 | 1,050 | 14.04 µs | 19.67 µs |

The separate two-branch control visited 1,050 candidates in each build,
with run p50 of 7.17 µs and 7.33 µs. These are warm single-client samples
with concurrent validation activity. The control has one sample per
build. The comparison measures repeated-probe overhead in this workload.
[Build logs, hashes and all timings](macos-samples.tar.gz).

Apply the [driver patch](repeated-branches.patch) with
`git apply --unidiff-zero`. Build `document_disjunction_bench` in separate
target directories and run each binary with an existing fixture-parent
directory. Set `EMDA_BRANCH_REPEATS=12` and `EMDA_EXPECTED_PROBES=24` on
the baseline or `EMDA_EXPECTED_PROBES=2` on the candidate. Set
`EMDA_BRANCH_REPEATS=1` and `EMDA_EXPECTED_PROBES=2` for the control.
