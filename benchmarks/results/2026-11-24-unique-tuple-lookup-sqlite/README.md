# Unique-tuple document lookup compared with SQLite

Release binaries built with Rust 1.85.0 at EMDA revision `1a827c3`
resolved 128 source rows against 512 targets using a partial unique
index on tenant and email. Every timed lookup was checked against all
expected rows, including 87 matches and 41 missing relations. SQLite
3.49.1 used its partial unique index for the equivalent left join.

Each host ran 24 alternating-order rounds, including four warm-up
rounds. Reported times are microseconds for one complete result,
including materialization as JSON values. Expected-result comparisons
and fixture writes were outside the measured queries. Both
engines used warm caches and one client. SQLite used WAL and FULL sync
for setup. Linux ran on VM tmpfs. These are read-only workload samples
with different engine validation paths. SQLite had the lower median on
both hosts for this workload.

| Host | Engine | Median µs | Min µs | Max µs |
| --- | --- | ---: | ---: | ---: |
| macOS ARM64 (APFS) | emda | 1227.1 | 641.1 | 1516.0 |
| macOS ARM64 (APFS) | sqlite | 503.0 | 213.5 | 669.7 |
| Linux ARM64 (tmpfs VM) | emda | 202.5 | 173.4 | 251.3 |
| Linux ARM64 (tmpfs VM) | sqlite | 61.4 | 57.3 | 90.0 |

The [raw archive](raw-artifacts.tar.gz) preserves all 96 timings,
including warm-ups, with per-case commands, build and environment logs,
source and binary hashes, and the benchmark source and lockfile.

To reproduce, extract the archive, set the local `database-engine` path
in `Cargo.toml` to a checkout at `1a827c3`, run
`cargo +1.85.0 build --release --locked`, and run the resulting
`tuple-lookup-bench` binary with a disposable `TMPDIR`. Preserve its
standard output, which contains the SQLite plan and one JSON record per sample.
