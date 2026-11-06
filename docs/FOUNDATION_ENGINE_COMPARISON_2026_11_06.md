# Local engine comparison, 6 November 2026

On the four-client mixed byte-key workload, the prototype had higher median
throughput than the other configurations tested, while SQLite had a lower
median write latency. SQLite also had lower median and p99 write latency in
the write-only workload. These measurements compare complete adapters on one
VM, not every database feature or a universal engine ranking.

We measured clean revision `bdbe7a3` against SQLite 3.49.1, PostgreSQL 18.6
and MongoDB 8.0.34. The matrix contains 96 successful runs: four repetitions
of six workloads for each configuration. Each number below is the median of
four run-level measurements; latencies are microseconds. The
[raw runs](../benchmarks/results/2026-11-06-foundation-engines.json) retain
run-level quantiles, run order, source and binary hashes, settings, versions and noise
telemetry. The [summary](../benchmarks/results/2026-11-06-foundation-engines-summary.json)
includes ranges, other quantiles and all six workloads.

## Four clients, mixed reads and writes

| Configuration | Operations/s | Read p50 | Write p50 | Write p99 |
| --- | ---: | ---: | ---: | ---: |
| Prototype | 10,753 | 57.0 | 609.5 | 1,312.8 |
| SQLite | 4,946 | 39.5 | 296.9 | 14,422.1 |
| PostgreSQL | 6,273 | 285.4 | 818.4 | 2,401.4 |
| MongoDB | 3,518 | 649.6 | 1,390.4 | 2,999.8 |

## Four clients, writes only

| Configuration | Operations/s | Write p50 | Write p99 |
| --- | ---: | ---: | ---: |
| Prototype | 5,901 | 636.9 | 1,405.3 |
| SQLite | 3,230 | 250.4 | 959.5 |
| PostgreSQL | 4,589 | 801.2 | 2,106.9 |
| MongoDB | 2,883 | 1,271.7 | 2,803.6 |

The prototype synchronizes V3 data and its metadata witness before
acknowledgement. SQLite used WAL and `synchronous=FULL`; PostgreSQL enabled
`fsync`, synchronous commit and full-page writes; MongoDB used journaled
acknowledgements with its journal interval tuned to 1 ms. The prototype and
SQLite used Rust loopback adapters; PostgreSQL and MongoDB used Python drivers
and native protocols through local container port forwarding. These settings
and adapters differ, and similar synchronization settings do not prove equal
power-loss guarantees. See the [comparison methodology](COMPARISON_METHOD.md)
for the six workload definitions and limitations.

The test used an Intel Xeon Platinum 8573C Linux VM with a four-CPU cgroup
quota and an overlay filesystem. Client, server and orchestration CPU sets
were separated; images and release binaries were prepared before measurement.
All pre-run quiet checks passed, with no recorded cgroup throttling, memory
limit event or direct reclaim during measured runs. VM steal reached 15.33%
in one PostgreSQL mixed run, and shared storage was not isolated. Workloads
were small, warm-cache and closed-loop; four repetitions are exploratory, not
production SLOs. This run does not test SQL queries, document queries, large
datasets, replication, scale-to-zero, physical power loss or off-host restore.

Reproduce with the pinned benchmark dependencies, prepared Docker images and
release `network_bench_server` described in the methodology, then use:

```sh
python3 scripts/compare_engines.py --directory /path/to/test-filesystem \
  --output /path/to/new-results.json --server /path/to/network_bench_server \
  --rounds 4 --backends prototype sqlite postgres mongo
python3 scripts/summarize_engines.py /path/to/new-results.json /path/to/summary.json
```

The earlier [ten-engine comparison](LOCAL_COMPARISON_2026_11_03.md) remains
separate: its source revision and observations differ from this run.
