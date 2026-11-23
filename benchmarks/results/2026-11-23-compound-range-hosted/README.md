# Hosted compound-range comparison

The measured GitHub Actions run compared EMDA-DB, SQLite 3.49.1,
PostgreSQL 18.6, and MongoDB 8.0.34 at revision `b42508b` on
Linux x86-64 ext4. One client queried a tenant equality prefix and
an integer lower bound. Each path returned and verified the same ordered
documents. All 36 backend rounds passed reopen or reconnect checks.

Each case used four fresh rounds, ten warmups, and fifty timed queries
per path. The table reports the median of four round medians in
microseconds, with the range of those medians in parentheses.

| Documents / matches | EMDA scan | EMDA planned | SQLite scan | SQLite index | PostgreSQL index | MongoDB index |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 / 10 | 52.33 (51.46–52.44) | 13.98 (13.69–14.03) | 22.46 (22.21–22.63) | 7.84 (7.65–7.91) | 79.28 (77.08–80.83) | 273.47 (262.99–276.20) |
| 1,000 / 10 | 497.78 (481.53–505.75) | 14.57 (14.32–14.78) | 178.51 (170.67–180.25) | 6.87 (6.70–6.95) | 106.22 (101.85–111.47) | 281.63 (267.84–291.35) |
| 1,000 / 100 | 523.55 (497.88–529.90) | 161.11 (152.21–164.12) | 225.98 (215.34–231.67) | 56.28 (53.75–57.76) | 311.27 (304.36–314.19) | 391.80 (383.70–393.78) |

SQLite's indexed path had the lowest median in all three cases. The
EMDA-DB and SQLite paths use in-process Rust adapters. PostgreSQL and
MongoDB include Python driver and loopback transport costs. Indexed paths
are selected, forced, or hinted and checked. The results compare this
warm, single-client workload and its adapters. Concurrent writes,
cold starts, and separate engine memory measurements remain outside it.

Setup used SQLite WAL with `synchronous=FULL`, PostgreSQL with `fsync`,
`synchronous_commit`, and `full_page_writes` enabled, and MongoDB with
`w=1, j=true`. Setup and reopen checks were outside the query timers.
All 36 pre-run quiet checks passed. The VM's recorded I/O wait reached
16.67% over full backend windows, with no recorded CPU throttling or
direct reclaim in those windows.

Reproduce with the [measured workflow mode](../../../docs/BENCHMARKS.md#compound-equality-prefix-and-range-comparison).
The [hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45161550863)
retains the comparison artifact. The [raw report](raw-results.json.gz)
contains all 3,600 timings, plans, pinned image identities, durability
settings, and telemetry. Its compressed SHA-256 is
`3f5fcaa2ea7797b4fd435b293e2c0947f8750d3aaccf2362adfa170e0f895b14`.
