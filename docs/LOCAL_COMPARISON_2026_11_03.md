# Local database comparison: 3 November 2026

The prototype had the highest median throughput in the six small byte-key workloads tested here. **SQLite still had faster median writes under concurrency and faster write-only p99.** These results compare complete adapters and their configured databases; they do not establish the fastest native engine or equivalent database features.

The completed matrix contains **264 successful runs**: ten engines, with a second SQLite binding/version control, six workloads and four repetitions. Another **36 runs** compare CPU affinity settings. All eleven configurations passed the separate process-kill/restart check. No storage-engine code or durability setting was weakened for this comparison.

## Main findings

- Four-client mixed workload: EMDA **10,397 operations/s**, SQLite **4,514 operations/s**; EMDA write p50/p99 **628/1,323 µs**, SQLite **288/13,816 µs**. The throughput ratio is 2.30× in this harness, while EMDA's median write latency is 2.18× SQLite's.
- Write-only workload: EMDA **5,825 operations/s**, SQLite **3,148 operations/s**; SQLite won both write p50 (**249 vs 629 µs**) and p99 (**903 vs 1,918 µs**). A lower mixed-workload tail does not imply a lower write-only tail.
- One-client mixed workload: EMDA write p50 **326 µs**, SQLite **403 µs**, but their run ranges overlap. Read-only throughput ranges also overlap. Four repetitions are exploratory evidence, not a statistical superiority claim.
- Concurrent CRUD: EMDA **7,547 timed operations/s**, SQLite **3,478**; SQLite still had lower median create, update and delete latency. Throughput includes two additional verification reads per cycle that are not counted as timed CRUD operations.
- Hosted Neon, D1, Atlas, hosted Turso, backend platforms and SpacetimeDB are **not measured**. Their required scenarios and the reasons they are not interchangeable with these local engines are listed in the [methodology](COMPARISON_METHOD.md#coverage-beyond-local-point-operation-tests).

## Reproducibility and limits

Measured source: [`5cbc807`](https://github.com/Eta06/EMDA-DB/commit/5cbc8079ff3877f5992552008491bae13dae13c3), with a clean working tree. The engine itself is unchanged from the previous V3 implementation. Differences from older benchmark sessions cannot be attributed to a new engine optimization. Source hashes, binary hashes, image digests, versions, settings, commands and per-run latency quantiles are retained.

The host was an Intel Xeon Platinum 8573C VM with CPUs 0–4 available, a four-CPU aggregate quota, 32 GiB memory limit, no swap, and `/workspace` overlay storage. Clients used CPU 0, servers CPUs 1–3, and orchestration CPU 4. Only one database ran at a time. Native containers had 2 GiB memory limits; embedded adapters did not have an equivalent hard limit. This is not a memory-efficiency comparison.

Durable acknowledgement settings were enabled: prototype V3 full inode sync, SQLite/Turso WAL FULL, PostgreSQL synchronous commit/fsync/full-page writes, Redis AOF always, MongoDB journal acknowledgements, RocksDB synchronous WAL writes, LMDB sync/metasync, DuckDB default durable commits and FoundationDB normal transactions. The [methodology](COMPARISON_METHOD.md) specifies versions, tuning, retries, transport and model differences. Different durability implementations and a process-restart check do not establish identical physical power-loss guarantees.

The datasets are small and warm, clients are closed-loop, and the Python load generator and bindings can limit throughput. Rust-adapter SQLite is 3.49.1; Python SQLite is 3.53.1, so that control also changes the engine version. DuckDB's analytical strengths and FoundationDB's distributed strengths are outside this test. No multi-key transactions, scans, analytical queries, scale-to-zero, replication, large-data workloads or production SLOs are evaluated.

## VM interference and what changed

Removing 27 verified obsolete Cargo caches from tmpfs freed **7.6 GiB** of cgroup memory. Source, current builds, dedicated baseline binaries, dependencies and measurement files were preserved. The interrupted 23-run pre-cleanup pass is retained separately and excluded from the final matrix. No global cache dropping or changes to unrelated services were used.

The main matrix recorded **zero CPU-quota throttling runs, zero direct-reclaim runs and zero memory-limit events**; all pre-run quiet checks passed. Host interference remained: reported VM steal reached **15.66%** during SQLite's fourth single-client mixed run and **15.20%** during its fourth concurrent mixed run. Those runs remain included. Affinity does not reserve physical cores or control shared storage.

The separate affinity calibration recorded one unsatisfied quiet check and one throttled run (2.776 ms of recorded throttling); maximum reported steal was **21.98%**. Pinning improved PostgreSQL's median throughput and latency, improved EMDA's median throughput and write p99 but slightly worsened its write p50, and worsened SQLite's median throughput and write latency in that sample. Write-p50 variability decreased for EMDA/SQLite but increased for PostgreSQL; tail ranges did not consistently tighten. **Pinning is a reproducibility control, not a universal performance or noise-removal guarantee.** All observations are retained below.

## Evidence and validation

- [264-run matrix](../benchmarks/results/2026-11-03-local-engines-matrix.json) and [median/range summaries](../benchmarks/results/2026-11-03-local-engines-summary.json).
- [36-run affinity calibration](../benchmarks/results/2026-11-03-affinity-calibration.json) and [summaries](../benchmarks/results/2026-11-03-affinity-summary.json).
- [Process-restart checks](../benchmarks/results/2026-11-03-local-engines-crash-checks.json): each configuration recovered 90 surviving values and 10 deletions after 120 acknowledged mutations, with clients held open until SIGKILL. This is not a power-cut test.
- [Memory cleanup record](../benchmarks/results/2026-11-03-noise-preparation.json) and [interrupted pre-cleanup measurements](../benchmarks/results/2026-11-03-interrupted-precleanup-matrix.json).
- [Passing CI on the measured revision](https://github.com/Eta06/EMDA-DB/actions/runs/41717836685): stable/MSRV formatting, linting and tests, optimized tests, and adapter/CRUD smoke checks. Local verification passed 90 Rust tests and 14 Python tests.

Recognized DuckDB write conflicts required 176 retries across the main matrix. Their delays are included in latency; no failed measurements were excluded. Other backend configurations reported no adapter-managed conflict retries; internal server retries are not comprehensively counted.

Use [the reproduction instructions](COMPARISON_METHOD.md#reproduction), then `python3 scripts/summarize_engines.py engines.json summary.json` to calculate run-level medians, ranges and variability. The tables below show medians of each run's quantiles, not quantiles pooled across every individual operation. Raw run records also include p95, p99.9, maximum latency, counts and telemetry.

## One client, mixed reads/writes, 64-byte values

Medians of four run-level measurements. Latencies are in microseconds (µs).

| Engine | Operations/s | Read p50 | Read p99 | Write p50 | Write p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 4,862 | 36.0 | 144.3 | 326.0 | 698.5 |
| SQLite 3.49.1 (Rust) | 3,701 | 52.3 | 185.3 | 402.9 | 1,351.0 |
| SQLite 3.53.1 (Python) | 4,046 | 62.8 | 212.1 | 377.4 | 973.4 |
| PostgreSQL 18.6 | 3,710 | 119.3 | 363.0 | 367.7 | 971.2 |
| Redis 8.2.11 | 3,103 | 122.6 | 360.9 | 469.9 | 1,035.1 |
| MongoDB 8.0.34 | 1,607 | 371.6 | 1,185.8 | 787.5 | 1,488.4 |
| Turso / pyturso 0.8.3 | 2,933 | 117.9 | 309.6 | 518.6 | 1,093.2 |
| RocksDB 9.8.4 | 4,698 | 43.8 | 184.8 | 313.5 | 927.7 |
| LMDB 0.9.36 | 3,847 | 47.0 | 138.8 | 414.6 | 860.0 |
| DuckDB 2.0.0 | 595 | 779.6 | 1,289.2 | 2,416.5 | 3,931.2 |
| FoundationDB 7.3.79 | 767 | 600.1 | 1,171.3 | 1,846.7 | 2,914.0 |

Run-to-run ranges (minimum–maximum of the four run-level measurements):

| Engine | Operations/s range | Write p50 range | Write p99 range |
| --- | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 4,527.5–5,294.7 | 284.1–349.6 | 642.4–815.9 |
| SQLite 3.49.1 (Rust) | 1,622.6–4,066.9 | 368.2–490.0 | 871.6–6,047.4 |
| SQLite 3.53.1 (Python) | 3,034.2–4,641.9 | 335.8–525.3 | 660.8–1,388.5 |
| PostgreSQL 18.6 | 2,639.1–3,954.0 | 343.9–489.0 | 766.3–1,971.5 |
| Redis 8.2.11 | 2,841.6–3,629.5 | 404.4–501.4 | 802.6–1,091.4 |
| MongoDB 8.0.34 | 1,378.6–1,762.8 | 772.1–903.7 | 1,277.2–2,786.1 |
| Turso / pyturso 0.8.3 | 2,488.0–3,144.6 | 470.8–643.6 | 936.6–1,221.2 |
| RocksDB 9.8.4 | 4,354.5–5,013.8 | 296.0–351.2 | 631.6–1,902.9 |
| LMDB 0.9.36 | 3,573.3–3,932.8 | 410.5–446.5 | 745.5–939.4 |
| DuckDB 2.0.0 | 562.7–604.0 | 2,381.5–2,488.4 | 3,093.6–4,495.7 |
| FoundationDB 7.3.79 | 753.7–802.1 | 1,830.9–1,889.5 | 2,681.3–3,164.2 |

## Four clients, mixed reads/writes, 64-byte values

Medians of four run-level measurements. Latencies are in microseconds (µs).

| Engine | Operations/s | Read p50 | Read p99 | Write p50 | Write p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 10,397 | 57.3 | 247.6 | 628.0 | 1,323.2 |
| SQLite 3.49.1 (Rust) | 4,514 | 37.2 | 193.8 | 287.9 | 13,815.7 |
| SQLite 3.53.1 (Python) | 4,694 | 49.1 | 242.7 | 309.5 | 26,854.8 |
| PostgreSQL 18.6 | 6,868 | 271.0 | 741.5 | 766.5 | 1,849.9 |
| Redis 8.2.11 | 6,280 | 521.5 | 1,335.6 | 672.0 | 1,412.0 |
| MongoDB 8.0.34 | 3,786 | 614.2 | 1,486.2 | 1,306.0 | 2,523.6 |
| Turso / pyturso 0.8.3 | 1,929 | 155.4 | 2,322.7 | 633.5 | 69,388.8 |
| RocksDB 9.8.4 | 4,482 | 627.2 | 2,680.2 | 843.2 | 3,003.7 |
| LMDB 0.9.36 | 4,339 | 61.5 | 263.0 | 1,699.8 | 3,005.6 |
| DuckDB 2.0.0 | 1,249 | 1,431.2 | 4,289.1 | 4,275.5 | 9,273.5 |
| FoundationDB 7.3.79 | 2,494 | 916.6 | 1,848.9 | 2,139.2 | 4,145.0 |

Run-to-run ranges (minimum–maximum of the four run-level measurements):

| Engine | Operations/s range | Write p50 range | Write p99 range |
| --- | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 10,277.0–10,705.4 | 621.7–633.8 | 1,236.6–2,315.0 |
| SQLite 3.49.1 (Rust) | 2,605.6–5,502.0 | 264.2–305.9 | 3,556.6–19,028.3 |
| SQLite 3.53.1 (Python) | 4,005.7–5,228.7 | 289.6–331.6 | 19,083.0–34,080.3 |
| PostgreSQL 18.6 | 6,314.5–7,364.9 | 714.2–820.2 | 1,666.0–2,244.0 |
| Redis 8.2.11 | 4,648.0–6,374.3 | 667.0–726.6 | 1,361.7–10,379.8 |
| MongoDB 8.0.34 | 3,685.7–3,875.5 | 1,265.4–1,330.4 | 2,320.5–2,604.3 |
| Turso / pyturso 0.8.3 | 1,857.3–2,169.8 | 589.9–665.1 | 56,169.9–78,897.8 |
| RocksDB 9.8.4 | 4,278.4–4,647.9 | 760.2–883.7 | 2,616.4–3,176.0 |
| LMDB 0.9.36 | 4,186.6–4,497.8 | 1,616.0–1,722.3 | 2,851.9–3,226.0 |
| DuckDB 2.0.0 | 1,238.6–1,261.6 | 4,257.1–4,295.9 | 8,883.3–9,555.1 |
| FoundationDB 7.3.79 | 2,275.7–2,687.4 | 2,041.7–2,281.3 | 3,551.1–4,153.4 |

## Four clients, reads only, 64-byte values

Medians of four run-level measurements. Latencies are in microseconds (µs).

| Engine | Operations/s | Read p50 | Read p99 |
| --- | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 30,000 | 112.7 | 347.9 |
| SQLite 3.49.1 (Rust) | 28,963 | 115.2 | 354.5 |
| SQLite 3.53.1 (Python) | 17,011 | 173.2 | 700.7 |
| PostgreSQL 18.6 | 13,745 | 242.6 | 710.7 |
| Redis 8.2.11 | 16,408 | 208.7 | 634.5 |
| MongoDB 8.0.34 | 5,798 | 604.0 | 1,502.9 |
| Turso / pyturso 0.8.3 | 8,215 | 423.2 | 1,221.1 |
| RocksDB 9.8.4 | 26,513 | 121.5 | 430.9 |
| LMDB 0.9.36 | 27,993 | 118.8 | 389.7 |
| DuckDB 2.0.0 | 2,942 | 1,099.2 | 2,829.6 |
| FoundationDB 7.3.79 | 5,797 | 644.0 | 1,348.9 |

Run-to-run ranges (minimum–maximum of the four run-level measurements):

| Engine | Operations/s range | Read p50 range | Read p99 range |
| --- | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 29,573.7–30,882.0 | 111.4–113.2 | 320.2–400.9 |
| SQLite 3.49.1 (Rust) | 28,005.8–32,379.1 | 91.9–118.4 | 329.5–423.5 |
| SQLite 3.53.1 (Python) | 16,616.9–18,877.7 | 157.8–177.2 | 631.5–715.3 |
| PostgreSQL 18.6 | 12,901.5–14,879.3 | 233.0–267.4 | 654.8–1,051.9 |
| Redis 8.2.11 | 15,226.7–19,491.9 | 167.9–220.9 | 584.9–698.9 |
| MongoDB 8.0.34 | 5,683.3–7,222.5 | 483.3–613.9 | 1,322.2–1,844.5 |
| Turso / pyturso 0.8.3 | 7,862.4–8,521.6 | 413.0–438.5 | 1,161.4–1,282.5 |
| RocksDB 9.8.4 | 24,920.6–28,050.2 | 118.3–122.6 | 378.6–512.9 |
| LMDB 0.9.36 | 25,458.5–29,119.8 | 114.7–126.0 | 364.4–461.7 |
| DuckDB 2.0.0 | 2,904.7–3,009.0 | 1,096.1–1,119.4 | 2,706.9–2,891.1 |
| FoundationDB 7.3.79 | 4,482.6–6,625.7 | 562.5–845.7 | 1,152.9–1,501.6 |

## Four clients, writes only, 64-byte values

Medians of four run-level measurements. Latencies are in microseconds (µs).

| Engine | Operations/s | Write p50 | Write p99 |
| --- | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 5,825 | 628.6 | 1,917.8 |
| SQLite 3.49.1 (Rust) | 3,148 | 249.0 | 902.9 |
| SQLite 3.53.1 (Python) | 3,060 | 257.9 | 2,296.8 |
| PostgreSQL 18.6 | 5,074 | 730.8 | 1,578.1 |
| Redis 8.2.11 | 5,438 | 669.5 | 1,475.9 |
| MongoDB 8.0.34 | 3,046 | 1,221.7 | 2,262.8 |
| Turso / pyturso 0.8.3 | 1,405 | 547.7 | 38,090.6 |
| RocksDB 9.8.4 | 2,721 | 1,129.3 | 4,368.1 |
| LMDB 0.9.36 | 2,337 | 1,614.5 | 2,622.9 |
| DuckDB 2.0.0 | 833 | 4,424.5 | 9,282.2 |
| FoundationDB 7.3.79 | 2,019 | 1,915.6 | 3,763.8 |

Run-to-run ranges (minimum–maximum of the four run-level measurements):

| Engine | Operations/s range | Write p50 range | Write p99 range |
| --- | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 5,281.0–6,320.7 | 593.8–685.3 | 1,159.3–2,680.2 |
| SQLite 3.49.1 (Rust) | 2,954.3–3,570.8 | 238.5–255.3 | 719.3–8,622.7 |
| SQLite 3.53.1 (Python) | 2,972.9–3,132.9 | 243.3–275.1 | 771.1–3,735.4 |
| PostgreSQL 18.6 | 4,735.2–5,570.8 | 670.4–758.4 | 1,504.1–2,470.7 |
| Redis 8.2.11 | 4,530.7–5,840.8 | 645.2–700.2 | 1,260.5–4,911.9 |
| MongoDB 8.0.34 | 2,952.5–3,064.9 | 1,213.4–1,230.2 | 2,160.5–3,508.7 |
| Turso / pyturso 0.8.3 | 1,292.9–1,506.2 | 534.7–590.1 | 9,411.1–53,645.4 |
| RocksDB 9.8.4 | 2,667.3–2,947.1 | 804.5–1,227.6 | 4,068.6–6,701.6 |
| LMDB 0.9.36 | 2,186.9–2,541.8 | 1,538.4–1,693.1 | 2,325.6–5,520.8 |
| DuckDB 2.0.0 | 808.9–835.7 | 4,360.9–4,548.9 | 9,188.9–10,159.8 |
| FoundationDB 7.3.79 | 1,688.7–2,037.1 | 1,892.1–2,182.6 | 3,447.8–5,756.7 |

## Four clients, mixed reads/writes, 4 KiB values

Medians of four run-level measurements. Latencies are in microseconds (µs).

| Engine | Operations/s | Read p50 | Read p99 | Write p50 | Write p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 5,709 | 129.8 | 971.0 | 876.3 | 2,482.2 |
| SQLite 3.49.1 (Rust) | 3,869 | 64.8 | 402.7 | 325.2 | 26,443.4 |
| SQLite 3.53.1 (Python) | 3,075 | 73.4 | 490.6 | 365.9 | 33,868.3 |
| PostgreSQL 18.6 | 4,218 | 490.2 | 1,598.5 | 1,008.5 | 2,654.6 |
| Redis 8.2.11 | 3,203 | 897.8 | 2,637.1 | 1,164.7 | 2,627.9 |
| MongoDB 8.0.34 | 2,944 | 842.1 | 2,312.4 | 1,482.6 | 3,145.4 |
| Turso / pyturso 0.8.3 | 1,404 | 206.1 | 1,929.3 | 795.9 | 79,201.6 |
| RocksDB 9.8.4 | 3,241 | 685.6 | 3,509.5 | 999.1 | 3,496.6 |
| LMDB 0.9.36 | 4,020 | 87.0 | 773.3 | 1,589.3 | 2,959.6 |
| DuckDB 2.0.0 | 1,172 | 1,490.7 | 4,153.4 | 4,531.5 | 9,022.9 |
| FoundationDB 7.3.79 | 2,195 | 1,006.5 | 2,106.6 | 2,236.6 | 4,674.7 |

Run-to-run ranges (minimum–maximum of the four run-level measurements):

| Engine | Operations/s range | Write p50 range | Write p99 range |
| --- | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 4,783.3–6,535.3 | 751.8–959.0 | 1,786.1–4,038.5 |
| SQLite 3.49.1 (Rust) | 3,504.4–4,132.8 | 298.3–350.8 | 18,973.8–33,909.7 |
| SQLite 3.53.1 (Python) | 2,829.6–3,794.5 | 351.3–375.4 | 19,116.4–33,932.6 |
| PostgreSQL 18.6 | 4,032.0–4,461.4 | 926.5–1,016.5 | 2,262.2–3,225.5 |
| Redis 8.2.11 | 2,883.1–3,515.3 | 1,013.7–1,223.2 | 2,407.1–11,981.2 |
| MongoDB 8.0.34 | 2,802.9–3,056.9 | 1,431.0–1,536.2 | 2,786.7–3,252.6 |
| Turso / pyturso 0.8.3 | 1,350.2–1,535.8 | 743.5–831.1 | 56,335.9–104,113.1 |
| RocksDB 9.8.4 | 3,182.7–3,765.2 | 873.0–1,031.1 | 2,854.7–4,270.8 |
| LMDB 0.9.36 | 3,873.2–4,135.9 | 1,553.3–1,647.7 | 2,909.1–3,462.3 |
| DuckDB 2.0.0 | 1,166.1–1,186.7 | 4,430.0–4,588.6 | 8,689.7–9,525.5 |
| FoundationDB 7.3.79 | 2,069.0–2,269.1 | 2,190.6–2,331.2 | 3,954.6–4,751.1 |

## Four clients, create/read/update/delete cycles, 64-byte values

Medians of four run-level measurements. Latencies are in microseconds (µs).

| Engine | Operations/s | Create p50 | Read p50 | Update p50 | Delete p50 | Delete p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 7,547 | 599.2 | 62.1 | 591.6 | 588.3 | 1,239.2 |
| SQLite 3.49.1 (Rust) | 3,478 | 285.6 | 45.9 | 286.4 | 285.2 | 3,818.7 |
| SQLite 3.53.1 (Python) | 3,298 | 297.4 | 53.7 | 294.4 | 303.1 | 6,168.5 |
| PostgreSQL 18.6 | 4,591 | 786.5 | 273.4 | 784.1 | 768.6 | 1,831.4 |
| Redis 8.2.11 | 3,507 | 742.2 | 582.6 | 747.0 | 729.1 | 2,782.9 |
| MongoDB 8.0.34 | 2,579 | 1,332.8 | 640.4 | 1,303.8 | 1,279.1 | 2,559.9 |
| Turso / pyturso 0.8.3 | 1,247 | 647.5 | 172.2 | 635.0 | 645.7 | 55,774.0 |
| RocksDB 9.8.4 | 2,987 | 863.7 | 580.5 | 862.0 | 816.6 | 2,601.0 |
| LMDB 0.9.36 | 3,127 | 1,553.5 | 59.8 | 1,527.5 | 1,541.5 | 3,099.3 |
| DuckDB 2.0.0 | 929 | 4,573.8 | 1,331.7 | 4,388.9 | 2,668.1 | 5,565.1 |
| FoundationDB 7.3.79 | 1,887 | 1,928.0 | 709.4 | 1,909.0 | 1,933.8 | 4,474.4 |

Run-to-run ranges (minimum–maximum of the four run-level measurements):

| Engine | Operations/s range | Create p50 range | Delete p99 range |
| --- | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | 7,402.6–7,647.3 | 594.4–604.0 | 1,029.3–1,417.7 |
| SQLite 3.49.1 (Rust) | 3,058.2–3,562.1 | 281.8–291.4 | 3,537.6–8,758.7 |
| SQLite 3.53.1 (Python) | 3,054.0–3,566.8 | 279.4–316.8 | 2,167.1–18,845.0 |
| PostgreSQL 18.6 | 2,679.7–5,322.0 | 684.8–824.2 | 1,363.6–22,870.1 |
| Redis 8.2.11 | 3,105.5–4,277.8 | 661.7–774.5 | 1,527.8–4,236.3 |
| MongoDB 8.0.34 | 2,395.7–2,642.7 | 1,309.8–1,425.5 | 2,132.3–3,956.2 |
| Turso / pyturso 0.8.3 | 1,171.5–1,468.8 | 594.5–694.4 | 53,552.1–60,389.9 |
| RocksDB 9.8.4 | 2,952.7–3,391.1 | 754.5–941.8 | 1,846.6–3,595.8 |
| LMDB 0.9.36 | 3,029.4–3,292.9 | 1,496.5–1,591.4 | 2,455.5–4,416.8 |
| DuckDB 2.0.0 | 904.2–937.3 | 4,502.5–4,722.3 | 5,426.0–5,950.0 |
| FoundationDB 7.3.79 | 1,817.1–1,990.8 | 1,899.0–1,978.7 | 2,643.0–9,634.3 |

## Paired CPU-affinity calibration

Six repetitions per engine per mode, using the four-client 64-byte mixed workload. Both modes run after the memory cleanup and use the same quiet gate. “Unrestricted” means CPUs 0–4 available to each client/server; it does not mean unrestricted host access. CV is sample standard deviation divided by mean across the six run-level write-p50 measurements; six runs are insufficient for a strong statistical claim.

| Engine | Affinity | Operations/s | Write p50 (µs) | Write p99 (µs) | Write p50 CV | Write p99 range (µs) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| EMDA prototype 0.1.0 | unrestricted | 9,993 | 616.6 | 1,812.4 | 6.0% | 1,159.0–2,816.2 |
| EMDA prototype 0.1.0 | pinned | 10,570 | 630.3 | 1,422.5 | 4.7% | 1,119.2–5,170.3 |
| SQLite 3.49.1 (Rust) | unrestricted | 5,378 | 278.1 | 8,634.2 | 6.0% | 2,055.9–18,813.6 |
| SQLite 3.49.1 (Rust) | pinned | 5,050 | 287.5 | 18,855.4 | 4.1% | 1,359.0–33,810.9 |
| PostgreSQL 18.6 | unrestricted | 4,416 | 1,014.7 | 4,844.0 | 6.5% | 2,071.3–11,309.3 |
| PostgreSQL 18.6 | pinned | 6,707 | 774.4 | 1,844.4 | 8.6% | 1,570.2–6,148.2 |

## Recorded interference

These are cgroup/VM observations, including work performed by the benchmark itself. I/O wait during a durable-write test is expected and is not automatically external interference.

| Observation | Main matrix | Affinity calibration |
| --- | ---: | ---: |
| Successful runs | 264 | 36 |
| Failed runs | 0 | 0 |
| Pre-run quiet checks not satisfied | 0 | 1 |
| Runs with CPU quota throttling | 0 | 1 |
| Runs with cgroup memory-limit events | 0 | 0 |
| Runs with direct memory reclaim | 0 | 0 |
| Highest reported VM steal during a run | 15.65% | 21.98% |

