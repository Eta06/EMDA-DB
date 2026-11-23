# Document queries and concurrent updates on `13d4b90`

## Concurrent acknowledged updates and indexed reads

The Linux ARM64 ext4 run used Rust 1.85.0 release binaries, four available
CPUs and 500 preloaded JSON documents with a unique email index. Each
client updated its own document and immediately queried the new email
through the index. There were 100 update/read cycles per client, four
rounds each at 1, 2, 4 and 8 clients, and three engines. Engine order
rotated. Both EMDA-DB paths used read-set transactions and synchronized
V3 witnesses. SQLite 3.49.1 used a unique expression index, WAL and
`synchronous=FULL`. The benchmark verifies each returned ID and JSON
value and checks every primary document and the index after clean reopen.
SQLite also runs `integrity_check`.

All 48 runs passed. The report contains 18,000 update samples and 18,000
indexed-read samples. Throughput includes both operations. The table
gives median throughput and its range across four runs, medians of
per-run nearest-rank p50 and p99 latencies, and median process peak RSS.

| Clients | Engine | Operations/s | Range | Update p50 / p99 µs | Indexed read p50 / p99 µs | Peak RSS KiB |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Store | 8,001 | 7,874–8,107 | 251.08 / 412.17 | 5.33 / 9.92 | 6,870 |
| 1 | CheckpointStore | 5,538 | 5,534–5,604 | 316.08 / 456.79 | 53.65 / 60.21 | 6,172 |
| 1 | SQLite | 6,543 | 6,465–6,656 | 287.48 / 459.48 | 2.42 / 4.71 | 6,538 |
| 2 | Store | 6,823 | 6,706–9,476 | 586.13 / 779.36 | 5.58 / 9.19 | 8,910 |
| 2 | CheckpointStore | 6,181 | 5,864–6,237 | 443.44 / 755.77 | 145.92 / 383.13 | 8,236 |
| 2 | SQLite | 6,064 | 5,896–7,547 | 284.88 / 449.63 | 2.50 / 5.81 | 8,670 |
| 4 | Store | 9,893 | 9,675–10,053 | 799.04 / 869.69 | 5.71 / 7.42 | 13,036 |
| 4 | CheckpointStore | 7,987 | 7,780–8,120 | 849.82 / 976.94 | 144.48 / 191.94 | 13,098 |
| 4 | SQLite | 7,578 | 7,362–9,000 | 183.46 / 261.94 | 2.52 / 7.10 | 12,940 |
| 8 | Store | 9,885 | 9,798–10,000 | 1,605.30 / 1,801.69 | 5.85 / 7.65 | 18,464 |
| 8 | CheckpointStore | 7,617 | 7,546–7,946 | 1,953.98 / 2,202.67 | 137.71 / 193.48 | 18,592 |
| 8 | SQLite | 9,304 | 9,228–9,524 | 166.06 / 430.42 | 2.38 / 7.58 | 20,418 |

At eight clients, Store maintained about the same median throughput as at
four clients while its update p99 roughly doubled. SQLite had the lowest
eight-client update p99. CheckpointStore's indexed-read p99 was about
193 µs at eight clients. The harness counted 49 returned SQLite update
retries and zero confirmed EMDA-DB read or update conflicts. The SQLite
busy handler can wait without returning a retry. VM-wide I/O wait
reached 20% in one run. The table does not attribute latency to any
particular code change. RSS includes setup and clean reopen verification.
The raw VM and cgroup counters include background activity.

Six [macOS smoke runs](macos-smoke.json.gz) also passed reopen checks. They
used 256 documents, 30 cycles per client, and one run per engine with one
or eight clients. The macOS runs establish behavior on that host. The
repeated Linux runs supply the comparative latency distributions above.

[Raw Linux ARM64 report](linux-arm64-contention.json.gz)

To repeat, build `document_contention_bench` with Rust 1.85.0 in release
mode and use [the contention runner](../../../scripts/document_contention_compare.py)
with `--clients 1,2,4,8 --rounds 4 --cycles 100 --documents 500` on the
filesystem you want to measure. The workload uses distinct documents,
closed-loop clients and clean recovery. Physical power loss and
independent off-host restore remain deferred.

## Single-client compound ranges across four engines

The [hosted measured run](https://github.com/Eta06/EMDA-DB/actions/runs/45276835294)
completed 36 verified runs on an x86-64 Linux 6.8.0 runner using ext4.
Each workload selected an equality prefix and a bounded range with
known matching IDs. The runner compared the complete ordered decoded
documents and checked them again after reopening or restarting.
Each of the six paths produced 50 samples in each of four fresh
rounds per case, for 3,600 timed reads.

| Documents / matches | Store planned | Store scan | SQLite index | SQLite scan | PostgreSQL | MongoDB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 / 10 | 34.98 / 45.12 | 181.32 / 202.96 | 16.66 / 27.05 | 43.98 / 53.70 | 248.84 / 340.64 | 516.09 / 554.91 |
| 1,000 / 10 | 38.30 / 54.61 | 1,749.90 / 1,870.43 | 16.34 / 31.39 | 324.47 / 355.66 | 314.53 / 363.38 | 515.37 / 547.91 |
| 1,000 / 100 | 345.60 / 378.70 | 1,792.00 / 1,876.60 | 104.07 / 136.70 | 406.05 / 452.59 | 657.64 / 717.67 | 699.45 / 729.90 |

Cells are medians of per-run nearest-rank p50 / p99 in microseconds.
The Store planned path read fewer candidates than the collection scan
and had lower median latency in all three cases. SQLite's in-process
indexed path had the lowest measured p99 in each case.

The local Rust process interleaved the two Store paths and two SQLite
3.49.1 paths. PostgreSQL 18.6 with psycopg 3.3.7 and MongoDB 8.0.34
with PyMongo 4.15.5 used separate disposable servers and loopback
transport. PostgreSQL used synchronous commit, fsync and full-page
writes. MongoDB used `w:1` with journal acknowledgement. SQLite
used WAL with `synchronous=FULL`. The timed workload reads prepared
state. It does not compare write durability across engines. The
runner pinned client and server work to separate CPUs and checked
the actual indexed plans. All 36 pre-run quiet windows passed the
runner's checks. VM-wide I/O wait reached 7.69%. The local process's
peak RSS ranged from 5,964 to 8,904 KiB across both in-process
engines and all phases. The report lacks separate server RSS.

The [raw hosted report](hosted-compound-range.json.gz) includes all
samples, engine and driver versions, image IDs, selected plans,
startup and verification outcomes, and noise counters. The two
comparisons leave concurrent MongoDB and PostgreSQL document writes
and comparable server-level resource attribution open.
