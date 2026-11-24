# Concurrent durable document updates across five engines

The [measured workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45302643173)
ran revision `6cea60f` on hosted x86-64 Linux 6.8.0 with ext4. Each engine
preloaded 500 documents with the same primary IDs and unique email index.
Clients replaced distinct documents 100 times apiece, looking up and
verifying the acknowledged replacement by its new email after each write.
Each run checked the complete primary state and every current indexed
mapping after an embedded reopen or a clean server restart.

All 100 runs passed, producing 37,500 update and 37,500 lookup samples.
There were five rounds per engine and client count, with a different
engine order each round. The table gives median throughput and its range,
and medians of per-run nearest-rank p50 and p99 in microseconds.

| Clients | Engine | Operations/s | Range | Update p50 / p99 µs | Indexed read p50 / p99 µs |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Store | 8,748 | 6,543–9,231 | 189.1 / 390.4 | 12.9 / 24.3 |
| 1 | CheckpointStore | 4,175 | 3,461–4,518 | 331.6 / 869.6 | 100.8 / 126.9 |
| 1 | SQLite | 7,100 | 3,420–7,702 | 256.7 / 418.6 | 8.1 / 19.4 |
| 1 | PostgreSQL | 4,448 | 4,203–4,750 | 243.9 / 1,546.8 | 139.8 / 279.5 |
| 1 | MongoDB | 2,408 | 1,546–2,472 | 449.8 / 1,196.4 | 336.5 / 558.8 |
| 2 | Store | 6,403 | 2,527–6,672 | 570.0 / 1,002.2 | 18.6 / 38.6 |
| 2 | CheckpointStore | 5,318 | 1,670–5,444 | 560.5 / 1,076.4 | 228.5 / 258.9 |
| 2 | SQLite | 5,858 | 5,675–6,475 | 249.7 / 1,246.8 | 7.8 / 22.0 |
| 2 | PostgreSQL | 7,131 | 6,520–7,463 | 309.6 / 1,682.9 | 177.4 / 385.2 |
| 2 | MongoDB | 4,006 | 1,672–4,283 | 497.3 / 1,831.3 | 383.9 / 556.3 |
| 4 | Store | 4,255 | 3,700–6,626 | 1,163.5 / 16,644.7 | 18.6 / 33.9 |
| 4 | CheckpointStore | 4,414 | 3,972–4,738 | 1,436.0 / 3,280.9 | 231.1 / 347.8 |
| 4 | SQLite | 6,212 | 3,419–7,473 | 247.7 / 8,409.9 | 8.1 / 23.6 |
| 4 | PostgreSQL | 9,782 | 5,030–10,694 | 416.9 / 2,423.8 | 258.0 / 585.0 |
| 4 | MongoDB | 4,404 | 3,816–4,846 | 799.3 / 5,345.2 | 692.5 / 1,605.5 |
| 8 | Store | 5,993 | 3,911–6,571 | 2,354.3 / 12,183.4 | 18.8 / 35.8 |
| 8 | CheckpointStore | 4,366 | 2,860–4,511 | 3,095.9 / 19,664.0 | 231.5 / 361.4 |
| 8 | SQLite | 6,845 | 4,365–7,440 | 227.5 / 33,773.5 | 7.6 / 28.9 |
| 8 | PostgreSQL | 7,405 | 4,035–7,464 | 984.5 / 4,141.6 | 899.9 / 2,663.3 |
| 8 | MongoDB | 3,554 | 2,152–3,657 | 2,042.0 / 13,100.0 | 1,809.0 / 4,793.4 |

Both EMDA paths used synchronized V3 witnesses. SQLite 3.49.1 used
WAL with `synchronous=FULL`. PostgreSQL 18.6 used JSONB, synchronous
commit, fsync and full-page writes. MongoDB 8.0.34 acknowledged writes
with `w:1` and journaling. The server measurements include psycopg
3.3.7 or PyMongo 4.15.5, loopback and result decoding. The embedded paths
ran in-process on CPUs 0–1. The server clients shared that CPU set and
their containers ran on CPUs 2–3. The workload has one writer per
document. SQLite returned 99 update retries across the matrix. The
EMDA paths returned zero confirmed conflicts.

## Resource snapshots

All 40 server runs captured separate server cgroup CPU, memory and I/O
before and after timed client work. At eight clients, median timed
client CPU was 307 ms for PostgreSQL and 562 ms for MongoDB, with
server cgroup CPU deltas of 143 ms and 299 ms. End-of-work server
cgroup memory was 91.4 and 96.1 MiB. End-of-work Python client RSS was
52.3 and 53.2 MiB. Server cgroup memory includes file cache.
Embedded subprocess measurements cover the entire case, including
setup and verification. At eight clients, median embedded peak RSS
was 7,260 KiB for Store, 6,180 KiB for CheckpointStore and 6,092 KiB
for SQLite. Their respective median whole-process CPU times were
160, 451 and 60 ms. The two resource scopes are recorded separately.

All pre-run quiet checks passed, but VM-wide I/O wait reached 24.68%
during a run. The throughput ranges and raw telemetry show the
variability. Results cover clean restart of this fixture with the
stated acknowledgement settings and adapter costs.

## Raw cases

The [archive](raw-artifacts.tar.gz) includes the measured contention run,
the [hosted smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45302624818),
their compound-range and large-value process-recovery cases, the
[Linux CI benchmark smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45302624850),
and the 12-case embedded Linux ARM64 debug smoke. The smoke cases
establish functionality. The archive includes 51 command attempts,
29 sample reports, checksums and the aggregation script. See the
[benchmark instructions](../../../docs/BENCHMARKS.md#concurrent-document-updates-and-indexed-reads)
for reproduction.
