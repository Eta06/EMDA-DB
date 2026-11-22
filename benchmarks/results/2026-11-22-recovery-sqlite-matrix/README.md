# Clean recovery and durable-write resources against SQLite

On ARM64 macOS APFS and Linux ext4, the runner compared an ordinary EMDA
store, an authority-backed retained store cut over at its original path,
and SQLite 3.49.1. It seeded 16,384, 131,072 and 262,144 keys with identical
8-byte keys and 96-byte values. Each scenario applied six epochs of 4,096
ordered mutations in 32 durable batches per engine. Each epoch used a
different engine order. The first point read and all 108 full live-state
comparisons passed. Each retained scenario rotated four times.

## Results

Ready time includes opening and verifying one point. Scan time excludes
open and includes comparison of every ordered key and value against the
model. Write time covers an epoch of 32 durable batches and includes any
rotation. Time and process CPU columns are six-epoch medians. RSS and
sampled allocated disk columns are maxima across the six corresponding
reopen and mutation processes, respectively.

| Platform | Initial keys | Engine | Ready wall ms | Ready CPU ms | Reopen RSS MiB | Verified scan ms | Write wall ms | Write CPU ms | Write disk MiB |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS | 16,384 | EMDA ordinary | 38.61 | 7.25 | 4.25 | 11.92 | 486.70 | 12.57 | 5.05 |
| macOS | 16,384 | EMDA retained | 142.12 | 9.65 | 2.89 | 111.26 | 3742.75 | 147.82 | 6.10 |
| macOS | 16,384 | SQLite | 22.72 | 1.61 | 3.17 | 8.40 | 132.81 | 11.35 | 7.46 |
| macOS | 131,072 | EMDA ordinary | 46.26 | 24.05 | 18.00 | 44.98 | 321.01 | 6.97 | 19.03 |
| macOS | 131,072 | EMDA retained | 54.49 | 21.19 | 2.94 | 457.01 | 1850.82 | 118.26 | 44.27 |
| macOS | 131,072 | SQLite | 1.46 | 1.00 | 3.17 | 24.81 | 5.94 | 3.84 | 22.13 |
| macOS | 262,144 | EMDA ordinary | 80.88 | 45.88 | 33.70 | 93.70 | 366.08 | 6.52 | 34.76 |
| macOS | 262,144 | EMDA retained | 50.27 | 30.31 | 2.92 | 99.06 | 1861.60 | 190.60 | 87.94 |
| macOS | 262,144 | SQLite | 1.20 | 0.86 | 3.17 | 48.45 | 39.27 | 3.95 | 38.71 |
| Linux | 16,384 | EMDA ordinary | 13.83 | 10.24 | 6.11 | 7.26 | 375.04 | 11.33 | 4.70 |
| Linux | 16,384 | EMDA retained | 14.73 | 3.86 | 3.21 | 12.95 | 1657.55 | 71.35 | 6.10 |
| Linux | 16,384 | SQLite | 1.94 | 1.92 | 3.96 | 10.58 | 234.11 | 13.22 | 6.08 |
| Linux | 131,072 | EMDA ordinary | 48.00 | 43.45 | 20.47 | 39.69 | 269.17 | 7.38 | 18.39 |
| Linux | 131,072 | EMDA retained | 44.06 | 31.23 | 3.21 | 24.01 | 1998.69 | 115.96 | 44.30 |
| Linux | 131,072 | SQLite | 1.02 | 1.01 | 3.96 | 28.64 | 104.18 | 5.51 | 20.73 |
| Linux | 262,144 | EMDA ordinary | 48.16 | 46.60 | 36.45 | 74.50 | 158.90 | 5.20 | 34.04 |
| Linux | 262,144 | EMDA retained | 53.39 | 35.94 | 4.66 | 45.78 | 2064.48 | 177.68 | 87.95 |
| Linux | 262,144 | SQLite | 0.76 | 0.75 | 3.96 | 54.76 | 156.95 | 5.66 | 37.43 |

At 262,144 keys, retained reopen used 91% less peak process RSS than
ordinary reopen on macOS and 87% less on Linux. It used less startup CPU
than ordinary replay at that size. Its write phase still used about
29 times the ordinary store's median CPU on macOS and 34 times on Linux.
The retained disk peak includes the original-path archive, both
generations during rotation, and the authority directory. SQLite had the
lowest median first-read time in every configuration. At the largest
size its verified live-state scan was faster than retained on macOS
and slower on Linux. These results identify rotation/write cost and
startup verification as separate optimization targets.

## Reproduction and scope

Use the [runner instructions](../../../examples/authorized_recovery_bench.md)
with `--sqlite` and `EMDA_BENCH_SUFFIX_WORK=60000000`. The harness revision
was `432ef76` and the storage engine revision was `e0fded0`. The
[raw archive](raw-results.tar.gz) contains all 336 phase logs and
measurements, environment details, binary hashes and the table data.
SQLite used WAL with `synchronous=FULL`, the default automatic checkpoint,
and no checkpoint on last-connection close. Every SQLite reopen saw a
WAL of approximately 4 MiB. SQLite checks per-value CRCs at read time.
EMDA verifies the full retained base or complete ordinary log at open.
The retained authority is local to this experiment and still performs
its separate durable updates.

These are fresh-process, warm-cache trials on one macOS host and its Linux
VM, with no controlled host quiet windows or cache eviction. macOS
measurements overlapped development test activity. Several wall-time
samples varied substantially, especially during file synchronization.
RSS excludes OS cache and other processes. The 25 ms disk sampler can
miss short-lived stages. These tests exercised clean reopens and live
state, with no physical power-loss test or independent off-host restore.
