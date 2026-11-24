# Million-key recovery with four writers

Each backend starts with 1,048,576 eight-byte keys and deterministic
96-byte values. Four writers submit 32,768 mutations in 256 durable
batches per epoch. Four epochs rotate backend order. Every epoch opens
each backend in a fresh process, verifies a point read, and uses a separate
process to compare every key and value in an ordered scan against the
workload model. SQLite runs WAL with `synchronous=FULL` and per-value CRCs.
Its reopen does less integrity work than the EMDA paths. SQLite uses a
connection per writer. Retained workers submit batches through a mutex
around the single authority owner.

## macOS APFS

All 51 phases passed on macOS ARM64 with Rust 1.85.0 at `f1d7dee`.
The checkpoint rolled over in every epoch and the retained catalog rotated
once. Ready time includes open and a checked point read. Write time includes
synchronous maintenance. Timings are medians across four epochs. Memory
and sampled disk figures are phase maxima. The macOS workers ran uncapped.

| Engine | Ready wall ms | Ready CPU ms | Reopen peak RSS MiB | Mutation peak RSS MiB | Write epoch ms | Verified scan ms | Sampled allocation peak MiB | Rotations | Rollovers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ordinary | 252.97 | 247.45 | 128.06 | 128.61 | 1351.99 | 301.50 | 144.24 | 0 | 0 |
| checkpoint | 109.66 | 102.55 | 5.20 | 7.31 | 2073.76 | 321.01 | 222.22 | 0 | 4 |
| retained | 100.89 | 89.53 | 7.03 | 7.56 | 5213.31 | 169.24 | 358.94 | 1 | 0 |
| sqlite | 1.54 | 0.62 | 3.23 | 5.75 | 51.64 | 159.83 | 154.66 | 0 | 0 |

## Hosted Linux memory ceiling

All 51 phases passed at `9dd1fb5` on hosted Linux. The 24 mutation,
reopen, and verify phases for checkpoint and retained workers ran with a
hard and soft 48 MiB process address-space limit. Fixture setup, initial
cutover, and the ordinary and SQLite controls ran uncapped. The checkpoint
rolled over four times and the retained catalog rotated once under the
ceiling. All 16 ordered scans across the four backends verified
between 1,045,597 and 1,045,598 live entries per epoch.

| Engine | Ready wall ms | Ready CPU ms | Reopen peak RSS MiB | Mutation peak RSS MiB | Write epoch ms | Verified scan ms | Sampled allocation peak MiB | Rotations | Rollovers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ordinary | 414.98 | 414.69 | 131.30 | 131.53 | 57.58 | 361.74 | 139.78 | 0 | 0 |
| checkpoint | 157.54 | 157.41 | 6.10 | 7.36 | 1059.63 | 373.92 | 217.67 | 0 | 4 |
| retained | 186.44 | 186.25 | 7.31 | 7.43 | 230.02 | 268.74 | 358.95 | 1 | 0 |
| sqlite | 0.80 | 0.80 | 4.96 | 6.82 | 68.44 | 280.20 | 137.73 | 0 | 0 |

The [Linux memory archive](linux-memory-artifacts.tar.gz) contains the
[completed memory job] metadata and log, command and binary identity,
phase logs, measurements, the CI verifier, and the summary script.

[completed memory job]: https://github.com/Eta06/EMDA-DB/actions/runs/45393021332/job/138444179727

## Reproduction and limits

Use the [phase runner](../../../examples/authorized_recovery_bench.md)
with arguments `1048576 4 --sqlite --wide-suffix --checkpoint --writers 4`.
The [macOS archive](macos-artifacts.tar.gz) contains the phase logs, attempt
record, binary hash, measurements, and summary script. The clean isolated
target build followed a shared-target build attempt that linked a stale
baseline library. The archive records both build outcomes.

A fresh process still sees the OS page cache. Process RSS excludes cache
and total project memory. The retained disk samples include the source
archive and authority files, and can miss short allocation peaks. This run
covers clean reopens and the stated workload only. Physical power-loss
testing and independent off-host restore remain deferred.
