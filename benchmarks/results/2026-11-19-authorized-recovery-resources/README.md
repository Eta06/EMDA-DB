# Recovery resources through authority-backed rotation

Each run seeded 131,072 keys on both backends, applied 32,768 ordered puts
and deletes in 256 durable batches per backend, and reopened in a fresh
process after each of eight epochs. After every reopen, a separate
process compared the entire ordered key/value state with the workload
model. All comparisons passed. Both backends ended each run with 130,699
live keys. The retained store ended at a 59,592-byte suffix with the
1-million work budget and a 611,274-byte suffix with the 60-million budget.
The ordinary log ended at 20,227,516 bytes in both profiles.

## Measured results

Reopen and write time are medians of eight phases. Each write phase
submits 4,096 operations in 32 synchronized batches. RSS is the maximum
process RSS observed among those phases. Disk peak is the maximum
sampled allocated space during the write phases. The retained samples
include the authority directory and the original log and witness archive.

| Platform | Suffix work budget | Backend | Rotations | Reopen ms | Reopen RSS MiB | Write s | Write RSS MiB | Sampled disk peak MiB | Final disk MiB |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS APFS | 1M | Ordinary | 0 | 23.62 | 17.84 | 0.172 | 18.14 | 19.63 | 19.63 |
| macOS APFS | 1M | Retained | 42 | 193.06 | 2.45 | 12.876 | 3.27 | 45.52 | 29.70 |
| macOS APFS | 60M | Ordinary | 0 | 32.55 | 17.86 | 0.172 | 18.16 | 20.14 | 20.14 |
| macOS APFS | 60M | Retained | 5 | 195.44 | 3.17 | 3.110 | 4.50 | 44.97 | 30.87 |
| Linux ext4 | 1M | Ordinary | 0 | 20.13 | 20.13 | 0.009 | 20.17 | 19.30 | 19.30 |
| Linux ext4 | 1M | Retained | 42 | 134.41 | 2.28 | 6.604 | 2.78 | 43.81 | 29.72 |
| Linux ext4 | 60M | Ordinary | 0 | 21.32 | 19.05 | 0.052 | 19.66 | 19.30 | 19.30 |
| Linux ext4 | 60M | Retained | 5 | 166.21 | 4.84 | 1.554 | 5.02 | 44.30 | 30.25 |

The 1-million work cap triggers rotation before the 640 KiB soft
threshold for this key distribution. The 60-million configuration
allowed larger suffixes and cut the number of rotations from 42 to 5
on each platform. The retained path had lower reopen RSS and higher
reopen latency in all four comparisons. The shorter final suffix did
not make reopen cheaper than full-log recovery in this workload.

## Reproduction and limitations

See the [runner instructions](../../../examples/authorized_recovery_bench.md)
and [environment and binary hashes](environment.txt). The two platforms
ran the same storage engine sources from `36d3ef7`. The 1M profiles used
the [archived initial harness](initial-harness.tar.gz). The 60M profiles
used `e58dfbf`, which adds a benchmark work-budget override and boxes
both backend handles. The paired ordinary control ran within each
profile. The `large` files contain the 1M measurements and the
`byte-threshold` files contain the 60M measurements. Each profile has
50 JSONL records and 50 raw subprocess logs.

These warm-cache runs represent one macOS ARM64 host and its Linux
ARM64 VM. Work on the host and guest overlapped. The authority directory
sat on the same local filesystem as the store. Process RSS excludes
filesystem caches and kernel memory. Disk sampling at roughly 25 ms
can miss staging peaks and cannot measure filesystem metadata or the
underlying allocations of copy-on-write extents. These are measured
workloads within configured budgets. They do not establish total
memory or disk caps, deployment latencies, or behavior after physical
power loss or independent off-host restore.
