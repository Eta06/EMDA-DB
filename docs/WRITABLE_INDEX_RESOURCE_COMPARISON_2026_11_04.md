# Writable index resource measurements

The experimental checkpoint-backed writer trades a disk-backed base index for a bounded in-memory suffix. This local measurement makes that tradeoff visible; it does not establish a total-memory limit or a competitor ranking.

The [benchmark](../examples/index_memory_bench.rs) creates witnessed stores with 128-byte keys and 64-byte values, checkpoints the initial keys, then commits 1,024 additional keys. It compares five modes in separate, freshly started processes: ordinary full-log recovery; ordinary recovery using a checkpoint while loading its live index; the writable `CheckpointStore`; a historical read-only checkpoint view; and a recovered read-only view. The historical view intentionally does not include or validate the suffix and therefore has different semantics. Each open is followed by 16 verified base-key reads and, except for the historical view, 16 verified suffix-key reads. The source log, witness, checkpoint and V3 metadata hashes are checked before and after the measurements.

Each figure below is the median of four processes. Lookup figures are medians of the 64 sampled base reads per mode. Peak RSS is Linux process high-water RSS after open and sampled reads; it excludes OS page cache and is not a quota. The 200,000-key fixture had a 42,828,358-byte log and a 29,200,100-byte checkpoint, including the separate 1,024-key suffix in the log.

| Filesystem, initial keys | Mode | Open (ms) | Peak RSS (MiB) | Base lookup (µs) |
| --- | --- | ---: | ---: | ---: |
| tmpfs, 10,000 | Full-log store | 4.03 | 4.13 | 1.01 |
| tmpfs, 10,000 | Checkpoint-backed writer | 5.68 | 1.92 | 29.47 |
| tmpfs, 200,000 | Full-log store | 89.03 | 47.76 | 3.27 |
| tmpfs, 200,000 | Checkpoint-backed writer | 109.31 | 1.93 | 31.59 |
| workspace overlay, 10,000 | Full-log store | 5.16 | 4.13 | 1.07 |
| workspace overlay, 10,000 | Checkpoint-backed writer | 5.65 | 1.88 | 29.26 |
| workspace overlay, 200,000 | Full-log store | 94.38 | 47.77 | 3.45 |
| workspace overlay, 200,000 | Checkpoint-backed writer | 104.32 | 1.94 | 32.18 |

On these warm-cache fixtures, the checkpoint-backed writer's sampled process RSS was much lower at 200,000 keys, but its base-key reads and open were slower than full-log recovery. The checkpoint-backed path returned about 69.22 MiB through read-like system calls over open and the sampled reads at 200,000 keys, compared with about 40.85 MiB for full-log recovery; these process counters include measurement overhead and are not physical I/O. Kernel `read_bytes` accounting reported zero for these warm runs. The ordinary checkpoint-assisted store's median open was 140.91 ms on tmpfs and 140.75 ms on the overlay, with about 47.6 MiB peak RSS; complete results also include both read-only views and suffix lookup samples.

CPU affinity and a quiet check were used on an Intel Xeon Platinum 8573C VM with a four-CPU cgroup quota. Several short measurements still reported VM steal; the largest was 16.7% in a 10,000-key case. The overlay backing storage is unknown. These are small, short, warm-cache, single-client measurements; they do not cover concurrent writes, cold cache, independent hardware, physical power loss, off-host restore, total memory or recovery-time bounds. Values and distributions will differ with key shape and workload.

The [tmpfs](../benchmarks/results/2026-11-04-writable-index-resource-tmpfs.json.gz) and [overlay](../benchmarks/results/2026-11-04-writable-index-resource-overlay.json.gz) records contain every sample, source and binary hashes, fixture identities, I/O counters and VM/cgroup telemetry. The [driver](../scripts/index_memory_compare.py) saves a partial report if a run is interrupted. The broader resource and recovery work remains open in the [foundation checklist](FOUNDATION_FOLLOW_UP.md).
