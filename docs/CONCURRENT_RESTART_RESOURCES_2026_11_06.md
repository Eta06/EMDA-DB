# Concurrent writes and clean-restart resources

On clean revision `6732a9d`, we measured the ordinary full-log writer and the
experimental checkpoint-backed writer on separate equivalently populated
fixtures. Each began with 128-byte keys, 64-byte values and 1,024 suffix keys
after a checkpoint. The 200,000-key fixture had a 42,828,358-byte log and a
29,200,100-byte checkpoint; the 1,000,000-key fixture had a 213,268,996-byte
log and a 146,000,100-byte checkpoint.

For each fixture and store, four fresh processes each ran four concurrent
workers performing 128 independent, unique-key synchronized writes. Before
each epoch, the process verified all earlier workload values and sampled 16
base and 16 original suffix values. It checked the new values after each epoch.
A fifth fresh process reopened the store and verified all 2,048 workload values
and the same base and suffix samples. Both backends passed all checks. These
are clean process restarts; they are not process-kill or power-loss tests.

| Base keys | Store | Median open, ms (range) | Maximum process peak RSS, MiB | Median epoch writes/s | Write p50 / p99, µs |
| ---: | --- | ---: | ---: | ---: | ---: |
| 200,000 | Full log | 112.6 (86.1–274.7) | 48.3 | 6,784 | 492 / 2,034 |
| 200,000 | Checkpoint | 130.7 (113.1–242.5) | 2.6 | 2,669 | 656 / 7,988 |
| 1,000,000 | Full log | 656.1 (434.1–1,073.2) | 232.0 | 6,172 | 483 / 3,628 |
| 1,000,000 | Checkpoint | 693.6 (557.4–723.6) | 3.1 | 3,142 | 417 / 6,563 |

Open medians use five fresh processes, including the final verification-only
process. Write throughput is the median of four epoch rates; latency
percentiles pool 2,048 individual writes per store and use nearest-rank p99.
The write timer includes starting the synchronized workers and joining them;
fixture setup and value checks are outside write timings. The process high-water
RSS includes workers and verification, but excludes kernel page cache and is
not a total-memory limit. The separate checkpoint-backed path used much less
sampled process RSS here; its measured throughput and p99 were lower and
higher respectively than the ordinary writer in these samples.

The [raw results](../benchmarks/results/2026-11-06-concurrent-restart-resources.json)
include all samples, fixture identifiers, source and binary hashes, process I/O
and noise telemetry. The Linux VM used an Intel Xeon Platinum 8573C, a four-CPU
cgroup quota and an overlay filesystem with unknown backing storage. Benchmark
processes used CPUs 0–3 and the driver CPU 4. Both initial quiet checks passed,
but individual processes recorded up to 12.62% VM steal; no CPU throttling or
direct reclaim was recorded. Kernel process `read_bytes` was zero in these
warm-cache runs. Read-like syscall counts and byte totals also include value
verification and instrumentation; they do not measure physical I/O.

Only one fixture per mode and size was used, with four write epochs. The base
data was sampled rather than exhaustively verified. Thread scheduling,
filesystem caching and VM interference affect these results. They do not
establish performance or resource bounds, cold-cache behavior, transactions,
crash recovery, tenant isolation, external-competitor performance or physical
power-loss durability. See the [benchmark instructions](BENCHMARKS.md#concurrent-writes-and-clean-restarts)
and the [remaining foundation work](FOUNDATION_FOLLOW_UP.md).
