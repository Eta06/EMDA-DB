# SQLite comparison: concurrent writes and clean restarts

We compared clean revision `0ed6f4d` with SQLite 3.49.1 using separate,
equivalently populated fixtures of 200,000 and 1,000,000 base keys, 1,024
additional keys, 128-byte keys and 64-byte values. SQLite used WAL and
`synchronous=FULL`, with a separate connection per writer and an
application-level value checksum. The ordinary prototype uses V3 witnessed
synchronization; the experimental checkpoint-backed path uses the same
witnessed log with a disk-backed base index. These durability mechanisms and
formats differ and this test does not establish equal power-loss guarantees.
Keys use a numeric prefix and repeated-byte padding, and values repeat a
deterministic byte; the data does not represent production entropy.

Each fixture had four fresh-process write epochs with four workers issuing 128
independent, unique-key writes each. Every epoch checked prior workload
values and sampled base and suffix values, then checked the newly written
values. A fifth fresh process verified all 2,048 workload values and the base
and suffix samples. All checks passed. The entire run was repeated with new
fixtures. The table shows the first and repeat results for one million base
keys; write p50, p99 and p99.9 pool 2,048 samples per run. Throughput is the
median of four epoch rates, and RSS is the maximum process high-water mark
across five processes.

| Store | Writes/s, first / repeat | Write p50, µs | Write p99, µs | Write p99.9, µs | Peak RSS, MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ordinary prototype | 6,989 / 7,364 | 491 / 460 | 1,082 / 1,021 | 2,960 / 1,313 | 232 / 232 |
| Checkpoint prototype | 3,242 / 3,176 | 385 / 406 | 6,817 / 6,507 | 10,921 / 9,448 | 3.1 / 3.1 |
| SQLite | 2,376 / 2,497 | 252 / 269 | 1,017 / 1,358 | 179,948 / 129,477 | 6.0 / 6.0 |

SQLite's median write latency was lower in these samples; the ordinary
prototype had higher throughput. SQLite had 12 and 13 writes above 10 ms in
the respective million-key runs, including maxima of 180.0 and 180.2 ms.
Its p99.9 is based on only a few observations and is sensitive to scheduling;
the results do not establish a general ranking or explain the cause of the
long stalls. At 200,000 keys the same order held for median write latency
and throughput; the raw results include all distributions and both sizes.

Median ordinary-store open time was 598 / 430 ms at one million keys, and
checkpoint-backed open was 588 / 518 ms. SQLite open was 0.528 / 0.499 ms,
but the comparison is not like-for-like: SQLite opens lazily, while the
prototype checks the covered log during open. A separate SQLite
`integrity_check`, outside its open and write timers, took a median 223 / 229
ms. That check and the prototype's log validation are different operations.
SQLite application-level value checksums were checked for the sampled
base/suffix values and every workload value. SQLite statement preparation is included in
its epoch elapsed time, and its final connection close is outside the write
timer. Process I/O includes verification and other measurement work.

The [first](../benchmarks/results/2026-11-06-sqlite-restart.json.gz) and
[repeat](../benchmarks/results/2026-11-06-sqlite-restart-repeat.json.gz) records
contain all samples, binary and source hashes, fixture sizes and noise
telemetry. Both runs used the same Linux VM, an Intel Xeon Platinum 8573C,
a four-CPU cgroup quota and an overlay filesystem with unknown backing
storage. The workload used CPUs 0–3 and the driver CPU 4. All initial quiet
checks passed; individual runs reported up to 2.06% VM steal, with no recorded
cgroup CPU throttling, memory-limit events or direct reclaim. Kernel process
`read_bytes` was zero for these warm-cache samples. Process RSS excludes OS
page cache, and log/checkpoint/database file sizes are not total disk usage.

Four epochs and two fixtures per store and size do not establish latency, memory or
recovery bounds. These are clean process restarts, not crash, physical
power-loss or off-host restore tests. They do not compare PostgreSQL, MongoDB
or the other engines on this larger workload. For reproduction and settings,
see [concurrent writes and clean restarts](BENCHMARKS.md#concurrent-writes-and-clean-restarts).
A [follow-up](FILE_CACHE_RECOVERY_2026_11_06.md) records advisory fixture
eviction, kernel read accounting and observed memory-reclaim interference.
