# Recovery measurements with fixture cache advice

We repeated the [SQLite and prototype clean-restart workload](SQLITE_RESTART_RESOURCES_2026_11_06.md)
on clean revision `16c8db6`, requesting Linux `POSIX_FADV_DONTNEED` for only
the disposable fixture files before each fresh process. The three benchmark
binaries had the same SHA-256 hashes as the warm-cache runs on `0ed6f4d`.
The hint is advisory: it does not prove eviction of every page or of caches
below the VM. All workload values and the sampled base/suffix values passed
the same clean-reopen checks as before.

For one million base keys, the table shows the two warm runs followed by
three runs requesting fixture eviction. Each figure is a median of five
fresh processes. SQLite's integrity check is separate from its lazy open and
is outside its open and write timers.

| Measurement | Warm first / repeat, ms | Eviction requested: first / repeat / more headroom, ms |
| --- | ---: | ---: |
| Ordinary prototype open | 598 / 430 | 499 / 469 / 465 |
| Checkpoint prototype open | 588 / 518 | 513 / 513 / 505 |
| SQLite open | 0.528 / 0.499 | 0.993 / 0.923 / 0.800 |
| SQLite integrity check | 223 / 229 | 6,982 / 7,234 / 7,030 |

The open and integrity operations do not provide equivalent verification:
the prototype validates its covered log during open, while SQLite's integrity
check validates SQLite structures and its application-level value checksums
were checked for sampled base/suffix and all workload values. Do not interpret
the numbers as a general database ranking or a comparison of identical
recovery guarantees. The data has numeric keys padded with a repeated byte
and repeated-byte values; it is not a representative production distribution.

After the eviction requests, Linux process `read_bytes` accounting reported
about 203–204 MiB per ordinary-store process, 343 MiB per checkpoint-store
process, and 224 MiB per SQLite process. The warm runs reported zero. Those
counters cover the complete process, including open, integrity/value checks,
writes and measurement overhead. They are kernel accounting estimates, not
proof of physical device reads; filesystem and host caches may still serve
them. The underlying storage for the VM's overlay was not independently
identified.

All initial quiet checks passed. During the first and second million-key
eviction runs, cgroup telemetry recorded 1,333 and 400 pages scanned during
direct reclaim respectively, concentrated in the first ordinary and checkpoint
processes. An environment interruption left our previous disposable fixture
behind; we requested eviction only for its known files. Cgroup memory use
fell by approximately 1 GiB immediately afterward, although that observation
does not establish why reclaim occurred. The third million-key run recorded
no direct reclaim. Across the completed eviction runs, VM steal reached 4.88%
in the separate 200,000-key case; no cgroup CPU throttling or memory-limit
events were recorded.

The raw records retain the [200,000-key run](../benchmarks/results/2026-11-06-cache-advice-200k.json.gz),
the million-key [first](../benchmarks/results/2026-11-06-cache-advice-million.json.gz),
[repeat](../benchmarks/results/2026-11-06-cache-advice-million-repeat.json.gz)
and [more-headroom](../benchmarks/results/2026-11-06-cache-advice-million-headroom.json.gz)
runs, and the [advice applied to our interrupted fixture](../benchmarks/results/2026-11-06-orphan-cache-advice.json).
The [interrupted partial run](../benchmarks/results/2026-11-06-cache-advice-interrupted.json.gz)
is retained separately and excluded from the table. These remain clean
restarts on one VM, not physical power-loss, off-host restore, or verified
cold-device tests.
