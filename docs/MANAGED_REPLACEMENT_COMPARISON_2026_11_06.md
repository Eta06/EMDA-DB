# Managed checkpoint replacement: local comparison

We compared baseline `dd5cfd0` with `46659ce`, which avoids a second
parent-directory sync after the managed selection or automatic-configuration
file has already been replaced and its parent successfully synchronized.
The required publication sync remains in place. If it fails, cleanup retains
its existing retry and ownership-marker behavior; callers still receive an
uncertain publication outcome. No disk format changes.

Separately built release binaries used identical benchmark example sources and
the same verified fixtures. Each variant ran 15 times per case in alternating
order. The timer includes checkpoint validation, synchronization and managed
publication; digest and sampled clean-reopen checks happen outside it. All
checkpoint digests matched and the sampled reopened values passed. Each
fixture used 32-byte keys, 64-byte values and 64 tail keys.

| Base keys | Baseline median (range), ms | Changed median (range), ms |
| ---: | ---: | ---: |
| 100 | 4.594 (3.637–6.791) | 4.391 (3.599–6.794) |
| 1,000 | 4.937 (4.241–6.604) | 4.526 (4.245–6.959) |

The [raw results](../benchmarks/results/2026-11-06-managed-replacement-paired.json)
contain the individual samples, source and binary hashes, revisions and noise
telemetry. This ran on a Linux VM with an Intel Xeon Platinum 8573C, a four-CPU
cgroup quota and an overlay filesystem. The benchmark and driver were pinned to
different CPUs. Both initial quiet checks passed; individual samples recorded
up to 16.67% VM steal, without observed cgroup CPU throttling or direct reclaim.
These warm, single-client samples overlap and do not establish a general
speedup or bound handoff pauses.

A separate [traced diagnostic](../benchmarks/results/2026-11-06-managed-replacement-syscalls.json)
counted 27 `fsync` calls before and 26 after for one fixture, including setup,
validation and reopen; the resulting digest matched. System-call tracing
changes scheduling, so its timings are not latency measurements. Neither
comparison tests physical power loss or compares SQLite or other databases;
this change does not address the separately observed SQLite write-latency gap.

To reproduce, build the two revisions separately with identical examples and run
the following on the desired test filesystem. Preserve the complete output:

```sh
python3 scripts/managed_rollover_before_after.py --directory /path/to/test-filesystem \
  --output /path/to/new-results.json --baseline-binary /path/to/baseline/checkpoint_rollover_bench \
  --baseline-revision dd5cfd0 --binary /path/to/changed/checkpoint_rollover_bench \
  --prepare-binary /path/to/changed/index_memory_bench --rounds 15 \
  --keys 100,1000 --key-bytes 32 --tail-keys 64
```
