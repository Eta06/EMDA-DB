# Independent-read admission

`enable_direct_read_limit` optionally limits concurrent independent key-value `get`, `get_many` and `scan` calls on `Store` and `CheckpointStore`. The default is disabled and does not track reads. Enabling requires exclusive mutable access to the store, so no untracked read can still be active during the transition; `OnlineStore` exposes the same operation before the handle is shared. Once enabled, `set_active_direct_read_limit` can reconfigure the limit and refuses reductions below active admissions. The maximum is 65,535 and zero rejects new direct reads with `WouldBlock`. `direct_read_status` reports the limit and active count, including lock and I/O waiters; both are `None` when disabled. A guard releases an admission on return or unwind. Supported in-process compaction handoffs preserve the setting; reopening disables it.

The limit applies only to these direct key-value calls. Transactions, snapshots, document APIs, maintenance, caller-owned buffers, other stores and the internal reads needed by compare-exchange and increment are outside it. Saturation or invalid input can mask a previous uncertain write and does not establish that the store is healthy. Accepted reads retain their existing health, consistency and corruption checks. Checkpoint-store invalid point/scan arguments are now checked before the health lock, changing error precedence. This is not a total-memory limit, persistent project quota, security boundary, fairness guarantee or completion-time guarantee.

A [280-run stress campaign](../benchmarks/results/2026-11-05-opt-in-read-stress.json) repeated seven admission, blocked-I/O, checkpoint-lock, transaction-independence and compaction-handoff tests 20 times each on tmpfs and the workspace overlay. All passed. The earlier always-on prototype also passed its [280-run stress campaign](../benchmarks/results/2026-11-05-always-on-read-stress.json). Three [120-second model campaigns](../benchmarks/results/2026-11-05-opt-in-read-fuzz.json) exercised 18,500 admission, 11,005 checkpoint and 14,011 controlled-transaction generated inputs without a finding. These finite tests do not exhaust thread schedules or storage failures. The workspace overlay's backing device was not independently verified; none of these tests simulates physical power loss or independent off-host restore.

## Read overhead

An initial always-on counter was compared against revision `70684e1` in [tmpfs](../benchmarks/results/2026-11-05-direct-read-always-on-tmpfs.json) and [workspace overlay](../benchmarks/results/2026-11-05-direct-read-always-on-overlay.json). In the four-client read-only cases its median per-run throughput was approximately 13% and 16% lower respectively. An experiment isolating the counter on a cache line had mixed results: tmpfs four-client read-only throughput improved while the same overlay workload regressed, and several other cases moved in opposite directions. It was not retained; the [patch](../benchmarks/experiments/direct-read-counter-alignment.patch.gz) and [tmpfs](../benchmarks/results/2026-11-05-direct-read-alignment-tmpfs.json)/[overlay](../benchmarks/results/2026-11-05-direct-read-alignment-overlay.json) measurements are retained.

The accepted design makes the counter opt-in. The [initial](../benchmarks/results/2026-11-05-opt-in-read-tmpfs.json) [runs](../benchmarks/results/2026-11-05-opt-in-read-overlay.json) and longer [tmpfs](../benchmarks/results/2026-11-05-opt-in-read-tmpfs-repeat.json)/[overlay](../benchmarks/results/2026-11-05-opt-in-read-overlay-repeat.json) repeats compare the previous engine, the new default with the counter disabled, and the new engine with a 65,535-call limit enabled. They use one or four closed-loop clients on 16 existing keys and 64-byte values, with read-only or 50% write workloads. Writes use the same synchronized acknowledgement path. Every run checks returned data and sampled values after reopening. In the repeat, read-only runs use 60,000 operations per client; mixed runs use 4,000 on tmpfs and 3,000 on the overlay. There are five rounds on tmpfs and four on the overlay. Each cell below is the **median of per-run read p50 / p99 microseconds** and the median overall operations per second; raw per-run summaries are retained, not individual samples.

| Filesystem | Clients / writes | Previous p50 / p99 | Disabled p50 / p99 | Enabled p50 / p99 | Previous / disabled / enabled ops/s |
| --- | --- | ---: | ---: | ---: | ---: |
| tmpfs | 1 / 0% | 0.3 / 0.4 | 0.3 / 0.4 | 0.3 / 0.4 | 2,477,888 / 2,382,977 / 2,402,670 |
| tmpfs | 1 / 50% | 0.4 / 0.7 | 0.4 / 0.6 | 0.4 / 0.7 | 458,368 / 478,356 / 472,553 |
| tmpfs | 4 / 0% | 0.7 / 15.3 | 0.7 / 14.2 | 0.8 / 8.6 | 3,110,598 / 2,970,215 / 3,495,004 |
| tmpfs | 4 / 50% | 0.8 / 2.5 | 0.9 / 2.5 | 0.8 / 2.4 | 192,625 / 157,784 / 200,478 |
| overlay | 1 / 0% | 0.4 / 0.45 | 0.4 / 0.4 | 0.4 / 0.4 | 2,150,696 / 2,174,375 / 2,086,962 |
| overlay | 1 / 50% | 1.3 / 5.05 | 1.3 / 3.75 | 1.3 / 4.2 | 6,816 / 6,803 / 7,156 |
| overlay | 4 / 0% | 0.8 / 1.1 | 0.8 / 1.1 | 0.9 / 1.2 | 3,457,283 / 3,408,628 / 3,298,548 |
| overlay | 4 / 50% | 1.2 / 6.5 | 1.4 / 9.35 | 1.5 / 8.3 | 14,471 / 14,166 / 14,187 |

Results are mixed. In particular, the four-client tmpfs mixed median fell from about 192,625 to 157,784 operations/s with the limit disabled in the repeat; individual runs varied widely, and the enabled variant was faster in that sample. The overlay mixed read p99 also increased. The data does not establish the cause or a general speedup. Both binaries use the same basic workload, but the candidate benchmark example adds an enabled-limit mode, so build and code-layout differences are another confounder. Every repeat quiet-window check passed and no measured run recorded cgroup CPU throttling; the tmpfs repeat observed up to 6.67% VM steal and 8.70% I/O wait, and the overlay repeat up to 17.5% steal and 14.75% I/O wait. Telemetry includes setup, verification and other VM activity. These are warm local comparisons on one VM, not a competitor ranking, cold-cache result, checkpoint-store comparison, quota proof or power-loss test.

With independently built release binaries, reproduce the paired comparison with:

```sh
python3 scripts/direct_read_compare.py --directory /path/on/test/filesystem \
  --before /path/to/before-kv-bench --after /path/to/after-kv-bench \
  --baseline-revision 70684e1 --output /tmp/read-comparison.json \
  --rounds 4 --operations 3000 --read-operations 60000 --include-limited
```
