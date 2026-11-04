# Shared snapshot comparison, 2026-11-04

This report measures one optimization, not completion of the database. Ordinary-store snapshots now retain immutable index roots, while writes prepare a copy when a snapshot still shares the root. The logical per-view and aggregate snapshot limits, synchronized write acknowledgement, conflict validation and failure behavior remain in force. This is not a total-memory limit: a write may copy the complete index, and old roots remain until their views are released.

We compared the engine before the change with revision `90a389b762c7a0b12b90157101bea1fba603bd01`. The document baseline binary was built from `3e4c88422cafed6bf06fbef453e0d5ac337bed0c`; the contention baseline was built from `a034286788a99c7b40346972834aea24bfd7d593`. The intervening changes did not change the engine implementation, and the raw reports contain the binary hashes and commands. Each benchmark checked its results and reopened the store; all 70 runs completed without a validation failure.

## Embedded document operations

A single client preloaded a collection and unique email index, then ran 200 measured create, read, update, indexed-query and delete cycles, after warmup. Five rounds rotated the before, after and SQLite 3.49.1 binaries at each size. The table pools 1,000 samples per size and variant. Values are **p50 / p99 microseconds**, calculated with nearest-rank percentiles. The SQLite adapter returns JSON text; the prototype also decodes JSON and validates its schema, so read work is not identical.

| Baseline documents | Indexed query before | Indexed query after | SQLite indexed query |
| ---: | ---: | ---: | ---: |
| 0 | 8.01 / 35.44 | 7.83 / 41.06 | 3.55 / 26.00 |
| 100 | 43.92 / 138.61 | 8.32 / 48.97 | 3.71 / 12.04 |
| 1,000 | 567.50 / 927.95 | 13.92 / 55.04 | 3.86 / 9.52 |
| 3,000 | 2,330.62 / 3,538.06 | 23.65 / 107.57 | 3.81 / 26.09 |

At 3,000 documents, this sample's indexed-query median fell about 98×. SQLite remained faster on each sampled operation. The full 3,000-document operation distributions were:

| Operation | Prototype before | Prototype after | SQLite |
| --- | ---: | ---: | ---: |
| Create | 2,818.42 / 4,592.00 | 951.70 / 1,527.28 | 173.31 / 550.88 |
| Read | 15.59 / 60.86 | 16.63 / 52.19 | 3.48 / 9.57 |
| Update | 2,792.41 / 4,898.37 | 929.47 / 1,573.22 | 176.57 / 574.20 |
| Indexed query | 2,330.62 / 3,538.06 | 23.65 / 107.57 | 3.81 / 26.09 |
| Delete | 2,767.08 / 4,675.28 | 930.01 / 1,549.65 | 173.42 / 574.48 |

The change is not uniformly faster: for example, at 100 baseline documents, create p99 rose from 653.00 to 916.92 µs and delete p99 from 635.83 to 2,025.69 µs in this sample. These small tails and all other measurements remain in the raw report; they need repeat measurements under controlled conditions before attributing the difference to the code.

## Concurrent snapshot workload

Five rotated before/after runs preloaded 9,000 keys, then used four closed-loop writers, each completing 100 two-key conservative transactions and retrying conflicts, alongside one continuous point reader. Writer latency includes its own retries. The reader retained a random reservoir of at most 20,000 observations per run and recorded the maximum separately. All successful updates and preloaded values were checked after reopening. This is not a constant offered-load or competitor comparison.

| Measurement | Before | After |
| --- | ---: | ---: |
| Median of five writer throughputs, operations/s | 287.28 | 834.46 |
| Writer latency p50 / p99, µs | 3,311.82 / 188,035.09 | 1,380.12 / 34,685.25 |
| Reader sampled p50 / p99, µs | 0.568 / 0.672 | 0.339 / 0.656 |
| Reader sampled p99.9 / p99.99, µs | 21.39 / 110.92 | 23.69 / 68.98 |
| Largest reader latency observed across runs, µs | 5,755.58 | 2,134.53 |

The reader's sampled p99.9 was higher after the change, while its sampled p99.99 and observed maximum were lower. Reservoir percentiles estimate the full reader distribution and are especially noisy at the tail. This closed-loop test cannot establish saturation throughput, fairness, worst-case latency, or a production guarantee.

## Environment and reproducibility

Both prototype revisions created witnessed V3 stores. SQLite used WAL and `synchronous=FULL`, but filesystem and durability semantics are not necessarily identical. Tests ran on a Linux VM with Intel Xeon Platinum 8573C CPUs, a four-CPU cgroup quota and the workspace overlay filesystem. The host and underlying storage device were not controlled. The document process and driver were pinned to separate CPUs; the concurrent workload used three CPUs for the workload and another for the driver.

All 60 document and 10 contention pre-run quiet checks passed. During the document windows, VM steal reached 3.97% and I/O wait 13.86%; during contention, they reached 3.20% and 6.82%, respectively. There were no recorded cgroup throttled periods or direct-reclaim events. These are VM/cgroup-wide counters, include fixture setup and cleanup, and are not per-engine resource measurements. The warm, small dataset, filesystem cache, different adapters, and short runs limit generalization. Physical power failure and independent off-host restoration were not tested.

The committed engine also passed a [21,135-input snapshot model fuzz run](../benchmarks/results/2026-11-04-shared-snapshot-fuzz.json), alongside targeted reader/writer, cancellation, quota, reopen, injected accounting-failure and panic tests. Short fuzz runs are not exhaustive and do not simulate physical power failure.

The [document raw results](../benchmarks/results/2026-11-04-shared-snapshot-document.json) and [compressed contention results](../benchmarks/results/2026-11-04-shared-snapshot-contention.json.gz) retain individual samples, run order, verification results, hashes and telemetry. Reproduce with `scripts/document_scale_compare.py` and `scripts/snapshot_contention_compare.py` using release binaries built at the stated revisions.
