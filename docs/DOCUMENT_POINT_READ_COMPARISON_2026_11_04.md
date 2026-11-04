# Standalone document-read comparison, 2026-11-04

The ordinary-store standalone document read previously copied the complete in-memory key index to obtain a transaction snapshot. The new path captures the collection-metadata and document locations together, checks the same stored values, and retains conservative generation validation and shared active-read admission. It changes no on-disk format or write synchronization. Explicit document transactions, indexed queries, writes, and the checkpoint-backed read still use their existing snapshot paths. See the [storage contract](STORAGE_TRANSACTION_CONTRACT.md) for the failure and conflict semantics.

We compared the prototype at revision `1c443b26afeabc6a11f6fcb39ae4aa4443105ed4` (before), the prototype at revision `3e4c88422cafed6bf06fbef453e0d5ac337bed0c` (after), and SQLite 3.49.1. Each run preloads a persistent collection and a unique email index, performs 20 warm-up and 200 measured create/read/replace/indexed-lookup/delete cycles on separate transient documents, and reopens and checks the baseline documents. Five rounds rotate the order of all three binaries per collection size. Every content/reopen check passed.

The table pools 1,000 samples per row and variant. Values are microseconds, **p50 / p99**, using nearest-rank percentiles. The SQLite adapter returns JSON text; the prototype decodes JSON and validates the schema, so the read paths are not identical.

| Live baseline documents | Prototype before | Prototype after | SQLite |
| ---: | ---: | ---: | ---: |
| 0 | 5.73 / 27.36 | 5.19 / 28.38 | 3.61 / 13.44 |
| 100 | 42.12 / 152.04 | 5.55 / 29.36 | 3.67 / 15.46 |
| 1,000 | 566.34 / 1,127.37 | 9.84 / 43.73 | 3.66 / 23.30 |
| 3,000 | 2,345.50 / 6,831.27 | 17.62 / 149.77 | 3.54 / 28.34 |

At 3,000 documents, this sample's prototype p50 fell by 99.25% (about 133×). A separate repeat at 1,000 and 3,000 documents also completed 30 runs without failure; its 3,000-document p50 was 2,294.15 µs before, 15.29 µs after, and 3.45 µs for SQLite. These numbers describe this specific workload, not a universal ranking or a production latency guarantee.

The change addresses only standalone `get`. At 3,000 documents, the first run's prototype indexed-lookup p50 was 2,362.55 µs before and 2,417.91 µs after, versus SQLite's 3.88 µs. The prototype still copies a bounded full index for indexed queries and transactional writes. At that size, median per-round whole-cycle rates were 351 operations/s before, 357 after, and 6,550 for SQLite in the first sample; the repeat reported 366, 445, and 8,075 respectively. Those rates include different adapters and validation costs and are not a general concurrency or throughput ranking.

## Setup and limitations

- Both prototypes ran in-process as Rust release binaries on one Linux VM, with a four-CPU cgroup quota, an Intel Xeon Platinum 8573C CPU and a workspace overlay filesystem. The benchmark process and driver were pinned to separate CPUs. Both prototype versions created V3 witnessed stores; SQLite used WAL with `synchronous=FULL` and autocommit. The durability and query semantics of these systems are not identical.
- The collection is warm, single-client and limited to 3,000 small documents. It does not represent large datasets, concurrent clients, cold storage, multi-node use, physical power failures or independent backup restoration. The prototype uses three key/index entries per indexed document in this fixture, in addition to collection metadata.
- In the first 60 runs, 59 pre-run quiet checks passed; one measurement window recorded CPU throttling. VM-wide steal reached 18.94% and I/O wait reached 13.68% in individual windows. All 30 repeat quiet checks passed, with no observed throttling or direct reclaim; individual windows still reached 8.59% steal and 12.82% I/O wait. These counters cover the entire VM/cgroup, including fixture setup and cleanup, and are not per-engine resource measurements. The host and underlying storage device were not controlled. Tail measurements, in particular, are sensitive to this interference.
- Each binary reports process high-water RSS and file sizes in the raw data. RSS includes fixture creation and verification, excludes the OS page cache, and is not a per-operation allocation measurement or a project memory limit. Fixture setup and final verification are outside the individual operation timers but inside the noise windows.
- Targeted tests cover conservative conflicts, shared admission and cleanup on panic, uncertain appends and V3 witness/sync failures, corrupt metadata and document values, atomic schema/document changes, and a store with more keys than a full transaction snapshot permits. The [local document-model fuzz run](../benchmarks/results/2026-11-04-document-point-read-fuzz.json) exercised 10,399 inputs without a reported finding. The full toolchain matrix, process-level checks and fuzzing still do not establish physical power-loss safety.

The [primary raw results](../benchmarks/results/2026-11-04-document-point-read-paired.json), [repeat results](../benchmarks/results/2026-11-04-document-point-read-confirm.json) and [original baseline](../benchmarks/results/2026-11-04-document-scale-before.json) include individual samples, binary hashes, order, configuration and telemetry. Reproduce using [the scaling harness](BENCHMARKS.md#embedded-document-scaling-harness) and provide `--baseline-binary` for the older release binary. Each path must point to a binary built from its stated revision.
