# Local engine comparison and Linux noise controls

This comparison exercises the prototype's implemented byte-key/value operations, not full feature parity. It includes SQLite (Rust adapter and a separately versioned Python control), PostgreSQL, Redis, MongoDB, Turso's Rust engine, RocksDB, LMDB, DuckDB, and a single-process FoundationDB cluster. SQL planning, document queries, subscriptions, replication, distributed fault tolerance, and large-data behavior are outside this workload.

## Durability settings and adapters

| Backend | Required acknowledgement behavior / configuration | Adapter |
| --- | --- | --- |
| Prototype | Existing V3 full inode sync before publication/acknowledgement; no engine changes in this comparison | Rust loopback adapter |
| SQLite | WAL, `synchronous=FULL`, autocommit, 30-second busy timeout | Rust/rusqlite; Python sqlite3 as a separate control |
| PostgreSQL | `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, autocommit | psycopg, native wire protocol |
| Redis | AOF, `appendfsync=always`, no skipped sync during rewrites; RDB snapshots disabled | redis-py, native protocol |
| MongoDB | Standalone WiredTiger, `w=1,j=true`; journal interval tuned to 1 ms, 512 MiB WiredTiger cache | pymongo, native protocol |
| Turso | `pyturso`, default writer mode, WAL, `synchronous=FULL`, autocommit; no experimental MVCC enabled | Python binding adapter |
| RocksDB | Raw byte keys/values, WAL enabled, per-write `sync=true`, including deletes | rocksdict binding adapter |
| LMDB | `sync=true`, `metasync=true`, no writemap/map_async, one transaction per mutation | Python lmdb binding adapter |
| DuckDB | Default persistent WAL/autocommit, three execution threads; bounded retry of write conflicts | Python binding adapter |
| FoundationDB | Normal durable transactions, single process, `single` redundancy and SSD storage; 1 GiB process memory setting | Matching 7.3 C client and Python binding |

The same loopback framing, byte values and CRC checks are used for adapters. Server databases retain their native protocols and container port forwarding. Python versus Rust bindings, SQL versus key-value models, checkpoint/maintenance work, storage formats and cache behavior differ. The Python SQLite build can have a different SQLite version from rusqlite, so it is not a pure isolation of binding overhead. Every actual version is recorded. FoundationDB's single-node setup does not measure its distributed strengths or establish high availability. MongoDB's tuned journal interval is not its default configuration. Redis's in-memory data model differs from the prototype's disk-offset value reads. DuckDB is primarily an analytical engine; this point-operation workload does not measure that purpose.

Conflicts are retried only for recognized DuckDB write conflicts and Turso busy/locked errors, with a 30-second deadline. Retry time is included in operation latency and counts are recorded. Other errors fail the run. Values and CRCs are checked, including after measurement. No synchronization is disabled to obtain faster numbers. Similar sync settings do not prove identical recovery or power-failure guarantees.

## Workloads and repetition

`scripts/compare_engines.py` runs six workloads: one-client mixed reads/writes, four-client mixed reads/writes, four-client reads only, writes only, mixed 4 KiB values, and CRUD cycles. All cases use deterministic per-client seeds and warm data. Each backend/case has four repetitions with rotating engine and case order. A fresh table/collection/key prefix is used in a native server for each invocation; native server files/WAL are reused across cases. Embedded adapters use a fresh store per invocation. Only one backend runs at a time. Runs with failures or noisy counters are retained, not silently excluded.

A CRUD cycle times create, read, update, and delete on a client-owned key. Two additional validation reads check the update and deletion outside the individual latency timers; they are included in wall-clock throughput. The same key is reused after deletion. Thus this measures repeated single-key CRUD, not insertion into a progressively larger index. Mixed-workload writes are upserts of existing keys. These historical runs did not measure range scans or multi-key transactions.

These are closed-loop tests: a client waits for a response before its next operation. Report read/write/CRUD p50, p95, p99, p99.9, maxima, throughput and run-to-run ranges; these are not open-loop service latency SLOs and do not correct coordinated omission. The Python load generator and binding adapters can themselves limit throughput, especially reads. No result is an isolated native-engine CPU benchmark.

## Linux controls

On the selected VM, the allowed CPUs are 0–4 with a four-CPU aggregate quota. Measurements use CPU 0 for the client, CPUs 1–3 for the selected server, and CPU 4 for the orchestration process. Child-only affinity changes avoid modifying unrelated services. The runner prepares images, binaries and dependencies before measurement; it runs no compiler, fuzz campaign, image pull or unrelated database concurrently. Native servers have a 2 GiB container limit. The small embedded datasets are not given an equivalent hard memory limit; this is not a memory-efficiency ranking.

A bounded pre-run quiet check samples available CPU usage, throttling, I/O counters, VM I/O wait and steal time for up to five one-second intervals. Its thresholds are CPU usage below 0.15 core, no new throttling, fewer than 2 MiB of cgroup I/O, and reported steal/I/O-wait below 2%. Missing counters are retained as unavailable; passing the check is not proof of an idle physical disk. The quiet check also requires no new memory-limit/OOM events or direct-reclaim scans when those counters are available. Each measured interval records before/after cgroup CPU, I/O, memory-limit events, reclaim counters and VM steal/I/O-wait. These counters include the workload and other activity in the VM, so they are not a per-engine attribution of resource use or pure external noise.

`--noise-calibration` alternates unrestricted use of the allowed CPU set and the pinned arrangement, with identical workload, code, quiet checks and durability. Repetitions and variability are reported for both; pinning is not assumed to improve latency or every percentile.

Before the final matrix, 27 obsolete EMDA Cargo build caches were removed from tmpfs, freeing approximately 7.6 GiB of cgroup memory. Active binaries, dedicated baseline binaries, dependencies, source and raw results were retained. An interrupted pre-cleanup pass is retained separately and is not combined with the final matrix. No global page-cache flush was performed.

Old zombie processes consume no CPU and cannot be fixed by sending them another kill signal. This VM has no swap and does not expose CPU-frequency governors or pressure-stall counters. The host scheduler, virtual CPU steal, shared storage, and other tenants remain outside this session's control. No real-time scheduling, global service shutdown, cache dropping, security changes, or disabling of database maintenance/durability is used. CPU affinity does not reserve physical cores.

## Reproduction

Install the pinned, optional packages from `scripts/benchmark-requirements.txt` into a dedicated environment. FoundationDB additionally requires the matching native `libfdb_c.so`; install the official 7.3.79 client or copy it from the recorded FoundationDB container image into the Python `fdb` package directory. Build the Rust adapter before measurements:

```sh
cargo build --release --locked --example network_bench_server
python3 -m pip install -r scripts/benchmark-requirements.txt
docker pull postgres:18-bookworm
docker pull redis:8.2-bookworm
docker pull mongo:8.0
docker pull foundationdb/foundationdb:7.3.79
python3 scripts/compare_crashes.py --directory /path/to/disk --output crashes.json
python3 scripts/compare_engines.py --directory /path/to/disk --output engines.json --rounds 4
python3 scripts/compare_engines.py --directory /path/to/disk --output noise.json \
  --noise-calibration --rounds 6 --backends prototype sqlite postgres
```

Choose valid disjoint client/server/housekeeping CPU sets for another host. Local Docker socket access is required. Containers bind published ports to localhost only and mount dedicated temporary data directories. The scripts remove only their own containers and synthetic data. Image digests, package versions, source and binary hashes, parameters, settings, failed runs and noise observations are saved. A process interruption can require cleaning up the explicitly named `emda-compare-*` resources from that interrupted invocation.

The crash check holds clients open, acknowledges 100 inserts, 10 replacements and 10 deletes, kills the server with SIGKILL, reopens the same data, and verifies 90 values and 10 absences. It is not a simulated power cut, storage reordering test, or an exhaustive crash-point campaign.

## Coverage beyond local point-operation tests

| System/category | Why no comparable local latency number is claimed |
| --- | --- |
| Neon | Managed PostgreSQL with separated compute/storage and scale-to-zero. A local PostgreSQL run is not a Neon measurement; cloud project, region, tier and wake/cold-state scenarios must be defined. |
| Cloudflare D1 | Managed remote SQLite-semantics service. Requires a provisioned database and account; latency includes region/network/Worker execution and service limits. Local SQLite is not a proxy measurement. |
| SpacetimeDB 2.x | Needs a versioned reducer/module, client and subscription workload and verified acknowledgement durability. This repository's byte-key harness does not implement or measure those semantics. No result is inferred from its implementation language or architecture. |
| Appwrite, Supabase, Firebase | Backend platforms bundling APIs, auth and other services; require a separate API/product benchmark. Underlying engine numbers are not platform latency claims. |
| MongoDB Atlas, hosted Turso and other managed offerings | Their hosted topology, storage, replication, quotas and network differ from the local engines measured here. |
| Other engines and feature workloads | This is a named shortlist, not every database or every action. Features not implemented in the prototype cannot honestly be benchmarked as equivalent. |

Primary configuration references: [SQLite synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous), [PostgreSQL WAL settings](https://www.postgresql.org/docs/18/runtime-config-wal.html), [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/), [MongoDB write concern](https://www.mongodb.com/docs/manual/reference/write-concern/), [Turso Python binding](https://github.com/tursodatabase/turso/tree/main/bindings/python), [RocksDB synchronous writes](https://github.com/facebook/rocksdb/wiki/Basic-Operations#synchronous-writes), [LMDB environment flags](https://lmdb.readthedocs.io/en/release/#environment-class), [DuckDB concurrency](https://duckdb.org/docs/stable/connect/concurrency), and [FoundationDB transactions](https://apple.github.io/foundationdb/developer-guide.html#transactions).
