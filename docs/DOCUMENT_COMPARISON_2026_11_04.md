# Initial JSON-document comparison, 2026-11-04

This is a small, single-client local baseline, not a general ranking or a claim of equivalent functionality. It compares the prototype at revision `a6e0b4d876d2d768534ae9599c83226959cd14a4` with SQLite 3.49.1, PostgreSQL 18.6, and MongoDB 8.0.34. The [complete result](../benchmarks/results/2026-11-04-document-comparison.json) retains every timed sample, round order, failure list, versions, image and binary hashes, configuration and VM/cgroup telemetry. All 16 runs completed and all content and reopen checks passed.

Each round performs 20 warm-up cycles and 300 measured cycles. A cycle creates a small JSON object, reads it by ID, replaces it, looks it up through a unique email index, and deletes it; each operation is timed separately. The table pools 1,200 samples per operation and engine across four rotated rounds. Values are microseconds, **p50 / p99**, using nearest-rank percentiles. The last column is the median of the four measured per-round rates, including the workload's validation and control overhead; it is not an independently offered-load throughput result.

| Engine | Create | Read by ID | Replace | Indexed lookup | Delete | Median operations/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Prototype | 262 / 612 | 5.64 / 24.54 | 259 / 648 | 8.24 / 35.37 | 264 / 738 | 5,708 |
| SQLite | 228 / 714 | 3.29 / 18.14 | 230 / 648 | 3.45 / 9.23 | 227 / 685 | 6,403 |
| PostgreSQL | 526 / 1,896 | 233 / 592 | 581 / 2,355 | 243 / 516 | 533 / 2,125 | 2,144 |
| MongoDB | 874 / 3,021 | 479 / 1,471 | 917 / 2,216 | 583 / 1,340 | 913 / 3,113 | 1,217 |

SQLite had the lower median on every operation in this run. Tail latency varied: PostgreSQL's four round rates ranged from approximately 1,205 to 3,678 operations/s, and its slowest observed replacement took 108 ms. The prototype's slowest delete took 3.33 ms. See the raw samples and individual rounds rather than treating pooled percentiles as a production forecast.

## Selected system-call diagnostic

A separate `strace -c` run used the same release binary and local overlay, with 20 warm-up and 20 measured cycles per engine. It traced only selected read, write, synchronization and extended-attribute calls and included setup and reopen. The prototype emitted 614 `pread64`, 137 `fsync`, 246 `fgetxattr` and 123 `fsetxattr` calls; SQLite emitted 15 `pread64` and 130 `fsync` calls. The prototype's xattr calls identify the V3 witness path in this diagnostic run. See the [prototype](../benchmarks/results/2026-11-04-document-prototype-strace.txt) and [SQLite](../benchmarks/results/2026-11-04-document-sqlite-strace.txt) trace summaries.

Tracing changes timings and the implementations use different caches, data representations and verification paths. The counts identify read amplification as a candidate for further profiling; they do not establish which calls caused the benchmark differences. Any caching or I/O change must preserve the existing synchronization, corruption-detection and recovery behavior.

## Workload and important differences

- The prototype and SQLite run in-process from the same Rust release binary. PostgreSQL and MongoDB run in separate disposable containers and use Python `psycopg` and `pymongo` over a loopback TCP connection, so their measurements include different client and network overhead. Container servers were pinned to CPUs 1–3; the driver and in-process backends used CPU 0. The VM had a four-CPU cgroup quota, an Intel Xeon Platinum 8573C CPU, and an overlay filesystem.
- Prototype mutations use their synchronized witnessed-store path. SQLite uses WAL, `synchronous=FULL` and autocommit. PostgreSQL uses JSONB with `fsync`, `synchronous_commit` and `full_page_writes` enabled. MongoDB uses `w=1, j=true`. These settings do not establish identical failure, replication or consistency guarantees. The benchmark does not record which witnessed format the prototype created, so do not use this result to infer V2 versus V3 performance.
- SQLite uses a JSON-valid text column and returns text to the Rust adapter; the prototype decodes documents into JSON values. PostgreSQL and MongoDB use their respective JSON/BSON representations and Python driver conversions. Each returns the data needed for the same simple content checks, but their type systems, query semantics and implementations differ.
- The prototype explicitly chooses its index. SQLite's explain plan selected the expression index. PostgreSQL's sequential scan was disabled for the checked plan, and MongoDB was given an index hint. This measures indexed access, not the effectiveness of an unforced query planner.
- The object is deleted before the next cycle, so the live dataset stays very small. The test does not measure large collections, concurrent clients, contention, sustained load, cold cache, multi-node behavior, recovery time, process crashes, physical power failure or independent backup restoration. Reopening and checking an empty collection verifies this test's final content only.
- Every pre-run quiet check passed. Individual VM-wide observations reached 2.39% steal and 13.82% I/O wait; none of the measured windows recorded cgroup CPU throttling or direct memory reclaim. These telemetry counters include other activity and are not per-engine CPU, memory, or I/O usage. The host and physical storage were not controlled.

To reproduce with the dependencies and images already available, run:

```sh
cargo build --release --locked --example document_bench
python3 scripts/document_compare.py --directory /path/on/test/filesystem \
  --binary target/release/examples/document_bench --output /tmp/document-comparison.json \
  --rounds 4 --cycles 300
```

The harness creates and removes only its own temporary data and containers. See [benchmark methodology](BENCHMARKS.md) for the remaining evidence required before performance or reliability claims.
