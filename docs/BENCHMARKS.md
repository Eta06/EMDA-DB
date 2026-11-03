# Benchmark plan

An initial local SQLite comparison harness exists; no general competitive performance claim has been established. The goal is to improve latency, throughput, scalability, and resource efficiency. **No performance optimization is accepted if it violates the durability, isolation, or recovery guarantees in force.** See the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md). No database can be assumed fastest at every operation or workload.

## Initial local harness

Run the Rust prototype and bundled SQLite through the same small, single-process harness. Both use byte keys, 64-byte values, and per-value CRC checks. Every acknowledged prototype mutation is covered by `sync_data` (concurrent mutations can share a sync); SQLite uses WAL, `synchronous=FULL`, autocommit, cached statements, and a 30-second busy timeout. These are not identical internal implementations or complete durability guarantees. The prototype verifies log records; SQLite has a different storage format and verification behavior.

```sh
# Arguments: backend, clients, operations/client, keys, write %, existing directory.
cargo run --release --locked --example kv_bench -- prototype 4 250 250 50 .
cargo run --release --locked --example kv_bench -- sqlite 4 250 250 50 .
```

Each run creates and removes a fresh temporary directory beneath the given directory; interruptions may leave a temporary directory behind. Use an appropriate measurement filesystem, not `/tmp` when it is RAM-backed. The harness initializes the data, warms up to 100 keys, and uses deterministic per-client random seeds. It prints completed operations, elapsed time, throughput, separate read/write p50/p95/p99/p99.9 and maximum latencies, per-client completion and worst-case latencies, SQLite version, and configuration. An operation error fails the run rather than being omitted from the results. CI only smoke-tests that both paths execute.

This is a **closed-loop, in-process microbenchmark** with a small fixed-size value, not a production workload. It does not capture coordinated omission, CPU/RSS or I/O metrics, cold-start latency, multi-project fairness, power-loss behavior, PostgreSQL/MongoDB, service/network overhead, or independent durability validation. Record those separately, repeat tests under controlled conditions, and do not use this narrow result as a universal database ranking.

## First recorded result

[Raw results and environment](../benchmarks/results/2026-11-03-overlay-kv-baseline.json) capture three runs per backend at revision `0bca7172cf62f619b5fa3261522c30f7940dc944`. The workload uses four clients, 250 cached keys, 64-byte values, and 1,000 operations per run with roughly half writes. It ran in a virtualized environment on an overlay filesystem whose underlying storage was not independently verified.

In these runs, prototype read p95 was 3.2–4.6 µs and SQLite read p95 was 6.1–6.3 µs. Prototype write p99 was 8.5–21.5 ms and SQLite write p99 was 0.67–1.09 ms. There were only 528 writes in each run, so the tail figures are particularly noisy; they identify a reason to profile writer contention, not a general ranking or proof of reliability. Neither backend's full durability behavior was validated, and their features and internal guarantees differ. See the raw file and limitations above before interpreting these numbers.

## Group commit comparison

The [raw comparison and environment](../benchmarks/results/2026-11-03-group-commit-comparison.json) record five mixed-workload runs per engine with eight clients, 1,000 operations per client, 250 cached keys, 64-byte values, and about 50% writes. The table shows the median of each run's measured value. The earlier engine and group-commit engine used the source revisions recorded in the file; SQLite was 3.49.1, in WAL mode with `synchronous=FULL` and autocommit.

| Measurement | Before group commit | With group commit | SQLite |
| --- | ---: | ---: | ---: |
| Mixed operations/second | 7,098 | 22,514 | 6,830 |
| Write p50 | 0.270 ms | 0.682 ms | 0.192 ms |
| Write p99 | 23.48 ms | 1.28 ms | 0.59 ms |
| Write p99.9 | 243.3 ms | 2.45 ms | 530.4 ms |

Grouping reduced the observed long waits and increased throughput in this small concurrent workload, while the typical write took longer. The p99.9 figure reflects only a few of roughly 4,000 writes per run, and the virtualized overlay filesystem's backing was not verified. These numbers do not establish durability, isolation, or performance superiority in general: SQLite supports SQL, transactions, and many features the prototype lacks. The file also includes single-client, four-client, read-only and write-only runs; process CPU and RSS include initialization and are not a standalone memory comparison. Use controlled, larger and out-of-cache workloads before drawing wider conclusions.

SQLite's [WAL documentation](https://www.sqlite.org/wal.html) describes sequential writes and reader/writer overlap. Its [`synchronous` documentation](https://www.sqlite.org/pragma.html#pragma_synchronous) states that WAL with `FULL` synchronizes after each transaction commit. The harness leaves SQLite's automatic checkpoint configuration at its default; a checkpoint can affect measured latency.

## Comparators

Start with PostgreSQL, SQLite, and Turso Database on the same machine where their supported operations and safety settings are comparable. Later compare PostgreSQL- and MongoDB-compatible operations, then hosted APIs such as Neon, Cloudflare D1, and MongoDB Atlas as a separate category. An embedded function call is not equivalent to a remote authenticated API request.

## Workloads

- Single-key read, insert, overwrite, delete, and atomic increment once supported.
- Uniform and hot-key traffic; independent-key and same-key concurrent writes.
- Read-heavy, balanced read/update, and write-heavy mixes. Use [YCSB's published workload definitions](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads) where compatible.
- Datasets that fit in cache and datasets that exceed available memory; varied key/value sizes and project counts.
- Warm operation, cold reopen, burst traffic, noisy neighbors, and bounded overload.
- Corrupt/truncated files, process termination, recovery, and backup/restore correctness.
- Deterministic fault injection and replay of synthetic or explicitly permitted, privacy-reviewed traces; check correctness as well as timing on each replay.
- Relational transactions and document queries only when the candidate actually implements equivalent semantics. [TPC-C](https://www.tpc.org/tpcc/) is a later transactional reference; do not label a partial imitation an official TPC-C result.

## Measurement and fairness

Record code revision, engine versions, compiler settings, hardware, filesystem, memory limits, network path, dataset, query/index definitions, durability and replication settings, warm-up, concurrency, and repetitions. Report throughput, p50/p95/p99 latency, failures and timeouts, CPU, memory, disk I/O, recovery time, and correctness. Include bounded offered load and overload behavior, not only successful requests; avoid coordinated omission when assessing tail latency. Compare the same operation and consistency/durability guarantees, and report configuration differences instead of disabling safety to win a benchmark.

Publish the benchmark code, commands, raw results, and analysis. CI should check benchmark tooling for correctness, not enforce noisy shared-runner performance thresholds. Hardware-specific optimizations require a portable fallback and equivalent behavior tests.
