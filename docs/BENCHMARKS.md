# Benchmark plan

An initial local SQLite comparison harness exists; no general competitive performance claim has been established. The goal is to improve latency, throughput, scalability, and resource efficiency. **No performance optimization is accepted if it violates the durability, isolation, or recovery guarantees in force.** See the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md). No database can be assumed fastest at every operation or workload.

## Initial local harness

Run the Rust prototype and bundled SQLite through the same small, single-process harness. Both use byte keys, the same configured value size (64 bytes by default), and per-value CRC checks. Every acknowledged prototype mutation is covered by `sync_data` (concurrent mutations can share a sync); SQLite uses WAL, `synchronous=FULL`, autocommit, cached statements, and a 30-second busy timeout. These are not identical internal implementations or complete durability guarantees. The prototype verifies log records; SQLite has a different storage format and verification behavior.

```sh
# Arguments: backend, clients, operations/client, keys, write %, existing directory, optional value bytes.
cargo run --release --locked --example kv_bench -- prototype 4 250 250 50 .
cargo run --release --locked --example kv_bench -- sqlite 4 250 250 50 .
# For example: 8 clients, one hot key, 4 KiB values.
cargo run --release --locked --example kv_bench -- prototype 8 500 1 50 . 4096
```

Each run creates and removes a fresh temporary directory beneath the given directory; interruptions may leave a temporary directory behind. Use an appropriate measurement filesystem, not `/tmp` when it is RAM-backed. The harness initializes the data, warms up to 100 keys, and uses deterministic per-client random seeds. Value preparation is outside each operation's latency measurement but inside overall elapsed time. It prints completed operations, elapsed time, throughput, separate read/write p50/p95/p99/p99.9 and maximum latencies, per-client completion and worst-case latencies, SQLite version, and configuration. An operation error fails the run rather than being omitted from the results. CI only smoke-tests that both paths execute.

This is a **closed-loop, in-process microbenchmark** with a configured fixed-size value, not a production workload. It does not capture coordinated omission, CPU/RSS or I/O metrics, cold-start latency, multi-project fairness, power-loss behavior, PostgreSQL/MongoDB, service/network overhead, or independent durability validation. Record those separately, repeat tests under controlled conditions, and do not use this narrow result as a universal database ranking.

## Warm-cache recovery harness

The recovery harness creates a new fixture using ordinary synchronized writes and measures reopening the same database repeatedly. `prepare` requires a new path and does not overwrite an existing store. The parent directory must already exist. A failed or interrupted preparation may leave its partial directory behind.

```sh
cargo run --release --locked --example recovery_bench -- prepare /path/to/new-fixture 100000
cargo run --release --locked --example recovery_bench -- measure /path/to/new-fixture 100000 10
# Optional final argument sets the fixed value size in bytes.
cargo run --release --locked --example recovery_bench -- prepare /path/to/large-fixture 1000 4096
cargo run --release --locked --example recovery_bench -- measure /path/to/large-fixture 1000 10 4096
```

Each open verifies every record checksum and rebuilds the index; three keys are checked after the open timer stops. The printed record count is the expected fixture size, not an independently counted number of distinct keys. Repeat measurements with the same prepared fixture and equivalent builds. These are warm-cache reopen timings: they do not measure cold storage, service wake-up, power-loss recovery, compaction, or backup/restore. The fixture remains on disk until its owner removes it.

## Large-write interference harness

The contention harness runs one client writing a large value while other clients repeatedly update independent 64-byte values. Both backends use the same checksum-verifying adapters as the key-value harness. The prototype synchronizes before returning each write; SQLite uses WAL, `synchronous=FULL`, and autocommit.

```sh
# backend, large value bytes, large writes, small clients, small writes/client, existing directory
cargo run --release --locked --example contention_bench -- prototype 8388608 32 4 1000 .
cargo run --release --locked --example contention_bench -- sqlite 8388608 32 4 1000 .
```

The harness creates and removes a temporary directory beneath the supplied directory; an interrupted run may leave it behind. Large values can consume substantial disk space: the prototype retains every update in its append-only log. The program validates the final values and checksums outside the timers. It reports latency for all small writes, for small writes that start while the large-write stream is active (including those that finish afterward), and for large writes. The overlap count may be small or zero; do not compare overlap percentiles without reporting the sample counts and duration. Preparation and final validation occur outside the timers. This closed-loop workload is intended to expose interference, not estimate service-level latency, fairness across projects, cold-storage performance, or a universal ranking.

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

## Buffered recovery and targeted writer wake-ups

The [raw comparison and environment](../benchmarks/results/2026-11-03-recovery-and-wakeups.json) use clean, separately built binaries from the recorded revisions. Recovery runs reopen the same prepared log five times per run, with seven runs per version. The table reports the median of each run's median open time; every open verifies the record checksums and rebuilds the index.

| Fixture | Before buffered replay | Buffered replay |
| --- | ---: | ---: |
| 100,000 records, 64-byte values | 108.0 ms | 33.9 ms |
| 1,000 records, 4 KiB values | 2.11 ms | 1.34 ms |
| 32 records, 128 KiB values | 1.07 ms | 1.07 ms |
| 100 records, 64-byte values | 0.48 ms | 0.46 ms |

For the 100,000-record fixture, an additional `strace` run counted 300,005 `pread64` calls before buffering and 147 after, including the three post-open key checks. The instrumented call counts help explain the improvement; `strace` timings are not used for the table. These are warm-cache measurements on virtualized overlay storage, not cold-storage or cloud wake-up measurements. The log is still scanned in full and the index remains unbounded.

The following writer comparison uses eight clients, 250 cached keys, 64-byte values, 2,000 operations per client, and roughly 50% writes. It reports medians from seven runs per engine. The before engine already used group commit; the after engine adds targeted wake-ups and buffered recovery. SQLite 3.49.1 uses WAL, `synchronous=FULL`, and autocommit.

| Measurement | Before targeted wake-ups | After targeted wake-ups | SQLite |
| --- | ---: | ---: | ---: |
| Mixed operations/second | 21,339 | 23,770 | 7,893 |
| Write p50 | 0.739 ms | 0.661 ms | 0.178 ms |
| Write p99 | 1.367 ms | 1.215 ms | 0.547 ms |
| Write p99.9 | 2.874 ms | 2.300 ms | 16.727 ms |

SQLite's typical write latency was lower, while the prototype had higher throughput in this particular workload. SQLite implements SQL, transactions, and other features the prototype lacks; the engines' full durability behavior has not been independently compared. Tail results are especially variable: SQLite's run-level write p99.9 ranged from 1.9 to 180.2 ms. In a separate eight-client, 4 KiB-value workload, the prototype's median write p99.9 rose from 1.78 to 5.01 ms after the change despite higher throughput; the limited samples and shared virtualized machine do not establish the cause. The raw file also includes single-client, four-, 16-, and 64-client, hot-key, and write-only runs. These closed-loop, cached-data measurements do not establish a general performance, resource-efficiency, or reliability ranking.

## Writer contention and grouped append comparison

The [raw results and environment](../benchmarks/results/2026-11-03-writer-contention.json) compare the prior engine, the engine with vectored appends and shorter lock holds, and SQLite 3.49.1. Each revision used identical benchmark sources and a separate, initially empty build directory. The table reports medians across seven runs of one client writing an 8 MiB value 32 times, with four clients each writing 1,000 independent 64-byte values. Small-write timings include requests that started during the large-write stream even if they completed afterward.

| Measurement | Before | After | SQLite |
| --- | ---: | ---: | ---: |
| Small-write p50 during large-write stream | 3.040 ms | 0.701 ms | 0.182 ms |
| Small-write p99 during large-write stream | 8.130 ms | 5.789 ms | 0.813 ms |
| Small writes sampled during the stream | 245 | 702 | 4,000 |
| Large-write p50 | 4.919 ms | 5.833 ms | 21.101 ms |
| Time for the large-write stream | 175.8 ms | 223.3 ms | 1,690.9 ms |

The faster small writes coincided with slower large writes in this test. The overlap cohorts and stream durations differ: the before-engine p99 has only about two or three observations in its slowest 1% per run. The closed-loop clients do not impose an equal arrival rate, so these figures do not establish fairness or predictable tail latency. SQLite's typical small-write latency was lower; its large writes took longer in this workload. Its default automatic checkpoints and the different engines' feature sets and storage behavior affect the comparison.

The raw file includes 1 MiB interference and multiple fixed-size key-value workloads. In five 64-client runs with 64-byte values and roughly 50% writes, the prototype's median run-level write p99 fell from 2.754 ms to 2.342 ms and throughput rose from 81,333 to 88,013 operations/second. In five eight-client runs with 4 KiB values, write p99 fell from 1.312 ms to 1.224 ms. Results were not uniformly better: median write p50 in the one-client 64-byte workload rose from 0.256 ms to 0.268 ms, and eight-client 64-byte write p99 rose from 1.342 ms to 1.396 ms. The run-to-run ranges overlap and tail observations are noisy.

Separately instrumented eight-client runs counted 2,273 versus 766 `statx` calls, and 2,282 `write` calls versus 475 `writev` plus 300 `write` calls. These counts include initialization and output, and tracing changes scheduling and batch composition; the recorded tracing times are not performance measurements. All comparisons ran on a virtualized overlay filesystem whose underlying storage was not independently verified. SQLite has SQL, transactions, and other features absent from the prototype, and the engines' full durability behavior was not independently validated.

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

Build each source checkout with a fresh, empty `CARGO_TARGET_DIR`; sharing or copying build directories across checkouts can reuse artifacts from the wrong source. Package-only cleanup may leave example binaries behind. Record the clean source revision and resulting binary hash for each comparison, and use the same benchmark harness and workload in each build.

Publish the benchmark code, commands, raw results, and analysis. CI should check benchmark tooling for correctness, not enforce noisy shared-runner performance thresholds. Hardware-specific optimizations require a portable fallback and equivalent behavior tests.
