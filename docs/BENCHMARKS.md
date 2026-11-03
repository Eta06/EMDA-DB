# Benchmark plan

A local comparison harness covers ten engines and six byte-key workloads; no general competitive performance claim has been established. The goal is to improve latency, throughput, scalability, and resource efficiency. **No performance optimization is accepted if it violates the durability, isolation, or recovery guarantees in force.** See the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md). No database can be assumed fastest at every operation or workload.

## Verified checkpoint reopen comparison: 2026-11-04

The [raw paired results](../benchmarks/results/2026-11-04-checkpoint-open-comparison.json) compare full replay with `open_with_checkpoint` at source revision `01de13a`. Each fixture used 64-byte values and DB02 batches of up to 256 puts. Checkpoint creation and fixture preparation finished before a bounded quiet check; eight timed warm-cache opens per mode alternated order, and recovered sample values were verified after each timer. The benchmark process ran on CPUs 1–3 and orchestration on CPU 4 of the same four-CPU-quota Intel Xeon Platinum 8573C VM, with `/workspace` overlay storage. The backing storage and host interference are not controlled.

| Fixture | Full replay median (range) | Checkpoint median (range) | Checkpoint creation | Log / index bytes |
| --- | ---: | ---: | ---: | ---: |
| 100,000 writes, 1,000 keys | 19.684 ms (17.445–20.914) | 8.884 ms (8.012–26.597) | 37.413 ms | 9,310,194 / 26,100 |
| 25,000 writes, 10,000 keys | 5.857 ms (5.580–8.590) | 4.492 ms (4.322–5.105) | 17.241 ms | 2,327,576 / 260,100 |

The checkpoint had lower median reopen time in both fixtures, but its 26.597 ms run was slower than every full-replay run in the first fixture. Eight repetitions are exploratory, not an SLO or broad ranking. Creation blocks writes and its time is separate from reopen measurements. The checkpoint path still hashes every covered log byte, loads the complete live index into memory and requires a trusted separately stored digest. It does not provide bounded recovery time, bounded process memory, an independent backup, or physical power-loss evidence.

Both quiet checks passed. Whole-cgroup counters for the timed loops recorded no CPU throttling, memory-limit events or direct reclaim. The highest reported VM steal during either loop was 0.77%; these counters include the benchmark, are not per-engine attribution, and do not rule out host or storage interference. Commands, source and binary hashes, per-run values, and telemetry are in the raw file. Reproduce after building `checkpoint_bench` in release mode with `python3 scripts/checkpoint_compare.py --directory /path/to/disk --output results.json`; adjust CPU sets for the host.

## Expanded local comparison and Linux noise controls: 2026-11-03

The [complete report](LOCAL_COMPARISON_2026_11_03.md) records 264 runs across EMDA, SQLite (two binding/version configurations), PostgreSQL, Redis, MongoDB, Turso, RocksDB, LMDB, DuckDB and FoundationDB. It includes single/concurrent mixed workloads, read-only, write-only, 4 KiB values and CRUD cycles, plus 36 paired CPU-affinity measurements and eleven successful process-restart checks. The [methodology](COMPARISON_METHOD.md) documents durable settings, native versions, adapter overhead, VM controls and systems not measured.

In the four-client mixed test, EMDA measured 10,397 operations/s versus SQLite's 4,514; write p50/p99 were 628/1,323 µs versus SQLite's 288/13,816 µs. SQLite won write-only p50 and p99. The tables report medians and ranges of run-level quantiles; they do not establish broad feature parity or universal superiority.

Removing obsolete tmpfs build caches freed 7.6 GiB. The main matrix recorded no CPU throttling, direct memory reclaim or memory-limit events. Host steal remained, and pinning did not consistently help all engines in the calibration. No noisy runs were silently discarded; the interrupted pre-cleanup pass is retained separately. No engine code or durability setting changed for this comparison.

## V3 latency optimization: 2026-11-03

The [raw 192-run matrix](../benchmarks/results/2026-11-03-metadata-latency-comparison.json) compares V2 revision `9ceec73` with V3 revision `34f5bdb` and bundled SQLite 3.49.1. Each of eight workloads ran four times per engine through both direct Rust calls and the unchanged loopback adapter; engine order rotated between rounds. The matrix covers 1/4/8 clients, read-only/write-only/mixed operations, and 64-byte/4-KiB/128-KiB values. Each run uses a fresh store and deterministic workloads. Source hashes, binary hashes, exact commands, per-run values, and median/min/max summaries are recorded. The host reported an Intel Xeon Platinum 8573C with a four-CPU cgroup quota; data lived on `/workspace` overlay storage whose backing device was not independently established.

V3 replaces two sequential per-batch synchronization barriers with one **full inode `fsync`** covering both records and a checksummed xattr boundary. It retains sync-before-acknowledgement, record verification, failure poisoning, and detection of truncation below an intact witness. A separate immutable identity anchor detects missing or substituted headers. This requires Linux filesystem support for durable user xattrs; it does not establish hardware power-loss safety. Existing V2 stores keep the old protocol until explicitly copied with `Store::upgrade_format`. See the [storage contract](STORAGE_TRANSACTION_CONTRACT.md) for the changed internal ordering and filesystem assumptions.

Single client, direct Rust API, 2,000 operations over 250 cached keys, 64-byte values, roughly 50% writes. Entries are medians of four run-level measurements:

| Engine | Operations/s | Read p50 | Write p50 | Write p99 |
| --- | ---: | ---: | ---: | ---: |
| Before (V2) | 3,079 | 1.5 µs | 568.6 µs | 2,276.9 µs |
| After (V3) | 6,457 | 1.4 µs | 281.8 µs | 631.8 µs |
| SQLite WAL/FULL | 5,941 | 3.5 µs | 304.2 µs | 648.8 µs |

The new median write latency was 50.4% below V2 and 7.4% below SQLite in this workload. The SQLite p50 range was 204.6–319.7 µs versus V3's 257.0–285.0 µs: SQLite was faster in some individual runs. The ranges overlap, so this small sample does not establish a statistically reliable advantage over SQLite. V3 also had a 2.89 ms write-p99 outlier in one run; medians do not remove that observation.

Four clients, loopback adapter, 1,000 operations/client, 250 cached keys, 64-byte values, roughly 50% writes:

| Engine | Operations/s | Read p50 | Write p50 | Write p99 |
| --- | ---: | ---: | ---: | ---: |
| Before (V2) | 5,193 | 73.3 µs | 1,325.6 µs | 2,471.6 µs |
| After (V3) | 7,928 | 73.5 µs | 820.2 µs | 2,101.7 µs |
| SQLite WAL/FULL | 4,608 | 40.1 µs | 314.4 µs | 11,301.8 µs |

Here V3 reduced median write latency by 38.1% versus V2, but SQLite's median write was still much faster. V3 improved throughput and had lower aggregate write-p99 than SQLite; SQLite's p99 varied from 1.51 to 19.13 ms. In the four-client **direct-call** case, V3 write p50/p99 were 505.9/1,125.9 µs versus SQLite's 245.8/1,071.0 µs, while throughput was 13,814 versus 5,290 operations/s. The raw data also records p99.9, maximum latencies, and per-client completion times: SQLite's rare writer stalls can be hidden even by p99. Grouping, lock scheduling, and wire/client overhead affect these distributions differently. With one loopback client, V3's median write was 343.4 µs versus SQLite's 329.5 µs. There is no across-the-board SQLite latency win.

[Verification evidence](../benchmarks/results/2026-11-03-metadata-verification.json) records 90 Rust tests across stable/MSRV and release configurations, a 160,711-input local fuzz campaign, a process-kill test, and a cross-filesystem local restore drill. A separate syscall-count run with 100 initialization writes plus 1,000 measured writes counted V2's 2,201 `fdatasync` + 7 `fsync` calls versus V3's 1,108 `fsync` calls; tracing perturbs timing and these are counts, not performance measurements. GitHub CI and the manually triggered recovery-fuzz workflow passed on the implementation revision. These checks do not prove protection against physical power loss.

To repeat, build the release examples from both revisions using the same Rust toolchain and separate target directories, then run:

```sh
python3 scripts/latency_matrix.py \
  --baseline-directory /path/to/before/target/release/examples \
  --baseline-revision 9ceec73732e6f95e0af0e432b4f29e44c3088f7b \
  --candidate-directory target/release/examples \
  --directory /path/to/measurement-disk \
  --output latency.json --rounds 4
```

These are short, warm, closed-loop tests on one virtualized host. No latency SLA, coordinated-omission correction, broad feature equivalence, or universal ranking follows from them. PostgreSQL and Redis were not rerun for this optimization; their previous measurements below are historical results of the V2 engine.

## Initial local harness

Run the Rust prototype and bundled SQLite through the same small, single-process harness. Both use byte keys, the same configured value size (64 bytes by default), and per-value CRC checks. Every acknowledged prototype mutation is synchronized before return (concurrent mutations can share a sync). V3 uses a full inode `fsync` for data plus its metadata witness; V2 uses log `fdatasync` followed by witness synchronization. SQLite uses WAL, `synchronous=FULL`, autocommit, cached statements, and a 30-second busy timeout. These are not identical internal implementations or complete durability guarantees. The prototype verifies log records; SQLite has a different storage format and verification behavior.

```sh
# Arguments: backend, clients, operations/client, keys, write %, existing directory, optional value bytes.
cargo run --release --locked --example kv_bench -- prototype 4 250 250 50 .
cargo run --release --locked --example kv_bench -- sqlite 4 250 250 50 .
# For example: 8 clients, one hot key, 4 KiB values.
cargo run --release --locked --example kv_bench -- prototype 8 500 1 50 . 4096
```

Each run creates and removes a fresh temporary directory beneath the given directory; interruptions may leave a temporary directory behind. Use an appropriate measurement filesystem, not `/tmp` when it is RAM-backed. The harness initializes the data, warms up to 100 keys, and uses deterministic per-client random seeds. Value preparation is outside each operation's latency measurement but inside overall elapsed time. It prints completed operations, elapsed time, throughput, separate read/write p50/p95/p99/p99.9 and maximum latencies, per-client completion and worst-case latencies, SQLite version, and configuration. An operation error fails the run rather than being omitted from the results. CI only smoke-tests that both paths execute.

This is a **closed-loop, in-process microbenchmark** with a configured fixed-size value, not a production workload. It does not capture coordinated omission, CPU/RSS or I/O metrics, cold-start latency, multi-project fairness, power-loss behavior, PostgreSQL/MongoDB, service/network overhead, or independent durability validation. Record those separately, repeat tests under controlled conditions, and do not use this narrow result as a universal database ranking.

## Loopback comparison with PostgreSQL and Redis

`scripts/network_compare.py` exercises the prototype, bundled SQLite, PostgreSQL and Redis through TCP clients on localhost. The Rust benchmark adapter exposes the prototype and SQLite to the same Python driver; it is loopback-only benchmark tooling with no authentication or service-level security. PostgreSQL and Redis use their own protocols and Python drivers, and container port forwarding introduces additional overhead. This is not an identical wire-protocol comparison.

Install the optional benchmark-only Python dependencies from `scripts/benchmark-requirements.txt`. Run disposable local PostgreSQL and Redis instances with persistent data directories on the measurement filesystem. The PostgreSQL instance must use database `benchmark`, user `postgres`, `fsync=on`, `synchronous_commit=on` and `full_page_writes=on`. Redis must use AOF with `appendonly=yes` and `appendfsync=always`. Set `EMDA_BENCH_PG_PORT`, `EMDA_BENCH_PG_PASSWORD`, and `EMDA_BENCH_REDIS_PORT` in the environment; credentials are not printed in results. The script creates and removes only its uniquely named table or keys.

```sh
cargo build --release --locked --example network_bench_server
python3 scripts/network_compare.py prototype --server target/release/examples/network_bench_server --directory . --clients 4 --operations 1000
python3 scripts/network_compare.py sqlite --server target/release/examples/network_bench_server --directory . --clients 4 --operations 1000
python3 scripts/network_compare.py postgres --directory . --clients 4 --operations 1000
python3 scripts/network_compare.py redis --directory . --clients 4 --operations 1000
```

All paths check the values and CRC32 checksums; Redis stores four extra checksum bytes with each value. The Python client uses deterministic workloads, checks all values after measurement, and fails rather than omitting an operation error. PostgreSQL and SQLite provide SQL and transactions that the prototype lacks; Redis keeps its dataset in memory and has distinct AOF behavior. Synchronization settings are not proof of identical crash or power-loss guarantees. Results are closed-loop and subject to coordinated omission, Python scheduling, cache and storage effects, and the network/container differences above. Do not rank whole database systems from this narrow byte-key workload.

The [raw new-format comparison](../benchmarks/results/2026-11-03-witness-network-comparison.json) records four runs each at one, four, and eight clients; read-only, write-only, and 4 KiB mixed workloads are included. The table shows the median run-level measurements for four clients, each issuing 1,000 operations across 250 cached keys with 64-byte values and roughly 50% writes. PostgreSQL 17.11 and Redis 7.4.12 ran in containers with data directories bind-mounted under the same `/workspace` overlay; the prototype and bundled SQLite 3.49.1 used loopback adapters on the host.

| Engine | Operations/s | Read p50 | Write p50 | Write p99 |
| --- | ---: | ---: | ---: | ---: |
| Prototype before witness | 8,651 | 0.080 ms | 0.753 ms | 1.473 ms |
| Prototype with witness | 5,338 | 0.073 ms | 1.322 ms | 2.224 ms |
| SQLite | 5,092 | 0.038 ms | 0.277 ms | 13.774 ms |
| PostgreSQL | 4,674 | 0.416 ms | 1.126 ms | 2.402 ms |
| Redis | 4,094 | 0.767 ms | 0.989 ms | 2.699 ms |

The new commit witness adds a second synchronized file and substantially increased write latency in this test. SQLite's median write was lower, while its run-level write p99 varied between 3.55 and 19.00 ms. These short, closed-loop runs include different protocols, drivers, container forwarding, and database features. They do not establish a general speed or durability ranking; the raw file contains individual results, binary hashes, settings, environment, and limits.

In a separate [process-crash check](../benchmarks/results/2026-11-03-durable-crash-checks.json), each engine acknowledged 100 writes, its benchmark server or container was killed, and the same storage was reopened. All 100 values were verified for each engine. This tests one abrupt process interruption after completed writes; it does not simulate power loss, storage reordering, or crashes at every commit boundary.

A [local restore drill](../benchmarks/results/2026-11-03-restore-drill.json) wrote a backup to a separate temporary filesystem, took the source path offline, restored 99 values and a deletion, and confirmed a later write was absent. Reproduce it with `cargo run --locked --example restore_drill -- <database-parent> <backup-parent>`. Both locations in the recorded run were on the same machine; this is not an off-site or independent failure-domain recovery drill.

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

## Avoiding notifications without waiters

[Raw results](../benchmarks/results/2026-11-03-notification-comparison.json) record seven runs per configuration using 250 cached keys, 64-byte values, 1,000 operations per client, and roughly 50% writes. Builds used separate initially empty target directories. The before binary is from `8103736`; revision `7871777` changes only documentation and recorded results.

| Clients | Before ops/s | After ops/s | Before write p99 | After write p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 6,778 | 7,204 | 1.439 ms | 0.601 ms |
| 8 | 21,963 | 22,675 | 2.044 ms | 1.334 ms |
| 64 | 74,204 | 86,754 | 4.821 ms | 3.262 ms |

These are medians of run-level measurements on the same virtualized overlay environment, with substantial scheduling and tail variability. Single-client write p50 increased slightly from 0.231 to 0.236 ms. SQLite's single-client p50 was 0.262 ms in this sample, but its eight-client p50 remained much lower: 0.195 ms versus the prototype's 0.676 ms. This does not establish a sustained single-client advantage or universal superiority. SQLite also implements SQL and transactions absent from this prototype.

A separate instrumented single-client run counted 981 futex calls before and three after, with exactly 489 `fdatasync` calls in both runs, including initialization. The optimization skips notifications when nobody is waiting; it does not skip synchronization. Tracing affects timings, which are not used in the table. Existing failure, queue-capacity, handoff, and concurrent-history tests exercise notification correctness.

## Historical index memory and lookup measurement

The Linux-only `index_memory_bench` and `scripts/index_memory_compare.py` compare the checkpoint-loaded writable store with the historical checkpoint view in separate fresh processes. They prepare a disposable fixture, alternate measured order, check every returned value, report each open and point-read time and Linux process RSS, and record VM/cgroup noise. The fixture has no log tail. The view holds a shared lock and does not replay or validate a tail, whereas the writable store does, so they have different semantics. RSS excludes the OS page cache and is not a memory quota. Both openings hash the full checkpoint prefix.

For example, from the repository root, with an existing directory for temporary fixtures and a new output path:

```sh
cargo build --release --example index_memory_bench --locked
python3 scripts/index_memory_compare.py --directory /tmp --output /tmp/index-memory-results.json
```

The default cases use 10,000 and 200,000 keys, 128-byte keys, 64-byte values, five repetitions per mode, and 16 point reads per repetition. These are warm-cache, direct API measurements on one VM, not a competitor or production comparison. `--keys`, `--key-bytes`, and `--rounds` change the workload; the two modes must not be described as offering identical recovery or concurrency behavior.

An initial [raw measurement](../benchmarks/results/2026-11-03-index-memory.json) on the earlier 1 MiB-block reader shows the tradeoff, using medians across five separate processes per mode:

| Keys | Opened writable store RSS | Historical view RSS | Writable median lookup | View median lookup |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 3,724 KiB | 1,380 KiB | 1.47 µs | 898 µs |
| 200,000 | 48,160 KiB | 1,384 KiB | 4.13 µs | 1,135 µs |

The fixture log and checkpoint were approximately 42.6 MB and 29.2 MB for 200,000 keys. Each view lookup read and checked its whole index block. Both initial quiet checks passed; individual samples recorded VM steal, including a short sample at 12.5%. These results expose a large lookup penalty and do not establish general performance. The initial harness read `VmHWM` after dropping the object and produced peak figures below an earlier RSS sample; those peak fields are invalid and must not be used. The current harness samples RSS while each object remains open and does not claim to measure peak memory.

A later [raw measurement](../benchmarks/results/2026-11-03-index-memory-smaller-blocks.json) uses 32 KiB verified blocks and prefix summaries, with the same fixture sizes and five repetitions per mode. The runs were separate, not paired on the same fixture:

| Keys | Earlier view median lookup | Smaller-block view median lookup | Smaller-block view RSS at open | Writable store median lookup |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 898 µs | 29.5 µs | 1,384 KiB | 1.33 µs |
| 200,000 | 1,135 µs | 30.6 µs | 1,420 KiB | 3.84 µs |

At 200,000 keys this is roughly a 37-fold reduction for the view's sampled median, while the writable store remained faster at point lookups. The view held 893 block summaries, and RSS after its sampled lookups was 1,452 KiB. Both new quiet checks passed, no cgroup throttling or direct reclaim was recorded, and the largest VM steal sample was 1.82%. These keys differ within their first 16 bytes; longer common prefixes can require extra verified block reads. This is a warm-cache result on one VM, not a general latency, memory or competitor claim.

## Separate-copy compaction comparison

`examples/compaction_bench.rs` prepares a synthetic store with 64-byte values, overwrites half of its keys, deletes one eighth, times a verified `compact_to` operation and checks selected live and deleted values after reopening the copy. `scripts/compaction_compare.py` alternates two separately built binaries across fresh fixtures, records each run and VM/cgroup noise, and reports medians and ranges. This measures separate-copy compaction, which blocks source mutations; it does not measure online compaction or physical power-loss behavior.

Build each revision in its own clean checkout and empty `CARGO_TARGET_DIR`. From the driver checkout, provide both binaries, their revisions, an existing temporary-fixture directory and a new result path:

```sh
python3 scripts/compaction_compare.py --directory /workspace --output /tmp/compaction-results.json \
  --before-binary /tmp/build-before/release/examples/compaction_bench --before-revision <before-commit> \
  --after-binary /tmp/build-after/release/examples/compaction_bench --after-revision <after-commit>
```

Each revision must contain the same benchmark example. Binary hashes are recorded, but the revision-to-binary association is supplied by the caller. The defaults use 200 and 5,000 source keys and five repetitions per version.

The [recorded comparison](../benchmarks/results/2026-11-03-compaction-grouped.json) builds revisions `c4c73e1` and `af617d6` in separate clean worktrees and empty target directories, then alternates their binaries across fresh fixtures. The candidate groups ordinary records before synchronization and still reopens and verifies the target before publication.

| Source keys | Live keys | Before median (range) | Grouped median (range) |
| ---: | ---: | ---: | ---: |
| 200 | 175 | 46.1 ms (45.0–55.5 ms) | 4.11 ms (3.50–10.68 ms) |
| 5,000 | 4,375 | 1,169.5 ms (1,120.7–1,186.2 ms) | 29.63 ms (26.49–30.84 ms) |

For 5,000 source keys, both versions produced a 406,903-byte compacted log from a 716,095-byte source log. The grouped median was approximately 39 times faster in this sample. Both quiet checks passed; the VM recorded no cgroup throttling or direct reclaim, with individual steal samples up to 4.35% during fixture setup, measurement and cleanup. These are five warm-cache runs on one virtualized filesystem, with 64-byte values and selected content checks. They do not establish power-loss safety, online compaction performance, or a competitor ranking.

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
