# Performance and feature ideas

These ideas are **planned or research**, not implemented features. Priorities reflect the current proposal; sequencing follows dependencies and evidence. No performance optimization may violate the durability, isolation, or recovery guarantees in the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md).

## Critical: deterministic fault injection

The initial test I/O boundary injects partial appends/disk-full errors, sync failures, short reads, and controlled concurrent schedules. Expand it to failures during recovery, directory synchronization, future checkpoints, compaction, and migrations, and verify atomicity as transaction support is added. Process-kill tests exist, but they do not replace these tests or simulate all power-loss behavior.

**Why now:** the engine needs a trustworthy correctness baseline before complex optimization or new query interfaces.

## Very high: workload replay and regression detection

Record synthetic workloads and, only with explicit permission and appropriate privacy handling, carefully sanitized real traces. Replay equivalent workloads across versions with reproducible seeds and configurations. Track p50/p95/p99 latency, throughput, errors, allocations, I/O, memory, correctness, and recovery. Publish raw results and method. Trace sanitization is not a guarantee of anonymity.

**Why now:** performance work needs measurable evidence. Shared CI runners should check that benchmark tooling works, not act as a stable performance laboratory.

## High: observability and profiling

Start with operation counts, latency distributions, I/O, synchronization, recovery time, memory, and contention. Add query timelines, index usage, slow-query reporting, compaction/write-amplification metrics, and human-readable plans when those components exist. Keep diagnostics bounded and avoid exposing user data or secrets.

## High: low-copy data access

Measure allocations and copies first. Explore borrowed views, efficient serialization, memory mapping, and low-copy record/batch paths where they improve the actual workload. Any borrowed data needs explicit lifetimes and safe interaction with mutation, compaction, and snapshot retention. Memory-mapped access has its own file truncation, mapping, and failure behavior; it is not a blanket guarantee of speed or safety. Current reads return owned values.

## High: change data capture and reactive queries

After transactional commits and durable ordering exist, explore subscriptions to collections, tables, and key ranges; filtered insert/update/delete events; durable resume positions; and a server-level streaming API such as WebSockets. Specify ordering scope, delivery and retry semantics, retention, authorization, and backpressure. Do not promise exactly-once delivery or a transport before it is designed and tested.

## High: time-to-live and expiration

Explore per-record TTL and collection defaults, efficient expiry scheduling, and automatic cleanup. Define clock behavior, snapshot visibility, transactions, change events, recovery, and replication semantics before implementation. Expiration is an explicit data-lifecycle operation, not an excuse to weaken committed-write guarantees.

## High: embedded-to-cloud portability

Design the same engine and documented data/query semantics for embedded applications and a standalone server, with explicit format and version compatibility, import/export, and migration tests. Cloud authentication, access control, quotas, network errors, and operational guarantees remain separate concerns; portability does not mean deployment modes have identical security or failure behavior.

## High: project scale-to-zero

Stop an inactive project's dedicated worker while keeping its data durable, and wake it when authorized work arrives. Shared platform services and stored data still have costs. Safe draining, ownership coordination, recovery, capacity limits, and cold-start behavior must be specified and tested first. See the [scale-to-zero design goals](SCALE_TO_ZERO.md).

## Medium: online schema and index operations

After transactions, snapshots, and stable indexes exist, investigate concurrent index construction, progress reporting, validation of schema changes, and resumable/recoverable migrations. Document when an operation blocks, cancels, or must be rolled back; do not promise zero downtime before measuring and testing it.

## Medium: native compression

Evaluate optional LZ4 and Zstandard and per-collection/table policies after the durable format and checksums are designed. Measure storage ratio, CPU, small-record behavior, decompression latency, and end-to-end performance. Compression must preserve data, version compatibility, and recovery behavior; no codec is assumed to win every workload.

## Research / long-term: adaptive storage

Investigate LSM trees for suitable write-heavy workloads, B+ trees for suitable lookup/range workloads, in-memory hot-data structures, adaptive caching/compaction, and per-collection/table configuration. These are workload hypotheses, not universal winners. Multiple storage strategies must share the same transaction and recovery contract. Start only after a stable baseline and measurements exist.
