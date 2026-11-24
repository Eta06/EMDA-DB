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

Release an inactive database's worker from shared compute capacity while keeping its data durable. Wake it when authorized work arrives. Shared services and durable storage retain their costs. Safe draining, ownership coordination, recovery, capacity limits, and cold-start behavior need tests. See the [scale-to-zero design goals](SCALE_TO_ZERO.md).

## Medium: online schema and index operations

After transactions, snapshots, and stable indexes exist, investigate concurrent index construction, progress reporting, validation of schema changes, and resumable/recoverable migrations. Document when an operation blocks, cancels, or must be rolled back; do not promise zero downtime before measuring and testing it.

## Medium: native compression

Evaluate optional LZ4 and Zstandard and per-collection/table policies after the durable format and checksums are designed. Measure storage ratio, CPU, small-record behavior, decompression latency, and end-to-end performance. Compression must preserve data, version compatibility, and recovery behavior; no codec is assumed to win every workload.

## Research / long-term: adaptive storage

Investigate LSM trees for suitable write-heavy workloads, B+ trees for suitable lookup/range workloads, in-memory hot-data structures, adaptive caching/compaction, and per-collection/table configuration. These are workload hypotheses, not universal winners. Multiple storage strategies must share the same transaction and recovery contract. Start only after a stable baseline and measurements exist.

## Agent context and natural-language search

Research an optional integration that retrieves project-scoped memory and relevant skills before the agent's first model call. Return a small set of source-linked excerpts with their dates, plus candidate skill IDs and versions from the application's approved catalog. The runtime loads the chosen skill. Treat recalled conversations as reference material and prefer the current version of a decision when a later record supersedes it.

- Combine exact filters, text search, and optional vector similarity. Distinguish messages written by Emir from messages about Emir, and return surrounding conversation when it helps explain a match.
- Resolve known fields in the application and pass typed filters to the engine. An agent can supply a time range directly. A website can map “Kadıköy’de 4+1, 10 milyona kadar” to location, room layout, and price filters. Distinguish the previous calendar month from the last 30 days and specify the time zone.
- Keep the engine's query path free of model calls. Accept precomputed query vectors and index document and skill embeddings asynchronously. Keep text search available while embeddings are pending, and check document versions so stale index entries cannot surface deleted or superseded content.
- Evaluate rules before optional classifiers such as [Jev](https://docs.typesafe.ai/introduction) or [Laya](https://huggingface.co/convaiinnovations/laya-multilingual) for routing, skill ranking, or memory labels. Keep classification outside the engine and allow low-confidence decisions to abstain.
- Prototype with a conversation archive and skill catalog. Test Turkish, English, mixed-language prompts, and decisions that change over time. Measure relevant results found, context tokens, sustained queries per second, and p95/p99 latency during bursts and concurrent writes. Report engine-only and end-to-end latency separately.

## Language bindings and interoperability

Start with embedded Node.js/TypeScript and Python bindings after the basic engine API and error contract are ready. Use the same Rust engine and begin with open/close, reads and writes, ordered scans, and atomic batches. Add document operations once the shared surface is tested.

- Pin each package build to an engine revision and record supported API and disk-format versions. Run the same compatibility cases across Rust, Node.js, and Python, including writing and closing a database in one language and reopening it in another. Cover binary values, integer boundaries, and writes with an uncertain outcome. Update the shared tests and affected bindings when engine behavior changes.
- Evaluate SQL and MongoDB interoperability against concrete user workflows. Compare import/export, a documented query subset, and existing-driver support as distinct deliverables. Validate any supported type and query mappings before making compatibility claims.

## Harbor, Lens, and Datasets

Harbor is the proposed managed service for EMDA-DB. Lens is the proposed client for Harbor and self-hosted EMDA-DB. Both use the same engine capabilities and data contracts. Harbor adds hosted provisioning, region selection, plan limits, backups, and operational management. Self-hosted operators configure supported engine and service controls through the same configuration model, including persistence, quotas, cache budgets, and automatic suspension. They provide their own infrastructure and integrations for capabilities such as replication, KMS, and off-host backups.

A Dataset is a named data unit directly inside a database. SQL, documents, and key-value are first-class Dataset models. In `test-db`, `Users` (SQL), `Product Prices` (key-value), and `JSON Info` (documents) are sibling Datasets. Lens can label the list “Datasets.” Define identifier quoting and reserved names for all three models, retain standard SQL table syntax, and specify cross-model visibility and transaction behavior.

- Pool compute across active databases with separate worker identities, storage access, and budgets. Enforce storage, write, network, CPU, RAM, I/O, and concurrency limits. A 5 GB live-data quota must reject an over-limit write before commit even during concurrent writes. Track indexes, logs, and temporary maintenance space against explicit physical budgets. Measure the impact of one database saturating its quota on other databases.
- Start with verified single-node provisioning, private or authorized access, and backup/restore. Add placement across hosts and user-selectable regions, then test replica durability, failover, rolling upgrades, point-in-time recovery, and eventually sharding. Treat an off-region backup and a live replica as separate guarantees. Validate restore and node-loss behavior before promising recovery objectives.
- Use least-privilege workload identities, scoped keys, encrypted transport and storage, and independently authorized backups. Plan algorithm and key versioning, key rotation, and restore across rotations. Evaluate hybrid TLS key agreement and post-quantum signatures separately.
- Keep application logic in the application by default. Selected Datasets could later opt in to isolated execution with explicit privileges and resource budgets. Keep SpacetimeDB as a performance and realtime reference.
- Expand the [competitor inventory](COMPETITOR_FEATURES.md) into a versioned capability matrix. Track SQLite, MySQL, PostgreSQL, and MongoDB as the primary references. Use Redis, Turso, DuckDB, RocksDB, LMDB, FoundationDB, and Cloudflare D1 for relevant specialized capabilities. Compare equivalent workloads and durability settings.

## Storage modes and runtime policy

- **Durable SSD** is the proposed default. Keep the authoritative data on persistent storage and cache frequently used pages within a RAM budget.
- **Durable RAM-first** keeps the active data in RAM with a durable recovery representation on disk. Acknowledge durable writes after the required log synchronization. Test capacity limits and reconstruction time before offering suspension for this mode.
- **RAM-only** deliberately holds ephemeral data. Document that worker or host loss can discard it. Automatic suspension that preserves data requires a durable copy.

Use one default runtime policy with bounded caching and fast recovery. Treat Instant, Balanced, and Economy as deferred research on cache size, reserved compute, and worker lifetime. Preserve the chosen storage mode's guarantees when tuning performance. Measure idle compute, cold open, and warm p95/p99 before deciding whether to expose presets.

## CLI and MCP management

- **Harbor CLI and MCP server.** Manage hosted databases, regions, lifecycle settings, quotas, and backup and restore operations through the Harbor administration API. Use scoped credentials, audit records, and explicit authorization for state-changing operations.
- **Lens CLI and MCP server.** Inspect databases and Datasets, run bounded queries, and perform supported client operations against Harbor and self-hosted EMDA-DB. Use the same connection, identity, and capability rules as Lens. Start with inspection and read-only operations, then add explicitly authorized writes and administration.
- Share typed operations and authorization checks with the corresponding API. Test that a read-only credential cannot mutate data or change service settings, regardless of the interface used.
