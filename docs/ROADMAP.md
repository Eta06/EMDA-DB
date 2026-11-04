# Database roadmap

The product name is a placeholder. This roadmap describes intended work, not implemented features or a release schedule. Its ambition is broad parity with relevant PostgreSQL and MongoDB capabilities plus useful new ideas. The [competitor inventory](COMPETITOR_FEATURES.md) records the currently researched capabilities and their sources; it is a living inventory, not an exhaustive list of every release-specific option. New discoveries and changed requirements should be added without implying feature compatibility before it is implemented and tested.

See [foundation and storage follow-up](FOUNDATION_FOLLOW_UP.md) for deferred validation and the current priority 0/1 checklist.

## Step numbering and current status

In the implementation discussion, **step 1** refers to priority 0 below (correctness), **step 2** to priority 1 (durable core), **step 3** to priority 2 (document model), and **step 4** to priority 3 (SQL). Steps 1–3 are still in progress; the checklists below and in the linked follow-up documents identify completed and remaining work. An experimental feature or a passing short test campaign is not the same as its full exit criteria.

The physical power-loss test and independent off-host restore drill remain specifically deferred for later sessions. They are separate from the still-testable work: bounded writable-index and memory management, automatic safe log reclamation and bounded recovery, broader overload/resource controls, richer document queries and operators, and more representative concurrency, fault and performance evidence. See [foundation follow-up](FOUNDATION_FOLLOW_UP.md) and [document follow-up](DOCUMENT_FOLLOW_UP.md) for details.

## Current baseline

The prototype provides byte-key operations, a checksummed append log, fail-closed handling of incomplete records, concurrent readers and bounded group commit, experimental backup/restore, separate-copy compaction, and an advisory ownership lock. It is not production-ready. The in-memory key index is unbounded, live-store compaction is an opt-in ordinary-store prototype, multi-key transactions are an early bounded prototype, and some durability and corruption cases remain unresolved. See [architecture and limitations](ARCHITECTURE.md).

## Priority 0: correctness and measurable foundations

These are prerequisites for trusting any feature built above the engine:

**Status: in progress.** V3 stores on supported Linux filesystems synchronize log data and a metadata witness together; V2 retains its two-sync path. Existing stores can be copied and verified into the current format. Boundary fault tests, multi-key concurrent histories for independent operations, coverage-guided fuzzing, a local cross-filesystem restore drill, and a ten-engine comparison exist. Backup tests cover byte-by-byte truncation and corruption, competing restores, and symlink protection. Broader crash and directory-sync fault models, off-host restore drills, resource measurements, and physical power-failure validation remain before stronger reliability claims or this priority's full exit criteria are met.

- Define operation, error, concurrency, acknowledgement, and recovery semantics, including unknown write outcomes.
- Harden and version the log format; distinguish incomplete writes from corruption as far as the format allows, and fail safely when the distinction cannot be made.
- Handle durable directory creation and metadata changes; define filesystem and hardware assumptions explicitly.
- Expand the initial I/O fault-injection and malformed/truncation tests to cover directory synchronization, crashes at commit boundaries, model-based concurrency, and fuzz testing. Process termination tests do not prove power-loss safety.
- Exercise the experimental backup/restore implementation under broader failure models and independent restore drills before claiming that stored data is protected.
- Expand the initial SQLite comparison harness for existing CRUD, concurrency, recovery, memory, and I/O; compare equivalent operations and durability settings with PostgreSQL, SQLite, Turso, and MongoDB where their models allow.

Exit evidence: documented guarantees and known failure boundaries, repeatable fault tests, verified restores, and published benchmark configuration and results. Do not claim production readiness or competitor performance parity before these exist.

## Priority 1: durable storage and core operations

- Persistent on-disk index and bounded memory/cache management; avoid requiring every key or value to fit in RAM. A verified checkpoint, historical reader, recovered read-only view and experimental writable checkpoint-backed store exist. The writable path bounds its suffix overlay, supports prototype transactions, and can opt into on-demand automatic checkpoint rollover with two reusable slots and a separately started background worker. Ordinary recovery still loads all keys, and checkpoint openings hash the complete covered log prefix.
- Log checkpoints, in-place/online compaction, garbage collection, and bounded recovery time. Verified separate-copy compaction exists for both writable paths; the checkpoint-backed variant streams its indexes and emits a checkpoint, and both have opt-in blocking Linux atomic handoffs that retain the old copy. An opt-in ordinary-store wrapper copies with concurrent callbacks and pauses them for final verification and exchange; callers can retain the previous store or explicitly reclaim it after the synchronized handoff. An explicitly started ordinary-store worker can schedule reclamation, but automatic retry and crash-leftover handling, checkpoint-store online compaction and total-memory limits remain open.
- Ordered range scans, pagination and bulk reads: **prototype implemented** with entry/byte limits. Bounded read-only snapshots offer stable pages within one process and have per-store aggregate key/count limits. Single-key compare-exchange and signed 64-bit increment are implemented and serialized against mutations. Atomic bulk writes use bounded batches.
- Atomic batches and optimistic serializable read-modify-write transactions: **prototype implemented** with bounded snapshots and DB02 records, including explicit older-reader incompatibility. The default mode conflicts on any intervening mutation; an opt-in bounded read-set mode validates observed points and complete ranges. Broader model checking, workloads, and transaction resource controls remain open.
- Safe coordination between concurrent readers and writers, connection/request limits, and fair overload handling.
- Explicit limits and storage quotas. Optional per-open log-length, live-key count and logical live-key byte caps exist; persistent project-wide quotas, other files and total memory limits remain open. No performance optimization may violate the durability, isolation, or recovery guarantees in force; see the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md).

Exit evidence: independent concurrent clients produce correct results, transactional writes are atomic, recovery preserves acknowledged writes under the documented fault model, and memory stays within configured limits in measured workloads.

## Priority 2: document data and queries

**Status: experimental prototype.** The [document interface](DOCUMENT_MODEL.md) and [follow-up checklist](DOCUMENT_FOLLOW_UP.md) describe the scope and remaining work. The interface implements a deliberately limited subset of the items below over the shared transaction engine. It includes atomic CRUD, conditional and bulk writes, schemas, bounded collection and indexed queries including array predicates, complete-tuple equality, single-field range and safely implied partial-index planning, compound/unique/partial indexes, basic aggregation and ID lookup. Resumable bounded index builds, drops and schema validation are available; richer query operators, broader concurrency/fault/recovery coverage, comparable document benchmarks and a query optimizer remain open. MongoDB compatibility is not claimed.

- Collections, typed fields, nested values, schema validation, and explicit missing/null behavior.
- Document create/read/update/delete, upsert, bulk operations, conditional updates, and atomic operators.
- Filters, projection, sorting, pagination, and indexed equality and range queries.
- Secondary, unique, composite and partial indexes; maintain indexes atomically with records.
- Aggregation, relationships/lookup and richer operators as semantics are specified and tested.
- Query plans, statistics, explainability, and bounded execution.

Exit evidence: representative MongoDB-style CRUD and queries work through a documented interface with correctness, concurrency, recovery, and benchmark coverage. Compatibility with a particular MongoDB version or wire protocol is a separate decision.

## Priority 3: relational and SQL interfaces

- Tables, typed columns, constraints, primary and foreign keys, and schema migrations.
- SQL parsing and planning; begin with a documented subset of `CREATE`, `INSERT`, `SELECT`, `UPDATE`, and `DELETE`.
- Joins, aggregates, sorting, indexes, transactions, and query diagnostics in measured increments.
- Expand supported SQL features and standards compatibility only with behavior and regression tests.

Exit evidence: a documented SQL subset works over the shared transaction and storage engine. Full PostgreSQL SQL or wire compatibility is not implied.

## Priority 4: secure service and project isolation

- Add a bounded network API and SDKs with timeouts, authentication, authorization, TLS, and audit-safe diagnostics.
- Define project/user identities, roles, data access rules, and least-privilege service boundaries.
- Enforce project isolation at each layer and test cross-project access, unauthorized operations, malformed requests, and resource exhaustion.
- Control per-project requests, connections, CPU, memory, storage, and background work; use admission control when capacity is exhausted.
- Build project lifecycle and safe sleep/wake separately from the embedded engine. Store data durably while workers sleep; specify cold-start, ownership, and active-transaction behavior. See [scale-to-zero design goals](SCALE_TO_ZERO.md).

Exit evidence: isolation and overload tests pass and operational limits are measurable. A process boundary alone does not establish complete isolation or high availability.

See [project isolation design goals](PROJECT_ISOLATION.md) for threat boundaries, possible isolation tiers, and the evidence required before public claims.

## Priority 5: performance and scalability

- Profile the full workload before changing the engine; track latency distributions, throughput, resource use, failures, and correctness.
- Evaluate the initial group commit implementation under broader workloads and failure models; investigate efficient binary encoding, cache policies, concurrency strategies, and compact indexes with equivalent durability semantics.
- Evaluate SIMD and assembly only for measured hotspots, with CPU feature detection, portable fallbacks, fuzzing, and identical correctness tests.
- Add project fairness, capacity-aware scheduling, cold-start optimization, and safe multithreading based on measured results.
- Compare against PostgreSQL, MongoDB and other relevant systems using [BENCHMARKS.md](BENCHMARKS.md); publish raw results and configurations.

The goal is to be competitive or better on specific measured workloads. No engine can be assumed to outperform every competitor on every operation, hardware configuration, and durability setting.

## Priority 6: replication and operations

- Replication and failover with explicit consistency and acknowledgement semantics.
- Point-in-time recovery, independent backups, restore drills, retention, and disaster recovery.
- Monitoring, tracing, metrics, audit events, query diagnostics, alerts, and capacity planning.
- Horizontal partitioning/sharding only after single-node and replication behavior are sound.
- Rolling upgrades, format migrations, imports/exports, SDK compatibility, and operational tooling.

Exit evidence: controlled node/process failures and restore tests meet documented recovery objectives; distributed failure tests demonstrate the stated guarantees.

## Priority 7: broader features and smarter tooling

The [competitor inventory](COMPETITOR_FEATURES.md) tracks additional capabilities to investigate, including full-text and geospatial search, vector similarity, time-series data, TTL, change streams/realtime subscriptions, more index types, server-side logic, and ecosystem tooling. Availability, compatibility, and implementation order remain decisions, not promises.

The prioritized [performance and feature ideas](PERFORMANCE_AND_FEATURE_IDEAS.md) also cover fault injection, workload replay, observability, low-copy access, compression, online index/schema operations, embedded/cloud portability, and adaptive storage. Their correctness and transaction prerequisites determine implementation order.

Potential differentiators to validate with real users and benchmarks:

- Query and index recommendations that explain expected tradeoffs.
- Optional resource-aware scheduling and predictable cold-start controls.
- Clear data-access explanations, diagnostics, and recovery guidance.
- Schema-change analysis and safer migration planning.
- Optional AI-assisted query or schema suggestions using explicitly configured data-access and privacy boundaries.

Suggestions must not silently weaken authorization or durability, destroy data, or change a project's configuration without an explicit mechanism chosen by its owner. Benchmark potential improvements before enabling them by default.
