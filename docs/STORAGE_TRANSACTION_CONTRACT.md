# Storage and transaction contract

This document separates **current prototype behavior** from **design targets that are not implemented**. It is not a production durability, isolation, or recovery guarantee. The same core contract must eventually govern key-value, document, and SQL interfaces when they operate on the same database. Each new interface must state its scope and any restrictions before it is shipped.

## Non-negotiable optimization rule

No performance optimization is accepted if it violates the durability, isolation, or recovery guarantees in force for the feature. This rule also protects the prototype's documented behavior; the absence of a production guarantee is not permission to weaken its write synchronization, coordination, corruption detection, or recovery behavior. This applies to zero-copy access, caching, memory mapping, async I/O, compression, group commit, SIMD, assembly, and adaptive storage alike. The faster path must preserve the same contract, fail safely, and pass the same correctness and fault tests. Benchmarks compare equivalent guarantees; disabling synchronization or weakening isolation to obtain a better number does not meet this rule.

## Current prototype: one store in one process

### Successful writes and ordering

- Each `put` or `delete` appends one checksummed record and returns success only after `File::sync_data` covering that record and the in-memory index update complete. One log leader appends records in queue order. Concurrent single-key mutations can share a synchronization; grouping does not make them an atomic multi-key transaction. Deleting a missing key writes a deletion record. Deleted and superseded values remain in the append-only log and full-log backups; delete does not perform secure erasure.
- Readers share an index read lock and can observe the previous indexed value while a writer synchronizes; the index update is exclusive. A read beginning after a successful write returns that value unless a later mutation replaces or deletes it, or an I/O/corruption error occurs. An overlapping read can occur before or after the index update.
- A single successful mutation concerns one key. There is no multi-key transaction, atomic batch, range query, user-visible sequence number, or change stream. `get` returns an owned byte vector; the engine does not offer zero-copy reads.
- An OS advisory lock prevents cooperating processes from opening the same store at once. This is not a security boundary or a guarantee against processes that ignore the lock.
- The commit queue limits encoded records awaiting completion to 64 MiB and 1,024 records, with up to 64 records per sync. Writers wait when admission is full. This bounds encoded bytes, not total process memory or the number of caller threads; there is no timeout or service-level admission policy. Backups wait for in-flight writes and exclude new writes while the log is copied.

### Crash, power failure, and uncertain outcomes

- After a successful return, the prototype has requested synchronization of the appended log data. The intended process-crash behavior depends on the filesystem and device honoring that request. The tests cover reopening after a killed process, not every crash point or hardware failure.
- If append or synchronization returns an error, some or all bytes may already have been written. Unacknowledged mutations in the affected group and queue fail with **unknown** outcomes, and this store instance refuses further reads and writes until reopened. A complete but unacknowledged record can appear after recovery. A panic in the commit leader also fails the store and wakes waiting writers. There is no request-id or retry-deduplication guarantee.
- On reopen, the engine scans the log in append order and refuses to open an incomplete final record, invalid header, or checksum error without truncating the log. An apparently incomplete tail can result from an interrupted unacknowledged write or storage damage; the prototype cannot distinguish them and does not automatically repair either case. An existing store with a lock marker but no log is not silently recreated as empty. The prototype has no independent durable high-water mark: loss of all store files, or truncation exactly at a valid record boundary (including an empty log), may go undetected. Independent verified backups and a stronger commit/recovery design are required.
- Store initialization synchronizes the path and its ancestor directories, but no universal power-loss guarantee is established: underlying filesystem and device behavior matters, and actual power-cut testing is absent. Full-log backup/restore is experimental and depends on supported filesystem operations; independent backups and restore drills are still necessary. Do not use this prototype for important production data.

### Isolation, snapshots, and interfaces

- There are **no transaction isolation levels** today. The lock behavior above covers individual in-process key/value operations only; it does not provide multi-key atomicity, snapshot isolation, serializable transactions, or distributed consistency.
- There are no user-visible snapshots, snapshot lifetimes, version garbage collection, or compaction. The log grows without bound and its key index is held in memory.
- SQL and document interfaces do not exist, so no atomicity or consistency guarantee across interfaces is currently available.

### Corruption and format compatibility

- Header and record CRC32 checks detect some accidental corruption on reopen; current value bytes are checked on read. CRC32 is not authentication, cannot detect every possible error, and cannot reconstruct damaged data. A detected checksum failure is reported; no automatic repair from replicas or backups exists. Backups add a length and SHA-256 digest and revalidate records, but are unencrypted, unauthenticated, and not proof of protection against all storage failures.
- Records use the experimental `DB01` marker. Unknown or invalid markers are rejected. The software version and on-disk format version are separate; the current prototype has no stable backward-compatibility, downgrade, or migration promise. See [VERSIONING.md](VERSIONING.md).

## Proposed contract for future design

The following are acceptance targets, not existing guarantees. Exact algorithms and API behavior must be specified, reviewed, and tested before release.

1. **Durability:** Define the exact acknowledgement point, storage and filesystem assumptions, crash and power-failure model, partial I/O behavior, and how an unknown result is reported. Acknowledged writes must survive the documented failures under those assumptions; failed requests must never be silently treated as definitely absent. Backups and restores require separate verification.
2. **Transaction isolation:** Document every supported level and its anomalies. A **serializable default within one database** is a proposed starting point, subject to design and benchmarks; it is not implemented or finalized. If offered, serializable transactions must prevent dirty reads, non-repeatable reads, phantoms, and write skew. Serializability alone must not be described as a real-time ordering guarantee.
3. **Cross-interface atomicity:** Within a single supported transaction and database, key-value, document, and SQL operations must use the same commit and isolation machinery. Record and index updates commit together or not at all. Cross-database, cross-project, external service, and distributed transaction behavior must be specified separately; no guarantee is assumed.
4. **Ordering and consistency:** Define operation and commit order, read-your-writes, concurrent conflict behavior, and what a client may observe during and after failure. For streams and replication, define the scope of ordering, acknowledgement, retry, and delivery semantics explicitly.
5. **Snapshots and garbage collection:** Define snapshot visibility, lifetime, resource limits, cancellation/expiry behavior, and how retained versions are reclaimed. Compaction and garbage collection must not invalidate an active supported snapshot or remove data needed for recovery. Any timeout must be explicit rather than silently changing a result.
6. **Corruption:** Specify which corruption is detected, when the engine stops safely, and how recovery from a verified backup or replica works. Checksums alone must never be presented as data recovery.
7. **Formats:** Version record, index, checkpoint, snapshot, API, and protocol formats independently where appropriate. Define supported upgrades, downgrades, migration and rollback, and fail safely for unknown versions. Compatibility claims require tests using persistent fixtures from the supported versions.
8. **Sleep and wake:** Stopping an idle project worker must not weaken this contract. Define draining of in-flight operations, snapshots and background jobs, durable stored data, ownership fencing, bounded wake-up retries, and recovery errors. No scale-to-zero behavior is implemented today; see [SCALE_TO_ZERO.md](SCALE_TO_ZERO.md).
9. **Project boundaries:** Transactions, caches, indexes, snapshots, logs, streams, and recovery must not access another project's data without explicit authorization. Enforce and test this in the service, engine, and storage layers; see [PROJECT_ISOLATION.md](PROJECT_ISOLATION.md). No tenant security is implemented today.

## Evidence required before a guarantee is advertised

Deterministic partial-append, injected disk-full and sync errors, short-read errors, a controlled concurrent read during sync, grouped-sync failure and panic tests, queue-limit tests, corrupted headers/payloads, process termination after acknowledged concurrent writes, and backup/restore checks exist. Further work must exercise interruption at every commit/backup boundary, directory-sync and actual power-loss models, broader model-based concurrent histories, and persistent format-compatibility fixtures. A process kill alone is insufficient for power-loss claims. Snapshot, compaction, index, and cross-interface tests must be added as those features are implemented. Report the assumptions and any untested failure cases alongside the results.
