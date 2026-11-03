# Competitor feature inventory

Research snapshot: 3 November 2026. This is a structured inventory of major engine and operational capabilities, not an exhaustive enumeration of every command, extension, managed-service feature, or release-specific option. PostgreSQL references below are to the current PostgreSQL 18 documentation; MongoDB references are to its current database manual. MongoDB features can depend on server version, edition, deployment topology, or Atlas. PostgreSQL extensions and external services are identified where relevant. Verify exact details before using a feature as a compatibility or benchmark requirement.

**Current prototype:** embedded byte-key/value `put`, `get`, and `delete`; checksum-verified append log; single log writer with bounded group commit and concurrent readers; fail-closed incomplete-tail handling; experimental full-log backup/restore; advisory exclusive lock. It has none of the SQL, document, multi-key transaction, index, server, cloud, or distributed features listed below unless explicitly stated. This table is a research backlog, not a feature commitment.

## Models and operations

| Capability | PostgreSQL | MongoDB | Prototype |
| --- | --- | --- | --- |
| Primary data model | Relational tables and rows; JSON/JSONB supported | Document collections and BSON documents | Byte keys and values |
| Create and manage structures | Databases, schemas, tables, columns, constraints | Databases, collections, optional validation | Store directory only |
| Create records | `INSERT`, including multi-row forms | Insert one or many documents | Single key `put` |
| Read by key | `SELECT` with indexed predicates | `find`/`findOne` and indexed filters | Single key `get` |
| Update | `UPDATE`, conditional expressions | Update one/many, update operators, update pipelines | Replace one value via `put` |
| Delete | `DELETE` | Delete one/many | Single key `delete` |
| Upsert | `INSERT ... ON CONFLICT` | Upsert options on supported operations | Overwrite and single-key compare-exchange are supported; no document or SQL upsert |
| Bulk operations | Multi-row SQL, `COPY` | Bulk write and multiple-document operations | Bounded bulk reads; no bulk writes or atomic batches |
| Filtering and projection | SQL `WHERE` and selected expressions | Query operators and projected fields | No filters or projection |
| Sort, limit, pagination | `ORDER BY`, `LIMIT`, cursors | Sort, limit, cursors | Bytewise ordered scan with exclusive bounds, bounded pages and process-local read-only snapshots |
| Joins and relationships | Joins and foreign keys | `$lookup`, references and embedded documents | None |
| Grouping and aggregation | `GROUP BY`, aggregates, window functions | Aggregation pipeline and operators | None |
| Schema rules | Types, defaults, checks, constraints | Flexible documents; optional schema validation | Keys/values are uninterpreted bytes |
| JSON-like data | `json`/`jsonb` types and operators | BSON document model | Bytes only |
| Time-series support | Timestamp types, partitioning and extensions | Time-series collections; availability/features vary | None |
| Geospatial | Typically PostGIS extension; basic geometric types in core | Geospatial indexes and queries | None |
| Full-text search | Built-in full-text search | Text indexes; Atlas Search is a separate managed feature | None |
| Vector similarity | Extension such as pgvector | Atlas Vector Search where available | None |

## Indexes and query execution

| Capability | PostgreSQL | MongoDB | Prototype |
| --- | --- | --- | --- |
| Primary/unique indexes | Primary keys and unique indexes | Unique indexes and document `_id` | In-memory key-to-log-offset index |
| Index types | B-tree, hash, GiST, SP-GiST, GIN, BRIN | Single/compound, multikey, text, geospatial, hashed, wildcard and other indexes | No persistent index or secondary indexes |
| Composite and partial indexes | Supported | Compound and partial indexes | None |
| Index-assisted query planning | Planner, statistics and `EXPLAIN` | Query planner and `explain` | No query planner |
| TTL expiration | Generally requires scheduling or extensions | TTL indexes | None |
| Disk-backed indexes and cache controls | Supported | Supported by storage engine | All keys currently reside in memory; values on disk |

## Transactions, consistency and durability

| Capability | PostgreSQL | MongoDB | Prototype |
| --- | --- | --- | --- |
| Atomic operations | SQL transactions | Single-document writes atomic; multi-document transactions supported with deployment restrictions | One mutation serialized and synced; no multi-key transaction |
| Isolation and concurrency | MVCC, locks, multiple isolation levels | Concurrency controls and transaction/read/write concerns | Concurrent readers and grouped single-key writes; no transaction isolation levels |
| Compare-and-set / conditional update | Conditional `UPDATE` and transaction patterns | Conditional filters on update | Single-key byte-value compare-exchange; no multi-key conditions |
| Write-ahead log and recovery | WAL and crash recovery | Journaling and recovery; guarantees depend on configuration and write concern | Append log and checksums; incomplete tails rejected without automatic repair |
| Checkpoints, compaction, cleanup | Checkpoints, vacuum and related maintenance | Storage-engine maintenance; details depend on engine/deployment | Verified separate-copy compaction; no live compaction or checkpoints; full log replay |
| Backup and point-in-time recovery | `pg_dump`, physical backups and WAL-based recovery when configured | Backup tooling and deployment-specific/Atlas backup options | Experimental full-log backup/restore; no PITR |
| Replication | Physical streaming and logical replication | Replica sets and replication | None |
| High availability/failover | Standby and external orchestration patterns | Replica set election/failover | None |
| Partitioning/sharding | Table partitioning; automatic distributed sharding requires additional technology | Sharded clusters | None |
| Change notifications | `LISTEN`/`NOTIFY`, logical decoding | Change streams on supported deployments | None |

## Security, operations, ecosystem

| Capability | PostgreSQL | MongoDB | Prototype |
| --- | --- | --- | --- |
| Authentication and authorization | Roles, grants, authentication methods and row-level security | Users, roles, privileges and supported authentication methods | None; local advisory lock only |
| Transport encryption | TLS when configured | TLS when configured | No network service |
| At-rest encryption | Typically filesystem/storage or additional tooling | Options depend on edition/deployment and infrastructure | None |
| Observability | Logs, statistics, `EXPLAIN`, monitoring extensions | Logs, profiler, metrics and deployment tooling | None |
| Extensions and server-side logic | Extensions, functions, triggers and procedural languages | Aggregation and server/deployment features; availability varies | None |
| Import and export | `COPY`, dump and restore tooling | Import/export and dump/restore tooling | None |
| Client interfaces | PostgreSQL protocol and drivers in many languages | MongoDB wire protocol and drivers in many languages | Rust library only |
| Managed service and dashboard | Supabase and other vendors offer managed platforms | MongoDB Atlas offers a managed platform | None |
| Automatic idle sleep | Not an intrinsic per-database PostgreSQL capability; orchestration such as Neon's adds it | Depends on managed offering and plan, not a general engine promise | None |

## Other systems to evaluate

These are different categories of systems; an engine, a database service, and a complete backend platform should not be treated as equivalent products.

| System | Capabilities and points to assess | Prototype status |
| --- | --- | --- |
| [SQLite](https://www.sqlite.org/wal.html) | Embedded relational SQL database, transactions and indexes; WAL allows readers alongside a writer but only one writer at a time per database. | Byte key/value operations only; no SQL or transactions. |
| [Redis](https://redis.io/docs/latest/get-started/) | In-memory data structures, atomic operations, replication and configurable persistence. Assess data structures, expiration, pub/sub, streams and operational semantics by version. | No in-memory database mode, TTL, pub/sub or replication. |
| [Turso Database](https://docs.turso.tech/tursodb/concurrent-writes) | Rust, SQLite-compatible embedded engine. Its default allows one writer; MVCC concurrent writes require explicit configuration and conflict retries. Some features are experimental. | No SQL or comparable concurrent transaction mode. |
| [SpacetimeDB](https://spacetimedb.com/docs/) | In-memory relational state, transactional reducers, durable commit log and subscriptions. Current server licensing is BSL and must be reviewed before use. | No relational model, reducers, subscriptions or replicated service. |
| [FoundationDB](https://apple.github.io/foundationdb/) | Distributed ordered key-value database with ACID transactions; assess clustering, fault tolerance and operational cost. | No multi-key transactions or distribution. |
| [RocksDB](https://rocksdb.org/docs/) | Embedded ordered key-value engine with an LSM tree, WAL, compaction, column families, snapshots, iterators and batching. | Separate-copy compaction and ordered scans; no live compaction, column families or multi-key batches. |
| [LMDB](https://www.symas.com/lmdb) | Embedded memory-mapped key-value engine with ACID transactions, concurrent readers, a single writer, named databases and cursors. | No multi-key transactions, snapshot API, named databases or cursors. |
| [DuckDB](https://duckdb.org/docs/stable/) | Embedded analytical SQL engine with columnar execution, joins, aggregations and external-file queries. | No SQL or analytical execution; point writes do not measure DuckDB's main strengths. |
| [Cloudflare D1](https://developers.cloudflare.com/d1/) | Managed SQLite-semantics databases with per-database resource limits and overload behavior. | Embedded local prototype; no hosted service or management plane. |
| [Neon](https://neon.com/docs/introduction/architecture-overview) | Serverless PostgreSQL architecture separates compute and durable storage and supports idle compute suspension. | No remote storage layer or automatic project sleep/wake. |
| [Appwrite](https://appwrite.io/docs/products/databases) | Backend platform with database APIs, permissions and other services; compare developer experience and project configuration separately from engine throughput. | No cloud platform or SDK. |

## Measured comparison coverage

The [local comparison](LOCAL_COMPARISON_2026_11_03.md) measures byte-key CRUD for the prototype and nine other engines, with a second SQLite binding/version configuration. These measurements do not imply SQL/document/transaction feature parity. The [coverage boundaries](COMPARISON_METHOD.md#coverage-beyond-local-point-operation-tests) explain why hosted platforms and SpacetimeDB are not represented by local engine numbers.

## Primary references

- [PostgreSQL feature matrix](https://www.postgresql.org/about/featurematrix/), [SQL language](https://www.postgresql.org/docs/18/sql.html), [indexes](https://www.postgresql.org/docs/18/indexes.html), [concurrency](https://www.postgresql.org/docs/18/mvcc.html), [WAL](https://www.postgresql.org/docs/18/wal-intro.html), and [replication](https://www.postgresql.org/docs/18/different-replication-solutions.html).
- [MongoDB overview](https://www.mongodb.com/docs/manual/introduction/), [CRUD](https://www.mongodb.com/docs/manual/crud/), [indexes](https://www.mongodb.com/docs/manual/indexes/), [replication](https://www.mongodb.com/docs/manual/replication/), [sharding](https://www.mongodb.com/docs/manual/sharding/), and [change streams](https://www.mongodb.com/docs/manual/changeStreams/).

Use the vendor documentation to check details when turning any row into a design or compatibility claim. See [ROADMAP.md](ROADMAP.md) for priorities and [BENCHMARKS.md](BENCHMARKS.md) for benchmarking methodology.
