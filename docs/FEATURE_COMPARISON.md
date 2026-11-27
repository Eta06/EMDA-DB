# Feature comparison

This page compares what EMDA-DB does today with engines people commonly
weigh it against. It compares capabilities by operation and deployment
model. It does not claim wire, SQL-dialect or file-format compatibility with
any of them, or feature parity. EMDA-DB is an experimental 0.2.0
development version; the other engines are mature production systems with
far broader feature sets than these rows show.

EMDA-DB entries describe `main` at `12e6de8f` and link to its own documents.
Other entries cite official documentation, checked on 2026-11-27. Behavior
that depends on a version or configuration is qualified. "Server" means a
separate process reached over a network or socket; "embedded" means a
library inside the application process.

| | EMDA-DB 0.2.0 | SQLite | PostgreSQL | MySQL 8.4 (InnoDB) | MariaDB | MongoDB | Redis |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Deployment | Embedded Rust library; macOS and Linux | Embedded C library [^sqlite-about] | Server | Server | Server | Server (standalone, replica set or sharded) | Server |
| Primary data model | Byte keys, JSON documents and SQL tables over one storage engine | SQL tables | SQL tables | SQL tables | SQL tables | JSON-like documents | In-memory data structures |
| SQL | Documented subset through a Rust API ([subset](SQL_SUBSET.md)) | Full dialect | Full dialect | Full dialect | Full dialect | No (query language and aggregation pipeline) | No |
| Joins | `INNER` and `LEFT`, up to eight tables, bounded | Including `RIGHT` and `FULL OUTER` since 3.39.0 [^sqlite-339] | Full | Full | Full | `$lookup` left outer join within one database [^mongo-lookup] | No |
| Foreign keys | Single column, `NO ACTION`/`RESTRICT`, always enforced | Off by default; enabled per connection with `PRAGMA foreign_keys` [^sqlite-fk] | Composite keys, `CASCADE`/`SET NULL`/`SET DEFAULT` [^pg-fk] | Yes (InnoDB) [^mysql-fk] | Yes (InnoDB) | No | No |
| Subqueries | Uncorrelated `IN`, `EXISTS` and scalar, depth 4, bounded | Yes, including correlated | Yes, including correlated | Yes, including correlated | Yes, including correlated | Not SQL; pipeline stages | No |
| Secondary indexes | Documents: compound, unique, partial. SQL: 1–4 columns, unique, online build | Yes | Yes, several index types | Yes | Yes | Yes | No (keys only; modules aside) |
| Transactions | Serializable optimistic transactions across keys, documents and SQL statements | Serializable [^sqlite-iso] | Read Committed by default; Serializable via SSI [^pg-iso] | Repeatable Read by default; Serializable available [^mysql-iso] | Repeatable Read by default (InnoDB) [^mariadb-iso] | Multi-document; replica set (4.0+) or sharded cluster (4.2+), not standalone [^mongo-tx] | `MULTI`/`EXEC` without rollback [^redis-tx] |
| Durable acknowledgement | Every write is synced before it is acknowledged; on macOS a barrier-ordered log sync plus `F_FULLFSYNC` of the commit witness ([contract](STORAGE_TRANSACTION_CONTRACT.md)) | Configurable (`synchronous`) | Configurable (`synchronous_commit`, `fsync`) | Configurable (`innodb_flush_log_at_trx_commit`) | Configurable | Configurable write concern and journaling | AOF `everysec` by default; `always` optional [^redis-persist] |
| JSON | Native document collections with schema validation | JSON functions on text [^sqlite-json] | `json` and `jsonb` types [^pg-json] | Native `JSON` type [^mysql-json] | `JSON` is an alias for `LONGTEXT` with a validity check [^mariadb-json] | Native | Via modules |
| Replication and failover | Not yet ([roadmap](ROADMAP.md)) | Not built in | Built in [^pg-ha] | Built in | Built in | Built in | Built in |
| Network service and auth | Not yet ([roadmap](ROADMAP.md)) | None (embedded) | Yes | Yes | Yes | Yes | Yes |

## Reading the table

- EMDA-DB's SQL is a bounded subset. Statements, rows visited, rows
  returned, sort buffers, groups and subquery values all have limits, and
  exceeding one fails instead of truncating. The [SQL subset](SQL_SUBSET.md)
  lists every bound and every unsupported form.
- One multi-statement SQL transaction stages at most 1,024 keys and
  16 MiB. On the ordinary `Store`, a transaction's snapshot holds at most
  16,384 keys; `CheckpointStore` does not have that limit.
- The rows describe capability, not speed. Measured comparisons are in
  [performance](PERFORMANCE.md), each with its own settings and caveats.
- The [competitor inventory](COMPETITOR_FEATURES.md) tracks a longer list of
  capabilities and engines.

## Sources

[^sqlite-about]: SQLite, [About SQLite](https://www.sqlite.org/about.html).
[^sqlite-339]: SQLite, [release 3.39.0](https://www.sqlite.org/releaselog/3_39_0.html): "Add (long overdue) support for RIGHT and FULL OUTER JOIN."
[^sqlite-fk]: SQLite, [Foreign key support](https://www.sqlite.org/foreignkeys.html): constraints "are disabled by default … so must be enabled separately for each database connection."
[^sqlite-iso]: SQLite, [Isolation in SQLite](https://www.sqlite.org/isolation.html): "Transactions in SQLite are SERIALIZABLE," except shared-cache connections with `read_uncommitted`.
[^sqlite-json]: SQLite, [JSON functions](https://www.sqlite.org/json1.html).
[^pg-fk]: PostgreSQL, [Constraints](https://www.postgresql.org/docs/current/ddl-constraints.html).
[^pg-iso]: PostgreSQL, [Transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html): "Read Committed is the default isolation level"; Serializable uses Serializable Snapshot Isolation.
[^pg-json]: PostgreSQL, [JSON types](https://www.postgresql.org/docs/current/datatype-json.html).
[^pg-ha]: PostgreSQL, [High availability, load balancing and replication](https://www.postgresql.org/docs/current/high-availability.html).
[^mysql-iso]: MySQL 8.4, [InnoDB transaction isolation levels](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html): "The default isolation level for InnoDB is REPEATABLE READ."
[^mysql-fk]: MySQL 8.4, [FOREIGN KEY constraints](https://dev.mysql.com/doc/refman/8.4/en/create-table-foreign-keys.html).
[^mysql-json]: MySQL 8.4, [The JSON data type](https://dev.mysql.com/doc/refman/8.4/en/json.html).
[^mariadb-json]: MariaDB, [JSON data type](https://mariadb.com/kb/en/json-data-type/): "JSON is an alias for LONGTEXT COLLATE utf8mb4_bin," with a `JSON_VALID` check added automatically.
[^mariadb-iso]: MariaDB, [SET TRANSACTION](https://mariadb.com/kb/en/set-transaction/): Repeatable Read "is the default isolation level for InnoDB."
[^mongo-lookup]: MongoDB, [`$lookup`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/): "Performs a left outer join to a collection in the same database."
[^mongo-tx]: MongoDB, [Production considerations for transactions](https://www.mongodb.com/docs/manual/core/transactions-production-consideration/): minimum feature compatibility version 4.0 for replica sets and 4.2 for sharded clusters; "standalone deployments do not support transactions."
[^redis-tx]: Redis, [Transactions](https://redis.io/docs/latest/develop/using-commands/transactions/): "Redis does not support rollbacks of transactions."
[^redis-persist]: Redis, [Persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/): fsync every second is "the suggested (and default) policy."
