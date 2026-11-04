# Document prototype follow-up

The initial document interface is bounded and experimental. This checklist records remaining work; it does not waive the [foundation and storage follow-up](FOUNDATION_FOLLOW_UP.md), physical power-loss test, or independent off-host restore drill.

- [x] Add a versioned JSON collection namespace and explicit schema, missing/null and typed-value semantics.
- [x] Add atomic CRUD, upsert, conditional updates, bounded bulk writes, and transactions that can also access unreserved raw keys. Test failure rollback, unique constraints, concurrent writers and acknowledged process termination on both writable backends.
- [x] Add bounded single/compound, unique and partial indexes, indexed equality/range pages, complete collection scans, filtering, scalar sorting and projection. Atomically maintain index and document entries; detect and rebuild damaged declared indexes within small-collection limits.
- [x] Add bounded count/group/sum aggregation and ID-based relationship lookup with explicit semantics and tests.
- [ ] Expand transaction history/model checking, interruption and storage-fault injection specifically for document/index operations. Run longer fuzz campaigns and retain any reproducer. A process kill is not a power failure.
- [ ] Measure and optimize document workloads with equivalent durability and comparable behavior against SQLite, MongoDB and other suitable alternatives. Publish workload definitions, distributions, concurrency, resource use, failures and noise; do not infer performance from the earlier byte-key baseline.
- [ ] Add scalable online index build/drop and schema migration with durable progress, bounded resource use, atomic publication, writer coordination and crash recovery. Current create, drop, rebuild and schema changes have small limits and may conflict or block.
- [ ] Design richer schema types and validation, comparison/update/aggregation operators, array and multikey behavior, relationships, diagnostics, statistics and a cost-based planner; define compatibility and limits before implementation. Current lookups are by ID, aggregation is limited, and collection queries require a complete bounded scan.
- [ ] Consider durable cursors/snapshots, cancellation, request budgets and more scalable transactions. Page calls outside one transaction can observe intervening writes. Existing budgets are not total memory or project quotas.
- [ ] Add an authenticated service and genuine project isolation in the later security/service priority. The raw reserved prefix is not an isolation boundary; uncooperative raw writes can corrupt document metadata or create undetected omissions until bounded verification.
- [ ] Establish versioned migration, import/export and compatibility policy before a stable release. Neither MongoDB wire/query compatibility nor a downgrade promise exists.
