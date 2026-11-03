# Changelog

Notable changes are recorded here. See [the versioning policy](docs/VERSIONING.md) for release and compatibility rules.

## [Unreleased]

The package version is currently `0.1.0`, but no version has been tagged or published yet.

### Added

- Concurrent multi-key operation histories checked against a sequential model and recovery, plus backup tests for every truncation and single-byte corruption position, competing restores, symlink destinations, and directory-sync failure after publication. A 363,780-input recovery fuzz campaign found no crash or sanitizer failure.

- A ten-engine local comparison with 264 repeated measurements across mixed/read/write/CRUD workloads, 36 paired CPU-affinity runs, eleven successful process-restart checks, raw evidence and documented wins/losses.
- Optional MongoDB, Turso, RocksDB, LMDB, DuckDB and FoundationDB benchmark adapters, a separately versioned Python SQLite control, pinned optional dependencies, and disposable localhost-only server orchestration.
- Read-only Linux CPU, I/O, memory-reclaim and steal telemetry, bounded quiet checks, child CPU affinity, reproducible result summaries, and protocol/CRUD CI checks. Storage-engine behavior and durability settings are unchanged.

- A 192-run V2/V3/SQLite latency comparison with raw distributions, source/binary hashes, observed wins and losses, and V3 fault/fuzz/restore verification evidence.

- V3 metadata-backed commit witnesses on supported Linux filesystems: one full inode `fsync` covers records and the witness before publication and acknowledgement. V2 and legacy stores remain readable and writable.
- Verified non-overwriting `Store::upgrade_format` copies, persistent V2 fixtures, log-only rollback detection, metadata fault/panic and delayed-sync tests, and exhaustive small append/witness crash-state combinations.
- A repeatable latency matrix that rotates baseline/candidate/SQLite order across direct and loopback workloads and records source/binary hashes.

- An experimental Rust key-value engine with `put`, `get`, and `delete` operations.
- Checksummed append records, synchronized writes, reopen recovery, and a single-process ownership lock.
- Concurrent-reader and serialized-writer coordination, plus tests for corruption, incomplete records, and abrupt process termination.
- GitHub Actions checks for formatting, linting, and tests on the minimum supported Rust version and stable.
- Architecture notes, a competitor feature inventory, a benchmark plan, and a prioritized roadmap.
- An attribution and notices file, versioning policy, and automated GitHub release workflow.
- A regression test that rejects unknown on-disk record versions without modifying the log.
- Instructions for trying the Apache-licensed Rust library from a Git dependency.
- A storage and transaction contract distinguishing current behavior from proposed guarantees, with a rule against optimizations that violate durability, isolation, or recovery.
- A README progress view and a prioritized assessment of proposed performance and database features.
- Design goals for safe project scale-to-zero, including draining, recovery, ownership, and cold-start testing.
- Project isolation threat boundaries, proposed protection layers and tiers, and validation requirements.
- Initial deterministic fault injection for partial appends, simulated disk-full/sync failures, short reads, writer panics, and controlled concurrency.
- Experimental checksummed full-log backup/restore that refuses to overwrite existing destinations.
- A local closed-loop key-value benchmark harness for the prototype and bundled SQLite; it is a narrow baseline, not a general performance ranking.
- Raw results and environment details for an initial small SQLite comparison.
- Benchmark p99.9, maximum, and per-client latency reporting to expose rare stalls and skew.
- Bounded group commit for concurrent single-key writes, with tests for shared synchronization, failure, panic, admission limits, and backup coordination.
- Raw before/after benchmark runs against the prior engine and SQLite, including per-client and tail latency details.
- Configurable benchmark value sizes for exploring workloads beyond small 64-byte values.
- A reusable warm-cache recovery benchmark with non-overwriting fixture preparation.
- Buffered log replay with the same record checks and tests for interrupted reads, truncation, corruption, and backup validation across buffer boundaries.
- Targeted writer wake-ups with periodic leadership rechecks and tests for delayed handoff, failed batches, poisoned locks, and capacity waiters.
- Clean-build recovery and writer comparisons, including SQLite and raw latency distributions across several workloads.
- A large-write interference benchmark that checks small-write latency alongside the prototype and SQLite.
- Vectored group appends, bounded off-lock write preparation, and shorter index read-lock lifetimes, with partial-I/O, reservation, and concurrent-history tests.
- Raw clean-build results for large-write interference and several concurrent key-value workloads, including observed regressions and SQLite comparisons.
- New-format logs with a synchronized, checksummed commit witness; boundary-truncation, witness corruption, crash-boundary, legacy-upgrade, and backup/restore tests.
- A non-overwriting `Store::upgrade_legacy` operation that copies and verifies a legacy store into the new format without modifying its source.
- A loopback key-value comparison harness for the prototype, SQLite, PostgreSQL, and Redis with explicit durability settings and checksum validation.
- Raw loopback comparison results and abrupt-process-restart checks for the prototype, SQLite, PostgreSQL, and Redis.
- A coverage-guided recovery fuzz target with synthetic legacy and witnessed seeds, plus scheduled and manually triggered CI fuzzing.
- A repeatable restore drill that verifies a point-in-time backup in a separate temporary location.

### Changed

- Successful commits skip condition-variable notifications when no writer is waiting; synchronization and failure wake-ups remain intact.

- Incomplete log tails and a missing log for an existing store now fail closed without silently truncating or recreating the log.
- Readers can continue using the previous indexed value while a writer synchronizes; successful mutations still sync before becoming visible and returning.
- Store initialization synchronizes the created directory path and ancestors; power-loss behavior is not yet proven.

### Limitations

- This is a prototype, not a production database. SQL, document queries, multi-key transactions, compaction, bounded index memory, network access, and distributed features are not implemented. Backup files are unencrypted and retain deleted historical values.
