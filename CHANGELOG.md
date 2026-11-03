# Changelog

Notable changes are recorded here. See [the versioning policy](docs/VERSIONING.md) for release and compatibility rules.

## [Unreleased]

The package version is currently `0.1.0`, but no version has been tagged or published yet.

### Added

- A sequential reference-model check for 64 short concurrent histories combining transactions, atomic batches, independent writes and bulk reads, with recovery checked after every history.
- Separate-process checkpoint-index memory and lookup benchmarks, including the substantial historical-view lookup penalty and the measured improvement from smaller verified blocks.
- A shared-lock historical checkpoint reader that keeps a bounded block directory, verifies each index block on access, and reads values from the verified log prefix. It does not replay or validate the tail and is not a live writable index. A short 297,135-input fuzz run is recorded in `benchmarks/results/2026-11-03-checkpoint-view-fuzz.json`.
- Paired warm-cache checkpoint versus full-replay measurements, including checkpoint creation cost, individual runs, Linux noise telemetry and observed outliers.

- Explicit witnessed-store index checkpoints with a separately trusted SHA-256 digest, historical key-peak enforcement, non-overwriting publication and suffix replay. Checkpoint opening still hashes the covered log prefix and loads the index into memory; checkpoints are plaintext derived data, not backups.
- A checkpoint parser fuzz target and fixed V2/DB02 index fixtures, including forged-checkpoint and unknown-codec cases.
- Atomic ordered multi-key write batches, encoded as one synchronized `DB02` record with individually checked nested records. Older revisions cannot open logs containing `DB02`; witnessed V2/V3 stores are required.
- Bounded optimistic serializable read-modify-write transactions with point/range reads, read-your-writes, rollback on callback failure and conservative store-wide conflict detection. Callbacks are never automatically retried. A preliminary 440,108-input recovery fuzz run included a valid DB02 seed; provenance and limits are recorded in `benchmarks/results/2026-11-04-atomic-batch-fuzz.json`.

- Bounded, process-local read-only index snapshots with stable paginated scans and lifetime tied to the open store. Per-store limits cover active snapshot count and aggregate copied keys/key bytes; they do not cap total memory.
- Optional per-open log-file length limits with serialized mutation admission, concurrent oversubscription checks, rejection before replay and unknown-outcome handling. The limit is not persistent and does not cover memory or other files.
- Optional per-open live-key count limits, with serialized admission, existing-key updates and reusable slots after deletion. Recovery enforces the cap on every historical prefix; this is not a process-memory guarantee.
- Verified separate-copy compaction that rechecks the source log/index, copies live keys, reopens and verifies the destination, and publishes without overwriting. Mutations are blocked during copying; the source remains intact and memory remains unbounded.
- Ordered byte-key scans with exclusive bounds, pagination, and per-page limits of 1,024 entries and 32 MiB of key/value bytes. Each page captures the index at one instant; pages do not share a transaction snapshot.
- Synchronized single-key compare-exchange for conditional put/delete, serialized against concurrent writers; mismatches make no write and I/O failure retains unknown-outcome semantics.
- Synchronized signed 64-bit atomic increments using checked arithmetic and little-endian encoding, with missing keys treated as zero.
- Bounded multi-key reads capturing one index state, with input ordering, missing-key and duplicate-key handling.
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

- Historical checkpoint views use smaller verified index blocks and bounded prefix summaries. Ambiguous prefixes require a verified block read before choosing a search branch. The on-disk checkpoint format and trusted-digest requirements are unchanged. A 171,991-input model-based search fuzz run is recorded in `benchmarks/results/2026-11-03-checkpoint-order-fuzz.json`.
- Successful commits skip condition-variable notifications when no writer is waiting; synchronization and failure wake-ups remain intact.

- Incomplete log tails and a missing log for an existing store now fail closed without silently truncating or recreating the log.
- Readers can continue using the previous indexed value while a writer synchronizes; successful mutations still sync before becoming visible and returning.
- Store initialization synchronizes the created directory path and ancestors; power-loss behavior is not yet proven.

### Limitations

- This is a prototype, not a production database. SQL, document queries, automatic live compaction, bounded writable-index memory, network access, and distributed features are not implemented; transactions are a bounded prototype. Backup files are unencrypted and retain deleted historical values.
