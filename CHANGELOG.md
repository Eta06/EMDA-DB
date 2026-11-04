# Changelog

Notable changes are recorded here. See [the versioning policy](docs/VERSIONING.md) for release and compatibility rules.

## [Unreleased]

The package version is currently `0.1.0`, but no version has been tagged or published yet.

### Added

- An optional per-open cap on live key bytes, enforced during serialized mutations and every historical recovery state, including ordered batch operations. It is a logical key-byte limit, not a process-memory or project quota.
- A paired workload for comparing uncapped writes with the live-key byte cap, including post-reopen verification and VM noise telemetry.
- Optional transaction deadlines with typed pre-commit expiry, timed FIFO admission and tests that preserve unknown outcomes for commit I/O errors.
- Fork/exec lock-lifecycle tests and an explicit process-ownership policy for stores and historical checkpoint views.
- A persistent V3 mixed-record compatibility fixture, including its separately stored metadata witness.
- A larger concurrent serial-history check spanning all transaction modes, including unexpired deadlines, with recovery after every history.
- Explicit, cooperative cleanup of newly tracked backup, restore, upgrade, checkpoint and compaction staging data. It uses versioned ownership markers and parent-directory advisory locks, skips unrecognized entries, and leaves older unmarked staging paths for manual review.
- Optional FIFO admission for read-set transaction commits, with bounded queue and failure/panic wake-up handling. It orders only participating transactions and can increase median latency or retries. A short model-fuzz run is recorded in `benchmarks/results/2026-11-04-read-set-fifo-fuzz.json`.
- An opt-in serializable read-set transaction mode that validates point reads and complete scan ranges, allowing unrelated writes to avoid conservative conflicts. Read dependencies and validation work are bounded; absent-key insert/delete cycles that restore absence need not conflict. Two short model-fuzz campaigns are recorded in `benchmarks/results/2026-11-03-read-set-fuzz.json`.
- A two-key transaction comparison against SQLite, including per-operation latency, retry counts, validation after reopen and observed tail-latency regressions.
- An additional FIFO transaction comparison recording both lower disjoint-key tail latency and higher median latency and shared-key retry costs in the sampled workload.
- Upgrade staging and publication process-kill tests for V2 and V3 sources, plus injected parent-directory sync failure and a regression test for staging-lock release with duplicated descriptors.
- Process-kill checks at backup and restore staging and publication boundaries, including successful retry or reopening on the running OS. Interrupted staging can leave private files or directories containing data; physical power loss is not tested.
- Process-kill tests during synchronized compaction staging and immediately after publication but before parent-directory sync. The first can leave a private staging directory; the second sees a complete copy on the still-running OS. Neither is a physical power-loss test.
- A per-open-store 64 MiB aggregate budget for encoded records staged by concurrent transactions, with admission before staging and reservation cleanup on replacement, completion and unwind. The budget excludes other allocations and is not a total-memory quota.
- Explicit read-only snapshot cancellation and optional monotonic deadlines checked at creation and operation boundaries. Expired snapshots can be reclaimed on later snapshot admission or an explicit reaping call; no background timer runs and ongoing I/O is not interrupted. Transaction lifetimes are unaffected.
- A sequential reference-model check for 128 short concurrent histories combining both transaction modes, atomic batches, independent writes and bulk reads, with recovery checked after every history.
- Separate-process checkpoint-index memory and lookup benchmarks, including the substantial historical-view lookup penalty and the measured improvement from smaller verified blocks.
- A paired separate-copy compaction comparison using independently built revisions, raw timings, content checks and VM-noise telemetry.
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

- Restore and upgrade explicitly release private staging locks, preventing a duplicated descriptor from unnecessarily extending the staging lock after successful release. Long-lived store-lock fork/exec behavior remains to be specified.
- Separate-copy compaction synchronizes bounded groups of ordinary `DB01` records, rebuilds the target index by reopening, and still verifies the copy before publication. Grouping does not make compaction online or bound its total memory.
- Historical checkpoint views use smaller verified index blocks and bounded prefix summaries. Ambiguous prefixes require a verified block read before choosing a search branch. The on-disk checkpoint format and trusted-digest requirements are unchanged. A 171,991-input model-based search fuzz run is recorded in `benchmarks/results/2026-11-03-checkpoint-order-fuzz.json`.
- Successful commits skip condition-variable notifications when no writer is waiting; synchronization and failure wake-ups remain intact.

- Incomplete log tails and a missing log for an existing store now fail closed without silently truncating or recreating the log.
- Readers can continue using the previous indexed value while a writer synchronizes; successful mutations still sync before becoming visible and returning.
- Store initialization synchronizes the created directory path and ancestors; power-loss behavior is not yet proven.

### Limitations

- This is a prototype, not a production database. SQL, document queries, automatic live compaction, bounded writable-index memory, network access, and distributed features are not implemented; transactions are a bounded prototype. Backup files are unencrypted and retain deleted historical values.
