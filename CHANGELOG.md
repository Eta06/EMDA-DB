# Changelog

Notable changes are recorded here. See [the versioning policy](docs/VERSIONING.md) for release and compatibility rules.

## [Unreleased]

- Make retired-stage tests tolerate a briefly held fixture lock while still requiring eventual cleanup and preservation of unsafe entries; add held-lock regression checks.

- Check acknowledged writes from concurrent workers across process-kill races on V2/V3 and both writable recovery paths.

- Check sampled SQLite values and checksums after a clean reopen in the direct key-value benchmark, outside the measured timings.

- Extend the checkpoint-backed serial-history model with an independent write and a second reader, and permit longer local campaigns.

- Add a staging cleanup model fuzzer for malformed markers, unexpected payloads, symlinks and repeat cleanup; include it in the scheduled recovery workflow.

- Record a 96-run local byte-key comparison against SQLite, PostgreSQL and MongoDB, including latency distributions, durability settings and noise telemetry.

- Add deterministic process-kill checks for V2/V3 grouped independent writes, including a previously acknowledged write and partial grouped-record handling.

- Exercise actual atomic-batch process termination at log and witness I/O boundaries on both writable backends and record a local staging/restore comparison.

- Synchronize staging payload removal before deleting its ownership marker. Failed synchronization retains the marker for retry; already synchronized publication avoids a redundant sync, and unchanged cleanup no longer syncs the directory. Add initialization, backup, restore, upgrade and cleanup interruption checks; physical power-loss behavior remains unverified.

- Add a synthetic commit-recovery matrix covering atomic batches and independent record prefixes, torn V2/V3 witnesses, and both writable recovery paths.

- Add concurrent document deadline-race checks covering atomic indexes and raw keys, confirmed expiry, potentially uncertain operation timeouts, and clean reopen on both writable backends.

- Add an ARM64 Linux VM setup and a designated result directory for the independent Mac mini benchmark; development can continue against other commits while it runs.

- Add an external test-request workflow and a reproducible quieter-host document-query comparison with SQLite, PostgreSQL, and MongoDB.

- Persist a checkpoint-backed collection-deletion cursor after a full page, reducing repeated tombstone scans. The first cursor step upgrades the marked collection to metadata version 4, which older document readers reject; existing version 3 markers still resume. Final deletion verifies all namespaces are empty and removes the cursor atomically with metadata. Cursor records add log bytes and remain subject to the existing limits.

- Add irreversible, resumable collection deletion in bounded atomic steps. Starting a drop writes version 3 collection metadata that older document readers reject. Access to the affected collection is blocked until deletion completes; underlying transaction and recovery limits continue to apply.

- Make per-open aggregate snapshot reservation limits configurable and observable on both writable stores. Reductions below live reservations fail unchanged; supported compaction handoffs preserve limits, and reopening resets them.

- Add opt-in per-open independent key-value read admission, enabled with exclusive store access before sharing. Once enabled, at most 65,535 calls may be admitted, and saturated `get`, `get_many` and `scan` return `WouldBlock`. Reopening disables the limit. Checkpoint invalid read arguments now fail validation before store health is checked. Other read APIs have separate controls.

- Compare validated scalar filters without allocating encoded byte buffers, retaining the index encoding order and rejecting non-indexable values.

- Add bounded same-object array element matching for nested document filters, with a shared one-million-element visit limit and safe partial-index presence proofs. The new `DocumentFilter` variant requires exhaustive downstream Rust matches to be updated; no multikey index is added.

- Add per-open independent-write admission limits and status to both writable stores. The default and hard maximum are 65,535 calls; saturated admissions return `WouldBlock` before the storage lock or submission, even if a prior write left the store unhealthy. Transactions use their separate admission limit. Limits survive supported in-process compaction handoffs and reset on reopen.

- Add per-open encoded-byte and record-count limits and status for ordinary-store group commit. Limit reductions cannot undercut reservations; waiting writers recheck changed limits. Supported in-process ordinary compaction handoffs preserve the setting, while reopening resets it.

- Extend concurrent document serial-history and recovery checks to include pre-cancellation, cancellation after staging and cancellation racing a transaction.

- Include equality predicates in the shared 256-value, 4 MiB encoded-input and 1,000,000-comparison document-filter budgets. Filters that exceed the combined limits now fail even when an oversized branch would short-circuit; this changes previously accepted queries.

- Make pre-commit writer-barrier and checkpoint transaction state-lock waits observe cancellation and deadlines. Callback work, other lock waits and I/O remain cooperative and do not have a completion-time bound; a started commit retains its success or unknown-outcome semantics.

- Add sparse in-memory checkpoint lookup offsets while continuing to verify the complete SHA-256 block before every point read. [Paired measurements](docs/CHECKPOINT_LANDMARK_COMPARISON_2026_11_05.md) record modest median improvements, mixed tails and additional per-block memory.

- Document the conservative lifecycle for unmarked legacy staging paths; cleanup continues to retain them unless an operator separately establishes their role and redundancy.

- Enforce a checkpoint-backed store's per-open view byte and block limits while streaming explicit, managed and automatic rollover candidates. An over-limit candidate is now rejected before publication instead of leaving an unusable checkpoint at its destination. Independent `checkpoint_to` copies and compaction retain their previous behavior; these bounds are not total disk quotas.

- Reject existing zero-byte logs on ordinary open instead of silently treating an interrupted initialization as an unwitnessed legacy database. Explicitly verified empty legacy logs can be copied to a new witnessed store with `Store::upgrade_empty_legacy`; valid zero-byte legacy backups restore as new witnessed empty stores, changing their restored format and identity.

- Treat a dangling commit-witness symlink as an existing invalid entry instead of allowing store creation or legacy fallback.

- Plan bounded unions of complete compound index tuples for required scalar equality and `In` predicates. At most 256 distinct tuples are probed; excess, unindexable or incomplete predicates fall back safely. This adds access-path variants but does not introduce array-element matching or general boolean union planning.

- Limit each open store to 64 active transactions by default, with configurable lower limits and status reporting. New over-limit transaction calls now fail before their callbacks; the limit resets on reopen and does not bound total memory or direct writes.

- Record paired transaction-admission measurements and retain an unmerged atomic-counter experiment with mixed results and its failure-handling regression.

- Bound combined encoded JSON inputs from update conditions, set and array values, and min/max operands to 4 MiB. Previously accepted larger combined inputs now fail, and array-input limit error text has changed.

- Count encoded update inputs without producing a JSON buffer, using a portable exact string path and retaining `serde_json` number formatting. Record the large-input cost and paired optimization measurements.

- Add checked multiplication, set-if-missing, typed scalar minimum/maximum and non-overlapping field rename updates. `DocumentUpdate` is now non-exhaustive, so downstream matches must include a wildcard arm.

- Allow each open store to set and inspect its transaction reservation limit, preserving it across guarded compaction while rejecting reductions below active reservations. The limit resets on reopen and is not a total-memory quota.

- Stream grouped statistics during complete scans while preserving scan and filter error precedence.

- Add opt-in scoped cleanup for matching retired-store stages, with full current-log verification, lock and contents checks, restartable ordered removal, worker status and process-kill tests. Legacy generic stages and named previous paths still require inspection; no total quota or power-loss guarantee is implied.

- Add bounded grouped statistics for up to 16 signed integer fields, with exact 128-bit sums, counts, minima, maxima, approximate means and transaction snapshot validation.

- Add cooperative cancellation and optional deadlines for key-value and document transactions on both writable stores. FIFO waiters can cancel while queued; cancellation reported before commit is distinct from an I/O error with an unknown outcome.

- Add optional, stoppable expired-snapshot reapers for both writable stores, with busy-read protection and worker status and failure reporting.

- Plan bounded unions of deduplicated scalar `In` probes on eligible single-field indexes, including safely implied partial indexes, while preserving aggregate scan limits and transaction read-set checks.

- Add optional whole-log and checkpoint admission limits to read-only checkpoint recovery, checking log length before full-prefix verification and preserving suffix limits.

- Add bounded document `In` and `NotIn` value-membership predicates, sharing input and comparison limits with array predicates. The planner treats a singleton indexable `In` as a required equality and may use presence implications. These new `DocumentFilter` variants require downstream exhaustive matches to be updated; array filter limit error text has changed.

- Run the cross-engine smoke on the Ubuntu 22.04 hosted image while MongoDB 8.0 rejects the Linux 7.0.0 kernel observed on GitHub's Ubuntu 24.04 runner.

- Capture bounded, password-redacted diagnostics when a disposable benchmark server exits or fails to become ready.

- Record repeated compound-range comparisons with SQLite, PostgreSQL and MongoDB, and add a disposable cross-engine CI smoke workflow.

- Plan compound equality-prefix ranges only when required predicates also prove all later indexed fields are present.

- Defer scheduled reclamation on a proven pre-compaction maintenance-lock conflict and expose a deferral count; ambiguous I/O and cleanup failures still stop the worker.

- Stream bounded collection-query filtering and retain only the requested window for unsorted results, while preserving full validation and error precedence. Record paired performance and fuzz evidence.

- Add per-open checkpoint file and block-directory admission limits, preserved across writable-store rollover and blocking compaction handoffs.

- Publish a partial-index comparison against verified SQLite plans, including raw results and an observed execution-order effect.

- Select partial indexes for complete equality tuples and single-field ranges only when required query predicates prove the partial condition.

- Harden metadata fault-test fixture reopens against transient inherited locks during parallel process-kill tests.

- Plan exact equality queries on complete compound index tuples without omitting documents missing unconstrained fields.

- Plan bounded single-field scalar range queries with intersected bounds and stable ID-order results.

- Record and retain a rejected scan-exhaustion optimization after mixed before-and-after measurements.

- Plan nonunique single-field equality queries over complete index ranges, with candidate and byte budgets across pages.

- Fail closed when an indexed document exceeds its size limit, even when it also exceeds the remaining page budget.

- Add bounded array membership, any/all and exact-length document query predicates. These new `DocumentFilter` variants require downstream exhaustive matches to be updated.

- Add an opt-in, stoppable ordinary-store reclamation worker with log-size and growth thresholds and explicit handoff/cleanup failure reporting.

- Record verified online compaction retention-versus-reclamation timings and named log lengths.

- Harden checkpoint fault-test fixture reopens against transient inherited locks and extend reclamation cleanup-failure checks.

- Hold the replacement store's write barrier while reclaiming its predecessor, preventing an uncertain concurrent write during cleanup.

- Add opt-in guarded online compaction with post-handoff reclamation of the redundant previous store, explicit cleanup-error reporting, and preserved staging on failure or unwind.

- Publish paired planner metadata-reuse results and repeat, including observed tail regressions. Exercise the planner comparison harness in CI.

- Reuse transaction-scoped collection metadata when a planned query probes its selected index.

- Record a paired scan, planned-query, explicit-index and SQLite document comparison, including raw samples and VM noise.

- Add opt-in rule-based unique-equality index selection for bounded filtered document queries, with a scan fallback and access-path reporting.

- Exercise atomic document-array, unique-index and raw-key commits under every V2/V3 append cut and selected synchronization and metadata failures.

- Add atomic bounded JSON array push, add-unique, remove and first/last pop updates to both document backends. These new `DocumentUpdate` variants require downstream exhaustive matches to be updated.

- Check concurrent document, index and raw-key transactions against real-time serial histories and recovery on both writable backends.

- Publish paired document-scaling and concurrent shared-snapshot measurements, including SQLite, tail regressions and raw samples.

- Ordinary-store snapshots share immutable index roots; writes copy a shared root when necessary, with generation and identity checks before publication. Logical snapshot budgets and storage guarantees remain in force.

- A reproducible contention harness compares concurrent two-key transactions and point reads while retaining bounded reader-latency samples.

- Recorded a rotated before/after document-read comparison and repeat across growing indexed collections, including raw latency samples and VM interference.

- Standalone ordinary-store document reads avoid copying the full key index while preserving conservative conflict detection, value verification, uncertain-write handling and shared active-read admission.

- The embedded document comparison can preload an indexed collection, verify it after reopening, and record process high-water RSS, store format and file sizes.

- Recorded workspace-overlay document recovery checks and a selected-syscall diagnostic for the initial document comparison.

- Expanded online-compaction source-failure checks and recorded longer compaction, index and schema model-fuzz runs.

- Concurrent increment histories verify acknowledged writes across repeated guarded compaction handoffs and reopen.

- An initial single-client JSON-document comparison records raw SQLite, PostgreSQL and MongoDB results and the limits of the workload and adapters.

- Experimental opt-in Linux ordinary-store compaction can prepare and catch up a copy while guarded callbacks continue, then pauses callbacks for verification and atomic exchange. The previous store is retained; logical caps, error boundaries and format implications are documented.

- Resumable document schema migrations validate bounded batches, persist progress and enforce both active and pending schemas on concurrent writes until publication.

- Resumable incremental document index builds and drops persist bounded progress, coordinate with concurrent writers, and publish or remove indexes atomically. Collection metadata advances to version 2; see the document guide for uniqueness and recovery semantics.

- An opt-in, stoppable background worker schedules managed checkpoint checks and reports errors without changing the underlying rollover or durability contract.

- A blocking Linux checkpoint-store compact-and-replace handoff retains the prior log and preserves managed selection, automatic checkpoint settings and per-open limits. Its process tests do not establish power-loss behavior.

- A blocking Linux compact-and-replace handoff uses a verified sibling and atomic directory exchange, retains the old copy, preserves per-open limits, and rejects stale lock descriptors. Failure and process-kill tests do not establish power-loss safety.
- Experimental document collections, JSON schemas, atomic CRUD/bulk updates, conditional operators, compound/unique/partial indexes, bounded queries, index verification and repair, basic aggregation and ID lookup over the existing transaction engine. See the document guide for limits and compatibility.

The package version is currently `0.1.0`, but no version has been tagged or published yet.

### Added

- Opt-in persistent automatic checkpoint rollover when a checkpoint-store suffix limit would be exceeded. Two engine-owned slots are safely reused while snapshots retain open files; recognized abandoned stages are cleaned on managed open. This does not reclaim the live log or bound total memory or disk.

- Optional whole-log, historical live-key count and historical live-key byte limits during managed-selection rebuild. These are per-call logical limits, not total-memory or persistent project quotas.

- Backup, restore, upgrade, checkpoint and separate-copy compaction synchronize destination directory ancestry before reporting success. An ancestor-sync error after publication reports an uncertain destination; injected failures cover each level.

- A reproducible comparison of managed and unmanaged checkpoint rollover, including the cost of persisting selection and observed VM interference.

- Managed checkpoint publication and recovery synchronize each directory ancestor needed to retain the selected paths; injected ancestor-sync failures fail closed.

- Explicit full-log rebuilding of managed checkpoint selections when the previous checkpoint is unavailable or its suffix exceeds managed limits. The operation holds the store lock and rejects malformed selection metadata, but its in-memory recovery is not resource-bounded.

- Opt-in managed checkpoint selection with synchronized atomic replacement, fail-closed reopening, fault injection, and process-interruption checks. The selection file and its directory are trusted metadata; ordinary `Store` and older revisions do not enforce it. Automatic scheduling and physical power-loss validation remain open.

- A reproducible process-RSS and creation-time comparison for ordinary and checkpoint-backed snapshots.
- On-demand reclamation of expired, idle checkpoint-backed snapshots, skipping views with active reads.
- Bounded process-local checkpoint-backed snapshots, with stable point and paginated reads across later writes and checkpoint rollover, explicit cancellation and optional operation-boundary deadlines.
- A reproducible checkpoint range-width transaction benchmark with a SQLite comparison and post-reopen verification.
- Bounded complete-range validation for checkpoint-backed read-set transactions. Scans observe their entire requested range, allowing writes outside it while detecting phantoms and changes beyond a returned page.
- Optional checkpoint-backed transaction validation for observed point versions, allowing unrelated writes to proceed. Retained dependencies share the transaction reservation budget.
- Operation-boundary deadlines for checkpoint-backed conservative and FIFO transactions, including timed queue admission and a distinct pre-commit expiry result. Commit I/O errors remain potentially uncertain.
- Optional FIFO admission for checkpoint-backed transactions. The queue orders participating callbacks and commits; other transactions and writes remain outside it and can still cause conflicts.
- Conservative optimistic transactions for the checkpoint-backed store, with staged point/range operations, shared staged-record reservations and atomic synchronized commits. Any intervening mutation conflicts.
- Atomic conditional batches for the checkpoint-backed store, applying an ordered batch only when all bounded point-value conditions match. This does not detect ABA changes or validate ranges.
- Verified separate-copy compaction for the checkpoint-backed store, streaming live keys and producing a separately trusted checkpoint for the copy without rebuilding full source or target indexes. Source mutations wait; switchover and log reclamation remain manual.
- An optional per-open whole-log byte limit for the checkpoint-backed store, enforced before recovery adoption and serialized writes. Checkpoint rollover does not reclaim log space; the limit is not a persistent project quota.
- Bounded, consistent multi-key reads for the checkpoint-backed store, preserving order and duplicate keys while excluding concurrent mutations during each read.
- Full-log backup for the checkpoint-backed store, and streaming record validation during backup, restore and format upgrade without materializing a full key index. The backup format and its independent verification requirements are unchanged.
- Bounded checkpoint-store recovery of a complete unacknowledged tail, validated and synchronized before adoption. Incomplete records and suffix-limit failures leave the log and witness unchanged; recovery synchronization errors remain uncertain.
- Explicit live checkpoint rollover that verifies and switches the checkpoint-backed store without releasing its exclusive lock. Checkpoint selection and the independently trusted digest remain the caller's responsibility.
- Explicit checkpoint rollover for the checkpoint-backed writer, streaming its verified base and bounded suffix into a new checkpoint while retaining the historical key-count peak. Reopening with the separately trusted digest clears the overlay; rollover is not automatic and still hashes the full log prefix.
- An experimental writable checkpoint-backed store with a bounded suffix overlay, serialized synchronized key operations, conditional updates, increments and atomic blind batches. It requires manual checkpoint rollover and does not provide total-memory limits.
- A read-only recovered checkpoint view that validates and replays a bounded witnessed suffix into an in-memory key overlay, with ordered scans and fail-closed handling of unacknowledged tails.
- Explicit release of privately owned compaction staging locks before reopening or publication, preventing an inherited descriptor from delaying the handoff.
- In-flight log-byte reservations allow independent writes to share a sync under the per-open log-length cap without oversubscription.
- The resource-limit comparison can measure the log-length cap as well as the live-key byte cap.
- A paired overlay-filesystem comparison records throughput and latency tradeoffs for grouped writes under the log-length cap.
- Grouped synchronization for existing-key updates under live-key count or byte caps; membership-changing operations remain serialized.
- A paired overlay-filesystem benchmark recording the grouped capped path's throughput, tail latency and median-latency tradeoffs.
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

- Store captured checkpoint range keys in packed buffers and reserve their budget in bounded quanta. Limits and complete-range validation remain in force; unused reservations are released before the page is returned.
- Restore and upgrade explicitly release private staging locks, preventing a duplicated descriptor from unnecessarily extending the staging lock after successful release. Long-lived store-lock fork/exec behavior remains to be specified.
- Separate-copy compaction synchronizes bounded groups of ordinary `DB01` records, rebuilds the target index by reopening, and still verifies the copy before publication. Grouping does not make compaction online or bound its total memory.
- Historical checkpoint views use smaller verified index blocks and bounded prefix summaries. Ambiguous prefixes require a verified block read before choosing a search branch. The on-disk checkpoint format and trusted-digest requirements are unchanged. A 171,991-input model-based search fuzz run is recorded in `benchmarks/results/2026-11-03-checkpoint-order-fuzz.json`.
- Successful commits skip condition-variable notifications when no writer is waiting; synchronization and failure wake-ups remain intact.

- Incomplete log tails and a missing log for an existing store now fail closed without silently truncating or recreating the log.
- Readers can continue using the previous indexed value while a writer synchronizes; successful mutations still sync before becoming visible and returning.
- Store initialization synchronizes the created directory path and ancestors; power-loss behavior is not yet proven.

### Limitations

- This is a prototype, not a production database. SQL, document queries, automatic live compaction, bounded writable-index memory, network access, and distributed features are not implemented; transactions are a bounded prototype. Backup files are unencrypted and retain deleted historical values.
