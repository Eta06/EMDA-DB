# Foundation and storage follow-up

This is the handoff for work remaining from roadmap priorities 0 and 1. It is a checklist, not a statement that the prototype is production-ready. Update it when work is completed, and link the relevant implementation and evidence.

## Deferred to later Codex sessions

- [ ] Physical power-loss validation on explicitly identified disposable hardware. Define the filesystem, storage device and failure procedure; check acknowledged writes, metadata witness behavior, recovery and preserved evidence. A process kill is not a physical power failure. Do not perform destructive testing on a production server.
- [ ] Off-host backup and restore drill on an independently identified target. Verify restored content and report transfer, filesystem, failure and isolation assumptions. A separate directory or filesystem on the same VM is not an off-host drill.

## Priority 0: remaining local foundation work

- [ ] Expand interruption and failure injection across initialization, commit, backup, restore, upgrade and every directory-sync boundary. Record which persistence states the model covers and does not cover.
- [x] Exercise process termination during synchronized compaction staging and after publication before parent-directory sync. The latter establishes only what remains visible on the running OS, not what survives a physical power failure.
- [x] Exercise process termination after backup and restore staging is synchronized and immediately after their publication. Verify the source/backup and the resulting copy or retry on the still-running OS; physical power failure remains untested.
- [x] Exercise process termination at upgrade staging and publication for persistent V2 and V3 sources, and inject parent-directory sync failure. These process-level tests do not establish power-loss safety.
- [ ] Define and test the supported fork/exec lifecycle for long-lived store and checkpoint-view locks. A duplicated store-lock descriptor can retain ownership after the original store object is dropped; staging locks are explicitly released, but the store lock must not be released while any supported owner still uses it.
- [ ] Extend concurrent history and recovery testing beyond the current short histories. A 128-iteration model check now mixes seven concurrent operations: both transaction modes, atomic batches, independent writes and bulk reads, with a reopen after every iteration; the check is not exhaustive. Add persistent fixtures for future supported format versions.
- [ ] Extend recovery fuzzing with additional targets, malformed inputs, longer campaigns and replayable regression cases. The 363,780-input local recovery run is recorded in `benchmarks/results/2026-11-04-foundation-fuzz.json`; the historical checkpoint reader was included in `benchmarks/results/2026-11-03-checkpoint-view-fuzz.json`, and interleaved read-set histories in `benchmarks/results/2026-11-03-read-set-fuzz.json`. These runs are not exhaustive.
- [ ] Measure memory, I/O and recovery behavior under workloads representative of the intended deployment, including large data and repeated restarts. Existing local point-operation comparisons are a baseline, not a comprehensive performance ranking.
- [ ] Keep write acknowledgement, unknown outcomes, corruption handling and compatibility limitations aligned with `STORAGE_TRANSACTION_CONTRACT.md` as behavior changes.

## Priority 1: durable storage and core operations

- [x] Add an explicit on-disk index checkpoint verified against a separately trusted digest and suffix replay, with historical peak accounting and fail-closed corruption/format checks. The checkpoint is a derived plaintext file, not a backup or a security boundary when its digest is attacker-controlled.
- [x] Add a bounded-memory historical checkpoint reader with per-block integrity checks and a shared store lock. It does not validate or replay the later log tail and is not a live writable index.
- [ ] Implement a live disk-backed index and bounded memory/cache management; opening a checkpoint still loads the full key index into memory and hashes the complete covered log prefix.
- [x] Add verified, non-overwriting compaction into a separate store while keeping the source intact and blocking concurrent mutations during the copy. Bounded groups of ordinary records synchronize before verification and publication; append, sync and pre-publication failure paths are tested.
- [ ] Design and implement automatic checkpoints, live/online compaction, garbage collection and bounded recovery. Explicit index checkpoints still hash the full covered prefix. Protect readers and snapshots during reclamation. Separate-copy compaction currently needs multiple in-memory indexes, requires manual switchover and does not include later source writes.
- [ ] Design safe discovery and cleanup of abandoned private staging files and directories after process termination. Tested interruptions can leave `.db-compact-*`, `.db-restore-*` and `.db-upgrade-*` directories or `.db-backup-*` files containing database data; there is no automatic cleanup or cross-process ownership check for them.
- [x] Add initial bytewise ordered scans with exclusive bounds and pagination. Each page is limited to 1,024 entries and 32 MiB of key/value data; ordinary scan pages can see changes. Continue until an empty page; a byte-limited page can be short.
- [x] Add single-key compare-exchange for conditional put/delete, synchronized on success and serialized against mutations; it does not detect ABA changes or provide multi-key transactions.
- [x] Add signed 64-bit atomic increment with checked overflow and specified byte encoding; it is serialized against mutations.
- [x] Add bounded multi-key reads from one captured index state, preserving input order and missing keys. This is not a multi-key transaction.
- [x] Implement bounded atomic multi-key write batches as single synchronized DB02 records, including failure, crash-state, format-compatibility and recovery checks. DB02 records are unreadable by earlier revisions; batch writes require witnessed stores.
- [ ] Profile and reduce compare-exchange/increment/batch contention without weakening their semantics; they currently exclude all concurrent writers, including writers to other keys.
- [x] Implement a bounded optimistic serializable read-modify-write transaction prototype, including point/range reads, read-your-writes, rollback on callback error, conservative store-wide conflict detection and atomic DB02 commit. Independent operations and group commit are not transactions.
- [x] Bound the aggregate encoded staged-record bytes held by concurrent transactions on one open store, with admission before staging and cleanup on completion or unwind. This is not a total-memory or cross-store quota.
- [x] Add opt-in bounded read-set validation that allows unrelated writes while preserving serializable point and range observations. The default transaction mode retains its store-wide conflict rule.
- [ ] Extend transaction model checking, fault campaigns, benchmarks and resource limits. Both modes require a bounded full-index snapshot and do not automatically retry callbacks; read-set validation compares complete ranges while holding the writer barrier and does not provide absence-version tracking.
- [x] Add bounded, process-local read-only snapshots with stable paginated scans; each snapshot is released on drop and borrows the store.
- [x] Add per-store aggregate limits for active snapshots, copied key count and copied key bytes; release reservations on drop.
- [x] Add explicit read-only snapshot cancellation and optional operation-boundary deadlines; cancellation releases the copied index and reservation and expired reads fail.
- [x] Reclaim expired, idle snapshot views on later snapshot admission or an explicit reaping call, without interrupting in-progress reads. Busy views are skipped.
- [ ] Add any desired idle-time reclamation policy, transaction cancellation/deadlines and safe live-log reclamation compatible with all snapshot lifetimes. There is no snapshot reaper thread; idle expired snapshots can retain resources, and deadlines cannot interrupt I/O. Snapshot counters are not total process-memory or cross-store quotas.
- [ ] Add configurable request/connection limits, fair overload handling and cancellation/timeouts without violating unknown-outcome semantics. The current embedded API does not include a production server.
- [x] Add an optional per-open log-file length cap with concurrency-safe admission, rejection before replay and unknown-outcome handling.
- [x] Add an optional per-open live-key count limit, allowing existing-key updates and freeing slots after deletions, with serialized admission. Enforce it during recovery for every historical prefix; a historical peak can require compaction into a separate copy before opening under a lower cap.
- [ ] Enforce persistent project-wide memory and storage quotas, including other files, recovery and background work; scan response limits and the per-open log cap do not bound total store memory or total disk usage. Limited stores currently serialize writers and must be benchmarked and optimized without oversubscription.
- [ ] Demonstrate the priority's exit criteria: independent concurrent clients, transactional atomicity, acknowledged-write recovery under the documented fault model, and measured memory within configured limits.

No optimization may weaken the durability, isolation or recovery guarantees in force. Preserve failures and unresolved gaps in reports rather than marking them complete without evidence.
