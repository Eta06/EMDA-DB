# Foundation and storage follow-up

This is the handoff for work remaining from roadmap priorities 0 and 1. It is a checklist, not a statement that the prototype is production-ready. Update it when work is completed, and link the relevant implementation and evidence.

## Deferred to later Codex sessions

- [ ] Physical power-loss validation on explicitly identified disposable hardware. Define the filesystem, storage device and failure procedure; check acknowledged writes, metadata witness behavior, recovery and preserved evidence. A process kill is not a physical power failure. Do not perform destructive testing on a production server.
- [ ] Off-host backup and restore drill on an independently identified target. Verify restored content and report transfer, filesystem, failure and isolation assumptions. A separate directory or filesystem on the same VM is not an off-host drill.

## Priority 0: remaining local foundation work

- [ ] Expand interruption and failure injection across initialization, commit, backup, restore, upgrade and every directory-sync boundary. Record which persistence states the model covers and does not cover.
- [ ] Extend concurrent history and recovery testing beyond the current short independent single-key and two-key histories. Add persistent fixtures for future supported format versions.
- [ ] Extend recovery fuzzing with additional targets, malformed inputs, longer campaigns and replayable regression cases. The 363,780-input local recovery run is recorded in `benchmarks/results/2026-11-04-foundation-fuzz.json`; it is not exhaustive.
- [ ] Measure memory, I/O and recovery behavior under workloads representative of the intended deployment, including large data and repeated restarts. Existing local point-operation comparisons are a baseline, not a comprehensive performance ranking.
- [ ] Keep write acknowledgement, unknown outcomes, corruption handling and compatibility limitations aligned with `STORAGE_TRANSACTION_CONTRACT.md` as behavior changes.

## Priority 1: durable storage and core operations

- [ ] Implement a durable on-disk index and bounded memory/cache management; the existing in-memory index grows with the number of keys.
- [x] Add verified, non-overwriting compaction into a separate store while keeping the source intact and blocking concurrent mutations during the copy.
- [ ] Design and implement crash-safe checkpoints, live/online compaction, garbage collection and bounded recovery. Protect readers and any future snapshots during reclamation. Separate-copy compaction currently needs multiple in-memory indexes, requires manual switchover and does not include later source writes.
- [x] Add initial bytewise ordered scans with exclusive bounds and pagination. Each page is limited to 1,024 entries and 32 MiB of key/value data, but pages do not share a stable snapshot. Continue until an empty page; a byte-limited page can be short.
- [x] Add single-key compare-exchange for conditional put/delete, synchronized on success and serialized against mutations; it does not detect ABA changes or provide multi-key transactions.
- [x] Add signed 64-bit atomic increment with checked overflow and specified byte encoding; it is serialized against mutations.
- [x] Add bounded multi-key reads from one captured index state, preserving input order and missing keys. This is not a multi-key transaction.
- [ ] Implement bulk writes. Profile and reduce compare-exchange/increment contention without weakening their semantics; both currently exclude all concurrent writers, including writers to other keys.
- [ ] Specify and implement atomic multi-key batches and transactions, isolation, conflict behavior and rollback. Record and index updates must recover atomically. Independent operations and group commit are not transactions.
- [ ] Add stable snapshots and explicit lifetime/resource management where required for multi-page scans and transactions.
- [ ] Add configurable request/connection limits, fair overload handling and cancellation/timeouts without violating unknown-outcome semantics. The current embedded API does not include a production server.
- [x] Add an optional per-open log-file length cap with concurrency-safe admission and unknown-outcome handling.
- [x] Add an optional per-open live-key count limit, allowing existing-key updates and freeing slots after deletions, with serialized admission.
- [ ] Enforce persistent project-wide memory and storage quotas, including other files, recovery and background work; scan response limits and the per-open log cap do not bound total store memory or total disk usage. Limited stores currently serialize writers and must be benchmarked and optimized without oversubscription.
- [ ] Demonstrate the priority's exit criteria: independent concurrent clients, transactional atomicity, acknowledged-write recovery under the documented fault model, and measured memory within configured limits.

No optimization may weaken the durability, isolation or recovery guarantees in force. Preserve failures and unresolved gaps in reports rather than marking them complete without evidence.
