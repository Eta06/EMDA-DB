# Initialization and staging boundaries, 6 November 2026

Store initialization creates a log and a separate identity witness. Backup,
restore, upgrade, checkpoint and compaction also use marked staging paths.
These paths must not be mistaken for completed stores or lose the marker that
allows a later cleanup to recognize an interrupted operation.

The staging cleanup order now synchronizes the absence of a payload before
removing its ownership marker. If that synchronization fails, the marker is
retained. This applies to automatic drop cleanup and explicit generic or
retired-stage cleanup. A parent sync that already completed after successful
publication avoids a duplicate sync when the original stage path is absent;
if a hard-link fallback left that path behind, its later removal is synced.
An unchanged explicit cleanup does not issue a sync and is not a filesystem
capability test. No log, witness, checkpoint or backup format changed.

## Covered interruption points

- Ten initialization boundaries cover log creation, metadata and header,
  log sync, each witness creation/write/sync boundary, and the final store
  directory sync. Each point is exercised with a returned error and a killed
  process. An incomplete witness is rejected without rewriting evidence;
  a fully visible witness can reopen on the still-running OS.
- Six backup-construction boundaries cover the header, a partial data copy,
  complete data, footer, file sync and validation. V2 and V3 sources are
  checked under both injected errors and process termination. A partial output
  is never published; the source can be reopened and a subsequent backup and
  restore verified.
- Witness creation inside restore and upgrade is interrupted at four points
  for both V2 and V3 sources. Restore and upgrade additionally cover eight and
  ten log-copy, validation, witness, lock and directory boundaries respectively.
  Each case preserves the source and backup, leaves no published destination,
  cleans up its own marked stage, and successfully retries.
- Cleanup tests inject parent-sync errors for generic and retired stages and
  kill a process immediately after payload removal is synced. They check that
  the ownership marker remains available, active locks block concurrent
  cleanup, and retry removes only the marked stage. Separate checks cover a
  previously synchronized publication and a lingering published hard link.
- A V3 identity-anchor test checks a missing witness, every shorter length,
  an extra byte, every single-byte corruption and valid-checksum changes to
  magic, identity and offset. Two failed opens preserve the log, anchor and
  metadata witness in each case.
- A separate commit-state matrix checks 26,640 synthetic reopens spanning
  every byte prefix of a three-operation atomic batch or three independent
  records, each prefix of a torn old/new witness, V2 and V3 formats, and
  ordinary and checkpoint-backed recovery. Atomic batches reopen entirely
  before or after the batch; complete prefixes of independent records may be
  recovered separately. Failed opens preserve the supplied log and witness;
  successful tail adoption advances the boundary and preserves earlier
  acknowledged values. The independent records are generated sequentially
  to construct the same possible persisted byte prefixes, so this matrix
  does not itself exercise a live group-commit schedule.
- A process-kill check interrupts actual atomic batches at the append,
  partial-append, log-sync and witness-write/sync boundaries. It covers V2
  and V3 commits from both writable backends: 32 selected process kills in
  all. While paused, the child retains its exclusive store lock. After a kill,
  successful opens on both recovery paths retain earlier acknowledged values
  and recover the original state or the full batch. Partial-batch opens fail
  without modifying the log or witness, preserving the earlier log prefix.
  The log-sync points apply only to V2; V3 synchronizes the log and witness
  together. This check does not simulate loss of OS or device caches.

The new cases exercise 76 selected process-termination scenarios and 74
returned-error scenarios across initialization and backup/restore/upgrade,
plus cleanup-order failures, 77 damaged-anchor variants, the synthetic
commit-state matrix and the 32 commit-I/O process kills. They use synthetic
data and the running OS. A process
kill is **not** a physical power failure;
the checks do not prove persistence ordering on a real device, exhaust all
storage errors or concurrent schedules, or replace an independent off-host
restore. Those checks remain open in the [foundation follow-up](FOUNDATION_FOLLOW_UP.md).
