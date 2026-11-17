# Bounded recovery: next implementation slice

Design proposal, based on committed revision `71e1099`. No format or behavior
change is authorized by this document. Existing whole-store online reclamation
is separate from the proposed live-log reclamation.

## First slice: generation-qualified observations, without reclamation

Introduce an internal `GenerationId` and `GenerationLocation` containing a
log identity plus the existing complete `ValueLocation`. Give each open log
an immutable identity derived from its validated witnessed header. Legacy logs
have no such identity: retain the existing code path and reject any future
operation requiring generation-qualified locations until an explicit upgrade.
Do not synthesize a durable identity from a path, inode, offset, checksum or
process counter. Keep the on-disk format and checkpoint encoding unchanged in
this slice. Checkpoint decoding attaches the identity of the already validated
log; it must not accept a checkpoint for a different log merely because its
locations decode. Existing checkpoint identity and full-prefix validation remain
mandatory.

Plumb the qualified location through read-set point and range observations,
including overlay and checkpoint lookups, and compare the entire tuple under
the same existing state/writer barrier that validates the read set. Publish a
new immutable generation and its corresponding index as a single state change;
never read an epoch separately from a location. This slice does not introduce
an in-place generation switch: both components always identify the currently
opened append-only log. Preserve the existing conservative transaction
semantics and batch atomicity. Before implementation, inventory every place
that captures, compares, serializes or reads a location, including grouped
records, tombstones, snapshots, direct reads and recovery; do not convert only
one transaction path. Internal type separation should make accidental
unqualified comparisons a compile error where practical.

Acceptance tests: equal offsets, lengths and checksums from distinct witnessed
log identities conflict; identical qualified observations remain equal; a
changed location within one generation conflicts; range observations and
missing/tombstone transitions keep their current semantics; DB02 batches and
grouped records replay unchanged; snapshots and in-flight transactions survive
checkpoint rollover; legacy behavior and V2/V3 reopen remain unchanged; a
checkpoint from another log is rejected. Add deterministic barrier-controlled
concurrent tests to ensure readers never capture a mismatched identity/index
pair, and run existing transaction, checkpoint, corruption and full suite tests.
Tests must exercise engine paths, not just tuple equality. This adds a safety
precondition, not a reclamation or recovery-time guarantee.

## Required design before removing live history

* Define a versioned authoritative retained-data format, full recovery and
  downgrade rejection, migration, backup/restore, limits and interrupted
  publication. Old readers must fail closed, not replay a suffix as full
  history. Every relocated generation needs a fresh durable identity; no
  physical location may be reused under the same identity. Specify collision
  handling and exhaustion rather than assuming identity reuse is harmless.
* Define how the complete retained state and every acknowledged atomic batch
  are reconstructed, including deletes, grouped records, concurrent writes and
  uncertain commits. A trusted checkpoint must cover the retained data, not
  point to deleted bytes. Preserve current corruption verification of all bytes
  that current recovery verifies; a saved digest without reading current bytes
  is not equivalent. Therefore scanning unbounded retained data cannot promise
  bounded recovery time. A real bound needs explicit retained-data and I/O
  bounds, with rejection/backpressure when limits are reached.
* Specify witness coupling and durability ordering for data, manifest,
  selection and independent trusted rollback boundary. A manifest epoch alone
  does not detect rollback. Enumerate each crash and sync-error state, including
  uncertain publication, and reopen fail-closed without deleting either
  potentially authoritative copy. Verify the actual bytes and witness before
  retiring anything; do not infer durability from rename alone.
* Pin the generation and all needed files for every read, snapshot,
  transaction and in-flight I/O before publishing a replacement; close the
  admission race with publication under one synchronization protocol. Checked
  pin overflow fails admission. A deadline cannot revoke an active reader.
  Unlink only after durable authority switches and all pins and old handles are
  gone, with restartable cleanup and directory sync. Stalled readers may delay
  reclamation indefinitely; expose that fact rather than promise a hard quota.
* Define lock ownership across fork, descriptor duplication, exchange, old
  handles and process exit. Do not unlock an open-file-description lock while
  any owner still relies on it. Arm cleanup guards before fallible operations,
  classify unlock failures and uncertain outcomes, and test candidate and
  retired locks on Linux. The historical intermittent failure in
  `HANDOFF_STAGING_CLEANUP_2026_11_14.md` remains undiagnosed; inherited locks
  are a hypothesis, not justification for broad retries.
* Add crash-boundary, injected I/O, stale-handle, concurrent pin/publication,
  cross-generation ABA, corruption, rollback, upgrade/downgrade and bounded
  resource tests, plus representative performance evidence. Physical
  power-loss and independent off-host restore evidence remain deferred.

Until these conditions are implemented and validated, retain live history and
use the existing verified whole-store handoff only under its documented rules.

## Implemented prerequisite and remaining work

The ordinary `Store` read-set validator now captures the validated log identity
with its immutable index state and qualifies present point and range locations
when comparing snapshots to current state. Missing entries retain their prior
membership semantics; legacy logs retain unqualified comparisons. The test for
cross-identity ABA models a publication by changing the in-memory identity; no
live generation replacement is implemented or authorized. The identity and index
must continue to be published together under the state lock if replacement is
introduced.

Checkpoint-backed read-set point and range observations now also bind present
base and overlay locations to the identity captured under the state lock. Tests
model a change of identity; they do not switch the underlying log. Absence and
empty ranges retain membership semantics. Direct reads, snapshots and checkpoint
encoding still use ordinary locations; validation and corruption checks remain
unchanged. This is still a partial prerequisite: grouped records, identity/view
publication and snapshot lifetime require further engine-level evidence, and
conservative transactions and ordinary two-point reads now also compare the
captured identity. Actual switching still requires coordinated publication and
old-log lifetime protection. No reclamation or format change follows from this
work.

Ordinary snapshots capture a shared log handle with their index under the
state lock, and release it on cancellation or expiry after active reads finish.
This adds a reference, not a descriptor. Transaction get and scan use that
captured handle while holding the snapshot read lock. Direct point, bulk,
range and two-point reads retain the log captured with their locations.
Online copy's initial verification and snapshot copy use the snapshot log;
its later replay and final verification, writes, and other maintenance still
use the store's log directly. No operational switch is supported. Retired-log
cleanup will also need to respect snapshot and registry lock order.
