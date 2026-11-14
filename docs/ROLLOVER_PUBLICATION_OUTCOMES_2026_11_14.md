# Unmanaged checkpoint rollover publication outcomes

Revision `41a2c5d2825c1ec64767df653f1840fc928fd4af` adds
`CheckpointStore::rollover_to_detailed`. `NotPublished` means this call did not
begin publishing its destination; a pre-existing destination or temporary
staging artifacts may still exist. `OutcomeUnknown` requires inspection before
retry. The existing method retains its prior error type. This API applies only
to unmanaged rollover, not managed checkpoint selection or compaction handoff.

After a returned failure the prior view remains selected. A failed call may
leave a published checkpoint, but does not return its digest; callers must
reconcile it using trusted metadata. Neither classification certifies store
health or independent recoverability.

## Local checks

Integration tests cover successful rollover, missing parents, an existing
destination and preserved old views on V2/V3. A targeted test injects both a
checkpoint parent-sync failure and a corrupted checkpoint during the switch:
the old view and its acknowledged values remain readable, a later write
succeeds, and reopening the old checkpoint recovers both values. A managed
store rejects the unmanaged API before attempting publication.

The stable, Rust 1.85 and optimized stable all-target suites each passed 982
tests with four test threads. Both all-target Clippy checks and 129 Python
tests passed. The GitHub EIO workflow includes a small rollover check.

The [retained campaign bundle](../benchmarks/results/2026-11-14-rollover-publication-faults.json.gz)
contains six one-shot Linux campaigns on V2/V3 fixtures with both writable
recovery paths taking turns opening first. It injected EIO into selected
observed synchronization, data-write, read, open and namespace calls, and
EAGAIN into synchronization calls. Of 288 injected cases, 200 returned
`NotPublished` with the fresh target absent; 80 returned unknown, of which 76
left a visible checkpoint; and eight succeeded after an injected namespace
error. Each visible checkpoint was checked against a separately saved fixture
digest. Source, trusted checkpoint and reference-backup preservation were
checked.

The probe maps an initial handle-open failure to `NotPublished` before it
calls the detailed rollover API; that category includes both open failures and
failures returned by the rollover method.

The campaigns inject returned errors into selected observed calls; they do not
exhaust failure-only calls, partial syscalls, concurrent schedules or
persistence orderings. The workspace used an overlay of unknown backing, and
the operating system remained running. These results do not establish physical
power-loss safety or independent off-host recovery.

The retained reports can be independently rechecked with:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_one_shot_fs_results import verify
with gzip.open('benchmarks/results/2026-11-14-rollover-publication-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
