# Managed checkpoint rollover outcomes

Revision `c788dedca06a4f77972587665bbc2a1ad0da5dbb` adds the opt-in
`CheckpointStore::rollover_managed_detailed` API. It distinguishes failures
before either publication attempt, failures after checkpoint publication may
have begun but before this call began selection publication, and failures after
selection publication may have begun. These are conservative classifications:
a destination can pre-exist, staging artifacts can remain, and an error does
not return a new checkpoint digest or certify store health.

A selection-publication error disables the handle. Reopen the visible managed
selection, or explicitly recover with the previous trusted checkpoint or full
log if no selection is visible. The existing `rollover_managed` API and its
synchronization and recovery behavior are unchanged. External modification
and noncooperating access remain outside the supported storage contract.

## Fault checks

The [retained bundle](../benchmarks/results/2026-11-14-managed-rollover-publication-faults.json.gz)
contains 12 one-shot Linux campaigns with both a first-time managed selection
and an existing selection. Each uses V2/V3 fixtures and gives ordinary and
checkpoint-backed recovery paths a turn reopening first. The ordinary path
does not enforce a managed selection and is used only to read in this test.
Each visible checkpoint, including the previous selected checkpoint, is checked
against a separately saved fixture digest; source log, witness, reference
backup and previous checkpoint are checked for preservation. The visible
selection is checked before and after verification, and abandoned stages are
cleaned under the existing staging rules.
This first campaign verifies that the visible selection opens, but does not
independently parse its path to prove that it points to the intended checkpoint.

The campaigns injected EIO into selected observed sync, data-write, read,
open and namespace calls, and EAGAIN into sync calls:

| Initial selection | Injections | Neither publication started | Checkpoint uncertain | Selection uncertain | Success |
| --- | ---: | ---: | ---: | ---: | ---: |
| None | 404 | 204 | 80 | 104 | 16 |
| Existing | 500 | 300 | 80 | 104 | 16 |

In the 160 checkpoint-uncertain cases, the selection was unchanged and 152
targets were visible. In the 208 selection-uncertain cases, the new checkpoint
was visible; the selection was unchanged in 96 cases and changed in 112.
The 32 successful attempts followed injected namespace errors that did not
prevent the operation. Additional deterministic tests inject failures before
and after the selection rename and during checkpoint verification, check the
disabled handle where applicable, and reopen the appropriate selection.

These synthetic returned errors do not exhaust failure-only calls, partial
syscalls, concurrent schedules or persistence ordering. The workspace used an
overlay of unknown backing while the operating system remained running. A
visible selection after a sync failure is not proof of durable publication.
These checks do not establish physical power-loss safety or independent
off-host recovery.

To recheck the retained coverage, classifications and recorded hashes:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_one_shot_fs_results import verify
with gzip.open('benchmarks/results/2026-11-14-managed-rollover-publication-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
