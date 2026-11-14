# Managed rollover with an acknowledged log suffix

Revision `eb0d70c91895387c265423b49a9864a94c2a4cbd` extends the managed
rollover fault harness with a write acknowledged after the previous checkpoint.
It tests both first-time and existing managed selections. The previous
checkpoint does not contain the new key; recovery must replay its log suffix
when that older checkpoint remains selected. A newly published checkpoint must
contain the key.

Before injection, the harness creates a separate reference checkpoint and
saves its digest. The reference is produced by the same engine on the same
fixture, so it is not an independent backup or protection against a shared
implementation error. The verifier also checks the actual values, selection
path and selected digest, preserved previous checkpoint and original log and
witness. It checks the reference checkpoint and digest again after the
attempt and verification. Both writable recovery paths get a turn reopening
first; the ordinary path is used for reads only because it does not enforce
managed selection.

The [retained bundle](../benchmarks/results/2026-11-14-managed-rollover-suffix-faults.json.gz)
contains 12 one-shot Linux campaigns on V2/V3 fixtures. They inject EIO into
selected observed sync, data-write, read, open and namespace calls, and EAGAIN
into sync calls:

| Initial selection | Injections | Neither publication started | Checkpoint uncertain | Selection uncertain | Success |
| --- | ---: | ---: | ---: | ---: | ---: |
| None | 416 | 216 | 80 | 104 | 16 |
| Existing | 512 | 312 | 80 | 104 | 16 |

The probe maps an initial handle-open failure to `NotPublished` before it
calls the detailed rollover API, so that category includes both open failures
and failures returned by the API itself.

All 928 cases preserved the acknowledged suffix under the campaign's
checks. In 160 checkpoint-uncertain cases the selection was unchanged; 152
left the new checkpoint visible. All 208 selection-uncertain cases had the
new checkpoint visible: 96 kept the previous selection and 112 showed the new
selection. The 32 successful attempts followed injected namespace errors that
did not prevent completion. The report binding version is `2`, indicating
that the saved reference for the new checkpoint is distinct from the previous
checkpoint's digest.

The stable and Rust 1.85 probe tests and example Clippy checks passed, as did
132 Python tests. The parent revision's stable, Rust 1.85 and optimized stable
all-target suites each passed 986 tests, with the documented test-fixture retry
checked separately on the latter two configurations. GitHub's EIO workflow
includes a small V2 check for each suffix scenario.

A [follow-up](MANAGED_ROLLOVER_MODEL_2026_11_14.md) checks that the original
handle rejects reads and writes after selection-uncertain failures and adds
the detailed outcomes to the managed checkpoint fuzz model.

These are selected synthetic returned errors on an overlay filesystem of
unknown backing with the operating system running. They do not exhaust
failure-only calls, partial syscalls, concurrent schedules or persistence
ordering, and they do not establish physical power-loss safety or independent
off-host recovery. A visible selection after a sync error may not be durable.

To recheck the retained coverage, classifications and fixture hashes:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_one_shot_fs_results import verify
with gzip.open('benchmarks/results/2026-11-14-managed-rollover-suffix-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
