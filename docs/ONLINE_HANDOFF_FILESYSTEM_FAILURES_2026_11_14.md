# Online handoffs under returned filesystem errors

Revision `616d767120cbfbbfc58b083eddac0c8aa190431c` adds an online backend to
the filesystem-fault probe. The [retained bundle](../benchmarks/results/2026-11-14-online-handoff-faults.json.gz)
contains 18 one-shot campaigns: V2 and V3 source stores, both source-first
and sibling-first recovery orders, and selected `EIO` and `EAGAIN` returns
from synchronization, data-write, read, extended-attribute read, metadata,
path-read, lock, open and namespace calls.

| Injected error | Neither publication started | Candidate uncertain | Exchange uncertain | Success | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| EIO | 804 | 40 | 32 | 118 | 994 |
| EAGAIN | 804 | 40 | 32 | 118 | 994 |
| **Total** | **1,608** | **80** | **64** | **236** | **1,988** |

An injected error can be ignored or handled by a fallback, so a successful
operation is possible. In all 64 exchange-uncertain cases the live handle
rejected both reads and writes before it was dropped. On inspection, 56 had
the original log at the sibling path and eight still had it at the source.
The probe maps an initial source-handle open failure to `NotPublished` before
the handoff API is called, so that category includes pre-call failures as well
as errors returned by the API.

For each case the harness checks the original log, witness and V3 metadata,
the exact key set and acknowledged values in each visible store, and recovery
of the original using a separately saved checkpoint digest. It verifies that
the reference backup and checkpoint are unchanged, and checks recognized
top-level staging names after cleanup. The fixture has no concurrent writes
during the injected handoff; the [outcome report](ONLINE_HANDOFF_OUTCOMES_2026_11_14.md)
separately describes a deterministic concurrent-write test. A partial
candidate does not establish an independently recoverable current backup.

The probe test and example Clippy checks passed on stable Rust and Rust 1.85;
136 Python tests passed. The parent implementation's stable, Rust 1.85 and
optimized stable all-target suites each passed 993 tests. The GitHub EIO
workflow includes a small online V2 namespace-failure check.

These are selected synthetic returned errors on a running Linux VM with an
overlay filesystem of unknown backing. They do not exhaust calls reached only
after a failure, partial syscalls, concurrent schedules or persistence
orderings. They do not establish physical power-loss safety or an independent
off-host restore. Source and destination must be coordinated and inspected
after an exchange-uncertain outcome, even when both paths are visible now.

To recheck the retained reports and coverage:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_handoff_eio_results import verify
with gzip.open('benchmarks/results/2026-11-14-online-handoff-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
