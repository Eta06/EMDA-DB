# Compaction handoffs under returned filesystem errors

Revision `720b81395cd6f6ed6c7f37a561d321f7fa6df395` adds a separate
filesystem-fault harness for the consuming detailed compaction handoffs. The
[retained bundle](../benchmarks/results/2026-11-14-handoff-publication-faults.json.gz)
contains six one-shot Linux campaigns covering V2/V3 source stores, ordinary,
checkpoint-backed and managed checkpoint backends, and both source-first and
sibling-first verification orders. EIO was injected into selected observed
sync, data-write, read, open and namespace calls; EAGAIN was injected into
selected sync calls.

| Backend | Injections | Neither publication started | Candidate uncertain | Exchange uncertain | Success |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ordinary | 688 | 108 | 528 | 48 | 4 |
| Checkpoint-backed | 728 | 96 | 572 | 48 | 12 |
| Managed checkpoint | 1,376 | 384 | 920 | 48 | 24 |
| **Total** | **2,792** | **588** | **2,020** | **144** | **40** |

Among candidate-uncertain outcomes, 724 left a visible sibling and 1,296 did
not. In all 144 exchange-uncertain cases, both paths were visible; 132 had the
original log at the sibling and 12 still had it at the source. The 40
successes followed injected namespace errors that did not prevent completion.
The probe maps an initial source-handle or fixture-digest open failure to
`NotPublished` before calling the detailed API, so that category includes
both pre-call failures and failures returned by the handoff method.

For each case, the harness checks the original log and witness, V3 metadata,
selection and automatic-checkpoint owner marker where present. It verifies
the exact key set and acknowledged values in each visible store, reopens the
original through the checkpoint-backed path using its separately saved
checkpoint digest, and checks managed reopening of the live path. Reference
checkpoint and backup bytes are checked for preservation. The managed
fixture's reference backup predates its last acknowledged managed write, so
it is not treated as a backup of that final state. A partly configured
candidate is checked through ordinary full-log recovery; that does not prove
its managed configuration is usable. Recognized staging names are checked at
the fixture root and the top level of both store paths after cleanup; nested
automatic-checkpoint artifacts are not exhaustively checked.

The probe's test and example Clippy checks passed on stable Rust and Rust
1.85; 135 Python tests passed. The parent implementation's full stable, Rust
1.85 and optimized stable all-target suites each passed 989 tests. The GitHub
EIO workflow includes a small managed V2 namespace-failure check.

These are selected synthetic returned errors on a running Linux VM using an
overlay filesystem of unknown backing. They do not exhaust failures on paths
only reached after an error, partial syscalls, concurrent schedules or
persistence orderings. They do not establish physical power-loss safety or an
independent off-host recovery. An error after an exchange attempt can leave
the persisted outcome uncertain even when both paths are visible now.

To recheck the retained coverage, classifications and recorded hashes:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_handoff_eio_results import verify
with gzip.open('benchmarks/results/2026-11-14-handoff-publication-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
