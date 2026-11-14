# Automatic-checkpoint configuration outcomes

Revision `f0dba529c5714657f61b6530e2759021e2540e53` adds opt-in detailed
outcomes for enabling and disabling automatic checkpoints. `NotPublished`
means this call did not begin its configuration-publication attempt; it does
not say the prior configuration was absent or the store remains healthy.
`OutcomeUnknown` requires coordinating callers, dropping the disabled handle,
reopening and inspecting the configuration before retrying. The existing
methods retain their prior error types. Neither outcome certifies cleanup or
resolves a previous uncertain operation.

Deterministic checks cover first enable, re-enable and disable, before the
publication attempt and after publication but before parent synchronization.
They check classifications, preserved acknowledgements, disabled handles and
recovered configuration using a V2 fixture and the filesystem's default format
(V3 on this Linux filesystem). Additional checks cover a missing managed
selection, repeated idempotent calls and a corrupted configuration.

Revision `0e48af457380e31590cb8d30f322bd16b998c5c0` adds a separate
filesystem-fault probe. Its [retained bundle](../benchmarks/results/2026-11-14-automatic-configuration-faults.json.gz)
contains 18 campaigns covering V2/V3, both ordinary-first and managed-first
recovery orders, and selected `EIO` and `EAGAIN` returns from synchronization,
data-write, read, extended-attribute read, metadata, path-read, lock, open and
namespace calls.

| Configuration operation | Not published | Outcome unknown | Success | Total |
| --- | ---: | ---: | ---: | ---: |
| First enable | 708 | 240 | 220 | 1,168 |
| Re-enable | 1,052 | 112 | 292 | 1,456 |
| Disable | 1,036 | 112 | 292 | 1,440 |
| **Total** | **2,796** | **464** | **804** | **4,064** |

Every unknown-outcome case rejected both reads and writes on the same handle.
The probe maps failures while opening the managed handle to `NotPublished`
before calling the detailed API: 1,948 of that category had no opened handle.
The remaining 848 returned `NotPublished` with a disabled handle, illustrating
why this classification is not a health check. A returned syscall error can
also be ignored or handled by a fallback, so some operations succeeded.

Each case verifies the exact acknowledged key set after both recovery paths,
and restores the fully populated reference backup locally. It checks that the
source log, witness and V3 metadata, managed selection, selected checkpoint,
prior independently recorded checkpoint digest and backup bytes are unchanged
through recovery. The selected checkpoint digest is stored in the managed
selection; this fixture does not independently authenticate that selection.
Recognized top-level staging names in the fixture, source and configuration
directories are checked after cleanup; nested artifacts are not exhaustive.

The implementation's full stable, Rust 1.85 and optimized stable all-target
suites each passed 995 tests; one guarded tmpfs ENOSPC test was ignored in
each ordinary suite. Both all-target Clippy checks passed. The probe's test and
example Clippy checks passed on both toolchains, and 139 Python tests passed.
The GitHub EIO workflow includes a small V2 re-enable namespace-failure check.

## Concurrent configuration and atomic batches

Revision `e4534dfef49c8ec69e082a946276d3d95b41f00f` adds a test with three
writers submitting two-key atomic batches, two callers enabling or disabling
automatic checkpoints, and a concurrent scanner. A coordinated interim scan
observes the first acknowledged pair while that writer's later batches have
not yet started. Scans check that each pair is either fully present or fully
absent; after all threads finish, the test compares the entire acknowledged
key set and the observed configuration against the reopened store.

The test passed on stable Rust and Rust 1.85. One hundred optimized repetitions
passed on the local Linux overlay filesystem, totaling 200 fixtures across
forced V2 and the default format (V3 here). Each fixture attempts 24 atomic
batches, 32 configuration calls and 25 scans. The [metadata](../benchmarks/results/2026-11-14-automatic-configuration-concurrency.json)
and [raw log](../benchmarks/results/2026-11-14-automatic-configuration-concurrency.log.gz)
record the run. Both all-target Clippy checks passed for this revision.
The test uses sufficiently high suffix limits that it does not exercise
automatic rollover. It samples scheduling, uses normal close and reopen, and
does not inject faults or establish physical power-loss safety.

These checks use selected synthetic errors on a running Linux VM with an
overlay filesystem of unknown backing. They do not cover partial syscalls,
all error-only code paths, concurrent configuration callers, persistence
reordering, physical power loss or an independent off-host restore. Local
restoration of the fixture backup is not an off-host recovery drill.

To recheck retained classifications and selected-call coverage:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_configuration_eio_results import verify
with gzip.open('benchmarks/results/2026-11-14-automatic-configuration-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
