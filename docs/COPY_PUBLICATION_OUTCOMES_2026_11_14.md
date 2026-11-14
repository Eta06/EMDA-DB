# Checkpoint and compaction copy outcomes

Revision `bfb8158e0c1b8bd3706d2e482946a443b8a141cd` adds
`checkpoint_to_detailed` to both writable stores. Revision
`2a2e5f8c446fbd447b66b2c9bbab992fce06b3b4` adds `compact_to_detailed` for
separate copies. A `NotPublished` failure means this call did not begin
publishing its destination; an existing destination or staging remnants can
still exist. `OutcomeUnknown` needs inspection and reconciliation before
retry. A pre-existing destination conflict is conservatively unknown.
Neither outcome certifies source health, cleanup or an independent backup.
Panic unwinding and existing methods retain their prior behavior.

Successful checkpoint copies return a digest that the caller must retain in
separately trusted metadata. Checkpoint-backed compaction does the same for
the new copy's checkpoint. An uncertain failure returns no digest, even if a
destination is visible. This classification covers separate copies, not
rollover or compact-and-replace handoffs.

## Selected returned-error cases

The [checkpoint](../benchmarks/results/2026-11-14-checkpoint-publication-faults.json.gz)
and [compaction](../benchmarks/results/2026-11-14-compaction-publication-faults.json.gz)
bundles retain twelve one-shot campaigns each. They exercise V2/V3 and both
writers, with both writable recovery paths taking turns opening first. EIO was
injected into observed synchronization, data-write, read, open and namespace
calls; EAGAIN was also injected into observed synchronization calls. Each
target was fresh, and the source and reference backup were checked. Visible
copies were inspected on the running OS.

| Copy | Injected cases | Not published | Unknown, visible | Unknown, absent | Success |
| --- | ---: | ---: | ---: | ---: | ---: |
| Checkpoint | 500 | 388 | 88 | 8 | 16 |
| Compaction | 1,100 | 988 | 88 | 8 | 16 |

None of the 1,376 calls classified `NotPublished` left its fresh destination
visible. The 32 successes followed namespace errors that did not prevent
completion. Checkpoint copies were inspected using a digest saved from the
unchanged source before the attempted copy. For compacted checkpoint copies,
the test recomputes the digest from the produced file; this checks internal
consistency but is not independent authentication.

An independent verifier checks every retained campaign. For example:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_one_shot_fs_results import verify
for name in ('checkpoint', 'compaction'):
    with gzip.open(f'benchmarks/results/2026-11-14-{name}-publication-faults.json.gz', 'rt') as source:
        bundle = json.load(source)
    print(name, sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```

The first attempts to request EAGAIN used an incorrect command-line flag and
never started a campaign; the [CLI failure log](../benchmarks/results/2026-11-14-checkpoint-publication-cli-failure.log.gz)
is retained, and the corrected runs are in the bundle. These selected,
synthetic returned errors do not exhaust failure-only syscalls, partial
syscalls, all thread schedules or persistence ordering. The workspace uses an
overlay of unknown backing and the OS remained running. The results are not
physical power-loss or off-host recovery evidence.

## Concurrency and checks

The [concurrent-copy model](../benchmarks/results/2026-11-14-checkpoint-publication-model.log.gz)
checks checkpoint copies against acknowledgement bounds while four writers
make atomic paired updates. It covers both writable paths and V2/V3. One run
each on stable and Rust 1.85 and five optimized stable repetitions checked 48
checkpoint copies per run. Writers are scheduled alongside each copy, although
some can complete before checkpoint creation begins; these are finite samples.

The checkpoint production revision passed 974 all-target tests and the
compaction production revision passed 977 on stable, Rust 1.85 and optimized
stable with four test threads. Both all-target Clippy checks and 129 Python
tests passed for each revision. The updated checkpoint concurrent model was
rerun separately. The filesystem-EIO workflow includes small checks for both
checkpoint and compaction copy classifications.
