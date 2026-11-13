# Short writes and interrupted records

Linux file writes may complete only part of a requested buffer. The
[test-only interposer](../scripts/short_write_inject.c) forwards an actual
prefix to the kernel and either allows later calls to finish or returns EIO on
the next matching call. It selects one exact disposable fixture file and
records each intercepted call. The interposer is never linked into the engine.
The [driver](../scripts/short_write_campaign.py) checks the resulting files,
V2/V3 witnesses, atomic-batch results, reference backup and both writable
recovery paths. A separate [report verifier](../scripts/verify_short_write_results.py)
checks the recorded case matrix, outcomes, receipts and fixture hashes.

On clean revision `7eb92d74d4188876eb188087d66a31e6c8a2d8ff`, all 224 cases
passed on a Linux workspace overlay:

| Fault location and outcome | Cases | Observed behavior |
| --- | ---: | --- |
| Single-record and atomic-batch log writes, repeated short successes | 64 | The acknowledged write or complete batch reopened through both paths. |
| Single-record and atomic-batch log writes, short then EIO | 64 | The partial log was rejected without advancing its witness or changing recovery evidence. |
| V2 witness writes, repeated short successes | 40 | The write completed and reopened through both paths. |
| V2 witness writes, short then EIO | 40 | 24 unchanged witnesses allowed the complete uncertain record to be recovered; 16 damaged witnesses were rejected with evidence preserved. |
| Four-writer grouped `writev`, repeated short successes | 8 | All 2,048 acknowledged writes reopened through both paths. |
| Four-writer grouped `writev`, short then EIO | 8 | Both paths rejected the partial log and preserved the original evidence. |

For the eight failed grouped cases, the test also made a **separate copy** and
removed only the exact bytes its own injector had just appended. Both recovery
paths then verified the eight recorded prior acknowledgements on those copies.
That controlled check does not provide a way to identify a safe truncation
point after an arbitrary crash. The engine does not automatically repair a
torn log or witness; rejection can make a store unavailable until investigated
or restored. A returned EIO is not proof that an operation was absent.

The [raw report](../benchmarks/results/2026-11-14-short-write-faults.json.gz)
contains call traces, receipt lists, fixture hashes, binary/source hashes,
filesystem information and each recovery result. The independent verifier
checks recorded evidence; the probes checked file contents while the temporary
fixtures existed. The group test briefly delays its first write to encourage
queued writers and requires an intercepted `writev` rather than counting an
unreached injection as a pass.

If a case fails or is interrupted, the driver leaves its synthetic fixture in
the specified fixture directory and reports its path on ordinary errors.
The hosted workflow archives retained fixtures with the V3 witness xattr so
they can be inspected alongside the partial report. Successful cases remove
their disposable fixtures.

The [hosted filesystem workflow](https://github.com/Eta06/EMDA-DB/actions/runs/43526938622)
also passed all 224 cases on ext4 using clean revision
`0b85b76d9e1a151a78eed1d67f53c835f321ed8f`. The independent verifier checked
the retained [hosted report](../benchmarks/results/2026-11-14-short-write-hosted.json.gz),
which has the same case and outcome counts as the local run.

These are selected, synthetic userspace faults on a running OS; they do not
cover every schedule, filesystem behavior, partial I/O form, physical power
loss or off-host restore. The overlay's backing device was not independently
verified. The [hosted filesystem workflow](../.github/workflows/sync-eio.yml)
now runs the same matrix and verifier on Linux.
