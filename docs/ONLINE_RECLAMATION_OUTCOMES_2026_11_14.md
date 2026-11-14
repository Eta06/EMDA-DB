# Online reclamation outcomes

Revision `70eda753ad5c901eb1f788d0ce5d672eec59f8e2` adds
`OnlineStore::compact_and_reclaim_detailed`. Its
`OnlineReclaimFailure::Compaction` contains the existing online handoff
classifications: this call did not begin candidate publication, candidate
publication may have begun, or exchange may have begun. An exchange-uncertain
outcome disables the handle; coordinate callers, drop it and reopen both
paths before retrying. Independent callbacks can write while preparation runs,
so a candidate can omit later writes and is not necessarily a current backup.

`OnlineReclaimFailure::Reclamation` instead means the synchronized handoff
succeeded, but cleanup failed; it includes the successful handoff report. The
old copy may be partially removed or retained under an owned staging name.
This cleanup failure alone does not disable the new live handle, though
independent operations can fail. Neither classification proves cleanup,
secure erasure, source health or an independent backup. The original
`compact_and_reclaim` method and its error type are unchanged.

Deterministic checks exercise preflight, initial copying, the final barrier,
candidate publication, exchange and parent-sync failures, with V2 and the
default local format. They check that reclamation has not started after a
failed handoff, that exchange uncertainty blocks reads and writes, and that
acknowledged data can be reopened. Ten cleanup hook boundaries on both starting
formats verify the separate reclamation result and a usable recovered live
store. A concurrent test acknowledges a source write during preparation and
checks it survives a later not-published outcome. Public API checks cover
invalid options, destination conflicts and successful reclamation; unsupported
platform behavior has a portable integration test whose macOS run is pending.

Stable, Rust 1.85 and optimized stable all-target suites each passed 1,004
tests; one guarded tmpfs ENOSPC test was ignored by each ordinary suite.
Both all-target Clippy checks and formatting passed. These hook checks sample
specific boundaries on a running OS; they do not prove physical power-loss
safety, off-host restore or every filesystem failure ordering.

## Returned filesystem errors

Revision `4a48d1fe50a1aba263000f663a9d002c18f5d0e2` adds a separate
filesystem-fault probe. The [retained bundle](../benchmarks/results/2026-11-14-online-reclamation-faults.json.gz)
contains 18 one-shot campaigns covering V2/V3 initial stores, source-first and
backup-first verification orders, and selected `EIO` and `EAGAIN` returns
from synchronization, data-write, read, extended-attribute read, metadata,
path-read, lock, open and namespace calls.

| Injected error | Not published | Candidate uncertain | Exchange uncertain | Reclamation failed | Success | Total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| EIO | 804 | 40 | 32 | 160 | 154 | 1,190 |
| EAGAIN | 804 | 40 | 32 | 160 | 154 | 1,190 |
| **Total** | **1,608** | **80** | **64** | **320** | **308** | **2,380** |

All 64 exchange-uncertain outcomes blocked both reads and writes on the live
handle. In 56, the original log was visible at the previous path; in eight it
remained at the source. All 320 reclamation failures left the live handle
readable. In 160 the original was still at the previous path; in 160 that
path was absent and staging was visible. The probe records stage contents and
V3 log metadata, but does not assume the retired copy is complete or remove
those artifacts. A syscall error may be ignored or handled by a fallback, so
some operations succeeded. The probe maps initial source-handle open errors to
`NotPublished` before invoking the API; 300 cases in that category had no
opened handle.

Every case verifies the exact acknowledged key set from the live store and
from a local restoration of a separately saved full-log backup. Visible
siblings are also opened for candidate- and exchange-uncertain results.
The source, sibling, staged file bytes and V3 metadata, saved checkpoint and
backup evidence are checked for changes during verification. The saved
checkpoint is not assumed to apply to a newly compacted log.

The probe test and example Clippy checks passed on stable Rust and Rust 1.85;
144 Python tests passed. The GitHub EIO workflow includes a small V2 namespace
check. These are selected synthetic returned errors on a running Linux VM with
an overlay filesystem of unknown backing. They do not cover partial syscalls,
all error-only paths, concurrent writes, automated cleanup of the preserved
stages, persistence reordering, physical power loss or an independent off-host
restore. Local restoration is not an off-host recovery drill.

To recheck the retained classifications and selected-call coverage:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_reclamation_eio_results import verify
with gzip.open('benchmarks/results/2026-11-14-online-reclamation-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```

## Reference model

The existing online-compaction model now alternates the original and detailed
reclamation methods, including rejected destinations and invalid options. A
90-second run completed **4,611 inputs** without a finding. The
[metadata and hashes](../benchmarks/results/2026-11-14-online-reclaim-model.json),
[raw log](../benchmarks/results/2026-11-14-online-reclaim-model.log.gz),
[saved corpus](../benchmarks/results/2026-11-14-online-reclaim-model-corpus.tar.gz)
and [replay log](../benchmarks/results/2026-11-14-online-reclaim-model-replay.log.gz)
are retained. Of 245 corpus files, 53 force the initial V2 format and 192 use
the default (V3 on this Linux filesystem). Replaying them completed 247
reported runs. The configured maximum input was 96 bytes, but the fuzzer
reached a 24-byte generation limit in this short run. Both new fixed seeds
passed individually.

The model is sequential and does not inject I/O failures, model concurrent
callbacks or establish physical power-loss behavior. The scheduled and
manually triggered fuzz workflow already runs this target. To replay the corpus
on a suitable Linux machine with the pinned nightly toolchain and `cargo-fuzz`,
extract it into a new empty `extracted-corpus` directory and run:

```sh
cargo +nightly-2026-11-03 fuzz run online_compaction ./extracted-corpus -- -runs=245 -max_len=96 -timeout=15
```
