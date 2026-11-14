# Backup and restore publication outcomes

Revision `e1eee1fc894f0662a0204a19c637478cc7010f5e` adds opt-in
`backup_detailed` methods to both writable stores and `Store::restore_detailed`.
They distinguish a call that did not begin destination publication
(`NotPublished`) from one whose publication outcome is uncertain
(`OutcomeUnknown`). Existing methods and their error types remain available.

`NotPublished` concerns this call only: a destination may already exist, a
staging artifact may remain, and it does not establish source health. An
uncertain result needs inspection and reconciliation before retry. In particular,
a non-overwrite destination conflict is conservatively uncertain. A panic unwinds
without returning this classification. Neither classification proves a backup
is independent or recoverable on another host.

## Local filesystem-fault evidence

On revision `e1eee1f`, the following campaigns injected a single returned
error into selected syscalls observed on successful backup, checkpoint-store
backup and restore paths. Each combination covered V2 and V3 fixtures and both
orders for reopening via the writable recovery paths. The target paths were
fresh; visible published copies and the preserved source/reference backup were
checked on the running OS. An independent verifier checked the retained reports.

| Fault set | Injected cases | Not published | Unknown | Succeeded |
| --- | ---: | ---: | ---: | ---: |
| [Synchronization, EIO](../benchmarks/results/2026-11-14-publication-sync-eio.json.gz) | 142 | 82 | 60 | 0 |
| [Synchronization, EAGAIN](../benchmarks/results/2026-11-14-publication-sync-eagain.json.gz) | 142 | 82 | 60 | 0 |
| [Namespace, EIO](../benchmarks/results/2026-11-14-publication-namespace-eio.json.gz) | 40 | 4 | 12 | 24 |
| [Data writes, EIO](../benchmarks/results/2026-11-14-publication-data-eio.json.gz) | 62 | 62 | 0 | 0 |
| [Reads, EIO](../benchmarks/results/2026-11-14-publication-read-eio.json.gz) | 180 | 180 | 0 | 0 |
| [Open, EIO](../benchmarks/results/2026-11-14-publication-open-eio.json.gz) | 134 | 122 | 12 | 0 |

Of 700 injected cases, none of the 532 classified `NotPublished` left its
fresh target visible. Of 144 uncertain cases, 132 left a visible verified
target and 12 did not. The 24 successful calls followed injected errors in
namespace operations that did not prevent completion. Each report also contains
six uninjected controls, the filesystem, tool and binary hashes, traces, and
its selected syscall ordinals. Recheck a report with
`python3 scripts/verify_one_shot_fs_results.py <report.json.gz>`.

These selected, synthetic returned errors do not cover failures of additional
calls reached only on error paths, all schedules, partial syscalls, persistent
faults or storage persistence ordering. The workspace filesystem was an overlay
of unknown backing; the operating system remained running. The results are
not physical power-loss or off-host recovery evidence.

## Concurrency and checks

Test-only revision `7793e84` alternates the existing and detailed backup and
restore APIs while four writers update pairs of keys. Across ordinary and
checkpoint-backed writers and V2/V3 logs, each run checks 48 restored snapshots
against acknowledgement bounds and verifies each pair remains atomic. One
debug run each on Rust 1.85 and stable and five optimized stable repetitions
passed. These finite schedules do not establish exhaustive concurrency safety.

On production revision `e1eee1f`, the stable, Rust 1.85 and optimized
all-target suites each passed 961 tests, with four test threads. The updated
concurrency test was rerun separately on both toolchains and optimized stable.
Both all-target Clippy checks, formatting, and the 128-test Python script suite
also passed. The filesystem-EIO workflow now includes a small publication
classification check. This change did not include a performance comparison;
the [direct mutation report](DIRECT_MUTATION_OUTCOMES_2026_11_14.md) records
the latest sampled write measurements and their limits.
