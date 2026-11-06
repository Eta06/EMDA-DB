# Metadata, path and lock syscall failures

A survey of successful fixture operations found filesystem metadata, path
resolution and lock calls that were not part of the earlier syscall-error
matrix. On clean revision `24746c157db12b7f807bdf293c9da57910f8cfa8`, the
expanded driver injected one returned `EIO` or `EINTR` at every eligible call
observed in its successful fixture paths. Each injection was checked with
ordinary and checkpoint-backed recovery taking turns reopening first.

The [archived reports](../benchmarks/results/2026-11-06-metadata-lock-faults.tar.gz)
contain all six runs, their selected ordinals, traces, results, versions and
binary hashes. An independent verifier checked that the selected calls and
both recovery orders were present. Across 26 V2/V3 operation controls per
errno, all **3,732 injected cases** completed their preservation and recovery
checks on the workspace overlay:

| Call group | Cases for each errno | Operation errors | Operation successes |
| --- | ---: | ---: | ---: |
| Metadata: `statx` | 1,416 | 1,396 | 20 |
| Path resolution: `readlink` | 164 | 164 | 0 |
| Lock/descriptor: `flock`, `fcntl` | 286 | 250 | 36 |

Other candidate calls in these sets were not observed in these fixtures.
The 20 successful metadata cases per errno all injected the error into a
metadata query on the fixture's `checkpoint.digest` file; the operation
continued and the checkpoint and recovery checks succeeded. The 36 successful
lock cases per errno injected errors into `flock(..., LOCK_UN)` during cleanup;
both recovery paths reopened successfully in those process-level checks. These
successes are retained explicitly, rather than counted as propagated errors.

The driver covers its observed initialization, write, atomic batch, backup,
restore, upgrade, checkpoint, rollover and separate-copy compaction paths.
It injects returned errno values, not actual signals or hardware failures. It
does not enumerate all later error-path calls, arbitrary concurrent schedules,
physical persistence reorderings or off-host recovery. A shared VM and overlay
filesystem do not establish physical power-loss behavior.

CI runs a bounded subset of each new group. To reproduce the full local groups,
build `sync_failure_probe` in release mode, then run
`scripts/sync_eio_campaign.py` with `--syscall-set metadata`, `path-read` or
`lock` and `--error EIO` or `EINTR`; use a new output path for each run. The
independent `scripts/verify_one_shot_fs_results.py` checks each complete
one-shot report.
