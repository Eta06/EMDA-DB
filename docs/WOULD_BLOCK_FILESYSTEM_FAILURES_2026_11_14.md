# Filesystem `EAGAIN` injection

On clean revision `6fb127733e246b748a2d3dd3d645a142d9ed5e22`, six local
Linux `strace` campaigns injected a single returned `EAGAIN` at each selected
syscall ordinal observed on a successful path. Each ordinal was tested with
both writable recovery paths reopening first, on V2 and V3 fixtures. Every
campaign used 26 successful controls: 13 operations in each format, including
initialization, single writes, atomic batches, backup, restore, upgrade,
checkpoint, compaction and checkpoint-backed variants.

| Syscalls | Injected cases | Reported error | Reported success | Recorded evidence |
| --- | ---: | ---: | ---: | --- |
| `fsync`, `fdatasync` | 746 | 746 | 0 | [sync](../benchmarks/results/2026-11-14-eagain-sync.json.gz) |
| `flock`, `fcntl` | 286 | 250 | 36 | [lock](../benchmarks/results/2026-11-14-eagain-lock.json.gz) |
| Writes and xattr updates | 206 | 206 | 0 | [data](../benchmarks/results/2026-11-14-eagain-data.json.gz) |
| File opens | 682 | 682 | 0 | [open](../benchmarks/results/2026-11-14-eagain-open.json.gz) |
| Namespace changes | 148 | 92 | 56 | [namespace](../benchmarks/results/2026-11-14-eagain-namespace.json.gz) |
| Xattr reads | 176 | 176 | 0 | [xattr](../benchmarks/results/2026-11-14-eagain-xattr.json.gz) |
| Total | 2,244 | 2,152 | 92 | |

The harness checked acknowledged data, source and reference backup
preservation, visible published copies and staging cleanup. Successful results
were checked for the requested effect; some cleanup or lock errors can occur
after the operation's result is already determined. Failed write outcomes
remain uncertain. The separate report verifier confirmed each selected
ordinal, injected errno and recovery order in the retained reports.

This campaign injects returned errors on a running OS; it does not simulate a
partial syscall, every error-only path, physical power loss or off-host restore.
It tests selected single-threaded filesystem operations, not transaction error
classification or every concurrency schedule. The
[transaction conflict report](TRANSACTION_CONFLICT_SAFETY_2026_11_14.md)
documents the distinct transaction-marker checks. The VM's overlay backing
storage and physical persistence behavior are not independently characterized.

To verify a retained report, run:

```sh
python3 scripts/verify_one_shot_fs_results.py benchmarks/results/2026-11-14-eagain-sync.json.gz
```
