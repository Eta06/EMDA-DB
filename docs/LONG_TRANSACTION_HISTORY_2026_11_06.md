# Longer concurrent transaction histories — 2026-11-06

The [test](../tests/long_transaction_history.rs) runs four writers and two
readers against one open store. Each writer commits 64 transfers. Three writers
transfer left to right and one transfers right to left; each transaction
atomically updates both balances and its own sequence counter. Writers use a
mix of conservative and read-set transactions and retry only the documented
optimistic conflict, within a bound.

Each reader repeatedly fetches all six keys with one `get_many` call. An
independent invariant derives both balances from the four observed counters;
the test also checks that each reader's observed counters never move backward.
Negative controls show the invariant rejects fractured balances, a missing
balance update and invalid progress. After all 256 transfers, the test checks
the exact final state and reopens first through the ordinary path and then the
checkpoint-backed path.

On clean revision `327978c1305af7f664e6c2db259d6562804f0b9e`, twenty
optimized repetitions ran each V2/V3 and ordinary/checkpoint-writer
combination on each of two temporary filesystems:

| Fixture filesystem | Committed transfers | Checked live-reader snapshots | Result |
| --- | ---: | ---: | --- |
| `/tmp` tmpfs | 20,480 | 72,591 | Passed |
| Workspace overlay | 20,480 | 2,851,245 | Passed |

The temporary directory for the first run was the default `/tmp`, identified
as tmpfs immediately after the run. The overlay run explicitly selected the
workspace fixture directory; its backing device was not independently
identified. The [tmpfs record](../benchmarks/results/2026-11-06-long-history-tmpfs.json)
and [overlay record](../benchmarks/results/2026-11-06-long-history-overlay.json)
contain per-run observations and conflicts. Snapshot counts are schedule
observations, not a throughput comparison.

These checks sample concurrent schedules and selected transaction operations.
The invariant does not prove every history serializable, and the runs do not
kill a process during a transaction or establish physical power-loss safety.
The [foundation follow-up](FOUNDATION_FOLLOW_UP.md) retains the broader open
concurrency, failure, resource and compatibility work.
