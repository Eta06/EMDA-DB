# Current multi-store comparison with SQLite

Two local runs compared clean revision `ac6b3a3` with SQLite 3.49.1 after the
checkpoint-view descriptor change. Each run prepared 100 independent
databases with 1,000 16-byte keys and 64-byte values apiece, then opened all
100 and checked three point reads per database. Every prototype fixture used
V3. SQLite used separate WAL databases with `synchronous=FULL` and cached
prepared read statements. Six fresh processes sampled each mode per run in a
rotated and reversed order.

The table gives the first run and independent repeat. Open time is the median
total time for all 100 databases, not per-operation latency. RSS is the entire
measurement process after reads and excludes the OS page cache. Descriptor
counts include four process descriptors, one of which enumerates
`/proc/self/fd`.

| Mode | Open descriptors | RSS MiB, first / repeat | Open ms, first / repeat |
| --- | ---: | ---: | ---: |
| Prototype ordinary | 404 | 14.23 / 14.19 | 107.29 / 90.35 |
| Prototype checkpoint-backed | 404 | 2.13 / 2.07 | 51.66 / 40.44 |
| SQLite WAL/FULL | 304 | 14.96 / 14.96 | 70.57 / 29.88 |

The prototype's checkpoint path used one more descriptor per database in this
fixture, while retaining less sampled process RSS. The RSS results do not
include filesystem cache, total machine memory, background services or a
project quota. Opening and integrity semantics differ: the ordinary prototype
replays a log, the checkpoint path verifies its checkpoint and hashes the
covered log, and SQLite uses its own page cache and format. The timing winner
changed in the repeat, so these samples do not establish a general speed
ranking or a durable-write latency comparison.

Across both runs, 34 of 36 quiet-window checks passed. The shared VM reported
up to 5.56% steal and 9.62% I/O wait during samples, with no observed cgroup
throttling. The first run had up to 12,288 process physical-read bytes in a
sample; the repeat recorded none. Checkpoint open totals ranged from 29.60 to
76.18 ms in the first run and 28.62 to 55.31 ms in the repeat. SQLite ranged
from 22.67 to 118.47 ms and 25.83 to 33.85 ms respectively. These are
warm-cache VM samples, not cold-device or production-capacity measurements.

The [summary and checksums](../benchmarks/results/2026-11-14-current-multistore-sqlite-summary.json)
link the [first raw run](../benchmarks/results/2026-11-14-current-multistore-sqlite-first.json.gz)
and [repeat](../benchmarks/results/2026-11-14-current-multistore-sqlite-repeat.json.gz).
The broader [ten-engine comparison](LOCAL_COMPARISON_2026_11_03.md) is a
different workload and revision; it should not be combined into a single
ranking with this measurement.
