# Recurring Turso byte-key and process-restart comparison

Revision `ffcbf51` added the pinned local Turso adapter to the recurring
Linux cross-engine workflow. The same byte-key workload driver compares
ordinary EMDA, SQLite and Turso, and verifies the returned values. The
time measurement excludes fixture creation and final value checks. A
separate process-kill check verifies every acknowledged surviving value
and deletion after reopening.

The push-triggered smoke job ran two rounds on each of two workloads with
32 initial keys, for 12 successful runs. The workflow-dispatched measured
job ran three rounds on each of three workloads with 4,096 initial keys
and 64-byte values, for 27 successful runs. Backends and workload order
rotated between rounds. The separate restart checks passed all three smoke
fixtures and all six measured fixtures. Each held client connections open
through process termination and checked 90 values and 10 deletions
following 120 acknowledged mutations.

## Measured byte-key workloads

Medians of three runs per row on an Intel Xeon 6975P-C hosted Linux x86-64
runner with ext4. Client work used CPU 0, adapter work used CPUs 1–2 and
orchestration used CPU 3. All three backends used loopback transport.
The prototype used the synchronized acknowledgement path with a V3 log
on verified restart fixtures. SQLite 3.49.1 and Turso 0.8.3 both used
WAL with `synchronous=FULL`.

| Workload | Backend | Median operations/s | Range | Median write p50 / p99 (µs) |
| --- | --- | ---: | ---: | ---: |
| Single-client mixed | EMDA | 11,071 | 9,845–14,123 | 139.8 / 243.0 |
| Single-client mixed | SQLite | 15,019 | 14,836–15,358 | 77.0 / 162.9 |
| Single-client mixed | Turso | 6,355 | 6,192–6,489 | 226.2 / 500.3 |
| Four-client mixed | EMDA | 31,288 | 31,266–31,437 | 211.6 / 338.9 |
| Four-client mixed | SQLite | 16,766 | 16,688–18,921 | 76.6 / 3,274.6 |
| Four-client mixed | Turso | 540 | 480–610 | 3,991.0 / 144,942.9 |
| Four-client write-only | EMDA | 17,252 | 16,653–17,472 | 222.8 / 320.5 |
| Four-client write-only | SQLite | 11,608 | 10,837–12,529 | 64.6 / 219.0 |
| Four-client write-only | Turso | 422 | 401–446 | 3,984.2 / 113,018.9 |

These are measured adapter results on one runner. The Turso adapter
uses Python bindings, while EMDA and the bundled SQLite adapter use Rust.
The workload provides no independent attribution of adapter and storage
costs. All measured quiet checks passed. The raw telemetry reports no
CPU throttling or direct reclaim during the measured runs. The runner
did not expose memory-event counters. Host I/O wait reached 10.94%. Two quiet checks failed
during the separate smoke run, and both runs remain in the archive.
No adapter-managed busy retries were recorded in the measured runs.
CPU, I/O and memory telemetry describe the runner and its cgroup.
Physical power loss and independent off-host restore remain deferred.

## Coverage and evidence

Recurring Linux comparisons now cover SQLite, PostgreSQL, MongoDB and
Turso. Their workloads differ. Turso is in the local byte-key and restart
checks. PostgreSQL and MongoDB are in the compound-range, document
contention and large-value process-recovery checks.

The [raw archive](raw-artifacts.tar.gz) contains every reported run and
command attempt from the [push smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45370213829)
and [measured workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45370291912),
including the comparison jobs for PostgreSQL and MongoDB. Use
`scripts/summarize_engines.py` on the raw byte-key `samples.json`
files to regenerate the included summaries.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45370213854)
passed stable and Rust 1.85.0 validation, the optimized tests, all 45
standard benchmark smoke attempts and the four-writer recovery memory
gate. All 51 memory-gate phases passed, including 24 phases capped at
48 MiB of process address space. This change updates Python and CI.
The Rust source matches the [previous cross-platform validation](../2026-11-24-document-request-write-budget/README.md).
