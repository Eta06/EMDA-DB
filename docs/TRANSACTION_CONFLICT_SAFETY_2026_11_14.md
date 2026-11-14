# Identifying transaction conflicts safely

An optimistic validation conflict and a commit I/O failure can both be
reported as `WouldBlock`. Retrying a non-idempotent callback based only on
that kind can repeat a write whose commit outcome is unknown. Ordinary and
checkpoint-backed transactions now attach a private marker to the engine's
pre-commit conflict errors. Callers can use the public
`is_transaction_conflict(&error)` helper on a directly returned `io::Error`
before considering a retry. The existing error kind and displayed conflict
message remain unchanged; no database format changes.

The marker means the transaction's staged writes were not submitted. It does
not undo independent writes or other callback effects. Other errors can also
be pre-commit rejections, so a false result does not classify their outcome.
Error wrapping or replacing the error can discard the marker. For timed or
controlled transactions, inspect the `Operation` error inside the returned
enum; an engine-reported deadline or cancellation has its separate documented
meaning. Callers must reconcile uncertain commit errors instead of assuming a
rollback.

The focused tests cover conservative, read-set, FIFO, timed and controlled transactions,
both writable stores and document transactions. Callback and admission
`WouldBlock` errors are not marked. In separate ordinary and checkpoint
fixtures, a synchronization failure returned `WouldBlock` after an append:
the marker correctly returned false, the handle rejected later access, and
the attempted write appeared after recovery. Those are synthetic running-OS
failures, not power-loss tests. Benchmarks and the relevant transaction models
now retry only the explicit conflict marker instead of matching the error kind
or message.

On clean revision `36e73713b4f5d29238bf475de5571694809eb92c`, an
address-sanitized [read-set fuzz run](../benchmarks/results/2026-11-14-conflict-readset-fuzz.json)
completed 22,533 executions in 61 seconds without a finding. Its 514 starting
inputs included four new 128-byte mixed-history seeds, one per transaction
mode. The [archive](../benchmarks/results/2026-11-14-conflict-readset-fuzz.tar.gz)
retains the starting and resulting corpora and log. This finite serial model
does not exhaust thread schedules or inject I/O failures.

## Local transaction comparison

We compared clean baseline `5759a3dbbb2f9956fa6accec09098502984b4cec` with
`36e73713b4f5d29238bf475de5571694809eb92c` using separately built release
binaries, then repeated them in reverse order. Each run used four clients,
80 two-key transactions per client, and two repetitions for each workload
and engine. The baseline and candidate retries differ as described above;
the workload verified final counters after reopening. The prototype used V3
witnessed synchronization. SQLite 3.49.1 used WAL, `synchronous=FULL`,
`BEGIN IMMEDIATE` and application-level value checksums; implementation and
durability details differ.

Each table cell gives the first and repeat run's median p50 in microseconds.
The last column gives the candidate runs' median p99; each run has only 320
transaction samples, so its tail is especially sensitive to scheduling.

| Workload | Engine | Baseline p50, first / repeat | Candidate p50, first / repeat | Candidate p99, first / repeat |
| --- | --- | ---: | ---: | ---: |
| Disjoint | Ordinary conservative | 265.3 / 306.0 | 326.7 / 288.7 | 7,017.6 / 7,136.5 |
| Disjoint | Ordinary read-set | 298.1 / 322.7 | 305.6 / 363.3 | 5,440.6 / 6,185.5 |
| Disjoint | Checkpoint conservative | 207.7 / 261.5 | 250.2 / 260.3 | 10,889.3 / 18,455.9 |
| Disjoint | SQLite | 358.9 / 220.8 | 267.3 / 316.9 | 3,328.8 / 1,043.8 |
| Shared | Ordinary conservative | 293.4 / 295.1 | 282.4 / 321.6 | 7,026.6 / 6,925.0 |
| Shared | Ordinary read-set | 249.6 / 300.7 | 286.3 / 280.8 | 6,325.3 / 7,898.5 |
| Shared | Checkpoint conservative | 222.4 / 222.7 | 234.2 / 254.2 | 17,302.9 / 10,595.4 |
| Shared | SQLite | 249.7 / 287.4 | 298.7 / 204.3 | 3,544.3 / 630.3 |

The results are mixed: the candidate is slower in several samples and faster
in others, and SQLite's own p50 varies substantially between runs. The runs
are too small and noisy to attribute those differences to the marker or claim
a general performance improvement or regression. Initial quiet checks passed;
the shared Linux VM reported up to 3.28% steal and no cgroup CPU throttling
during the measured runs. Its overlay's backing device is unknown. The machine
used an AMD EPYC 9V74 CPU, a four-CPU cgroup quota, CPUs 0–3 for workers and
CPU 4 for the driver.

The [baseline](../benchmarks/results/2026-11-14-conflict-baseline.json.gz),
[candidate](../benchmarks/results/2026-11-14-conflict-candidate.json.gz),
[candidate repeat](../benchmarks/results/2026-11-14-conflict-candidate-repeat.json.gz)
and [baseline repeat](../benchmarks/results/2026-11-14-conflict-baseline-repeat.json.gz)
retain samples, source and binary hashes, settings and noise telemetry. These
are local prototype measurements, not physical power-loss, cold-device or
independent-host tests.
