# Structured transaction outcomes

Revision `9d09dc3a7bfa28068b53d84175853c1058945f18` adds opt-in structured transaction errors for ordinary and checkpoint-backed stores and document transactions. `Callback`, `Rejected`, `Cancelled` and `DeadlineExceeded` mean only that this transaction's staged changes were not submitted; independent callback writes or other side effects are not rolled back. A callback error can itself describe an independent write with an unknown outcome. `OutcomeUnknown` conservatively includes some pre-append failures, including certain quota rejections. A panic unwinds without returning this enum. Existing methods and direct mutation APIs retain their previous error types. See the [storage contract](STORAGE_TRANSACTION_CONTRACT.md).

## Failure classification and recovery

The retained `strace` runs inject returned errors into log synchronization or witness updates in concurrent, two-key transactions on V2/V3 ordinary and checkpoint-backed stores. Both recovery paths reopen every case, with both recovery orders represented. The driver records each failure's classification; the recovery probe rejects any recovered write classified as not submitted and checks that uncertain writes are either complete or absent. The independent report verifier rejects inconsistent classifications and recovery receipts. `EIO` and `EAGAIN` were each repeated twice for 64 cases; `EINTR` was run once for 32 cases.

| Injection | Cases | Acknowledged | Confirmed not submitted | Unknown | Unknown recovered |
| --- | ---: | ---: | ---: | ---: | ---: |
| EAGAIN | 64 | 103 | 192 | 64 | 64 |
| EIO | 64 | 97 | 192 | 64 | 64 |
| EINTR | 32 | 8,192 | 0 | 0 | 0 |

The [EAGAIN](../benchmarks/results/2026-11-14-detailed-eagain.json.gz), [EIO](../benchmarks/results/2026-11-14-detailed-eio.json.gz) and [EINTR](../benchmarks/results/2026-11-14-detailed-eintr.json.gz) reports contain case receipts and syscall traces; verify each with `python3 scripts/verify_concurrent_eio_results.py <report>`. The EINTR cases completed successfully after the interruptions were retried. These are selected synthetic syscall errors with the operating system running; the tests do not prove power-loss safety or cover all schedules. New CI repeats the EAGAIN detailed-mode campaign.

The focused unit and integration tests exercise callback and validation failures, timeout and cancellation, cancellation during synchronization and automatic checkpoint rollover, V2 log and V3 witness faults, and conservative treatment of quota rejections. Some unknown outcomes require reconciliation: recovery alone may not identify whether a particular request was applied.

## Concurrent histories and process termination

An optimized run of the mixed serial-history model on the same code checked 4,096 histories on the workspace overlay, with 3,059 successful and 5,133 conflicting transactions, plus 2,877 successful and 1,219 mismatching compare-exchanges. A second [4,096-history run on tmpfs](../benchmarks/results/2026-11-14-detailed-tmpfs-history.log.gz) recorded 3,321 successful and 4,871 conflicting transactions, and 2,780 successful and 1,316 mismatching compare-exchanges. The model rotates among the original and detailed transaction interfaces, transaction modes, V2/V3 and both writable backends, and reopens through both recovery paths. These are finite samples, not an exhaustive serializability proof.

Ten local repetitions of the process-kill test produced [160 cases](../benchmarks/results/2026-11-14-detailed-kill.json.gz) and 9,732 observed acknowledgements, all of which were checked after reopening. The workload alternates the new interface with existing transaction modes, and verifies a deliberately staged transaction is absent. All 160 cases reopened on both paths. The child process is killed but the operating system stays running, and observed acknowledgements and sampled schedules are not a physical power-loss model.

The stable and Rust 1.85 debug suites and the optimized suite each passed 946 tests, and the Python script suite passed 126 tests. Both stable and Rust 1.85 all-target Clippy checks passed. These checks are not production-readiness evidence.

## Local transaction performance

A paired local comparison used clean baseline `14a1c0f4df6bb9ec66c5ed145bd4af49f5b04a39` and candidate `9d09dc3a7bfa28068b53d84175853c1058945f18`, separate release binaries, alternating order, V3 synchronization and verified two-key transactions with fresh fixtures. The [raw paired report](../benchmarks/results/2026-11-14-detailed-transaction-pair.json.gz) has 128 runs across one and four clients, disjoint and shared keys, four transaction modes, 200 transactions per client and four rounds. The existing transaction APIs were measured; this is not a direct measurement of the new enum's overhead. Median p50s moved in both directions. The disjoint four-client read-set median moved from 338.0 to 447.4 µs in the first run series. A [longer follow-up](../benchmarks/results/2026-11-14-detailed-readset-repeat.json.gz), with 1,000 operations per client and four rounds, measured 284.0 versus 284.5 µs for disjoint keys and 324.7 versus 318.9 µs for shared keys. These noisy samples do not establish a regression or its absence.

The [SQLite comparison](../benchmarks/results/2026-11-14-detailed-transaction-sqlite.json.gz) used 80 runs at the candidate revision, SQLite 3.49.1 in WAL mode with `synchronous=FULL` and `BEGIN IMMEDIATE`, cached statements and application-level value checksums. The prototype used synchronized optimistic transactions with explicit conflict retries. Storage, locking and error behavior differ. Median p50 / p99 below are in microseconds, calculated across four runs per case; four-client runs contain 800 samples each.

| Workload | Prototype ordinary read-set p50 / p99 | Prototype checkpoint read-set p50 / p99 | SQLite p50 / p99 |
| --- | ---: | ---: | ---: |
| Disjoint, 1 client | 197.6 / 450 | 191.5 / 446 | 244.2 / 624 |
| Disjoint, 4 clients | 316.1 / 6,325 | 256.6 / 5,497 | 223.0 / 657 |
| Shared, 1 client | 184.1 / 415 | 180.1 / 401 | 198.9 / 433 |
| Shared, 4 clients | 267.2 / 6,827 | 230.5 / 11,579 | 217.6 / 2,114 |

The local comparison is mixed and SQLite's sampled four-client tail was lower. The shared Linux VM uses an AMD EPYC 9V74 with a four-vCPU quota; measurements used CPU affinity, warm caches and an overlay filesystem with unknown backing storage. The first paired run observed up to 10.61% VM steal and 18.18% I/O wait, with no cgroup CPU throttling observed during its measured intervals. Telemetry includes workload activity and is not an independent device measurement. No comprehensive competitor ranking or general latency guarantee follows.
