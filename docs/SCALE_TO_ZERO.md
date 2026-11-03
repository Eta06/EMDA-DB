# Project scale-to-zero: design goals

Scale-to-zero is **planned, not implemented**. Its purpose is to stop an inactive project's dedicated database worker while retaining its data on durable storage, then restart it when authorized work arrives. The shared router, control plane, identity services, monitoring, and storage infrastructure still consume resources. A sleeping project also retains stored data and backups; it is not zero-cost.

## Proposed lifecycle

1. **Active:** The worker accepts operations subject to project authorization and resource limits.
2. **Draining:** After a configured inactivity period, stop admitting new work to the current worker while checking for active operations, transactions, snapshots, and work that requires the worker to remain available. A concurrent request must be handled without losing it: either cancel the drain safely or trigger a subsequent wake.
3. **Sleeping:** Stop the dedicated worker only after in-flight work is safely resolved and the engine's documented synchronization/close requirements are met. Retain database files and the metadata required to find them.
4. **Waking:** Coordinate concurrent wake requests so there is only one authorized owner of the database, restart the worker, recover and verify its data, then admit requests after readiness checks. Bound queueing and wait times; return an explicit retryable error if capacity or recovery is unavailable.
5. **Failed:** If startup, synchronization, or recovery fails, report a clear error and retain diagnostic state. Do not silently start an empty database or delete data to make the wake succeed.

The state machine, timeouts, allowed cancellation, persistent state, and retry rules require a detailed implementation design. A process lock alone is not sufficient proof of distributed ownership: if multiple hosts can manage a project, fencing and storage ownership must prevent two workers from writing the same database simultaneously.

## Correctness and resource requirements

- Follow the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md). Sleeping must not weaken durability, isolation, or recovery guarantees. In-flight writes with uncertain outcomes must not be retried as though they definitely failed.
- Active transactions and snapshots must follow their documented lifetimes. A worker cannot be stopped in a way that silently invalidates an operation or borrowed view. Future change streams, subscriptions, TTL jobs, backups, and scheduled work need explicit sleep/wake semantics.
- Authenticate and authorize wake requests at the appropriate boundary, and limit wake storms, idle churn, queue sizes, concurrent starts, and noisy projects.
- Share CPU fairly and enforce configured limits. Memory cannot necessarily be reduced instantly without failure; minimums such as 100 MB and limits such as 500 MB/1 vCPU are hypotheses until tested for a defined workload. Admission control is required if demand exceeds capacity.
- Live database files require a storage design with the engine's required filesystem and durability semantics. An object-storage service may hold files or backups but is not assumed to be a PostgreSQL-style or custom-engine live filesystem.
- Make idle periods, always-on options, wait behavior, and cold-start expectations visible to project owners when those features exist.

## Acceptance evidence

Before advertising this feature, test simultaneous first requests, a request during draining, worker and control-plane crashes, restart during recovery, full storage, project isolation, active transactions and snapshots, queue limits, and recovery from backups. Measure p50/p95/p99 warm and cold latency, resource use, sleep/wake time, and failures for varied data sizes and workloads. Report the shared overhead as well as dedicated per-project resources. Process-kill tests alone do not prove power-loss safety.
