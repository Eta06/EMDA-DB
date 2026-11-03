# Project isolation: design goals

Strong isolation is a core requirement. **It is not implemented today.** The prototype is a local embedded store; its advisory file lock only coordinates cooperating processes. It does not authenticate users, enforce project permissions, isolate tenants, or provide a security sandbox.

No shared software/hardware system can promise absolute isolation against every possible failure. Define a concrete threat model, layers, and measured guarantees instead of calling process boundaries or quotas "perfect isolation."

## Trust boundaries

Treat project owners, app users, incoming requests, record contents, database files, network traffic, and any future extensions as untrusted at the appropriate boundary. A public project ID identifies a project; it is not an authorization secret. Platform operators and the underlying operating system, hypervisor, storage, and hardware remain privileged parts of the threat model.

Differentiate the developer/owner of a project from its end users. Verify identity, project scope, token audience and expiry, and permission for each operation. Default to deny. Do not infer that someone may access every project merely because they have a platform account, or link accounts by email alone. Client apps must never receive master storage or infrastructure credentials.

## Layers to design and test

1. **API and identity:** Authenticated project routing; project-scoped credentials, users, roles, and data-access rules; bounded requests and rate limits. Unauthorized requests must not obtain another project's data or sensitive metadata.
2. **Engine and storage:** Resolve every database, cache key, index, snapshot, log, backup, and event stream to its owning project. Ensure filesystem and object-store access controls enforce that ownership; a path prefix alone is not authorization. Reject malformed paths, symlinks where unsafe, corrupted files, and traversal attempts.
3. **Runtime:** Explore a separate least-privilege worker per active project, with constrained files, network, processes, and service credentials. Linux process isolation, namespaces, and cgroups help limit access and resource use but share the host kernel. They are not equivalent to separate machines or protection against every kernel or hardware failure.
4. **Resources:** Apply per-project bounds and admission controls for CPU, memory, connections, requests, disk, background work, and storage. Test fairness and noisy neighbors. A project can be throttled, queued, or rejected under documented conditions; absolute CPU or memory guarantees require actual capacity reservation.
5. **Failure and lifecycle:** A project worker failure should not take down other workers or corrupt their data. Sleep/wake must preserve authorization, durable data, and ownership; see [SCALE_TO_ZERO.md](SCALE_TO_ZERO.md). A single host or shared control-plane failure can still affect multiple projects.
6. **Backups and operations:** Scope backup, restore, retention, export, logs, metrics, and diagnostic access to authorized operators and projects. Consider encryption and key management as explicit features; do not claim confidentiality or recoverability until implemented and tested.

Any future support for developer-supplied executable code needs a separate sandbox and build/run threat model. Declarative records or queries must not gain arbitrary filesystem, network, or host-code access.

## Isolation tiers to evaluate

The baseline proposal is project-scoped workers with least-privilege OS and storage controls on shared infrastructure. Stronger optional tiers could use per-project virtual machines or dedicated hosts where warranted. All tiers still require authentication, safe database code, secure operations, and tested recovery; stronger isolation increases resource use and cost. No tier or pricing is implemented or promised yet.

## Evidence before public claims

- Reject forged or expired credentials, incorrect project scopes, unauthorized operations, and cross-project identifiers.
- Test file, cache, snapshot, index, backup, and event-stream access across projects, including traversal and malformed input.
- Subject one project to CPU, memory, storage, connection, and request exhaustion while measuring other projects; test process termination and recovery.
- Test wake storms, simultaneous ownership attempts, stale workers, and control-plane failures. Verify any fencing and storage-ownership mechanism under the intended failure model.
- Exercise backup/restore, log access, role changes and revocation. Re-run isolation tests for new interfaces and deployment modes.

Publish the threat model, supported isolation tier, assumptions, known limitations, and test results. Never claim stronger isolation than the implementation and evidence support.
