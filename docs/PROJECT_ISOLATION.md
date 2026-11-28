# Project isolation: design goals

The [self-hosted server](SELF_HOSTED_SERVER.md) enforces authenticated project
routing, role checks, separate stores, and connection and log admission
limits. The server's processes, service accounts, file provisioning, backups
and host remain distinct trust boundaries.


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

## Credential lifecycle

Self-hosted credentials belong to one project and carry their own expiry,
role and optional dataset scope. Operators can publish a complete replacement
policy without restarting and rotate or revoke keys for new requests. The Linux
service mounts the operator-owned live policy directory read-only.
For Harbor, keep human membership and service identities separate from their
keys. Bind each grant to the tenant, project, role and dataset scope. Define
delegation, issuance, rotation and revocation with an audit record, and
verify that a project owner cannot grant access in another tenant. Single-table SQL grants support scoped reads and mutations within
these boundaries. Managed users, service-account key management, SQL policies
spanning multiple tables and row-level policies remain future work.

The host-local key authority uses separate management tokens and project
grants for listing and managing scoped data keys. The data service consumes
the resulting durable policy independently. Hosted identity provisioning and
the remote gateway remain to integrate.
[Delegated key API](SELF_HOSTED_SERVER.md#delegated-key-management-api)

## Deployment boundaries

- **Shared self-hosted process.** Credentials, operations and store handles
  resolve within one configured project. Private directories and startup
  checks reject symlinks, hard links, non-regular store entries and ownership
  mismatches at project storage paths. An in-process compromise or a
  privileged local actor can affect all projects in that process.
- **OS-enforced separation.** Run one server instance per project under a
  separate unprivileged service account. The [Linux systemd deployment](SELF_HOSTED_SERVER.md#linux-isolation-with-systemd) assigns distinct dynamic users and cgroup limits. Expose only each instance's own data, TLS key and
  credentials mounted. On Linux, constrain the process and its descendants
  with cgroup v2 CPU, memory, I/O and process limits, and enforce storage
  quotas on the project volume for logs, metadata and temporary files. Use
  filesystem and network namespaces, a read-only system filesystem, and no
  ambient privileges. For mutually untrusted or highly sensitive workloads,
  a VM can reduce reliance on a shared kernel.
- **Managed lifecycle.** Run a separate control plane for authentication,
  routing, quotas and ownership. A dormant project should consume storage
  without a running database worker. The self-hosted server can already
  release idle embedded stores within its shared process. A managed wake needs a single
  authenticated owner with fencing, per-project and global concurrency caps,
  and queue bounds. Shutdown must stop admitting writes, drain active
  transactions or preserve their uncertain outcome, then release ownership.
  Host provisioning, fencing and lifecycle recovery still need
  implementation and failure tests.

See [Linux cgroup v2](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html),
[systemd process controls](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html)
and [systemd resource controls](https://www.freedesktop.org/software/systemd/man/latest/systemd.resource-control.html).

## Cryptography and data protection

- **In transit.** The server requires `X25519MLKEM768` by default. It combines
  ephemeral X25519 with NIST ML-KEM-768 to protect confidentiality against
  collection today and decryption with a future quantum computer. The
  default TLS cipher uses AES-256-GCM. A deliberate compatibility setting
  allows classical key agreement. TLS credentials still use the certificate
  and signature algorithms configured by the operator. [IETF RFC 10024](https://datatracker.ietf.org/doc/html/rfc10024)
  and [NIST FIPS 203](https://csrc.nist.gov/pubs/fips/203/final).
- **Identity.** Evaluate ML-DSA for certificate signatures once the issuing
  chain, TLS library and clients support the same profile. Plan dual support
  during migration and test downgrade, rotation and revocation behavior.
  [NIST FIPS 204](https://csrc.nist.gov/pubs/fips/204/final).
- **At rest and in backups.** The server currently uses the existing log
  and witness formats. At-rest confidentiality requires a separately
  managed encrypted volume or future authenticated encryption of log,
  checkpoint and backup data. Plan per-project 256-bit keys, versioned
  envelope encryption and independent key management. Test format upgrades,
  key rotation, encrypted restore, and fail-closed behavior with lost or
  wrong keys before making an engine encryption claim.

## Evidence before public claims

- Reject forged or expired credentials, incorrect project scopes, unauthorized operations, and cross-project identifiers.
- Test file, cache, snapshot, index, backup, and event-stream access across projects, including traversal and malformed input.
- Subject one project to CPU, memory, storage, connection, and request exhaustion while measuring other projects; test process termination and recovery.
- Test wake storms, simultaneous ownership attempts, stale workers, and control-plane failures. Verify any fencing and storage-ownership mechanism under the intended failure model.
- Exercise backup/restore, log access, role changes and revocation. Re-run isolation tests for new interfaces and deployment modes.

Publish the threat model, supported isolation tier, assumptions, known limitations, and test results. Never claim stronger isolation than the implementation and evidence support.
