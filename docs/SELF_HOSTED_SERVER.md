# Self-hosted EMDA-DB server

The experimental `emda-server` binary serves key-value, JSON document and SQL
operations over TLS 1.3. It uses the same witnessed storage engine as the
embedded Rust API. The server is opt-in at build time. macOS and Linux are
supported by the engine.

```sh
cargo build --release --locked --features server --bin emda-server
install -d -m 700 ./emda-data
./target/release/emda-server ./server.json
```

Run the server as an unprivileged account. Its provisioning paths and their
parents must be controlled by the operator. Use a PEM certificate chain valid
for the server DNS name and a matching PEM key. Store the private key where only
the service account and trusted administrators can read it. The server prints
`READY address:port` after checking project storage paths, opening always-on
projects and binding successfully.

Generate a separate random 32-byte secret for each credential. Give the client
the 64-character lowercase hexadecimal encoding of the secret and store the
SHA-256 digest of the **raw 32 bytes** as lowercase hex in the server config.
Choose an explicit Unix expiry time. Reloading credentials requires restarting
the server with the changed configuration.

```json
{
  "bind": "127.0.0.1:7443",
  "data_root": "./emda-data",
  "tls_cert": "./server-cert.pem",
  "tls_key": "./server-key.pem",
  "require_hybrid_kex": true,
  "timeout_ms": 5000,
  "max_connections": 32,
  "max_concurrent_wakes": 2,
  "projects": [
    {
      "id": "example",
      "max_in_flight": 4,
      "idle_timeout_ms": 30000,
      "data_log_bytes": 4000000000,
      "sql_log_bytes": 1000000000,
      "max_live_keys_per_store": 16384,
      "credentials": [
        {
          "id": "deploy",
          "token_sha256": "<64 lowercase hex digits>",
          "expires_unix_seconds": 1900000000,
          "role": "admin"
        }
      ]
    }
  ]
}
```

Paths are relative to the current process directory when given as relative
paths. Project and credential IDs accept 1–48 lowercase ASCII letters, digits
and hyphens. A secret digest may be used only once in a server configuration.
Each configured project has separate `data/` and `sql/` stores, owned by the
server account and under private directories.

## Scoped API credentials

Each credential has its own identity, role and expiry. Omit `scope` for
access to all datasets in the project at the specified role. Use a scope for
applications and automation that only need particular keys or collections.

```json
{
  "id": "public-site",
  "token_sha256": "<64 lowercase hex digits>",
  "expires_unix_seconds": 1900000000,
  "role": "write",
  "scope": {
    "kv_prefixes": {"public/": "read", "ingest/": "write_only", "ingest/audit/": "read"},
    "document_collections": {"catalog": "read", "events": "write"},
    "sql_tables": {"products": "read", "measurements": "write"}
  }
}
```

An explicit scope must be an object. The maps inside it default to empty.
Prefixes match original
UTF-8 key bytes, and the longest matching prefix sets the KV permission.
Collection names match exactly. A `write` grant includes reading, and
`write_only` permits mutations and denies read requests. Mutation results
can still reveal whether a key or document existed. The key's role caps every grant, so a `read` key cannot write. Scoped keys can operate
on rows within allowed datasets. SQL scopes cover standalone SELECT, INSERT,
UPDATE and DELETE on the named table. Joins, CTEs, subqueries, schema changes,
and writes to tables with foreign-key relationships require project-wide
credentials. The engine checks these restrictions in the same transaction
as the statement. Creating collections also requires a project-wide key. Self-hosted
operators issue, rotate and revoke credentials by updating the server
configuration and restarting it. End-user account lifecycle and online
key management are planned for Harbor.

## Rust client

Enable the `remote-client` feature and use `database_engine::remote::Client`.
Provide a trusted CA bundle and the server name from its certificate. The
client verifies the certificate and requires hybrid TLS with the same
parameters as the default server. It supports KV, documents and parameterized
SQL. It preserves signed 64-bit SQL integers and performs no automatic retries.
Use the SDK error category to distinguish a connection failure before sending
a request from an uncertain response after sending began.

```rust
use database_engine::SqlValue;
use database_engine::remote::{Client, ClientOptions, Operation};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::with_ca_pem(
        ClientOptions {
            address: "127.0.0.1:7443".parse()?,
            server_name: "localhost".into(),
            project: "example".into(),
            credential: "deploy".into(),
            token: std::env::var("EMDA_API_TOKEN")?,
            timeout: Duration::from_secs(5),
        },
        "ca.pem",
    )?;
    let rows = client.request(&Operation::sql(
        "SELECT name FROM users WHERE id = ?",
        &[SqlValue::Integer(1)],
    )?)?;
    println!("{rows}");
    Ok(())
}
```

## TLS and request protocol

The default requires the `X25519MLKEM768` group and
`TLS_AES_256_GCM_SHA384`. Clients unable to negotiate both are rejected.
Setting `require_hybrid_kex` to `false` allows TLS 1.3 classical groups for
compatibility. Hybrid key agreement remains preferred in compatibility mode.
TLS early data and session resumption are disabled. Clients must verify the
server certificate and DNS name. This policy protects key establishment for
observed sessions using hybrid exchange. Certificate authentication, stored
data and backups require their own protection. See the [isolation threat
model](PROJECT_ISOLATION.md#cryptography-and-data-protection).

Each TLS connection accepts one request and emits at most one response. Encode
both as a four-byte unsigned big-endian length followed by that many UTF-8 JSON
bytes. Both bodies are bounded at 256 KiB. The request contains `version: 1`,
`project`, `credential`, `token` and an `operation` object with an `op` field.
A response is `{"ok":true,"result":...}` or `{"ok":false,"error":"..."}`.

| Operation | Fields in addition to `op` | Minimum role |
| --- | --- | --- |
| `kv_get` | `key` | `read` |
| `kv_put` | `key`, lowercase `value_hex` | `write` |
| `kv_delete` | `key` | `write` |
| `document_create` | `collection`, `schema` with a `fields` array | `admin` |
| `document_get` | `collection`, `id` | `read` |
| `document_insert`, `document_replace` | `collection`, `id`, `document` | `write` |
| `document_delete` | `collection`, `id` | `write` |
| `sql` | `statement`, optional `params` | `read` for queries and EXPLAIN, `write` for DML, `admin` for DDL |

KV keys are 1–1,023 UTF-8 bytes. Missing KV keys and document IDs in existing
collections return `null`. A found KV value is lowercase hex. Document
deletion returns a boolean. SQL parameters and row cells are JSON scalars.
Clients must use a JSON decoder that preserves signed 64-bit integers exactly.
SQL result objects contain either `columns` and `rows`, or a `status` and,
for DML, an affected `rows` count. Each SQL request runs in one bounded
transaction. DDL requiring online maintenance is outside this protocol.

Malformed messages, unsupported versions, authentication, authorization,
limits, engine errors and commit ambiguity have distinct response codes.
Project existence and invalid credentials share `unauthorized`. Responses
exclude SQL text, storage paths, token data and engine diagnostics. Before
commit expiry returns `timeout` if a response can be sent. If a write response
is lost or reports `outcome_unknown`, reconcile with a read or application
operation ID before repeating the write.

## Admission and storage limits

- The global connection cap covers unauthenticated handshakes and partial
  frames. Excess sockets close immediately. After authentication, a separate
  per-project in-flight cap returns `busy`. Requests have a deadline from
  acceptance and responses have a separate one-second write window.
- Engine deadlines are cooperative. I/O already in progress can outlive them.
  Each SQL statement is limited to 4,096 visited rows and 128 returned rows,
  among other bounds. A response exceeding the frame size returns `limit`.
- The two configured log caps apply at open and before appending, including
  concurrent write reservations. The sum bounds the encoded logs. Witnesses,
  lock files, filesystem allocation, backups and host storage are separate.
  Overwriting or deleting a value also appends a record. Reclamation and
  project-wide physical disk limits require separate management.
- Projects in one server process share a UID, address space and global
  connection pool. Provision independent processes and service accounts for
  OS-enforced access separation. See the [deployment
  boundaries](PROJECT_ISOLATION.md#deployment-boundaries).

## Project sleep and wake

Set `idle_timeout_ms` to 100–86,400,000 for a project to start with its
stores closed and close them again after inactivity. Omit it or set it
to `null` for an always-on project. The idle clock starts at the completion
of the last operation. Active operations hold both store handles until
their transaction result is known. The shared listener, credential table
and idle monitor stay available while project stores are closed.

The server authenticates and authorizes an operation before it can wake a
project. One operation opens and recovers both stores under exclusive locks,
using the same limits configured for warm operation. At most
`max_concurrent_wakes` cold opens run across the server (between 1 and 128,
default 2). A request encountering a concurrent state transition or a full
wake budget receives `busy` without executing. Warm operations can acquire
store handles concurrently within the project admission limit. Failed filesystem or
recovery opens return `unavailable` and delay the next attempt for one
second. The server logs the diagnostic for operators. The store returns the same error when an initialized project's
records log has gone missing. No request is retried by the server. The
regular connection deadline applies before opening, between the two store
opens and before statement execution. A replay already in progress can
outlive it.

Sleeping releases engine handles, locks and in-memory store state in the
shared process. Dedicated worker shutdown, fencing across hosts and measured
cold-start and memory targets remain managed-service work.

## Linux isolation with systemd

Use the [systemd template](../deploy/systemd/emda-server@.service) for one
project per service process. It invokes `--single-project`, which rejects
configurations with multiple projects. Instances receive separate dynamic users,
private state directories, private temporary and device namespaces, restricted
filesystem writes and process, memory and CPU limits.

Choose lowercase instance IDs up to 26 bytes and reserve the corresponding
`emda-<instance>` account names for systemd. Install the binary as
`/usr/local/bin/emda-server` and the unit as
`/etc/systemd/system/emda-server@.service`. Use a distinct unprivileged
port and one project entry per instance. For an instance named `example`,
provide `server.json`, `tls-cert.pem` and `tls-key.pem` as root-owned files
in `/etc/emda/projects/example/`. Configure the following values in
`server.json` in addition to the project limits above.

```json
{
  "bind": "127.0.0.1:7443",
  "data_root": "stores",
  "tls_cert": "tls-cert.pem",
  "tls_key": "tls-key.pem"
}
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now emda-server@example.service
```

With `--single-project`, relative TLS paths are resolved alongside
`server.json`. The data root is relative to the service working directory.
systemd receives readiness after successful initialization and socket bind.
SIGINT and SIGTERM close the listener and wait for admitted connections to
finish. The template allows 90 seconds for stopping before systemd can
force termination.

The template allows one CPU, uses a 384 MiB memory pressure threshold and
512 MiB hard ceiling, and allows 48 tasks. Keep the connection cap under the
task limit with headroom for the listener, idle monitor and engine threads.
Set host-specific limits in a systemd drop-in. The engine's own log quotas
still apply. For a hard physical state-directory quota, use a quota-enabled
project volume or `StateDirectoryQuota=` on systemd 258+ with filesystem
project quotas enabled. A service can start on a filesystem that cannot
enforce that directive, so verify quota enforcement on the actual deployment.
[systemd filesystem quota settings](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#StateDirectoryQuota=)

The [Linux isolation test](../scripts/tests/systemd_project_isolation.sh)
starts two temporary instances, checks both directions of file access,
checks cgroup limits and runs authenticated requests over TLS. Run it only
on a disposable systemd host with passwordless sudo. The unit stays running
with its listener even if the project's engine stores sleep.
