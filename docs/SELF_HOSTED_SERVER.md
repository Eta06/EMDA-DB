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
`READY address:port` after opening every project and binding successfully.

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
  "projects": [
    {
      "id": "example",
      "max_in_flight": 4,
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
