# EMDA-DB

EMDA-DB is an embedded Rust database for **macOS and Linux**. A durable
key-value engine powers both the byte-key API and indexed JSON document
queries. It runs inside your application.

**0.2.0 · experimental development version.** Build from source to try it.
The Rust API and disk formats can change before a stable release.

## What works today

- **Key-value storage.** Point reads and writes, ordered scans, atomic batches,
  compare-and-exchange, and optimistic transactions. The ordinary store can
  group independent writing transactions into one durable sync.
- **JSON documents.** Collections with schema validation, compound, unique and
  partial indexes, filters, projection, sorting, basic aggregates, and indexed
  one-to-one and one-to-many lookups. See the [document guide](docs/DOCUMENT_MODEL.md).
- **Recovery and maintenance.** Checksummed logs, commit witnesses, verified
  backup and restore, checkpoint-backed stores, and retained storage with
  quota-aware rotation. The [storage contract](docs/STORAGE_TRANSACTION_CONTRACT.md)
  describes durable acknowledgement, uncertain outcomes and filesystem requirements.

## What's still in development

- **SQL** is in development. The embedded key-value and document interfaces are
  the APIs available on the baseline documented here.
- **Server and operations.** The roadmap covers a network API, authentication,
  project isolation and project-wide quotas, safe automatic sleep and wake
  (scale-to-zero), replication and failover.
- **Clients and platforms.** Windows, language SDKs, Harbor and Lens are
  planned. macOS and Linux are the current test targets.

See the [roadmap](docs/ROADMAP.md) and [milestone progress](docs/ROADMAP_PROGRESS.md)
for scope and outstanding acceptance tests.

## Try it

Clone the repository and run the atomic batch tests with Rust 1.85 or newer.

```sh
git clone https://github.com/Eta06/EMDA-DB.git
cd EMDA-DB
cargo test --locked --test atomic_batch
```

To embed it, clone EMDA-DB next to your Rust application's directory and add
this to your application's `Cargo.toml`.

```toml
[dependencies]
database-engine = { path = "../EMDA-DB" }
```

```rust
use database_engine::Store;

fn main() -> std::io::Result<()> {
    let db = Store::open("./example-data")?;
    db.put(b"greeting", b"hello")?;
    assert_eq!(db.get(b"greeting")?.as_deref(), Some(b"hello".as_slice()));
    Ok(())
}
```

## Measured performance

These are workload-specific results from the revisions recorded in each
report. In the macOS comparison below, eight clients ran durable,
serializable two-key transactions on disjoint keys.

| Engine | Transactions/s | p99 latency |
| :-- | --: | --: |
| EMDA ordinary store, read-set validation | 642–1,114 | 14.1–35.4 ms |
| SQLite | 261–487 | 5.8–14.9 ms |
| PostgreSQL 17 | 1,254–2,766 | 7.7–10.1 ms |

EMDA led SQLite on throughput in this case. PostgreSQL led on throughput, and
both engines had lower median latency with one client. Shared-key workloads showed
more retries and much higher tail latency in EMDA and PostgreSQL.
[Transaction benchmark and raw results](benchmarks/results/2026-11-26-transaction-engine-comparison/README.md)

- **Clean reopen, 262,144 initial keys.** On macOS, EMDA's ordinary store
  reached the first read in a median 80.88 ms at 33.70 MiB peak process RSS.
  Retained storage took 50.27 ms at 2.92 MiB. SQLite took 1.20 ms at 3.17 MiB.
  The report includes Linux, full-scan and write results.
  [Recovery comparison](benchmarks/results/2026-11-22-recovery-sqlite-matrix/README.md)
- **Retained storage, 1,048,576 keys.** A warm-cache macOS reference workload
  used 9.7–11.5 MB peak worker RSS and a fresh process reached the first read
  after cleanup in 103–132 ms. Some write stalls exceeded one second.
  [Reference workload and raw results](benchmarks/results/2026-11-26-reference-deployment-r1/README.md)

## Durability evidence

- **Physical power cut.** One Raspberry Pi 5 run on Samsung NVMe with ext4
  recovered all 497 acknowledged atomic batches (994 values) after power
  returned. [Power-cut test](benchmarks/results/2026-11-26-physical-power-cut/README.md)
- **Off-host restore.** An ordinary-store backup created on macOS restored
  and verified all 99 expected records on a separate Linux host.
  [Restore test](benchmarks/results/2026-11-26-offhost-store-backup-restore/README.md)

## Project records

- [Benchmark catalog and reproduction instructions](docs/BENCHMARKS.md)
- [Architecture](docs/ARCHITECTURE.md) and [failure coverage](docs/FOUNDATION_FAILURE_MATRIX.md)
- [Changelog](CHANGELOG.md) and [release policy](docs/VERSIONING.md)
- [Previous README snapshot, November 22, 2026](README_HISTORY.md) and
  [maintainer handoff](docs/MAINTAINER_HANDOFF.md)
- [Contributing](CONTRIBUTING.md)

Licensed under [Apache License 2.0](LICENSE). See [NOTICE](NOTICE).
