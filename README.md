# EMDA-DB

EMDA-DB is an embedded database written in Rust for **macOS and Linux**. It has a
byte-key store and a JSON document interface built on the same storage engine.

**Experimental · 0.1.0 unreleased.** Use disposable data for
evaluation. The Rust API and disk formats can change before a stable release.

## What works today

- **Key-value storage.** Put, get, delete, ordered scans, atomic batches, and
  optimistic transactions.
- **Documents.** JSON collections with schema validation, secondary indexes,
  filters, projection, sorting, and basic aggregation.
- **Recovery.** Checksummed logs with a commit witness, verified backup and
  restore, and experimental checkpoint-backed and retained stores.

The [storage contract](docs/STORAGE_TRANSACTION_CONTRACT.md) explains what an
acknowledged write means, when an outcome is uncertain, and which filesystem
assumptions apply. Platform and format details are in the
[architecture](docs/ARCHITECTURE.md) and [version policy](docs/VERSIONING.md).

## Try it

Clone the repository and run the atomic batch tests with Rust 1.85 or newer.

```sh
git clone https://github.com/Eta06/EMDA-DB.git
cd EMDA-DB
cargo test --locked --test atomic_batch
```

To embed the prototype, add `database-engine` as a Git dependency and pin a
reviewed commit. For example, add this to your `Cargo.toml`.

```toml
[dependencies]
database-engine = { git = "https://github.com/Eta06/EMDA-DB.git", rev = "7fa05928613718acd741386f8a00cd79630723fd" }
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

See the [document guide](docs/DOCUMENT_MODEL.md) for JSON examples and index
semantics.

## Where we’re going

The first priority is a dependable macOS and Linux foundation. The long-term
goal is a database service that can stop idle project workers while retaining
durable data, strongly isolate projects, and keep latency, CPU, and RAM costs
low. We compare equivalent workloads with other engines and publish the
settings, results, and trade-offs.

SQL, a hosted service, tenant isolation, and automatic sleep and wake are on the
[roadmap](docs/ROADMAP.md). The current engine runs inside your application.

## Project records

- [Benchmarks and reproduction instructions](docs/BENCHMARKS.md), including the
  [macOS and Linux recovery comparison with SQLite](benchmarks/results/2026-11-22-recovery-sqlite-matrix/README.md)
- [Changelog](CHANGELOG.md) and [release policy](docs/VERSIONING.md)
- [Previous README snapshot, November 22, 2026](README_HISTORY.md) and
  [maintainer handoff](docs/MAINTAINER_HANDOFF.md)
- [Contributing](CONTRIBUTING.md)

Licensed under [Apache License 2.0](LICENSE). See [NOTICE](NOTICE).
