# Database engine prototype

This is a standalone storage-engine prototype. Its name is a placeholder. It provides byte-key/byte-value operations that could eventually underpin document and SQL interfaces. Those interfaces are not implemented.

The engine source is available under Apache License 2.0. A separately operated managed cloud service and other products may be offered commercially; no hosted service is implemented in this repository yet.

## Current progress

The package version is **0.1.0, unreleased**. Statuses describe the current prototype, not production guarantees. We do not assign completion percentages to features without a defined and tested scope.

| Area | Status | Current behavior or next step |
| --- | --- | --- |
| Embedded key-value core | Prototype | Byte-key `put`, `get`, and `delete`; one store per process with an advisory lock. |
| Persistence and recovery | Prototype, limited | Checksummed log and `sync_data` on writes; incomplete-tail recovery. Power-loss safety and backup/restore are not established. |
| Concurrent access | Prototype, limited | Concurrent readers and one serialized writer. Multi-key transactions and isolation levels are not implemented. |
| Versioning and CI | In progress | Changelog, format marker, release workflow, and automated tests exist; no release has been tagged and the release workflow has not run on a real tag. |
| SQL, document model, and queries | Planned | Require transaction semantics, schema/query design, and indexes. |
| Bounded memory, compaction, and backups | Planned | The index is currently unbounded; the log grows indefinitely; backup tooling is absent. |
| Cloud, isolation, sleep/wake, and replication | Planned | There is no network server or managed service yet. |

## Featured directions

Our intended features include a shared engine for key-value, document, and SQL interfaces; deterministic fault injection; reproducible workload replay; observability; change streams and reactive queries; TTL; embedded-to-cloud portability; optional compression; and safely optimized low-copy access. [Project scale-to-zero](docs/SCALE_TO_ZERO.md) remains planned: inactive projects can stop their dedicated workers while their data remains stored. Online migrations and adaptive storage are longer-term research. See [performance and feature ideas](docs/PERFORMANCE_AND_FEATURE_IDEAS.md) for prerequisites and status.

**No performance optimization may violate the durability, isolation, or recovery guarantees in force.** See the [storage and transaction contract](docs/STORAGE_TRANSACTION_CONTRACT.md) and [project isolation design goals](docs/PROJECT_ISOLATION.md) for current limitations and the evidence required before making a guarantee.

The engine appends checksummed records to a log, synchronizes each mutation before returning, and rebuilds an in-memory key-to-file-offset index when reopened. Values are read from disk. A single writer is coordinated with concurrent readers within the process; an advisory lock prevents multiple cooperating processes from opening the same database directory. An incomplete final log record is discarded on recovery. Fully present records with invalid headers or checksums cause opening to fail.

To try the experimental Rust library from another project:

```toml
[dependencies]
database-engine = { git = "https://github.com/Eta06/EMDA-DB.git" }
```

For reproducible use, pin a reviewed Git revision with the `rev` field. This crate is not currently published to crates.io, and its API and on-disk format are not yet stable.

```rust
use database_engine::Store;

fn main() -> std::io::Result<()> {
    let db = Store::open("./example-data")?;
    db.put(b"product:1", b"headphones")?;
    assert_eq!(db.get(b"product:1")?, Some(b"headphones".to_vec()));
    db.delete(b"product:1")?;
    Ok(())
}
```

This is an early experiment, **not a production database**. It does not yet offer transactions across keys, SQL, documents, queries, secondary indexes, compaction, bounded index memory, authentication, a network server, project isolation, distributed replication, backup tools, or an automatic sleep/wake service. Its append-only log grows indefinitely and recovery scans it entirely. Checksums detect corruption but cannot recover damaged data. File locking is advisory and depends on filesystem behavior. It currently targets Unix-like systems and has only been tested on Linux. Durability depends on correct filesystem and storage-device behavior; tests cannot prove protection against all hardware failures. An I/O error during a mutation leaves its outcome uncertain; the instance refuses further operations until reopened.

To develop this repository, install Rust 1.85 or newer and Python 3.11 or newer, then run:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo test --release --locked
python3 -m unittest discover -s scripts/tests -v
```

Explore the [changelog](CHANGELOG.md), [versioning policy](docs/VERSIONING.md), [architecture](docs/ARCHITECTURE.md), [storage and transaction contract](docs/STORAGE_TRANSACTION_CONTRACT.md), [prioritized roadmap](docs/ROADMAP.md), [competitor feature inventory](docs/COMPETITOR_FEATURES.md), and [benchmark plan](docs/BENCHMARKS.md). No performance claims have been made. See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution and verification expectations.

Licensed under [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution and notices.
