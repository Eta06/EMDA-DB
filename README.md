# Database engine prototype

This is a standalone storage-engine prototype. Its name is a placeholder. It provides byte-key/byte-value operations that could eventually underpin document and SQL interfaces. Those interfaces are not implemented.

The engine appends checksummed records to a log, synchronizes each mutation before returning, and rebuilds an in-memory key-to-file-offset index when reopened. Values are read from disk. A single writer is coordinated with concurrent readers within the process; an advisory lock prevents multiple cooperating processes from opening the same database directory. An incomplete final log record is discarded on recovery. Fully present records with invalid headers or checksums cause opening to fail.

```rust
use database_engine::Store;

let db = Store::open("./example-data")?;
db.put(b"product:1", b"headphones")?;
assert_eq!(db.get(b"product:1")?, Some(b"headphones".to_vec()));
db.delete(b"product:1")?;
# Ok::<(), std::io::Error>(())
```

This is an early experiment, **not a production database**. It does not yet offer transactions across keys, SQL, documents, queries, secondary indexes, compaction, bounded index memory, authentication, a network server, project isolation, distributed replication, backup tools, or an automatic sleep/wake service. Its append-only log grows indefinitely and recovery scans it entirely. Checksums detect corruption but cannot recover damaged data. File locking is advisory and depends on filesystem behavior. It currently targets Unix-like systems and has only been tested on Linux. Durability depends on correct filesystem and storage-device behavior; tests cannot prove protection against all hardware failures. An I/O error during a mutation leaves its outcome uncertain; the instance refuses further operations until reopened.

With Rust 1.85 or newer installed, run:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo test --release --locked
```

Explore the [architecture](docs/ARCHITECTURE.md), [prioritized roadmap](docs/ROADMAP.md), [competitor feature inventory](docs/COMPETITOR_FEATURES.md), and [benchmark plan](docs/BENCHMARKS.md). No performance claims have been made. See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution and verification expectations.

Licensed under [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution and notices.
