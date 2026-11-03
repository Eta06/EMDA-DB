# Database engine prototype

This is a standalone storage-engine prototype. Its name is a placeholder. It provides byte-key/byte-value operations that could eventually underpin document and SQL interfaces. Those interfaces are not implemented.

The engine source is available under Apache License 2.0. A separately operated managed cloud service and other products may be offered commercially; no hosted service is implemented in this repository yet.

## Current progress

The package version is **0.1.0, unreleased**. Statuses describe the current prototype, not production guarantees. We do not assign completion percentages to features without a defined and tested scope.

| Area | Status | Current behavior or next step |
| --- | --- | --- |
| Embedded key-value core | Prototype | Byte-key `put`, `get`, bounded bulk reads, `delete`, conditional compare-exchange, signed 64-bit increment, and ordered paginated scans; one store per process with an advisory lock. |
| Persistence and recovery | Prototype, limited | On supported Linux filesystems, V3 synchronizes log data and a metadata witness in one full inode sync; V2 retains two syncs. Both detect loss below an intact boundary; incomplete tails fail closed. Legacy stores retain their earlier limitations. Power-loss safety is not established. |
| Backup and restore | Prototype | Full-log backup verifies record checksums and a SHA-256 digest; restore uses a new path. Independent backups and disaster recovery remain operational responsibilities. |
| Concurrent access | Prototype, limited | Concurrent writes can share a sync; preparation and value reads do not hold the queue or index lock. Multi-key transactions and isolation levels are not implemented. |
| Benchmarking | Expanded local baseline | Ten engines across six byte-key workloads: 264 repeated measurements, 36 CPU-affinity comparisons, and eleven process-restart configurations. VM interference is recorded; broader feature workloads remain planned. |
| Versioning and CI | In progress | Changelog, format marker, release workflow, and automated tests exist; no release has been tagged and the release workflow has not run on a real tag. |
| SQL, document model, and queries | Planned | Require transaction semantics, schema/query design, and indexes. |
| Bounded memory and compaction | Limited prototype | `compact_to` creates a verified copy of live keys at a new path; memory and live-log growth remain unbounded, and switchover is manual. |
| Cloud, isolation, sleep/wake, and replication | Planned | There is no network server or managed service yet. |

The latest [ten-engine comparison](docs/LOCAL_COMPARISON_2026_11_03.md) includes SQLite, PostgreSQL, Redis, MongoDB, Turso, RocksDB, LMDB, DuckDB and FoundationDB. In the four-client mixed workload, the prototype delivered **10,397 operations/s versus SQLite's 4,514**, while SQLite had faster median writes: **288 µs versus 628 µs**. SQLite also won write-only p99 latency. All 300 performance runs and eleven process-restart checks passed. The report includes run ranges, adapter differences and remaining VM interference; these are small local workloads, not a universal ranking.

The earlier [V3 optimization comparison](docs/BENCHMARKS.md#v3-latency-optimization-2026-11-03) measured a **50% reduction in single-client median write latency** versus V2. The expanded comparison changes benchmark tooling and VM preparation, not engine durability or storage code.

## Featured directions

Our intended features include a shared engine for key-value, document, and SQL interfaces; deterministic fault injection; reproducible workload replay; observability; change streams and reactive queries; TTL; embedded-to-cloud portability; optional compression; and safely optimized low-copy access. [Project scale-to-zero](docs/SCALE_TO_ZERO.md) remains planned: inactive projects can stop their dedicated workers while their data remains stored. Online migrations and adaptive storage are longer-term research. See [performance and feature ideas](docs/PERFORMANCE_AND_FEATURE_IDEAS.md) for prerequisites and status.

**No performance optimization may violate the durability, isolation, or recovery guarantees in force.** See the [storage and transaction contract](docs/STORAGE_TRANSACTION_CONTRACT.md) and [project isolation design goals](docs/PROJECT_ISOLATION.md) for current limitations and the evidence required before making a guarantee.

The engine appends checksummed records to a log, synchronizes every successful mutation before returning, and rebuilds an in-memory key-to-file-offset index when reopened. Concurrent single-key writes can share one synchronization; this is not a multi-key transaction. Values are read from disk. A single log writer is coordinated with concurrent readers within the process; an advisory lock prevents multiple cooperating processes from opening the same database directory. Incomplete or invalid records cause opening to fail without truncating the log.

`Store::scan` returns keys in bytewise order with exclusive optional bounds. Each call captures the index at one instant, but separate pages can see concurrent changes. A page contains at most 1,024 entries and 32 MiB of key/value bytes. Because the byte limit can make a page short, continue using its last key until a page is empty.

`Store::compact_to` creates a verified, non-overwriting copy containing only live keys. It blocks mutations while copying, leaves the source unchanged, and does not automatically switch applications to the copy. Writes accepted by the source after it returns are not in the copy. It needs memory for multiple indexes and is not an online or bounded-memory compactor.

`Store::open_with_log_limit` optionally caps the log file length for that open instance. It counts the header, overwrites and tombstones; at the cap even deletion can be rejected. The setting must be supplied again on reopen, serializes writers, and does not cap the index, other files or total project storage. Normal `Store::open` has no limit.

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

This is an early experiment, **not a production database**. It does not yet offer transactions across keys, SQL, documents, queries, secondary indexes, automatic live compaction, bounded index memory, authentication, a production network service, project isolation, distributed replication, or an automatic sleep/wake service. Its append-only log grows indefinitely and recovery scans it entirely. Deletion adds a tombstone: earlier values remain in the log and full-log backups. Checksums detect some corruption but cannot recover damaged data. New stores can detect log truncation before an intact commit witness; rollback of both log data and witness (including V3 filesystem metadata) can still go undetected. `Store::upgrade_format` creates a verified copy of an older database at a new path. V3 requires Linux user xattrs and full `fsync`; existing V2 stores keep their two-sync path. Missing V3 metadata fails closed. Use the backup/restore API for copies, or preserve xattrs as well as the log and identity file. Backups are unencrypted full-log copies and are not authenticated. File locking is advisory and depends on filesystem behavior. Backup restore publication is supported on Linux and Apple platforms; only Linux has been tested. Durability depends on correct filesystem and storage-device behavior; tests cannot prove protection against all hardware failures. An I/O error during a mutation leaves its outcome uncertain; the instance refuses further operations until reopened.

To develop this repository, install Rust 1.85 or newer and Python 3.11 or newer, then run:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo test --release --locked
python3 -m unittest discover -s scripts/tests -v
```

Track outstanding foundation and storage work in the [follow-up checklist](docs/FOUNDATION_FOLLOW_UP.md).

Explore the [changelog](CHANGELOG.md), [versioning policy](docs/VERSIONING.md), [architecture](docs/ARCHITECTURE.md), [storage and transaction contract](docs/STORAGE_TRANSACTION_CONTRACT.md), [prioritized roadmap](docs/ROADMAP.md), [competitor feature inventory](docs/COMPETITOR_FEATURES.md), and [benchmark plan and recorded results](docs/BENCHMARKS.md). The recorded measurements reflect their stated test conditions; they are not a general performance ranking. See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution and verification expectations.

Licensed under [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution and notices.
