# Database engine prototype

This is a standalone storage-engine prototype. Its name is a placeholder. It provides byte-key/byte-value operations that could eventually underpin document and SQL interfaces. Those interfaces are not implemented.

The engine source is available under Apache License 2.0. A separately operated managed cloud service and other products may be offered commercially; no hosted service is implemented in this repository yet.

## Current progress

The package version is **0.1.0, unreleased**. Statuses describe the current prototype, not production guarantees. We do not assign completion percentages to features without a defined and tested scope.

| Area | Status | Current behavior or next step |
| --- | --- | --- |
| Embedded key-value core | Prototype | Byte-key operations, bounded bulk reads, atomic write batches, conditional updates, signed 64-bit increments, and ordered paginated scans; one store per process with an advisory lock. |
| Persistence and recovery | Prototype, limited | On supported Linux filesystems, V3 synchronizes log data and a metadata witness in one full inode sync; V2 retains two syncs. Both detect loss below an intact boundary; incomplete tails fail closed. Legacy stores retain their earlier limitations. Power-loss safety is not established. |
| Backup and restore | Prototype | Full-log backup verifies record checksums and a SHA-256 digest; restore uses a new path. Independent backups and disaster recovery remain operational responsibilities. |
| On-disk index checkpoint | Prototype | Explicit verified checkpoint and a bounded-memory historical reader; current writable recovery still loads the full index and hashes the covered prefix. |
| Concurrent access | Prototype, limited | Concurrent writes can share a sync. Bounded optimistic serializable transactions support point and range reads; any concurrent mutation conflicts. |
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

`Store::write_batch` atomically publishes an ordered set of puts and deletes in one synchronized record. It supports up to 1,024 operations and a 16 MiB encoded payload, and requires a witnessed V2/V3 store. A failed write can have an unknown outcome. Batches introduce `DB02` records: **older revisions cannot reopen logs containing them**. See [format compatibility](docs/VERSIONING.md#independent-compatibility-boundaries). A batch is a blind atomic write. `Store::transaction` additionally offers optimistic serializable read-modify-write transactions with bounded snapshots, point/range reads and rollback on callback error. Any intervening mutation causes a `WouldBlock` conflict; callbacks are never automatically retried, and external side effects are not rolled back. Transaction snapshots currently support at most 16,384 keys and 4 MiB of key bytes. Writes use the same DB02 format and compatibility rules.

For example, a transaction can read and update multiple keys together:

```rust
use database_engine::Store;

fn example(db: &Store) -> std::io::Result<()> {
    db.transaction(|tx| {
        let old = tx.get(b"account:1")?;
        if old.as_deref() == Some(b"open") {
            tx.put(b"account:1", b"closed")?;
            tx.put(b"audit:1", b"closed")?;
        }
        Ok(())
    })?;
    Ok(())
}
```

`Store::scan` returns keys in bytewise order with exclusive optional bounds. Each call captures the index at one instant, but separate pages can see concurrent changes. A page contains at most 1,024 entries and 32 MiB of key/value bytes. Because the byte limit can make a page short, continue using its last key until a page is empty.

For stable pagination, `Store::snapshot(max_keys, max_key_bytes)` copies a bounded index view and returns a read-only snapshot borrowing the open store. `Snapshot::get` and `Snapshot::scan` continue to see that view across later writes. Each snapshot supports at most 16,384 keys and 4 MiB of key bytes. An open store allows at most 16 active snapshots, 65,536 copied keys and 16 MiB of copied key bytes in total. These are not total process-memory limits. `Snapshot::cancel` clears the copied index and releases its reservation early; later reads fail. `Store::snapshot_until` accepts a monotonic deadline and rejects operations started after expiry, but it does not interrupt ongoing I/O or automatically release an expired snapshot: cancel or drop it. Snapshots do not survive reopening, and are not multi-key write transactions. Transaction deadlines and live-log reclamation remain planned.

`Store::compact_to` creates a verified, non-overwriting copy containing only live keys. It blocks mutations while copying, leaves the source unchanged, and does not automatically switch applications to the copy. Writes accepted by the source after it returns are not in the copy. It needs memory for multiple indexes and is not an online or bounded-memory compactor.

`Store::checkpoint_to` creates a separately stored index checkpoint and returns its SHA-256 digest. Keep that digest in trusted metadata **separate from the checkpoint**; `Store::open_with_checkpoint` requires it, verifies the checkpoint and the covered log prefix, and replays the remaining tail. An attacker who controls both the checkpoint and its purported trusted digest can forge it. The checkpoint contains keys, is unencrypted, and is not a backup. It still hashes the entire covered prefix and loads the index into memory, so recovery time and memory are not bounded by this feature. A small [paired reopen comparison](docs/BENCHMARKS.md#verified-checkpoint-reopen-comparison-2026-11-04) records both improvements and an outlier. Use ordinary `Store::open` to ignore an invalid or lost checkpoint and replay the full log.

`CheckpointView::open` provides a separate, read-only historical view without loading the full checkpoint index. It verifies the same trusted digest and log prefix at open, then verifies each index block when read. The view holds a shared advisory store lock for its lifetime: other views can coexist, but a cooperating writable `Store` cannot. It does not replay or validate the later log tail and may return older values; `has_log_tail` only reports whether bytes followed the checkpoint at open. Its per-view directory supports at most 131,072 blocks. Each call reads index blocks into a buffer of at most 32 KiB at a time; it may read multiple blocks, and scan results and values have their own limits. These are not aggregate process-memory quotas. Opening still hashes the entire covered log prefix. This view is not live writable recovery, and an independently trusted checkpoint digest remains essential.

`Store::open_with_log_limit` optionally caps the log file length for that open instance. It counts the header, overwrites and tombstones; at the cap even deletion can be rejected. The setting must be supplied again on reopen; an oversized log is rejected before replay. It serializes writers and does not cap the index, other files or total project storage. Normal `Store::open` has no limit.

`Store::open_with_limits` can also cap the number of live keys. Existing keys can be updated and deleting a key frees a slot. Recovery enforces the count on every historical log prefix, so an open can be rejected even when the final key count is smaller; a verified compacted copy can reduce that historical peak. The setting is not persisted and is not a process-memory limit; compaction can still use multiple indexes.

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

This is an early experiment, **not a production database**. It does not yet offer SQL, documents, general query planning, secondary indexes, automatic live compaction, bounded writable-index memory, authentication, a production network service, project isolation, distributed replication, or an automatic sleep/wake service. Its append-only log grows indefinitely and writable recovery scans or hashes it entirely. Deletion adds a tombstone: earlier values remain in the log and full-log backups. Checksums detect some corruption but cannot recover damaged data. New stores can detect log truncation before an intact commit witness; rollback of both log data and witness (including V3 filesystem metadata) can still go undetected. `Store::upgrade_format` creates a verified copy of an older database at a new path. V3 requires Linux user xattrs and full `fsync`; existing V2 stores keep their two-sync path. Missing V3 metadata fails closed. Use the backup/restore API for copies, or preserve xattrs as well as the log and identity file. Backups are unencrypted full-log copies and are not authenticated. File locking is advisory and depends on filesystem behavior. Backup restore publication is supported on Linux and Apple platforms; only Linux has been tested. Durability depends on correct filesystem and storage-device behavior; tests cannot prove protection against all hardware failures. An I/O error during a mutation leaves its outcome uncertain; the instance refuses further operations until reopened.

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
