# EMDA-DB

[![CI](https://github.com/Eta06/EMDA-DB/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Eta06/EMDA-DB/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Version 0.2.0, development](https://img.shields.io/badge/version-0.2.0%20development-orange.svg)](docs/VERSIONING.md)
[![MSRV 1.85](https://img.shields.io/badge/rust-1.85%2B-informational.svg)](Cargo.toml)

EMDA-DB is an embedded Rust database for **macOS and Linux**. One durable,
transactional storage engine serves three interfaces inside your
application: ordered byte keys, indexed JSON documents, and a documented
SQL subset.

**Experimental development version.** There is no release yet. Build from
source to try it; the Rust API and disk formats can change before a stable
release.

## Quick start

With Rust 1.85 or newer:

```sh
git clone https://github.com/Eta06/EMDA-DB.git
cd EMDA-DB
cargo test --locked --test atomic_batch
```

To embed it, clone EMDA-DB next to your application and add it to your
`Cargo.toml`:

```toml
[dependencies]
database-engine = { path = "../EMDA-DB" }
```

```rust
use database_engine::{DocumentDatabase, SqlDatabase, SqlOutcome, SqlValue, Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::open("./example-data")?;
    store.put(b"greeting", b"hello")?;

    let db = SqlDatabase::new(DocumentDatabase::new(&store));
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)", &[])?;
    db.execute("INSERT INTO users VALUES (?, ?)", &[SqlValue::Integer(1), SqlValue::Text("Ada".into())])?;
    if let SqlOutcome::Rows { rows, .. } = db.execute("SELECT name FROM users WHERE id = 1", &[])? {
        println!("{rows:?}");
    }
    Ok(())
}
```

## What works today

- **Key-value storage.** Point reads and writes, ordered scans, atomic
  batches, compare-and-exchange, and serializable optimistic transactions.
  Every acknowledged write is synced; the ordinary store can group
  independent transactions into one sync.
- **JSON documents.** Collections with schema validation; compound, unique
  and partial indexes; filters, projection, sorting, aggregates and indexed
  lookups. See the [document guide](docs/DOCUMENT_MODEL.md).
- **Embedded SQL**, through the Rust API. The [SQL subset](docs/SQL_SUBSET.md)
  lists every form and bound:
  - Tables with `PRIMARY KEY`, `NOT NULL`, `UNIQUE` and literal `DEFAULT`s;
    `DROP TABLE`;
    `ALTER TABLE ... ADD COLUMN`.
  - Online `CREATE [UNIQUE] INDEX`: 1–4 columns, up to 16 indexes per table.
  - `INNER` and `LEFT JOIN` of up to eight tables, using key, index or hash
    access.
  - Aggregates, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` and `OFFSET`.
  - Single-column foreign keys with `NO ACTION`/`RESTRICT`, up to 64 per
    table, always enforced.
  - Uncorrelated `[NOT] IN`, `[NOT] EXISTS` and scalar subqueries, nested
    up to four deep, materializing at most 32,768 values by default.
  - Serializable multi-statement transactions of up to 1,024 staged keys
    and 16 MiB.
  - `EXPLAIN SELECT`.
  Every statement is bounded: exceeding a limit fails instead of
  truncating. On the ordinary `Store` a transaction snapshot holds at most
  16,384 keys, which limits SQL table size there; `CheckpointStore` does
  not have that limit.
- **Recovery and maintenance.** Checksummed logs, commit witnesses, verified
  backup and restore, checkpoint-backed stores, and retained storage with
  quota-aware rotation. The [storage contract](docs/STORAGE_TRANSACTION_CONTRACT.md)
  defines acknowledgement, uncertain outcomes and filesystem requirements.

## Being built now

- **SQL.** `RIGHT`/`FULL` joins, aggregates over joins, correlated
  subqueries, composite and cascading foreign keys, `CHECK`, more
  `ALTER TABLE` actions, and faster scans, aggregates and indexed reads.
- **Service and operations.** A network API with authentication, project
  isolation and quotas, safe scale-to-zero, replication and failover.
- **Platforms and clients.** Windows and language SDKs are planned.

## Milestones

Progress on `main` at `12e6de8f`, from [roadmap progress](docs/ROADMAP_PROGRESS.md):

| Step | Area | Progress |
| --- | --- | ---: |
| 1 | Correctness and foundations | 99% |
| 2 | Durable storage and core operations | 99% |
| 3 | Document model and queries | 99% |
| 4 | SQL | 69% |
| 5 | Secure service and project isolation | 10% |
| 6 | Performance and scalability | 60% |
| 7 | Replication and operations | 20% |
| 8 | Broader features and tooling | 10% |

Steps 1–3 count checked items in their acceptance checklists: 415 of 419.
Three open items wait on the network service of step 5, and one is the
standing obligation to keep the storage contract current. Steps 4–8 score each roadmap
milestone as delivered, partly tested or not started. The percentages
track evidence against the [roadmap](docs/ROADMAP.md). They are not
effort estimates or a statement of release readiness.

## How it compares

EMDA-DB is an embedded library, like SQLite, and unlike the PostgreSQL,
MySQL, MariaDB, MongoDB and Redis servers. It is not wire- or
dialect-compatible with any of them, and its SQL is a bounded subset. The
[feature comparison](docs/FEATURE_COMPARISON.md) compares operations and
deployment models, with sources.

Representative same-session results at `c7ac663d` on macOS (Apple M6,
APFS), with PostgreSQL 19.0, the newest stable release on the measurement
date. EMDA and SQLite run in process; PostgreSQL is a local server reached
over a Unix socket, whose round trip alone took 9–14 µs. All three sync
every acknowledged write to the drive and run serializable transactions.
Values are p50 over two runs:

| Operation | EMDA | SQLite | PostgreSQL 19.0 |
| :-- | --: | --: | --: |
| `COUNT(*)` over 10,000 rows | 34–123 µs | 2.8–4.7 µs | 362–368 µs |
| Ten point UPDATEs in one transaction | 1.17–3.97 ms | 1.43–3.95 ms | 3.99 ms |
| Primary-key SELECT | 22.5–25.5 µs | 12.0–13.6 µs | 11–15 µs |
| One-row UPDATE | 3.97–3.99 ms | 3.05 ms | 3.96–3.97 ms |
| Ten-row index range | 55–59 µs | 13.0–13.5 µs | 19–20 µs |
| Predicate scan of 10,000 rows | 2.62–3.08 ms | 0.52–0.80 ms | 0.49–0.69 ms |
| `SUM(...) WHERE` over 10,000 rows | 1.67–2.19 ms | 0.25–0.46 ms | 0.34 ms |
| Three-table join with `LIMIT` | 45–252 µs | 8.0–14.7 µs | 36–39 µs |

EMDA counts rows faster than PostgreSQL, and in one of the two runs had
the fastest ten-update transaction. It is slower than the fastest engine on
the other operations shown. Its durable single-row writes are within about
1.3× of SQLite. PostgreSQL 17.12 results from the same day remain as a
historical baseline.

[Performance results](docs/PERFORMANCE.md) has the full SQL table, the
historical byte-key matrix across ten engines, document operations,
transactions and recovery, with links to raw results. MySQL and MariaDB
have not been measured; the latest MongoDB and Redis results are the
historical document and byte-key baselines.

## Durability evidence

- **Physical power cut.** One Raspberry Pi 5 run on NVMe with ext4
  recovered all 497 acknowledged atomic batches.
  [Power-cut test](benchmarks/results/2026-11-26-physical-power-cut/README.md)
- **Off-host restore.** A backup made on macOS restored and verified on a
  separate Linux host.
  [Restore test](benchmarks/results/2026-11-26-offhost-store-backup-restore/README.md)

These are single runs on one configuration each.

## Project records

- [Architecture](docs/ARCHITECTURE.md) and [failure coverage](docs/FOUNDATION_FAILURE_MATRIX.md)
- [Benchmark catalog](docs/BENCHMARKS.md) and [comparison method](docs/COMPARISON_METHOD.md)
- [Changelog](CHANGELOG.md) and [release policy](docs/VERSIONING.md)
- [Previous README snapshot](README_HISTORY.md) and [maintainer handoff](docs/MAINTAINER_HANDOFF.md)
- [Contributing](CONTRIBUTING.md)

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE).
