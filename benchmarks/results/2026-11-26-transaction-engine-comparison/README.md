# Serializable two-key transactions: EMDA, SQLite and PostgreSQL

One host, with equivalent correctness, isolation and durability settings
across engines, at revision `1f9490c3` plus the harness changes in this
commit. It extends the earlier [SQLite comparison](../../../docs/BENCHMARKS.md#two-key-transaction-comparison)
with PostgreSQL and with full device flushes on every engine.

## Workload and semantics

Each transaction reads two counters, checks that they are equal, and
increments both. Each client runs 100 transactions. *Disjoint* gives every
client its own pair; *shared* gives all clients one pair. Every engine
checks the final counters: each pair must equal 100, or 100 times the
clients for the shared pair.

| Engine | Isolation | Conflict handling | Durable acknowledgement |
| --- | --- | --- | --- |
| EMDA `Store`, read set | Serializable; read-set validation | Retry on conflict | Log `sync_data` and witness before return (`F_FULLFSYNC` on macOS) |
| EMDA `Store`, conservative | Serializable; any intervening write conflicts | Retry on conflict | As above |
| EMDA `CheckpointStore`, read set | Serializable; read-set validation | Retry on conflict | As above |
| SQLite 3.49.1 (rusqlite) | `BEGIN IMMEDIATE` (serialized writers), WAL | Busy timeout, no conflicts | `synchronous=FULL`, `fullfsync=ON`, `checkpoint_fullfsync=ON` |
| PostgreSQL 17.12 (`pgbench`, prepared statements) | `SERIALIZABLE` | `--max-tries` retries serialization failures | `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, `wal_sync_method=fsync_writethrough` |

On macOS, plain `fsync` and `fdatasync` do not flush the drive cache. The
SQLite harness now enables `fullfsync`, and PostgreSQL uses
`fsync_writethrough`, to match Rust's `sync_data`, which issues
`F_FULLFSYNC`. `pg_test_fsync` measured about 302 `fsync_writethrough`
operations per second on this volume, against about 59,000 for
`fdatasync`. A comparison with mismatched flushes would differ by orders of
magnitude.

PostgreSQL ran as a disposable user-level cluster in a private directory,
with no TCP listener, a private Unix socket and default memory settings. It
was removed afterwards.

## Host

macOS 27.0, Apple M6, 16 GiB, internal SSD with APFS, Rust 1.100.0, release
builds. There were three trials per configuration, with the backend order
rotated per trial. Ranges are across trials.

## Results

| Clients | Workload | Engine | Transactions/s | p50 | p99 | Conflicts or retries |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 1 | Disjoint | EMDA read set | 183 to 232 | 5.0 to 6.0 ms | 7.0 to 8.0 ms | 0 |
| 1 | Disjoint | EMDA conservative | 174 to 239 | 4.0 to 6.0 ms | 7.0 to 8.0 ms | 0 |
| 1 | Disjoint | EMDA checkpoint read set | 178 to 231 | 4.4 to 6.0 ms | 7.0 to 8.7 ms | 0 |
| 1 | Disjoint | SQLite | 295 to 457 | 2.4 to 3.8 ms | 4.2 to 5.1 ms | 0 |
| 1 | Disjoint | PostgreSQL | 263 to 416 | 2.8 to 4.0 ms | 4.6 to 8.3 ms | 0 |
| 8 | Disjoint | EMDA read set | 642 to 1,114 | 6.8 to 12.0 ms | 14.1 to 35.4 ms | 0 |
| 8 | Disjoint | EMDA conservative | 169 to 202 | 6.0 ms | 422 to 814 ms | 3,632 to 4,282 |
| 8 | Disjoint | EMDA checkpoint read set | 180 to 259 | 29.8 to 46.0 ms | 49.9 to 55.0 ms | 0 |
| 8 | Disjoint | SQLite | 261 to 487 | 1.2 to 3.1 ms | 5.8 to 14.9 ms | 0 |
| 8 | Disjoint | PostgreSQL | 1,254 to 2,766 | 1.7 to 6.0 ms | 7.7 to 10.1 ms | 0 |
| 8 | Shared | EMDA read set | 168 to 265 | 4.2 to 6.0 ms | 342 to 580 ms | 2,873 to 4,372 |
| 8 | Shared | EMDA conservative | 165 to 315 | 3.5 to 6.0 ms | 291 to 530 ms | 3,760 to 4,307 |
| 8 | Shared | EMDA checkpoint read set | 162 to 277 | 3.9 to 6.0 ms | 350 to 1,049 ms | 955 to 1,341 |
| 8 | Shared | SQLite | 248 to 473 | 1.3 to 3.9 ms | 5.0 to 13.7 ms | 0 |
| 8 | Shared | PostgreSQL | 262 to 301 | 3.8 to 3.9 ms | 329 to 804 ms | 3,077 to 3,831 |

The single-client shared rows are similar to the disjoint ones and are in
the raw data.

### Recovery and resource use

Eight disjoint clients ran the same workload three times
(`recovery-runs.json`). The embedded engines reopened and verified every
counter in 0.9 to 16.7 ms for EMDA and 1.4 to 1.7 ms for SQLite. Their peak
resident memory, workload included, was 3.1 to 3.3 MB for EMDA and 4.4 MB
for SQLite.

After an immediate (crash-like) shutdown, PostgreSQL's log reported
automatic recovery. Restart and verification took 374 to 386 ms. Its server
processes held 45 to 49 MB resident after the workload.

This dataset is tiny, so these figures say little about recovery at scale.
The [one-million-key restart comparison against SQLite](../../../docs/SQLITE_RESTART_RESOURCES_2026_11_06.md)
covers larger recovery.

## Findings

- **Disjoint transactions.** With pipelined commit, EMDA's ordinary-store
  read-set transactions scale with disjoint clients, but PostgreSQL scaled
  further. SQLite serializes writers, so it does not scale with clients
  but never conflicts.
- **Single client.** EMDA was slower than both alternatives with one
  client. A V2 commit on macOS synchronizes the log and then the external
  witness, two full flushes, while SQLite and PostgreSQL flush their WAL
  once. On Linux with xattrs, V3 makes one inode `fsync` per commit.
- **Checkpoint store.** `CheckpointStore` transactions did not scale with
  disjoint clients: about 180 to 260 per second with 8 clients, and a p50
  near 30 to 46 ms. The checkpoint-backed write path has no group commit, so
  every transaction synchronizes alone.
- **Contention.** Under a single shared pair, every optimistic engine
  (EMDA's modes and PostgreSQL's `SERIALIZABLE`) retried heavily, with
  hundreds of milliseconds at p99. SQLite's serialized writers avoided
  conflicts entirely.

There is no universal winner. Results depend on durability settings,
isolation strategy and contention.

## Limits

This is one machine and one filesystem, with a synthetic two-key workload
and 100 transactions per client. PostgreSQL was run through `pgbench` over
a local socket, the embedded engines in-process. Client and protocol costs
therefore differ and are part of what is measured. No other alternatives
were rerun here; the [ten-engine local baseline](../../../docs/LOCAL_COMPARISON_2026_11_03.md)
remains the broader reference.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every transaction and recovery run, both driver
scripts, the `pgbench` scripts, the PostgreSQL overrides and reported
settings, `pg_test_fsync` output, the SQLite version, the harness source,
the binary hashes, the host description and `SHA256SUMS`.
