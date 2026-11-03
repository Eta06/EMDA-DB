# Storage engine architecture

This document describes the current prototype, not a promised final design. The product and package names are placeholders. See the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md) for current behavior, proposed guarantees, and the rule that performance optimizations must not violate durability, isolation, or recovery guarantees.

## Boundaries

- `store` and `store/commit`: public key/value API, exclusive process ownership, bounded group commit, synchronization, and outcome handling.
- `record`: versioned binary framing, length limits, encoding, and checksum verification.
- `log`, `log_io`, and `recovery`: append and sync, positional reads, bounded buffered replay, the in-memory key-to-offset index, and the testable I/O boundary.
- `backup`: checksummed full-log backup and validated, non-overwriting restore.
- `directory`: synchronization of created store paths and their ancestors.
- `tests`: behavior across reopen, deterministic I/O failures, concurrent clients, corruption, backups, and process termination.

The public API supports `put`, `get`, and `delete` on nonempty byte keys and backup/restore. Values may be empty. A commit coordinator reserves bounded capacity before preparing a write outside the queue lock. It queues prepared records, appends each batch in queue order, synchronizes the log, and only then publishes index changes and returns success. Multi-record batches use vectored writes when the I/O implementation supports them, with a sequential fallback. A batch contains at most 64 records; admission limits reserved and queued encoded bytes and record count. A reader locks the index to select an immutable log location, then releases the lock before reading and verifying the value. A separate barrier makes backups wait for in-flight writes and prevents new writes during the copy. Completed writers are woken individually; queued writers periodically recheck leadership if a handoff is delayed. This does not provide a bound on scheduling or request latency. Grouping independent mutations does not provide multi-key transactions. An I/O error or leader panic leaves unacknowledged outcomes uncertain and makes that store instance refuse further operations until it is reopened.

## Log format

Records use a four-byte `DB01` magic/version, one-byte operation, little-endian 32-bit key and value lengths, a CRC32 of the header fields, the key and value bytes, and a CRC32 of the header and payload. Recovery reads the log sequentially through a buffer of at most 64 KiB, validates full records, and reconstructs the index in order. Large payloads can be read directly; the payload and unbounded index require additional memory. An incomplete trailing record, invalid header, or checksum error fails opening without truncating the log. An apparently incomplete tail cannot distinguish an interrupted unacknowledged write from certain underlying storage damage; no automatic repair is attempted. CRC32 detects accidental corruption; it does not prevent tampering.

The index stores keys, offsets, lengths, and value checksums in memory; it is not bounded. Values are read by offset and verified. The log grows without compaction. Recovery scans all records. The directory lock is advisory and assumes cooperating processes and a suitable filesystem. Store initialization synchronizes the created directory path and its ancestors; this does not prove power-loss behavior on all filesystems. Successful `sync_data` is not a guarantee against faulty hardware or filesystem behavior. Tests that kill a process do not simulate a power failure.

Backups copy the full log while writes are blocked, include a SHA-256 digest and length, verify record checksums, and publish without overwriting an existing path. The full log retains old and deleted values; a delete is a tombstone, not secure erasure. Restore validates a backup, rechecks the copied data and record checksums in a staging directory, and publishes the new directory without replacing an existing path. Neither digest nor CRC authenticates data, and backups are not encrypted. Publication followed by a directory-sync error has an uncertain outcome. Only Linux has been tested; platform and filesystem behavior remain assumptions. Backups should be stored independently and restored in drills before relying on them.

## Next boundaries to design

Separate and test transaction semantics, an on-disk index and compaction, cache management, broader fault injection, and power-loss assumptions. A worker lifecycle for project sleep/wake and a network API would be separate components from this embedded engine. SQL, documents, indexes, and transactions require explicit semantics; they are not implemented by exposing the current byte API.

Consider Rust SIMD or assembly only after profiling shows an end-to-end benefit. Optimized paths must retain a portable implementation, detect CPU support safely, and pass the same correctness and corruption tests.
