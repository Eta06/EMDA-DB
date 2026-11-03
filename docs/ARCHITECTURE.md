# Storage engine architecture

This document describes the current prototype, not a promised final design. The product and package names are placeholders.

## Boundaries

- `store`: public key/value API, exclusive process ownership, synchronization, and outcome handling.
- `record`: versioned binary framing, length limits, encoding, and checksum verification.
- `log`: append and sync, positional reads, recovery, and the in-memory key-to-offset index.
- `tests`: behavior across reopen, concurrent clients, corruption, truncated tails, and process termination.

The public API currently supports `put`, `get`, and `delete` on nonempty byte keys. Values may be empty. A reader-writer lock permits concurrent readers and serializes mutations. Each successful mutation appends one record and calls `sync_data` before publishing the index change and returning. On an I/O error during a write, the result is uncertain and that store instance refuses further operations until it is reopened.

## Log format

Records use a four-byte `DB01` magic/version, one-byte operation, little-endian 32-bit key and value lengths, a CRC32 of the header fields, the key and value bytes, and a CRC32 of the header and payload. Recovery validates full records and reconstructs the index in order. An incomplete trailing record is truncated; a complete invalid header or checksum fails opening. An apparently incomplete tail cannot distinguish an interrupted unacknowledged write from certain underlying storage damage; do not use the prototype for production data. CRC32 detects accidental corruption; it does not prevent tampering.

The index stores keys, offsets, lengths, and value checksums in memory; it is not bounded. Values are read by offset and verified. The log grows without compaction. Recovery scans all records. The directory lock is advisory and assumes cooperating processes and a suitable filesystem. Successful `sync_data` is not a substitute for backups or a guarantee against faulty hardware or filesystem behavior. The directory's creation and all ancestor entries are not proven durable across power loss. Tests that kill a process do not simulate a power failure.

## Next boundaries to design

Separate and test transaction semantics, an on-disk index and compaction, cache management, concurrent write coordination, checksummed snapshots/backups, and durable directory creation. A worker lifecycle for project sleep/wake and a network API would be separate components from this embedded engine. SQL, documents, indexes, and transactions require explicit semantics; they are not implemented by exposing the current byte API.

Consider Rust SIMD or assembly only after profiling shows an end-to-end benefit. Optimized paths must retain a portable implementation, detect CPU support safely, and pass the same correctness and corruption tests.
