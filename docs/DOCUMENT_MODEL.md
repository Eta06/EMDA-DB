# Experimental document model

This is an embedded prototype over the same witnessed storage and transactions as the byte-key API. It is not MongoDB compatible or production-ready. It runs on `Store` or `CheckpointStore` through `DocumentDatabase`. It has no network API, access control, project isolation, distributed transactions, automatic retries, or SQL interface. Read the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md) before using it.

## A small example

```rust
use database_engine::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue, Store};
use serde_json::json;

fn example(store: &Store) -> std::io::Result<()> {
    let db = DocumentDatabase::new(store);
    db.create_collection("people", CollectionSchema::default())?;
    db.create_index("people", IndexDefinition {
        name: "email".into(),
        fields: vec![vec!["email".into()]],
        unique: true,
        partial: None,
    })?;
    db.transaction(|tx| {
        tx.insert("people", "ada", &json!({"email": "ada@example.test"}))?;
        tx.put_raw(b"audit:ada", b"created")?;
        Ok(())
    })?;
    let page = db.query_index("people", "email", &IndexQuery {
        equal: vec![IndexValue::String("ada@example.test".into())],
        ..IndexQuery::default()
    })?;
    assert_eq!(page.documents[0].id, "ada");
    Ok(())
}
```

The caller supplies `serde_json` to construct `Value`s. Collection names and IDs are case-sensitive UTF-8 strings; IDs are supplied separately from the object and are not MongoDB `_id` values. Names and field path components contain 1–128 UTF-8 bytes; IDs contain 1–256 bytes. Paths are vectors of object member names; dots have no special meaning and array elements are not traversed. Documents must be JSON objects. Serialized documents are limited to 1 MiB, depth 64 and 131,072 nodes. These are logical limits, not total process-memory quotas.

## Collections, writes and transactions

Create collections explicitly with a `CollectionSchema`; an empty schema accepts arbitrary object fields within limits. Each of at most 64 distinct field rules declares a path, type, required flag and nullable flag. Missing and explicit null differ; nullable does not make a field optional. An intermediate non-object makes a nested path missing. Integer rules accept signed 64-bit integers. Unlisted fields are allowed. Replacing a schema validates all existing documents atomically, subject to a 256-document/32 MiB scan limit.

Insert fails when an ID exists. Replace requires an existing ID unless `upsert` is true. Delete reports whether the document existed. `update_if` requires all explicit field conditions, where `None` means missing and `Some(Null)` means null. Ordered set, unset and checked signed 64-bit increment operators are applied in memory first; intermediate non-objects, integer overflow and schema or index violations fail. Set creates missing intermediate objects; unset of a missing field is a no-op; increment of a missing field starts at zero. Upsert starts from an empty object. Bulk supports 1–128 ordered operations within one transaction. Collection deletion is atomic and limited to 768 keys and 32 MiB of scanned key/value bytes.

A document transaction can mix collection operations with `get_raw`, `put_raw` and `delete_raw` on unreserved byte keys. Its document, index, metadata and raw mutations use the same bounded atomic DB02 record, commit witness and synchronization as the underlying store; there is no cross-database or SQL transaction. A failed mutation aborts the entire document transaction even if its callback catches the error. Read errors do not automatically poison a transaction. Callback failure and panic discard staged changes, but external side effects are not rolled back. An I/O error during commit can have an unknown outcome and disables the underlying store until recovery. Conflicts return `WouldBlock` and callbacks are not retried automatically.

The default transaction conflicts on any intervening mutation. `transaction_with_read_set` validates point observations and complete scanned ranges, permitting unrelated writes subject to the underlying dependency limits. `transaction_until` exposes conservative, read-set and read-set-FIFO modes with operation-boundary deadlines; `DeadlineExceeded` means staged writes were not submitted, while an I/O timeout may have an unknown outcome. Neither callbacks nor I/O are forcibly interrupted. Underlying snapshot, staged-record, suffix, log and other limits also apply. The ordinary store's transaction snapshot is limited to 16,384 keys and 4 MiB of key bytes; the checkpoint path has different tradeoffs documented in the storage contract.

## Indexes and queries

Collections support up to 16 indexes, each on 1–4 distinct object paths. A partial index can require presence/absence or exact typed equality of a path. A missing indexed path omits that document; explicit null is indexed. Indexable values are null, booleans, signed 64-bit integers and UTF-8 strings of at most 256 bytes. Arrays, objects, floating-point and larger unsigned numbers are not indexable; a matching write or index build fails before commit. A compound tuple whose paths are all present is stored as one entry. A unique index enforces uniqueness across complete tuples, including null, only for documents satisfying its partial filter. String lengths and the underlying 1,024-byte key limit can reject long composite keys.

Document and index changes are maintained in one transaction. Index creation scans and validates at most 256 existing documents and 32 MiB of encoded input; unique conflicts abort the build. Index drop is atomic and limited to 768 index keys and 32 MiB of scanned key/value bytes. These maintenance operations block or conflict according to the chosen underlying transaction mode and do not provide an online large-collection migration.

`query_index` explicitly chooses an index. It supports equality on a tuple prefix and inclusive/exclusive lower and upper bounds on the next field. Ordering is null, false/true, signed integer, then UTF-8 string bytes, followed by ID bytes. Partial indexes only return matching indexed documents. Pages have at most 256 documents and 16 MiB of encoded document/ID bytes. A nonempty page returns a cursor; continue until a page is empty. Pages inside one transaction share its view; independently invoked pages can observe intervening writes and are not persistent snapshots. Each page reports the selected index, supplied bound shape, entries read and rows returned. Each returned entry and unique mapping is checked against its document; this does not detect every missing index entry caused by raw corruption. `verify_collection` compares all expected entries of declared indexes against actual entries for at most 256 documents, 8,192 index keys and 32 MiB of logical key/value bytes. It does not inspect keys for undeclared index names. `rebuild_index` can atomically remove and reconstruct a named index from valid documents when its drop, build and transaction limits are met; invalid documents or unsatisfied uniqueness prevent repair.

`query` performs a **complete bounded collection scan**, with existence/equality/typed-scalar comparisons, boolean composition, optional scalar sorting, projection, offset and limit. It fails rather than returning a silently truncated answer if its explicit scan limit (at most 1,024 documents) or 32 MiB scan budget is exceeded. The result includes actual scanned, matched, sorted and returned counts; this is a collection-scan plan, not an automatic cost-based optimizer. Sort accepts the same scalar types as indexes; missing sorts before null in ascending order, and IDs break ties in ascending order. Projection paths cannot overlap. Filtering and pagination occur before projection.

`aggregate` performs a bounded complete scan of at most 256 documents, with count, optional grouping by one scalar path and optional checked signed-integer sum. Missing and null sum values are skipped; non-integers and overflow fail. `lookup` follows a string document ID into another collection within the same transaction snapshot. Missing/null references and missing target IDs yield no related document. It limits related document bytes to 16 MiB and rejects a projected source query. These are narrow primitives, not MongoDB aggregation pipelines or general joins.

## Namespace, integrity and compatibility

The versioned byte prefix `00 65 6d 64 61 2d 64 6f 63 75 6d 65 6e 74 01` is reserved for document metadata, documents and indexes. The document transaction's raw methods reject this prefix. Direct raw `Store`/`CheckpointStore` access and older binaries do not enforce the reservation; applications must not use them to modify document-owned keys. The prefix is **not** a security or tenant-isolation boundary. Corrupt or unknown metadata and invalid documents fail when read, and queries check the entries they return; checksums and these checks do not repair corruption, authenticate data, or guarantee discovery of a missing index entry.

Collection metadata includes a version marker; unknown versions and fields fail closed. The document model uses the current experimental DB02 batch records, so older engines unable to read DB02 cannot reopen the resulting log. Earlier engines that can read the log still do not understand or protect document invariants. There is no stable format, migration or downgrade guarantee for this unreleased prototype. Backups contain plaintext document and index bytes in the full log, including prior values and deletions; they are not encrypted or authenticated. Existing independent-backup and restore requirements remain in force.
