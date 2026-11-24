# Experimental document model

This is an embedded prototype over the same witnessed storage and transactions as the byte-key API. It is not MongoDB compatible or production-ready. It runs on `Store`, `CheckpointStore`, `ManagedRetainedStore`, or `AuthorizedRetainedStore` through `DocumentDatabase`. It has no network API, access control, project isolation, distributed transactions, automatic retries, or SQL interface. Read the [storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md) before using it.

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

`DocumentDatabase::with_managed_retained_store` opens a document view over
an exclusive mutable borrow of a managed retained catalog. All document
transaction modes serialize on this backend. Concurrent or reentrant
attempts return `WouldBlock`. Reads merge the verified base, the committed
suffix, and the transaction's own staged changes. Documents, indexes and
unreserved raw keys commit in one bounded atomic record. Imported and
restored catalogs use the same document keys and metadata.

Deadlines and cancellation apply at operation boundaries and before
append, including after automatic reclamation. A failed transaction can
leave completed generation maintenance visible. The staged document and
raw-key changes share the ordinary atomic-batch count and byte limits.
Release the document view before using catalog operations such as backup
or explicit compaction.

`DocumentDatabase::with_authorized_retained_store` uses the same serialized
transaction model on an open authority-checked catalog, including a catalog
opened at its original path after cutover. It checks authority at admission
and again before returning a successful read-only result. Writes check
cancellation and deadlines before submitting the authority intent, then use
the authority-fenced durable append protocol. Once intent submission begins,
I/O is allowed to finish across a cancellation or deadline. Use
`transaction_detailed` to distinguish a rejected transaction from one whose
local append might have committed. Reopen after an authority error to resolve
the outcome against the trusted authority.
Opening the authorized catalog with automatic reclamation enables
synchronous rotation for document writes as suffix capacity is approached.
Cancellation or expiry during maintenance can leave the new generation in
place while the document batch remains unsubmitted.

Create collections explicitly with a `CollectionSchema`; an empty schema accepts arbitrary object fields within limits. Each of at most 64 distinct field rules declares a path, type, required flag and nullable flag. Missing and explicit null differ; nullable does not make a field optional. An intermediate non-object makes a nested path missing. Integer rules accept signed 64-bit integers. Unlisted fields are allowed. Replacing a schema validates all existing documents atomically, subject to a 256-document/32 MiB scan limit.

`FieldType::IntegerRange { min, max }` restricts signed JSON integers to
inclusive bounds. A JSON floating-point value such as `1.0` fails the
integer rule. `FieldType::StringBytes { min, max }` counts the bytes of
the decoded UTF-8 string, including multibyte characters. Both bounds
are inclusive and the configured maximum is at most 1 MiB. The ordinary
encoded document size limit still applies. These rules preserve the
path's required and nullable settings. Invalid bounds are rejected
before schema installation or migration.

Insert fails when an ID exists. Replace requires an existing ID unless `upsert` is true. Delete reports whether the document existed. `update_if` requires all explicit field conditions, where `None` means missing and `Some(Null)` means null. Ordered set, unset, checked signed 64-bit increment and array operators are applied in memory first; intermediate non-objects, integer overflow and schema or index violations fail. Set creates missing intermediate objects; unset of a missing field is a no-op; increment of a missing field starts at zero. Array push appends a value, and add-unique appends only when no existing element has the same `serde_json::Value` equality. Both create a missing array and its intermediate objects. Array remove deletes all equal elements; pop removes the first or last element; truncate keeps the first `max_len` elements; `ArrayKeepLast` keeps the last `max_len` elements in order; reverse reverses the existing order. Remove, pop, truncate, keep-last and reverse leave missing fields unchanged; all array operators reject a present non-array, including null. Values are validated, all changes remain within one transaction, and schema/index checks still apply. Array-valued index fields and multikey indexes are not supported. Across one update, condition expectations, set/set-if-missing values, array-value operands and min/max scalar operands share a 4 MiB encoded-JSON input budget. Repeated inputs are counted each time, even if a condition fails or an operation would make no change; paths and fixed-size arithmetic factors are outside this counter. Array entries examined or shifted by add-unique, remove and first-pop, together with elements discarded by truncate, the original array length when keep-last removes a prefix, and elements visited by reverse, are limited to 1,000,000. These limits do not bound caller-owned inputs, complex equality, other operations or total memory/time. Upsert starts from an empty object. Bulk supports 1–128 ordered operations within one transaction. One-shot `drop_collection` is atomic and limited to 768 keys and 32 MiB of scanned key/value bytes.

Additional ordered scalar operators use the same atomic update and schema/index checks. `SetIfMissing` writes only when the path is absent, treating null as present. `Multiply` uses checked signed 64-bit arithmetic and starts a missing field at zero; null, floating-point, larger unsigned integers and other non-integers fail. `Min` and `Max` set missing fields or compare indexable scalars in the index order: null, booleans, signed integers, then UTF-8 strings of at most 256 bytes. A present unsupported scalar or container fails, even if it would otherwise remain unchanged. `Rename` moves a present value and replaces the destination; a missing source creates no destination. Source and destination must be distinct and neither may be an ancestor of the other. Both paths traverse objects only, and a non-object traversal fails. Input paths and values are validated before conditions are evaluated. A later failure aborts the complete transaction, including raw-key and index changes. These operators do not imply MongoDB compatibility.


A document transaction can mix collection operations with `get_raw`, `put_raw` and `delete_raw` on unreserved byte keys. Its document, index, metadata and raw mutations use the same bounded atomic DB02 record, commit witness and synchronization as the underlying store; there is no cross-database or SQL transaction. A failed mutation aborts the entire document transaction even if its callback catches the error. Read errors do not automatically poison a transaction. Callback failure and panic discard staged changes, but external side effects are not rolled back. An I/O error during commit can have an unknown outcome and disables the underlying store until recovery. Conflicts return `WouldBlock` and callbacks are not retried automatically. Use `is_transaction_conflict(&error)` on a directly returned error to identify a pre-commit conflict; the error kind alone can also represent admission, callback or uncertain I/O failures.

The default transaction conflicts on any intervening mutation. `transaction_with_read_set` validates point observations and complete scanned ranges, permitting unrelated writes subject to the underlying dependency limits. `transaction_until` exposes conservative, read-set and read-set-FIFO modes with operation-boundary deadlines; `DeadlineExceeded` means staged writes were not submitted, while an I/O timeout may have an unknown outcome. `transaction_controlled` additionally accepts a cloneable cancellation token and optional deadline with the same modes. An engine-reported cancellation means no staged document, index or raw-key changes were submitted; arbitrary callback or commit I/O errors are reported separately and can have unknown outcomes. FIFO admission and the underlying pre-commit writer or transaction state-lock waits observe cancellation and expiry. Callbacks, other internal lock waits and I/O are not forcibly interrupted, completion time is not bounded, and callback side effects are not rolled back. Underlying snapshot, staged-record, suffix, log and other limits also apply. The ordinary store's transaction snapshot is limited to 16,384 keys and 4 MiB of key bytes; the checkpoint path has different tradeoffs documented in the storage contract.

Standalone `DocumentDatabase::get` on the ordinary store captures just the metadata and document log locations from one index state instead of copying the full index. It verifies each value and validates the metadata and document in the same order as before, then checks store health and the conservative generation under the writer barrier. Any intervening successful mutation can still return `WouldBlock`; an uncertain write still fails the read. It shares the store's active snapshot admission count, but does not reserve logical snapshot-key or key-byte budgets. It can therefore read an existing document in a store too large for a transaction snapshot. This is one read, not a multi-operation snapshot: explicit transactions retain their isolation and admission limits, and the checkpoint-backed `get` retains its existing behavior.

## Resumable collection deletion

For collection deletion beyond the atomic 768-key limit, `start_collection_drop` marks a collection for irreversible deletion. `advance_collection_drop` removes at most 128 document or index keys per atomic transaction; a separate empty step removes the metadata when every namespace is empty. Callers schedule and retry steps, including after reopening. `collection_drop_in_progress` reports the marker. Normal document operations on the marked collection return `WouldBlock`; existing transactions may conflict when they try to commit. Finish any index maintenance job, including cleanup after cancellation, and finish or cancel any schema migration first. A failed step commits nothing when the failure is known to precede submission; an I/O failure after submission retains the underlying unknown-outcome semantics. Do not recreate the collection until completion. Other collections remain available.

The initial marker uses collection metadata version 3. When a checkpoint-backed step returns a full page, it upgrades the marker to version 4 and atomically records a cursor alongside the deleted keys, avoiding repeated scans of earlier tombstones. The cursor continues to advance even on later short pages; a short first page can still be caused by the scan byte limit, so callers must continue until completion. Before removing metadata, the engine checks each namespace from its beginning and fails with `InvalidData` if any keys remain behind the cursor. It deletes the cursor and collection metadata together. Ordinary-store steps use their existing scan path; they can also resume a job containing a cursor. Existing version 3 markers without a cursor still work. Older document readers reject version 4. Cursor updates add log bytes, which can reach existing caps sooner. The raw key-value API can bypass the marker and must not be used to modify document-reserved keys. Each step still uses the backend's bounded transaction and scan machinery: in particular, the ordinary store can reject the operation when its full-index transaction snapshot exceeds its limits. A checkpoint-backed store can need a rollover when its suffix overlay fills. These steps are not a total-memory, time, recovery or project quota.

## Resumable schema migration

`start_schema_migration` records a pending schema, and `advance_schema_migration` validates at most 128 documents per transaction until an empty page atomically publishes it. Progress and the proposed schema persist across reopen and can be inspected with `schema_migration`; `cancel_schema_migration` removes the job without changing the active schema. Each stored document must fit the 1 MiB size limit before its value is read. A failed validation does not advance the cursor. Correct or delete the failing document and retry. One-shot `set_schema` is unavailable while a migration is pending.

From the start of a migration, every inserted or replaced document must satisfy **both** the active and pending schemas. This ensures that updates and insertions before a saved cursor cannot bypass validation. Deletions remain allowed. Incompatible old and new requirements may therefore prevent an in-place update until the migration is cancelled or conflicting documents are removed. As with index jobs, caller-scheduled steps can conflict and require retry, and existing backend transaction, memory and response limits apply. Incremental index jobs can run during a schema migration.

## Indexes and queries

Collections support up to 16 indexes, each on 1–4 distinct object paths. A partial index can require presence/absence or exact typed equality of a path. A missing indexed path omits that document; explicit null is indexed. Indexable values are null, booleans, signed 64-bit integers and UTF-8 strings of at most 256 bytes. Arrays, objects, floating-point and larger unsigned numbers are not indexable; a matching write or index build fails before commit. A compound tuple whose paths are all present is stored as one entry. A unique index enforces uniqueness across complete tuples, including null, only for documents satisfying its partial filter. String lengths and the underlying 1,024-byte key limit can reject long composite keys.

For larger collections, `start_index_build` creates a persistent build job, and `advance_index_build` scans at most 128 documents per transaction until an empty step atomically publishes the index. Writers maintain the pending index after the job starts, including inserts before the saved cursor. The index cannot be queried before publication. For unique builds, duplicate values may already exist in documents that have not yet been scanned; uniqueness is only guaranteed after completion. Each stored document must fit the 1 MiB size limit before its value is read. A duplicate or unsupported existing value stops the current step without committing its progress, so the conflicting document can be corrected or deleted and the step retried. A build can be cancelled, which starts incremental cleanup.

Document mutations and build steps require empty secondary index markers.
Unique owners must contain 1 to 256 bytes of valid UTF-8. Mutations, builds
and indexed queries check that length before materializing a unique owner.
Malformed values return `InvalidData`. A valid conflicting owner still
returns `AlreadyExists` when enforcing uniqueness.

`start_index_drop` makes an active index unavailable immediately and removes its uniqueness constraint; `advance_index_drop` removes at most 128 index keys per transaction and persists progress through the entry and unique-key namespaces. The cursor is the last key removed by a committed step. A later step fails with `InvalidData` before removing anything when a key in that namespace is still present at or before the cursor, and the unique phase likewise fails while any entry key remains. That failure is not submitted; earlier steps remain durable, and the job stays reserved until a consistent cursor can finish. This detects a skipped or stale cursor. It does not repair metadata or prove that every raw write into the reserved namespace will be noticed before the next advance. A name remains reserved until cleanup completes. `index_jobs` lists resumable builds and drops after reopen. Each step uses a normal bounded transaction; a confirmed pre-commit conflict can be retried using `is_transaction_conflict`. An unclassified `WouldBlock` may be an uncertain I/O outcome and requires recovery and inspection rather than an assumed abort. Steps run only when called—there is no autonomous maintenance service. The ordinary backend still requires a bounded full-index transaction snapshot; the checkpoint-backed backend has different limits. Neither provides total-memory quotas or a latency guarantee.

### Guarded progress for index builds and schema migrations

Generate a fresh `MaintenanceKey` for each job and persist its secret bytes
in independently protected storage before calling
`start_index_build_guarded` or `start_schema_migration_guarded`. After a
restart, restore the same key using `MaintenanceKey::from_bytes` and use the
matching guarded advance method for every step, including final publication.
The database stores a tag for the job kind, collection, definition and cursor.
The step verifies it before scanning. Invalid or missing tags fail with
`InvalidData`, including when the cursor names a valid ID. The ordinary
advance methods reject jobs carrying a tag with `PermissionDenied`.

Each successful step commits its updated tag and cursor atomically with
its work. A valid protected cursor remains usable after its document is
deleted through the document API. Normal writes continue enforcing
pending definitions. Cancellation still discards the job through the
existing API. If the key is lost, cancel the job and start over with a
new key. Existing unguarded jobs can continue with their current methods.
An older build that rejects unknown metadata fields cannot access a
collection while a pending guarded job adds its tag.

To migrate an unfinished unguarded job, persist a fresh key and call
`restart_index_build_guarded` or `restart_schema_migration_guarded` with
the expected definition from the job request. The transaction requires
the stored definition to match exactly and rejects a job that already has
a tag. It atomically adds a tag and clears the cursor, so subsequent
guarded steps check the collection from its beginning. Restart retains
pending index entries, which the rescan checks against their documents.
Normal writes still maintain the pending index and enforce the pending
schema. A staged old step can lose the transaction conflict and roll back.

Keep writers within the document API and protect the key from the store
and its backups. The basic guarded methods authenticate the job and cursor.
An independently managed job can also reject an older authentic cursor by
requiring its latest independently retained `MaintenanceProgress` token:

1. Save the fresh key and initial token before starting or restarting the
   guarded job. Compute it from the trusted request with
   `MaintenanceProgress::initial_index_build` or
   `MaintenanceProgress::initial_schema_migration`.
2. Have one controller per job. Use `transaction_detailed` and call the
   matching `advance_index_build_guarded_matching` or
   `advance_schema_migration_guarded_matching` on its transaction. The
   compare runs inside the transaction before the step reads documents or
   changes progress. A different authentic token returns `InvalidData`.
3. Save an independent durable intent before submitting the step. It can
   include the expected and provisional next token from inside the callback.
   The next token becomes current only after acknowledged commit. Record
   completion independently when the step returns no next token.
4. Clear the intent only after recording the acknowledged result or a
   confirmed abort. If interruption or an unknown commit leaves an intent,
   stop advancement until independent evidence resolves the result.
   Do not repopulate the trusted token from the database's current metadata.
   When the outcome cannot be established, recover or rebuild under a new
   job key from a separately trusted state.

The caller owns durability and ordering of that independent record and
must use the matching methods consistently. Database status calls read the
stored state. The comparison protects job progress relative to a trusted
token. It does not authenticate other reserved keys or detect a rollback
of both the database and the independent record.

Document and index changes are maintained in one transaction. One-shot `create_index` scans and validates at most 256 existing documents and 32 MiB of encoded input; unique conflicts abort the build. One-shot `drop_index` is atomic and limited to 768 index keys and 32 MiB of scanned key/value bytes. These one-shot maintenance operations block or conflict according to the chosen underlying transaction mode. Incremental builds and drops are described above; schema changes can use the separate resumable validation path described above.

`query_index` explicitly chooses an index. It supports equality on a tuple prefix and inclusive/exclusive lower and upper bounds on the next field. Ordering is null, false/true, signed integer, then UTF-8 string bytes, followed by ID bytes. Partial indexes only return matching indexed documents. Pages have at most 256 documents and 16 MiB of encoded document/ID bytes. A nonempty page returns a cursor; continue until a page is empty. Pages inside one transaction share its view; independently invoked pages can observe intervening writes and are not persistent snapshots. Each page reports the selected index, supplied bound shape, entries read and rows returned. Each encountered document is checked against its 1 MiB size bound before page-byte truncation, and each returned entry and unique mapping is checked against its document. This does not detect every missing index entry caused by raw corruption. `verify_collection` compares all expected entries of declared indexes against actual entries for at most 256 documents, 8,192 index keys and 32 MiB of logical key/value bytes. It checks the expected key and value length before reading each index value, then compares the value bytes. It does not inspect keys for undeclared index names. `rebuild_index` can atomically remove and reconstruct a named index from valid documents when its drop, build and transaction limits are met; invalid documents or unsatisfied uniqueness prevent repair.

Individual indexed query values still obey the scalar limits. When a compound boundary encodes to more than 1,024 bytes, the engine uses an equivalent boundary over the representable storage keys. An equality tuple too large to fit identifies an empty range. This applies to explicit index queries and indexes selected by the planner. Writes still require every stored key to fit the storage limit.

Save a named-index seek position with `IndexCursor::to_bytes` and decode it
with `IndexCursor::from_bytes` after reopening. The resume query must use the
same collection name and index definition, and its bounds must contain the
saved position. The anchor row may have been deleted. Each page reads the
current transaction view, so intervening updates can skip or repeat rows.
The cursor contains the encoded tuple and document ID in plaintext, with
a checksum for accidental damage. Keep it under caller control. It does
not authenticate a position, identify a database instance, or retain a snapshot.

For a consistent view across separate calls, borrow an existing
`Store::snapshot` with `DocumentSnapshot::new`, or borrow a
`CheckpointStore::snapshot` with `DocumentSnapshot::with_checkpoint_snapshot`.
Point reads, collection scans, planned queries and named-index pages read
the captured documents and index definitions. Keep using the same snapshot
for every page, including after checkpoint rollover. The underlying storage
snapshot's admission limits, deadline, cancellation and reaping rules
apply. Each query also enforces its usual document limits. These two
storage snapshots live in the current process.

`DocumentSnapshot::with_read_limits` optionally shares one logical
`DocumentReadLimits` budget across point reads, queries and pages on the
same wrapper, including metadata and indexes. It applies to both live
snapshot types and historical checkpoints. A request that exceeds the
budget returns an error without a partial result and exhausts the wrapper
for subsequent reads. Explicitly setting new limits starts a new budget
over the same captured state. This limits logical read units and bytes.

To resume the same historical view after restart, use `checkpoint_to`
on the ordinary or checkpoint store. Preserve the checkpoint and its
original covered log, plus the checkpoint digest and any cursor in trusted
caller storage. Close the writer, reopen `CheckpointView` with that digest,
and borrow it with `DocumentSnapshot::with_checkpoint_view`. Later log
records are outside the view. An open historical view holds a shared
store lock and excludes cooperating writers. Keep the original covered
bytes available through any compaction or reclamation. The checkpoint
contains locations into that log, so keep both files. The caller chooses
the historical checkpoint when resuming a cursor.

`query` performs a **complete bounded collection scan**, with existence/equality/typed-scalar comparisons, boolean composition, optional scalar sorting, projection, offset and limit. It fails rather than returning a silently truncated answer if its explicit scan limit (at most 1,024 documents) or 32 MiB scan budget is exceeded. The result includes actual scanned, matched, sorted and returned counts; this is a collection-scan plan, not an automatic cost-based optimizer. Sort accepts the same scalar types as indexes; missing sorts before null in ascending order, and IDs break ties in ascending order. Projection paths cannot overlap. Filtering and ranking use the full document. The window retains only the projected fields when a projection is requested. The scan validates every bounded document while filtering. It applies row, byte and stored-document size limits before materializing each payload. An over-limit row can therefore be rejected before checking its payload checksum. Already admitted documents retain their validation order. Retained scans may read the rest of a touched block to authenticate it. For unsorted queries it retains only the requested matching window. For sorted queries it checks every matching sort value and keeps at most the best `offset + limit` ranked entries. Scan or stored-document errors retain precedence over an earlier filtering or sort error. These bounds are not total-memory limits.


Value predicates `In` and `NotIn` compare the whole value at an object path against a list using exact `serde_json::Value` equality, including nested objects and arrays; integer and floating-point values retain that library's distinctions. `In` requires a present path and never matches an empty list. `NotIn` negates `In`, so it matches a missing path and every document with an empty list. Explicit null is a value, distinct from a missing path. Array values are compared as whole values, with no implicit element traversal or MongoDB compatibility claim.

`StartsWith { path, prefix }` matches strings at an object path by exact
UTF-8 prefix with case sensitivity. An empty prefix matches every string,
including the empty string. Missing paths and non-string values fail the
positive predicate. Prefixes may contain at most 256 UTF-8 bytes and share
the filter's input and comparison budgets. Matching performs no case folding
or Unicode normalization. A required positive prefix can select a string
range in an eligible single-field or compound scalar index, subject to the
normal partial-index and trailing-field rules. Negation can match missing
paths so a single negated prefix does not select a prefix range. Downstream
exhaustive Rust matches on `DocumentFilter` need a new arm for this variant.

Array predicates `ArrayContains`, `ArrayContainsAny`, `ArrayContainsAll`, and `ArrayLength` inspect the value at an object path without implicitly traversing through arrays. They match only arrays: a missing path, explicit null, or non-array does not match. Membership uses the same exact JSON equality. `Any` with no requested values never matches, while `All` with no values matches every array; duplicate requested values do not require duplicate elements. As with other predicates, applying `Not` negates the entire result, including for missing paths. Across all `Equal`, `StartsWith`, value-membership and array-value predicates in one filter, at most 256 requested values and 4 MiB of their encoded JSON are accepted. Repeated values and all branches count, even when a branch would short-circuit; typed scalar `Compare` operands and paths are outside that input counter. A query fails after 1,000,000 equality, prefix and membership comparisons across its evaluated documents; it never returns a truncated result. Short-circuit order affects the work performed, and nested equality can inspect substantial data, so these counters are not total CPU, memory or latency limits. The predicates also work in aggregation and transactional queries. Array predicates use a collection scan unless a different eligible equality in the same filter selects a scalar index; they do not add multikey indexing.

`ArrayElementMatch { path, filter }` explicitly matches when at least one **object element** in the array at `path` satisfies the complete nested filter relative to that element. For example, requiring both `color = red` and `quantity >= 5` means that one object must satisfy both, rather than different array elements satisfying each condition. Missing, null, non-array and empty arrays do not match. Non-object array elements are skipped even for a nested `Not`; nested `ArrayElementMatch` is allowed. Nested filters share the same depth, node, input and equality-comparison limits as the outer filter. A separate limit counts at most 1,000,000 visited elements across all element-match predicates and documents in one query, including skipped non-objects; exceeding it fails the query. Short-circuiting affects work performed, and these limits do not bound total CPU or memory. Aggregations and transaction queries use the same semantics. A required element match proves that its own array path exists for a partial-index presence guard; its element-relative predicates never prove root-field constraints. The planner does not use a multikey index; a separate required root predicate may select an eligible scalar index, after which element matching remains a residual filter. Adding the enum variant is a Rust source compatibility change for exhaustive downstream matches.

`ArrayAllElementsMatch { path, filter }` requires an array whose every
element is an object satisfying the relative filter. An empty array
matches. Missing paths, null, non-array values and any non-object element
fail the positive predicate. For example, it can require that every line
item has a positive quantity. To require a nonempty array as well, combine
the filter with `Not(ArrayLength { path, length: 0 })`. Universal and
existential matches can be nested and share the same element-visit,
depth, node, input and comparison budgets. A required positive match can
prove presence of its array path for a partial-index guard. The nested
conditions remain relative to each object. Downstream exhaustive Rust
matches on `DocumentFilter` need a new arm for this variant.

`query_planned` is an opt-in, rule-based alternative on both `DocumentDatabase` and `DocumentTransaction`. If the whole filter requires exact scalar equalities for every field of an eligible index, it can select that complete equality tuple, preferring the first qualifying unique index and then the first nonunique index. This includes compound indexes: a prefix alone does not qualify because documents missing later fields are omitted from that index. Otherwise it can select an eligible index for required scalar `Compare` and string prefix predicates, combining their tightest inclusive or exclusive bounds on the field after an equality prefix. It tries eligible single-field ranges before compound ranges, preserving the earlier rule-based preference. For a compound index, every later indexed field must also be provably present through required predicates; the equality prefix may be empty. This prevents missing-field documents from being omitted. Comparisons use the same documented scalar ordering as indexes, including ordering across types. A unique equality can return at most one valid candidate; nonunique equalities and ranges are read in complete pages. Range entries arrive in value order, so the planner restores ID order before applying the full filter, sort, pagination and projection. The query fails instead of returning a truncated answer when candidates exceed `max_scanned` or their original document key/value bytes exceed 32 MiB. The limit applies to candidates examined, not just matching rows.

Use `explain_query` on a document database, transaction or snapshot to
inspect the chosen access path and the exact index probes before execution.
Each probe shows the index name, leading equality values, optional bounds
on the next field and whether the probe constrains every field of a unique
index to equality. A complete collection scan has an empty probe list. The method validates the request
and reads collection metadata without visiting documents or index entries.
A transaction uses its staged index metadata, and a snapshot uses its
captured metadata. Actual candidate counts are available after execution
through `query_planned`.

The planner can find required scalar predicates inside conjunctions, double negations, and negated disjunctions. It treats `Not(NotIn(...))` as membership. It can tighten an index range with a required negated comparison when a separate required positive comparison on the same path guarantees that every match has an indexable scalar there. The negation reverses the bound and its inclusivity. A negated comparison alone can match a missing or non-indexable field, so it remains a residual filter without that proof. Negated equalities remain residual filters. A required singleton `In` with an indexable value is also an equality for planning, including partial-equality proof. Required comparisons whose tightest lower and upper bounds are both inclusive at the same typed scalar also supply an equality, including a bound derived from a guarded negated comparison. This can complete an exact tuple or extend a compound prefix so the planner can use later indexed fields. Adjacent bounds can also supply a single signed integer, such as `score > 4 AND score < 6`. The planner accounts for inclusive endpoints and checks signed endpoint arithmetic. Positive adjacent integer bounds can also prove a partial equality guard. Intervals spanning multiple values or scalar types continue to use the range rules.

For a required multi-value `In` whose values are all indexable, the planner may use a single-field index as a union of exact-value probes. It deduplicates input values by index encoding, combines the existing candidate and original-document-byte limits across all probes, restores ID order, and applies the complete filter before pagination and projection. Probes share the transaction's read-set protections. Equality and range choices retain their earlier priority. A multi-value list can start this union when all its surviving alternatives are indexable. The 256-value input limit bounds distinct single-field probes; these are not total time or memory guarantees.

After those existing choices, the planner can also probe a compound index when every indexed field has a required indexable equality or `In` predicate. It forms the Cartesian product of distinct values, up to 256 complete tuples. The exact-tuple union requires a product within that cap and a constraint on every indexed field. An implied partial index is eligible under the same guard rules. Full tuples prevent omitted missing fields from producing incomplete results. Candidate and byte budgets, transaction dependencies and final filtering remain shared across probes.

When multiple required multi-value membership lists constrain one field,
the planner intersects their indexable alternatives before building
single-field, compound-prefix, or complete-tuple probes. A fully indexable
required list covers every possible match. Other required lists can narrow
that set even when they include non-indexable alternatives, regardless of
the order of the predicates. Without such a starting list, the planner can
use a mixed list to start membership probes only when required scalar bounds
exclude all its non-indexable alternatives. If an intersection is empty,
the planner keeps the preceding bounded set of probes and applies the
complete filter.

When a required membership list constrains the same field as a planned
scalar range, the planner can intersect the list with the typed inclusive
and exclusive bounds. Values that cannot satisfy a scalar comparison are
discarded. Surviving values become deduplicated equality or compound-prefix
probes, subject to the same proof of partial-index coverage and trailing-field
presence as the range. An empty intersection keeps the existing range plan.
When earlier fields also use membership, the planner applies the same bounds
before counting complete compound tuples or extending a compound prefix.
If the narrower prefixes still exceed the 256-probe cap, the existing
membership-prefix range can remain eligible.
For bounded integer lists, the planner can use one probe per consecutive
run for each preceding membership prefix. A run of one value uses an
exact probe and a longer run uses an inclusive range. Values in the gaps
consume no candidate budget when the grouped probes fit the 256-probe cap.
The existing range fallback remains available for larger probe counts.
The normal candidate budgets and read-set checks apply to the resulting probes.
A complete compound membership query that exceeds the exact-tuple cap can
also group consecutive integers on its last field before applying the cap.
Each group still has a complete preceding equality prefix, and all resulting
probes share the 256-probe budget. If those probes exceed the cap, the planner
continues to its other access paths.
When every surviving value is an integer, consecutive values can share an
inclusive range probe. The planner reports this access as
`IntegerMembershipIndex`, with the number of preceding equality fields and
the number of probes. Holes remain separate, and indexed disjunctions retain
their existing deduplicated value probes.

A compound index can also answer a required scalar range after a prefix of
required indexable equality and `In` predicates, with at least one multi-value
`In` in that prefix. The planner builds at most 256 distinct prefixes and
applies the same range to each. Every indexed field after the range must be
provably present. Partial indexes also require proof of their guard. This plan
follows the earlier equality, range and membership choices in priority. It
shares the same candidate and byte limits and registers read-set dependencies
for every range, including empty ones. When no eligible path covers the full
query, the planner performs the bounded collection scan.

When those single-index choices do not apply, a required disjunction can
use a union of branch plans. The planner recognizes both `Or` and negated
`And` as disjunctions, including within an `And` or a negated `Or`.
Each branch must have an eligible index or a required disjunction with
complete branch coverage. The branch and its surrounding required
conjunction can supply equality fields, scalar bounds, presence proofs and
partial-index guards. A guard in one alternative does not qualify a sibling.
The union can combine equality, membership, range and compound prefix probes
across distinct indexes. A probe with the same index, equality tuple and
range bounds, including inclusivity, runs once in its first occurrence.
All branches share one cap of 256 distinct probes and the original
document and byte budgets. Hits from distinct probes consume budget on
each visit, even when their ranges overlap.
When the candidate count is exhausted, an index lookahead can prove there
is another candidate. The query rejects before fetching that candidate's
document or its unique-owner record.
The planner checks each candidate's stored length against the document size
limit, then charges its key and value against the remaining scan byte budget
before materializing the value. The first over-budget candidate returns a
limit error before JSON decoding and document-to-index comparison. Lengths
come from captured value locations, staged writes or retained entry framing.
The retained path can read a framing buffer to locate the entry. Admitted
values still undergo the usual integrity and schema checks.
The planner keeps the distinct candidate IDs and streams documents into
the requested window. Index and candidate budget errors take precedence
over filter and sort errors. Array-walking filters evaluate once in ID
order. They revisit the candidate IDs within the transaction when the
index probes return a different order. Other filters can run during the
probe and revisit IDs on failure to preserve error order. Every probe
participates in transaction validation, including empty scans.

Before falling back to prefix scans, the planner can combine independent
required disjunctions as compound membership predicates. The alternatives
must be literal equalities, nonempty `In` lists, or negated nonempty
`NotIn` lists on the same field. Nested disjunctions follow the same rule.
Every value must be an indexable scalar. This can complete a compound tuple or
enable a range on a later field. The probe cap applies to the product of distinct choices,
and the query still evaluates the original filter. Correlated alternatives
use the branch rules above.

If the existing access paths cannot cover the query, the planner can scan a
compound index for a contiguous leading tuple of required equalities or
membership alternatives. Required predicates must prove that every remaining
indexed field is present and imply any partial-index guard. Distinct prefixes
share the 256-probe cap. A prefix can match several rows of a unique index, so
the planner pages through those results under the same candidate and byte
limits and applies the full filter.

When no narrower plan qualifies, the planner can scan a single or compound
index if explicit `Exists` predicates prove that every indexed field is
present. Negation, ancestor paths and disjunctions use the same presence
proof rules as partial-index eligibility. Explicit null counts as present.
A partial index still requires proof of its guard. The `PresenceIndex`
path scans all entries in the eligible index under the normal candidate
and byte limits, then applies the original filter. This can avoid scanning
documents with missing fields or documents omitted by the partial guard.

A partial index is eligible only when a required predicate proves its filter. A required `Exists(true)`, equality, `In`, comparison, string prefix or array predicate proves presence of its path and each ancestor. A required `Exists(false)` proves absence of its path and each descendant. Negating `Exists` flips its test. For presence and absence proofs, negation swaps the conjunction and disjunction rules, and double negation cancels. `NotIn` matches missing paths. When negated, a nonempty `NotIn` proves presence just like `In`. For a typed equality guard, the planner must prove both bounds at that scalar on the guarded path. A required typed equality, comparison at that scalar, or nonempty membership list can prove a bound. A membership proof requires every value to be indexable and on the appropriate side of the bound. Negating `NotIn` has the same proof as `In`. Conjunction, disjunction and double negation use the same proof rules as presence guards. A negated comparison on its own proves neither bound because it can match a missing or non-indexable field. Presence and absence proofs also follow through a nonempty `Or` when every branch proves the same condition. Explicit null counts as present. Other negated value and array predicates supply no presence or absence proof. If no eligible index qualifies, the method falls back to `query`'s complete scan. The result reports the selected path and actual candidate-document counts. This can succeed for a large collection when the explicit complete scan exceeds its work limit, but it is not a cost-based planner and does not handle arbitrary compound constraints or general boolean implication.

The indexed path relies on the declared index being intact. It validates each encountered index entry and its unique mapping, but an entry removed by unsupported raw writes to the reserved namespace can cause an omission. `verify_collection` can detect such omissions within its existing small-collection limits. The query holds the same transaction view, read-set range dependencies, conflict behavior, document and response limits as the underlying indexed query; it does not introduce cross-store snapshots or a total-memory guarantee.

`aggregate` performs a bounded complete scan of at most 256 documents, with count, optional grouping by one scalar path and optional checked signed-integer sum. It groups matches as they are visited in document ID order and retains the groups and a scan page. Missing and null sum values are skipped. Non-integers and intermediate overflow fail after scan and filter validation. `lookup` follows a string document ID into another collection within the same transaction snapshot. Missing/null references and missing target IDs yield no related document. It admits up to 16 MiB of encoded related document bytes before reading the targets, counting repeats, and rejects a projected source query. These are narrow primitives, not MongoDB aggregation pipelines or general joins.

`lookup_unique_index` takes one local object path per field of a named
unique index, in index field order. The complete typed scalar tuple resolves
at most one related document per source row. A missing local field or
absent tuple yields no match. Explicit null can match an indexed null, and
other present values must be indexable scalars. A partial index restricts
the target population to its indexed documents. This form uses the same
bounded source scan, projection rule, and 16 MiB related-document byte
limit as ID lookup. It reads the unique owner, validates the target tuple
and its companion index entry, and observes missing owner keys in
read-set transactions. Correct results depend on maintained indexes.


`aggregate_stats` computes count, exact checked `i128` sum, minimum, maximum and an optional approximate floating-point mean for one to 16 distinct paths in a complete scan of at most 256 documents. Grouping uses the same typed scalar ordering and distinguishes missing from null; an empty ungrouped scan returns one zero-count group, while an empty grouped scan returns none. Each field skips missing and null independently; any present non-signed-integer value fails the operation. Matching documents are grouped as they are visited; the groups and a scan page remain in memory. Scan and filter failures take precedence over aggregation errors. The result lists the requested paths once; every group returns statistics in that same order. The exact mean is the returned sum and field count as a rational; `mean()` is approximate. The method sees transaction-staged changes and shares the scan's serializable range validation and logical limits, but is not a general aggregation pipeline, indexed aggregation, total-memory quota, or MongoDB compatibility promise.

`aggregate_stats_planned` applies the same statistics to candidates from a
qualifying index plan and reports the chosen access path. It falls back to a
complete scan when no index qualifies. The 256-document ceiling counts
index visits, including overlapping probes. Each matching document
contributes once. A single equality tuple streams rows in ID order.
Other plans retain candidate IDs to deduplicate matches. Filters without
array walks can aggregate in probe order on success. An array walk or a
probe-order evaluation error triggers a pass in ID order to preserve the
filter budget and error order. That pass can read up to the admitted
candidate bytes again. The planner assumes maintained indexes and
evaluates the filter on candidates. It shares the transaction's range
validation, including empty probes.

For example, with an open `DocumentDatabase` named `db`:

```rust
use database_engine::{AggregateStatsRequest, DocumentFilter};

let report = db.aggregate_stats("orders", &AggregateStatsRequest {
    filter: DocumentFilter::All,
    group_by: Some(vec!["region".into()]),
    fields: vec![vec!["amount".into()], vec!["items".into()]],
    max_scanned: 256,
})?;
for group in &report.groups {
    for (path, stats) in report.fields.iter().zip(&group.fields) {
        println!("{:?} {:?}: count={}, sum={}, mean={:?}",
            group.key, path, stats.count, stats.sum, stats.mean());
    }
}
```

### Cumulative transaction reads

`transaction_with_read_limits` combines the detailed transaction API with
caller-selected read units and logical byte limits. Each point request and
each scan page request costs one unit, even if it finds nothing. Each
admitted scan entry costs another unit. Point keys, admitted point values
and admitted scan key/value pairs count toward the byte limit. This includes
repeated reads, staged data, and internal document, metadata and index reads.
The first over-limit read fails the operation. Even if the callback handles
the error and returns success, the engine aborts before commit. The limits
do not measure underlying I/O, validation reads at commit, allocator use
or elapsed time. Deadlines, cancellation and per-operation bounds still apply.

`transaction_with_limits` also sets `max_validation_items`. An ordinary
read-set transaction compares its observations with committed state once,
before append. A checkpoint-backed read-set transaction can revalidate
inside the callback as well as before append, and both phases share the cap.
Each point check, including an absent point, costs one unit. Ordinary range
validation counts compared entry pairs, including an entry on only one side.
Checkpoint range validation counts entries encountered during its scan.
Initial capture and unchanged-generation validation cost zero units.
Conservative transactions check the store generation. Retained transactions
serialize access and still perform their applicable authority checks.
An engine rejection during final validation returns
`TransactionFailure::Rejected`. A limit error propagated by the callback
returns `TransactionFailure::Callback`. Staged document and raw-key updates
are discarded in either case. Independent writes keep their normal outcome.

### Moving a collection

`export_collection` returns a versioned JSON archive from one transaction's
view. It includes the active schema, index definitions and documents ordered
by ID. Pending index and schema maintenance must finish or be cancelled
first. The archive carries logical documents and definitions. The destination
rebuilds index entries using its normal schema, uniqueness and partial-index
checks.

`import_collection` creates a new named collection in one transaction.
Existing collections and orphan document or index keys are rejected.
Malformed archives, unsupported versions and any staging or validation
error abort the import. Use the detailed outcome to distinguish an unknown
commit from a rejected archive. After an unknown outcome, reopen and
inspect the destination before retrying.

The version 1 format admits up to 256 documents, 16 indexes and 8 MiB of
encoded JSON. Normal transaction key, byte and backend limits can reject a
collection below those ceilings. The archive contains plaintext data.
Preserve and verify its bytes separately if the transport or storage is
untrusted. Archive transfer preserves the logical collection and leaves
out raw keys, maintenance state, witnesses and log history.

## Namespace, integrity and compatibility

The versioned byte prefix `00 65 6d 64 61 2d 64 6f 63 75 6d 65 6e 74 01` is reserved for document metadata, documents and indexes. The document transaction's raw methods reject this prefix. Direct raw `Store`/`CheckpointStore` access and older binaries do not enforce the reservation; applications must not use them to modify document-owned keys. The prefix is **not** a security or tenant-isolation boundary. Invalid or unknown metadata and invalid documents fail when detected, and queries check the entries they return; checksums and these checks do not repair corruption, authenticate data, or guarantee discovery of a missing index entry. In particular, a well-formed but improperly replaced build or schema-migration cursor can skip documents and publish an incomplete index or schema. A later bounded verification or read may detect the inconsistency; publication does not independently revalidate the whole collection.

Collection metadata includes a version marker; unknown versions and fields fail closed. Starting an incremental index build, index drop or schema migration permanently upgrades that collection metadata to version 2. Incremental collection deletion uses version 3 for its irreversible marker and version 4 when a checkpoint-backed step has persisted a cursor. Older document readers reject unsupported versions; a full-log backup and compatible engine are required for recovery. The document model uses the current experimental DB02 batch records, so older engines unable to read DB02 cannot reopen the resulting log. Earlier engines that can read the log still do not understand or protect document invariants. There is no stable format, migration or downgrade guarantee for this unreleased prototype. Backups contain plaintext document and index bytes in the full log, including prior values and deletions; they are not encrypted or authenticated. Existing independent-backup and restore requirements remain in force.
