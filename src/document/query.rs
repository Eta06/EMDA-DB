//! Bounded indexed document queries with explicit ordering and pagination.
use super::{
    index::{self, IndexDefinition, IndexValue, TupleBuf},
    key,
    transaction::{DocumentTransaction, Metadata, StoredDocument},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::io;
use std::ops::ControlFlow;

pub(super) const MAX_QUERY_DOCUMENTS: usize = 256;
const MAX_QUERY_BYTES: usize = 16 * 1024 * 1024;
const CURSOR_MAGIC: &[u8; 8] = b"EMDAIC01";
const CURSOR_HEADER_BYTES: usize = CURSOR_MAGIC.len() + 32;
const CURSOR_CHECKSUM_BYTES: usize = 4;
const CURSOR_SCOPE_MEMO_ENTRIES: usize = 8;
const INLINE_INDEX_BYTES: usize = 1024;
const INLINE_INDEX_ENTRIES: usize = 32;
// Maximum length of any valid document key:
// PREFIX (15) + b'D' (1) + 2 + MAX_NAME_BYTES (128) + MAX_ID_BYTES (256) = 402.
const MAX_DOC_KEY_STACK: usize = 512;

type CursorScopeMemo = Vec<(String, IndexDefinition, [u8; 32])>;

thread_local! {
    static CURSOR_SCOPE_MEMO: RefCell<CursorScopeMemo> = const { RefCell::new(Vec::new()) };
}

fn cursor_scope(collection: &str, definition: &IndexDefinition) -> io::Result<[u8; 32]> {
    let found = CURSOR_SCOPE_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        let at = memo
            .iter()
            .position(|(name, def, _)| name == collection && def == definition)?;
        let entry = memo.remove(at);
        let scope = entry.2;
        memo.insert(0, entry);
        Some(scope)
    });
    if let Some(scope) = found {
        return Ok(scope);
    }
    let mut scope = Sha256::new();
    scope.update(CURSOR_MAGIC);
    scope.update(serde_json::to_vec(&(collection, definition)).map_err(io::Error::other)?);
    let scope: [u8; 32] = scope.finalize().into();
    CURSOR_SCOPE_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        memo.truncate(CURSOR_SCOPE_MEMO_ENTRIES - 1);
        memo.insert(0, (collection.to_owned(), definition.clone(), scope));
    });
    Ok(scope)
}

/// Packed index entry keys scanned for one page. Small pages stay on the
/// stack without heap allocation; larger pages spill into two contiguous
/// vectors rather than one allocation per entry.
struct IndexEntries {
    inline_bytes: [u8; INLINE_INDEX_BYTES],
    inline_ends: [u16; INLINE_INDEX_ENTRIES],
    bytes_len: usize,
    count: usize,
    spilled: bool,
    spill_bytes: Vec<u8>,
    spill_ends: Vec<usize>,
}

impl IndexEntries {
    fn new() -> Self {
        Self {
            inline_bytes: [0; INLINE_INDEX_BYTES],
            inline_ends: [0; INLINE_INDEX_ENTRIES],
            bytes_len: 0,
            count: 0,
            spilled: false,
            spill_bytes: Vec::new(),
            spill_ends: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.bytes_len = 0;
        self.count = 0;
        self.spilled = false;
        self.spill_bytes.clear();
        self.spill_ends.clear();
    }

    fn push(&mut self, entry: &[u8]) {
        if !self.spilled {
            if self.count < INLINE_INDEX_ENTRIES
                && self.bytes_len + entry.len() <= INLINE_INDEX_BYTES
            {
                self.inline_bytes[self.bytes_len..self.bytes_len + entry.len()]
                    .copy_from_slice(entry);
                self.bytes_len += entry.len();
                self.inline_ends[self.count] = self.bytes_len as u16;
                self.count += 1;
                return;
            }
            self.spill_bytes.clear();
            self.spill_bytes.reserve(self.bytes_len + entry.len());
            self.spill_bytes
                .extend_from_slice(&self.inline_bytes[..self.bytes_len]);
            self.spill_ends.clear();
            self.spill_ends.reserve(self.count + 1);
            for &end in &self.inline_ends[..self.count] {
                self.spill_ends.push(usize::from(end));
            }
            self.spilled = true;
        }
        self.spill_bytes.extend_from_slice(entry);
        self.spill_ends.push(self.spill_bytes.len());
        self.count += 1;
    }

    fn len(&self) -> usize {
        self.count
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn get(&self, index: usize) -> &[u8] {
        if !self.spilled {
            let start = index
                .checked_sub(1)
                .map_or(0, |prev| usize::from(self.inline_ends[prev]));
            let end = usize::from(self.inline_ends[index]);
            &self.inline_bytes[start..end]
        } else {
            let start = index.checked_sub(1).map_or(0, |prev| self.spill_ends[prev]);
            let end = self.spill_ends[index];
            &self.spill_bytes[start..end]
        }
    }
}

/// Reusable fixed-capacity stack buffer initialized with a constant prefix.
struct PrefixKeyBuf<const N: usize> {
    buf: [u8; N],
    prefix_len: usize,
}

impl<const N: usize> PrefixKeyBuf<N> {
    fn new(prefix: &[u8]) -> Self {
        assert!(prefix.len() <= N);
        let mut buf = [0; N];
        buf[..prefix.len()].copy_from_slice(prefix);
        Self {
            buf,
            prefix_len: prefix.len(),
        }
    }

    fn with_suffix(&mut self, suffix: &[u8]) -> &[u8] {
        let end = self.prefix_len + suffix.len();
        self.buf[self.prefix_len..end].copy_from_slice(suffix);
        &self.buf[..end]
    }
}

/// An inclusive or exclusive bound on the first field after equality fields.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexBound {
    /// A typed scalar in the index's ordering.
    pub value: IndexValue,
    /// Whether the bound includes matching values.
    pub inclusive: bool,
}

/// An opaque seek position from an earlier indexed page in a named index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexCursor {
    scope: [u8; 32],
    position: Vec<u8>,
}

impl IndexCursor {
    /// Encodes the position for storage between sessions.
    /// Keep these bytes private if indexed values or document IDs are sensitive.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(CURSOR_HEADER_BYTES + self.position.len() + CURSOR_CHECKSUM_BYTES);
        bytes.extend_from_slice(CURSOR_MAGIC);
        bytes.extend_from_slice(&self.scope);
        bytes.extend_from_slice(&self.position);
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
        bytes
    }

    /// Decodes a bounded persisted seek position. The checksum detects
    /// accidental damage. It does not authenticate the position.
    /// A query checks the position against its current index and bounds.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let overhead = CURSOR_HEADER_BYTES + CURSOR_CHECKSUM_BYTES;
        if !(overhead + 1..=overhead + crate::record::MAX_KEY).contains(&bytes.len())
            || !bytes.starts_with(CURSOR_MAGIC)
        {
            return Err(key::invalid("invalid index cursor format or length"));
        }
        let (data, checksum) = bytes.split_at(bytes.len() - CURSOR_CHECKSUM_BYTES);
        if crc32fast::hash(data).to_le_bytes() != checksum {
            return Err(key::invalid("invalid index cursor checksum"));
        }
        Ok(Self {
            scope: data[CURSOR_MAGIC.len()..CURSOR_HEADER_BYTES]
                .try_into()
                .expect("fixed cursor scope length"),
            position: data[CURSOR_HEADER_BYTES..].to_vec(),
        })
    }
}

/// A bounded scan of a named secondary index.
#[derive(Clone, Debug)]
pub struct IndexQuery {
    /// Equality values for an initial prefix of the indexed fields.
    pub equal: Vec<IndexValue>,
    /// Optional lower bound for the next field after `equal`.
    pub lower: Option<IndexBound>,
    /// Optional upper bound for the next field after `equal`.
    pub upper: Option<IndexBound>,
    /// Resume after a previously returned index entry.
    pub after: Option<IndexCursor>,
    /// Maximum documents returned, between 1 and 256.
    pub limit: usize,
}

impl Default for IndexQuery {
    fn default() -> Self {
        Self {
            equal: Vec::new(),
            lower: None,
            upper: None,
            after: None,
            limit: 100,
        }
    }
}

/// A returned ID and its complete JSON document.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentEntry {
    /// The collection-local ID.
    pub id: String,
    /// The stored object.
    pub document: Value,
}

/// Actual work and access path for one explicit indexed page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexQueryPlan {
    /// Index selected by the caller.
    pub index: String,
    /// Number of leading fields fixed by equality.
    pub equality_fields: usize,
    /// Whether a lower or upper range was supplied.
    pub has_range: bool,
    /// Whether the selected index has a partial filter.
    pub partial: bool,
    /// Number of index entries fetched from the store.
    pub entries_read: usize,
    /// Number of documents returned.
    pub returned: usize,
}

/// One bounded page. Resume with `next` until a page is empty. Separate calls
/// can observe intervening writes; pages within one transaction share a view.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentPage {
    /// Results ordered by index tuple, then document ID bytes.
    pub documents: Vec<DocumentEntry>,
    /// The last returned position, if this page was nonempty.
    pub next: Option<IndexCursor>,
    /// The selected index and actual page work.
    pub plan: IndexQueryPlan,
}

pub(super) struct IndexBounds {
    equal_prefix: Vec<u8>,
    pub(super) after: Vec<u8>,
    pub(super) before: Vec<u8>,
}

pub(super) fn index_bounds(
    index_prefix: &[u8],
    equal: &[IndexValue],
    lower: Option<&IndexBound>,
    upper: Option<&IndexBound>,
) -> io::Result<IndexBounds> {
    let mut equal_prefix = index_prefix.to_vec();
    for value in equal {
        value.encode(&mut equal_prefix)?;
    }
    let after = if let Some(lower) = lower {
        let mut bound = equal_prefix.clone();
        lower.value.encode(&mut bound)?;
        if lower.inclusive {
            bound
        } else {
            key::end(&bound)
        }
    } else {
        equal_prefix.clone()
    };
    let before = if let Some(upper) = upper {
        let mut bound = equal_prefix.clone();
        upper.value.encode(&mut bound)?;
        if upper.inclusive {
            key::end(&bound)
        } else {
            bound
        }
    } else {
        key::end(&equal_prefix)
    };
    Ok(IndexBounds {
        equal_prefix,
        after,
        before,
    })
}

// The planner needs the original encoded document and key lengths to enforce
// the collection scan's byte budget without serializing parsed JSON again.
pub(super) struct IndexScan {
    pub next: Option<Vec<u8>>,
    pub plan: IndexQueryPlan,
    pub document_key_value_bytes: usize,
    // IDs plus stored document bytes of the returned entries.
    pub response_bytes: usize,
}

pub(super) struct IndexScanOptions<'a> {
    pub after: Option<&'a [u8]>,
    pub byte_limit: Option<usize>,
    pub response_byte_limit: Option<usize>,
    pub candidate_limit: Option<usize>,
    pub unique_equality: bool,
    // Refuse, before reading its value, an entry that would exceed
    // `response_byte_limit`, instead of ending the page early.
    pub reject_response_overflow: bool,
}

pub(super) fn index_definition<'a>(
    metadata: &'a Metadata,
    name: &str,
) -> io::Result<&'a IndexDefinition> {
    metadata
        .indexes
        .iter()
        .find(|index| index.name == name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index does not exist"))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn consume_value(bytes: &[u8]) -> io::Result<usize> {
    match bytes.first() {
        Some(0x10) => Ok(1),
        Some(0x20) if matches!(bytes.get(1), Some(0 | 1)) => Ok(2),
        Some(0x30) if bytes.len() >= 9 => Ok(9),
        Some(0x40) => {
            let mut offset = 1;
            let mut decoded_len = 0_usize;
            let mut escaped_buf = [0_u8; index::MAX_INDEX_STRING_BYTES];
            let mut has_escape = false;
            loop {
                let byte = *bytes
                    .get(offset)
                    .ok_or_else(|| invalid_data("truncated index string"))?;
                offset += 1;
                if byte != 0 {
                    if decoded_len >= index::MAX_INDEX_STRING_BYTES {
                        return Err(invalid_data("index string is too long"));
                    }
                    if has_escape {
                        escaped_buf[decoded_len] = byte;
                    }
                    decoded_len += 1;
                } else {
                    let escaped = *bytes
                        .get(offset)
                        .ok_or_else(|| invalid_data("truncated index escape"))?;
                    offset += 1;
                    match escaped {
                        0 => break,
                        0xff => {
                            if decoded_len >= index::MAX_INDEX_STRING_BYTES {
                                return Err(invalid_data("index string is too long"));
                            }
                            if !has_escape {
                                escaped_buf[..decoded_len]
                                    .copy_from_slice(&bytes[1..1 + decoded_len]);
                                has_escape = true;
                            }
                            escaped_buf[decoded_len] = 0;
                            decoded_len += 1;
                        }
                        _ => return Err(invalid_data("invalid index escape")),
                    }
                }
            }
            let slice = if has_escape {
                &escaped_buf[..decoded_len]
            } else {
                &bytes[1..1 + decoded_len]
            };
            std::str::from_utf8(slice).map_err(|_| invalid_data("invalid UTF-8 index string"))?;
            Ok(offset)
        }
        _ => Err(invalid_data("invalid index tuple")),
    }
}

fn entry_id<'a>(entry: &'a [u8], prefix: &[u8], fields: usize) -> io::Result<&'a str> {
    let mut remaining = entry
        .strip_prefix(prefix)
        .ok_or_else(|| invalid_data("invalid index prefix"))?;
    for _ in 0..fields {
        let length = consume_value(remaining)?;
        remaining = &remaining[length..];
    }
    let id =
        std::str::from_utf8(remaining).map_err(|_| invalid_data("invalid document ID in index"))?;
    key::id(id).map_err(|_| invalid_data("invalid document ID in index"))?;
    Ok(id)
}

struct StoredPageOutcome {
    entries_read: usize,
    returned: usize,
    cursor_index: Option<usize>,
    document_key_value_bytes: usize,
    response_bytes: usize,
    stopped: bool,
}

impl DocumentTransaction<'_, '_> {
    /// Reads a page from a specific index. Partial indexes return only rows that
    /// satisfy their stored partial filter. Missing indexed fields are omitted.
    /// Index ordering is null, boolean, signed integer, then UTF-8 string bytes.
    pub fn query_index(
        &self,
        collection: &str,
        name: &str,
        query: &IndexQuery,
    ) -> io::Result<DocumentPage> {
        if query.limit == 0 || query.limit > MAX_QUERY_DOCUMENTS {
            return Err(key::invalid("query limit must be between 1 and 256"));
        }
        let metadata = self.metadata(collection)?;
        let definition = index_definition(&metadata, name)?;
        let scope = cursor_scope(collection, definition)?;
        if let Some(cursor) = &query.after {
            if cursor.scope != scope {
                return Err(key::invalid(
                    "cursor belongs to a different index definition",
                ));
            }
            let prefix = key::index(b'I', collection, name)?;
            entry_id(&cursor.position, &prefix, definition.fields.len())
                .map_err(|_| key::invalid("cursor contains an invalid index position"))?;
        }
        let mut documents = Vec::new();
        let scan = self.visit_index_with_metadata(
            collection,
            definition,
            query,
            &metadata,
            IndexScanOptions {
                after: query
                    .after
                    .as_ref()
                    .map(|cursor| cursor.position.as_slice()),
                byte_limit: None,
                response_byte_limit: Some(MAX_QUERY_BYTES),
                reject_response_overflow: false,
                candidate_limit: None,
                unique_equality: false,
            },
            |entry| documents.push(entry),
        )?;
        Ok(DocumentPage {
            documents,
            next: scan.next.map(|position| IndexCursor { scope, position }),
            plan: scan.plan,
        })
    }

    /// Visits matching documents in secondary-index order across as many
    /// internal pages as needed, lending each document ID and decoded
    /// `StoredDocument` without computing public cursor scopes or building
    /// JSON values for flat rows.
    ///
    /// Enforces the same metadata, bounds, checksum, schema, index-entry,
    /// unique-owner, read-set, and read-budget checks as `query_index`.
    /// `visit` runs while the backend point session is open and must not
    /// call back into this transaction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn visit_index_stored<E>(
        &self,
        collection: &str,
        name: &str,
        equal: &[IndexValue],
        lower: Option<&IndexBound>,
        upper: Option<&IndexBound>,
        unique_equality: bool,
        mut next_page_limit: impl FnMut() -> usize,
        mut visit: impl FnMut(&str, StoredDocument<'_>) -> Result<ControlFlow<()>, E>,
    ) -> io::Result<Result<(), E>> {
        let mut page_limit = next_page_limit();
        if page_limit == 0 {
            return Ok(Ok(()));
        }
        let metadata = self.metadata(collection)?;
        let definition = index_definition(&metadata, name)?;
        if equal.len() > definition.fields.len()
            || (equal.len() == definition.fields.len() && (lower.is_some() || upper.is_some()))
        {
            return Err(key::invalid("query bounds exceed the index field count"));
        }
        let index_prefix = key::index(b'I', collection, &definition.name)?;
        let IndexBounds {
            after, mut before, ..
        } = index_bounds(&index_prefix, equal, lower, upper)?;
        let initial_after = &after[..after.len().min(crate::record::MAX_KEY)];
        if before.len() > crate::record::MAX_KEY {
            before.truncate(crate::record::MAX_KEY);
            before = key::end(&before);
        }
        if initial_after >= before.as_slice() {
            return Ok(Ok(()));
        }
        let doc_prefix = key::collection(b'D', collection)?;
        let unique_prefix = if definition.unique {
            Some(key::index(b'U', collection, &definition.name)?)
        } else {
            None
        };
        let mut entries = IndexEntries::new();
        let mut tuple_buf = TupleBuf::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let limit = page_limit.min(MAX_QUERY_DOCUMENTS);
            let page_after = cursor.as_deref().unwrap_or(initial_after);
            let outcome = match self.visit_index_page_stored(
                definition,
                &metadata,
                &index_prefix,
                &doc_prefix,
                unique_prefix.as_deref(),
                page_after,
                &before,
                limit,
                &IndexScanOptions {
                    after: None,
                    byte_limit: None,
                    response_byte_limit: None,
                    reject_response_overflow: false,
                    candidate_limit: None,
                    unique_equality: false,
                },
                &mut entries,
                &mut tuple_buf,
                &mut visit,
            )? {
                Ok(outcome) => outcome,
                Err(visitor_error) => return Ok(Err(visitor_error)),
            };
            if outcome.stopped
                || outcome.entries_read == 0
                || unique_equality
                || (outcome.entries_read < limit && outcome.returned == outcome.entries_read)
            {
                return Ok(Ok(()));
            }
            page_limit = next_page_limit();
            if page_limit == 0 {
                return Ok(Ok(()));
            }
            let Some(cursor_index) = outcome.cursor_index else {
                return Ok(Ok(()));
            };
            cursor = Some(entries.get(cursor_index).to_vec());
        }
    }

    // The visitor's effects must remain private until the page passes all
    // index checks and admission limits.
    /// Counts index entries in one probe's key range without reading their
    /// documents, up to `limit`. Reads go through this transaction's admitted
    /// scan, so they see staged writes, join its read set and are charged to
    /// its read limits. Makes at most `max_scans` scan calls; an empty key
    /// range needs none. Returns the count capped at `limit`, the scan calls
    /// made, and whether the count is conclusive: it reached `limit` or the
    /// end of the range before the scan budget ran out.
    pub(super) fn count_index_entries(
        &self,
        collection: &str,
        definition: &IndexDefinition,
        query: &IndexQuery,
        limit: usize,
        max_scans: usize,
    ) -> io::Result<(usize, usize, bool)> {
        let index_prefix = key::index(b'I', collection, &definition.name)?;
        let IndexBounds {
            after, mut before, ..
        } = index_bounds(
            &index_prefix,
            &query.equal,
            query.lower.as_ref(),
            query.upper.as_ref(),
        )?;
        // The same truncation as an executed scan of this probe.
        let mut cursor = after[..after.len().min(crate::record::MAX_KEY)].to_vec();
        if before.len() > crate::record::MAX_KEY {
            before.truncate(crate::record::MAX_KEY);
            before = key::end(&before);
        }
        let (mut counted, mut scans) = (0, 0);
        loop {
            if counted >= limit || cursor.as_slice() >= before.as_slice() {
                return Ok((counted, scans, true));
            }
            if scans == max_scans {
                return Ok((counted, scans, false));
            }
            scans += 1;
            let page = (limit - counted).min(MAX_QUERY_DOCUMENTS);
            let rows = self.scan_admitted(&cursor, &before, page, |_, _| true)?;
            counted += rows.len();
            match rows.last() {
                Some((last, _)) if rows.len() == page => cursor = last.clone(),
                _ => return Ok((counted, scans, true)),
            }
        }
    }

    pub(super) fn visit_index_with_metadata(
        &self,
        collection: &str,
        definition: &IndexDefinition,
        query: &IndexQuery,
        metadata: &Metadata,
        options: IndexScanOptions<'_>,
        mut visit: impl FnMut(DocumentEntry),
    ) -> io::Result<IndexScan> {
        if query.limit == 0 || query.limit > MAX_QUERY_DOCUMENTS {
            return Err(key::invalid("query limit must be between 1 and 256"));
        }
        if query.equal.len() > definition.fields.len()
            || (query.equal.len() == definition.fields.len()
                && (query.lower.is_some() || query.upper.is_some()))
        {
            return Err(key::invalid("query bounds exceed the index field count"));
        }
        let index_prefix = key::index(b'I', collection, &definition.name)?;
        let IndexBounds {
            equal_prefix,
            after,
            mut before,
        } = index_bounds(
            &index_prefix,
            &query.equal,
            query.lower.as_ref(),
            query.upper.as_ref(),
        )?;
        let after = if let Some(cursor) = options.after {
            if !cursor.starts_with(&equal_prefix)
                || cursor >= before.as_slice()
                || cursor < after.as_slice()
            {
                return Err(key::invalid("cursor is outside the query bounds"));
            }
            cursor
        } else {
            after.as_slice()
        };
        // No stored key can extend a maximum-length prefix. An exclusive
        // lower bound can drop its remaining bytes. An exclusive upper bound
        // must also include a key equal to its truncated prefix.
        let after = &after[..after.len().min(crate::record::MAX_KEY)];
        if before.len() > crate::record::MAX_KEY {
            before.truncate(crate::record::MAX_KEY);
            before = key::end(&before);
        }
        let plan = |entries_read, returned| IndexQueryPlan {
            index: definition.name.clone(),
            equality_fields: query.equal.len(),
            has_range: query.lower.is_some() || query.upper.is_some(),
            partial: definition.partial.is_some(),
            entries_read,
            returned,
        };
        if after >= before.as_slice() {
            return Ok(IndexScan {
                next: None,
                plan: plan(0, 0),
                document_key_value_bytes: 0,
                response_bytes: 0,
            });
        }
        let doc_prefix = key::collection(b'D', collection)?;
        let unique_prefix = if definition.unique {
            Some(key::index(b'U', collection, &definition.name)?)
        } else {
            None
        };
        let mut entries = IndexEntries::new();
        let mut tuple_buf = TupleBuf::new();
        let outcome = self
            .visit_index_page_stored(
                definition,
                metadata,
                &index_prefix,
                &doc_prefix,
                unique_prefix.as_deref(),
                after,
                &before,
                query.limit,
                &options,
                &mut entries,
                &mut tuple_buf,
                |id, stored| {
                    let document = match stored {
                        StoredDocument::Flat(flat) => Value::Object(flat.to_map()),
                        StoredDocument::Full(document) => document,
                    };
                    visit(DocumentEntry {
                        id: id.to_owned(),
                        document,
                    });
                    Ok::<ControlFlow<()>, std::convert::Infallible>(ControlFlow::Continue(()))
                },
            )?
            .unwrap_or_else(|never| match never {});
        let cursor = outcome
            .cursor_index
            .map(|index| entries.get(index).to_vec());
        Ok(IndexScan {
            next: cursor,
            plan: plan(outcome.entries_read, outcome.returned),
            document_key_value_bytes: outcome.document_key_value_bytes,
            response_bytes: outcome.response_bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn visit_index_page_stored<E>(
        &self,
        definition: &IndexDefinition,
        metadata: &Metadata,
        index_prefix: &[u8],
        doc_prefix: &[u8],
        unique_prefix: Option<&[u8]>,
        after: &[u8],
        before: &[u8],
        page_limit: usize,
        options: &IndexScanOptions<'_>,
        entries: &mut IndexEntries,
        tuple_buf: &mut TupleBuf,
        mut visit: impl FnMut(&str, StoredDocument<'_>) -> Result<ControlFlow<()>, E>,
    ) -> io::Result<Result<StoredPageOutcome, E>> {
        entries.clear();
        let mut nonempty_entry = false;
        self.visit_admitted_keys(
            after,
            before,
            page_limit,
            |_, len| {
                nonempty_entry = len != 0;
                !nonempty_entry
            },
            |entry| {
                entries.push(entry);
                true
            },
        )?;
        let entries_read = entries.len();
        let mut returned = 0_usize;
        let mut used = 0_usize;
        let mut document_key_value_bytes = 0_usize;
        let mut cursor_index = None;
        let mut stopped = false;
        let mut visitor_error: Option<E> = None;
        if !entries.is_empty() {
            let peek_limit = if options.unique_equality {
                1
            } else {
                options
                    .candidate_limit
                    .map_or(entries_read, |cap| cap.min(entries_read))
            };
            let mut doc_key_buf = PrefixKeyBuf::<MAX_DOC_KEY_STACK>::new(doc_prefix);
            let mut peek_key_buf = PrefixKeyBuf::<MAX_DOC_KEY_STACK>::new(doc_prefix);
            let mut unique_key_buf =
                unique_prefix.map(PrefixKeyBuf::<{ crate::record::MAX_KEY }>::new);
            self.with_point_reader(|reader| {
                for index in 0..entries_read {
                    if options.unique_equality && returned != 0 {
                        return Err(invalid_data("unique index contains multiple matches"));
                    }
                    if options
                        .candidate_limit
                        .is_some_and(|limit| returned >= limit)
                    {
                        return Err(key::invalid("document scan exceeds the operation limit"));
                    }
                    let entry = entries.get(index);
                    let id = entry_id(entry, index_prefix, definition.fields.len())?;
                    let document_key = doc_key_buf.with_suffix(id.as_bytes());
                    let mut next_document_bytes = document_key_value_bytes;
                    let upcoming_end = peek_limit.max(index + 1).min(entries_read);
                    let encoded = reader
                        .read_document_bytes_with_peek(
                            document_key,
                            |len| {
                                next_document_bytes = document_key_value_bytes
                                    .checked_add(document_key.len())
                                    .and_then(|total| total.checked_add(len))
                                    .ok_or_else(|| {
                                        key::invalid("document scan byte count overflow")
                                    })?;
                                if options
                                    .byte_limit
                                    .is_some_and(|limit| next_document_bytes > limit)
                                {
                                    return Err(key::invalid(
                                        "document scan exceeds the operation limit",
                                    ));
                                }
                                if options.reject_response_overflow
                                    && options.response_byte_limit.is_some_and(|limit| {
                                        used.checked_add(id.len())
                                            .and_then(|total| total.checked_add(len))
                                            .is_none_or(|total| total > limit)
                                    })
                                {
                                    return Err(key::invalid(
                                        "lookup response exceeds its byte limit",
                                    ));
                                }
                                Ok(())
                            },
                            |push_key| {
                                for next_index in index + 1..upcoming_end {
                                    let next_entry = entries.get(next_index);
                                    let Ok(next_id) =
                                        entry_id(next_entry, index_prefix, definition.fields.len())
                                    else {
                                        break;
                                    };
                                    let next_key = peek_key_buf.with_suffix(next_id.as_bytes());
                                    if !push_key(next_key) {
                                        break;
                                    }
                                }
                            },
                        )?
                        .ok_or_else(|| invalid_data("index references a missing document"))?;
                    let next_used = used
                        .checked_add(id.len())
                        .and_then(|n| n.checked_add(encoded.len()))
                        .ok_or_else(|| key::invalid("query response size overflow"))?;
                    if options
                        .response_byte_limit
                        .is_some_and(|limit| next_used > limit)
                    {
                        break;
                    }
                    document_key_value_bytes = next_document_bytes;
                    let stored = Self::decode_stored(&encoded, &metadata.schema)?;
                    index::verify_entry_stored(
                        definition,
                        &stored,
                        index_prefix,
                        id,
                        entry,
                        tuple_buf,
                    )?;
                    if let Some(unique_buf) = unique_key_buf.as_mut() {
                        let unique_key = unique_buf.with_suffix(tuple_buf.as_slice());
                        if reader.read_unique_owner(unique_key)?.as_deref() != Some(id.as_bytes()) {
                            return Err(invalid_data("invalid unique index entry"));
                        }
                    }
                    used = next_used;
                    returned += 1;
                    cursor_index = Some(index);
                    match visit(id, stored) {
                        Ok(ControlFlow::Continue(())) => {}
                        Ok(ControlFlow::Break(())) => {
                            stopped = true;
                            break;
                        }
                        Err(err) => {
                            visitor_error = Some(err);
                            break;
                        }
                    }
                }
                Ok(())
            })?;
        }
        if let Some(err) = visitor_error {
            return Ok(Err(err));
        }
        if nonempty_entry && returned == entries_read {
            return Err(invalid_data("invalid index entry value"));
        }
        Ok(Ok(StoredPageOutcome {
            entries_read,
            returned,
            cursor_index,
            document_key_value_bytes,
            response_bytes: used,
            stopped,
        }))
    }
}

#[cfg(test)]
mod query_helper_tests {
    use super::*;
    use crate::document::PartialFilter;

    #[test]
    fn memoized_cursor_scope_matches_direct_sha256() {
        let defs = [
            IndexDefinition {
                name: "idx_a".into(),
                fields: vec![vec!["a".into()]],
                unique: false,
                partial: None,
            },
            IndexDefinition {
                name: "idx_a".into(),
                fields: vec![vec!["a".into()]],
                unique: true,
                partial: None,
            },
            IndexDefinition {
                name: "idx_p".into(),
                fields: vec![vec!["a".into()], vec!["b".into()]],
                unique: false,
                partial: Some(PartialFilter::Equals {
                    path: vec!["ok".into()],
                    value: IndexValue::Boolean(true),
                }),
            },
        ];
        for collection in ["c1", "c2", "items1"] {
            for def in &defs {
                let mut expected = Sha256::new();
                expected.update(CURSOR_MAGIC);
                expected.update(serde_json::to_vec(&(collection, def)).unwrap());
                let expected: [u8; 32] = expected.finalize().into();
                // Hit both cold and warm memo paths.
                assert_eq!(cursor_scope(collection, def).unwrap(), expected);
                assert_eq!(cursor_scope(collection, def).unwrap(), expected);
            }
        }
    }

    #[test]
    fn consume_value_validates_escaped_and_oversized_strings_without_allocating() {
        let mut encoded = Vec::new();
        for text in [
            "",
            "hello",
            "héllo😀",
            "\0",
            "a\0b\0\0c",
            &"a".repeat(256),
            &format!("{}\0{}", "a".repeat(127), "b".repeat(128)),
        ] {
            encoded.clear();
            IndexValue::String(text.into())
                .encode(&mut encoded)
                .unwrap();
            assert_eq!(consume_value(&encoded).unwrap(), encoded.len(), "{text:?}");
        }
        // 257-byte string (unescaped and escaped) must fail.
        let mut too_long = vec![0x40];
        too_long.extend(std::iter::repeat_n(b'a', 257));
        too_long.extend_from_slice(&[0, 0]);
        assert_eq!(
            consume_value(&too_long).unwrap_err().to_string(),
            "index string is too long"
        );
        let mut too_long_nul = vec![0x40];
        too_long_nul.extend(std::iter::repeat_n(b'a', 256));
        too_long_nul.extend_from_slice(&[0, 0xff, 0, 0]);
        assert_eq!(
            consume_value(&too_long_nul).unwrap_err().to_string(),
            "index string is too long"
        );
        // Invalid UTF-8 (unescaped and escaped) must fail.
        assert_eq!(
            consume_value(&[0x40, 0xff, 0, 0]).unwrap_err().to_string(),
            "invalid UTF-8 index string"
        );
        assert_eq!(
            consume_value(&[0x40, 0, 0xff, 0xff, 0, 0])
                .unwrap_err()
                .to_string(),
            "invalid UTF-8 index string"
        );
        // Invalid escape and truncation must keep exact error messages.
        assert_eq!(
            consume_value(&[0x40, b'a', 0, 1, 0, 0])
                .unwrap_err()
                .to_string(),
            "invalid index escape"
        );
        assert_eq!(
            consume_value(&[0x40, b'a', 0]).unwrap_err().to_string(),
            "truncated index escape"
        );
        assert_eq!(
            consume_value(&[0x40, b'a']).unwrap_err().to_string(),
            "truncated index string"
        );
    }
}
