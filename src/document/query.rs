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
const INLINE_INDEX_BYTES: usize = 512;
const INLINE_INDEX_ENTRIES: usize = 16;

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

fn write_collection_prefix(out: &mut TupleBuf, kind: u8, name: &str) -> io::Result<()> {
    key::name(name)?;
    out.clear();
    out.extend_from_slice(key::PREFIX);
    out.extend_from_slice(&[kind]);
    out.extend_from_slice(&(name.len() as u16).to_be_bytes());
    out.extend_from_slice(name.as_bytes());
    Ok(())
}

fn write_index_prefix(
    out: &mut TupleBuf,
    kind: u8,
    collection: &str,
    name: &str,
) -> io::Result<()> {
    write_collection_prefix(out, kind, collection)?;
    key::name(name)?;
    out.extend_from_slice(&(name.len() as u16).to_be_bytes());
    out.extend_from_slice(name.as_bytes());
    Ok(())
}

fn index_bounds_into(
    index_prefix: &[u8],
    equal: &[IndexValue],
    lower: Option<&IndexBound>,
    upper: Option<&IndexBound>,
    after: &mut TupleBuf,
    before: &mut TupleBuf,
) -> io::Result<()> {
    after.clear();
    after.extend_from_slice(index_prefix);
    for value in equal {
        value.encode_to_buf(after)?;
    }
    before.clear();
    before.extend_from_slice(after.as_slice());
    if let Some(lower) = lower {
        lower.value.encode_to_buf(after)?;
        if !lower.inclusive {
            after.end_in_place();
        }
    }
    if let Some(upper) = upper {
        upper.value.encode_to_buf(before)?;
        if upper.inclusive {
            before.end_in_place();
        }
    } else {
        before.end_in_place();
    }
    Ok(())
}

/// Skips `fields` encoded scalar tags without UTF-8 validation, returning the
/// raw `(tuple, id)` slices for read-ahead peek hints only. Every entry that
/// is actually read still passes full `entry_id` validation first.
fn peek_tuple_and_id<'a>(
    entry: &'a [u8],
    prefix: &[u8],
    fields: usize,
) -> Option<(&'a [u8], &'a [u8])> {
    let body = entry.strip_prefix(prefix)?;
    let mut offset = 0_usize;
    for _ in 0..fields {
        match body.get(offset)? {
            0x10 => offset += 1,
            0x20 => {
                if offset + 2 > body.len() {
                    return None;
                }
                offset += 2;
            }
            0x30 => {
                if offset + 9 > body.len() {
                    return None;
                }
                offset += 9;
            }
            0x40 => {
                offset += 1;
                loop {
                    let byte = *body.get(offset)?;
                    offset += 1;
                    if byte == 0 {
                        let escaped = *body.get(offset)?;
                        offset += 1;
                        match escaped {
                            0 => break,
                            0xff => {}
                            _ => return None,
                        }
                    }
                }
            }
            _ => return None,
        }
    }
    let id = &body[offset..];
    if id.is_empty() || id.len() > key::MAX_ID_BYTES {
        return None;
    }
    Some((&body[..offset], id))
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
        let metadata = self.metadata_fast(collection)?;
        let definition = index_definition(&metadata, name)?;
        let scope = cursor_scope(collection, definition)?;
        if let Some(cursor) = &query.after {
            if cursor.scope != scope {
                return Err(key::invalid(
                    "cursor belongs to a different index definition",
                ));
            }
            let mut prefix = TupleBuf::new();
            write_index_prefix(&mut prefix, b'I', collection, name)?;
            entry_id(&cursor.position, prefix.as_slice(), definition.fields.len())
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
        mut visit: impl FnMut(&str, &StoredDocument<'_>) -> Result<ControlFlow<()>, E>,
    ) -> io::Result<Result<(), E>> {
        let mut page_limit = next_page_limit();
        if page_limit == 0 {
            return Ok(Ok(()));
        }
        let metadata = self.metadata_fast(collection)?;
        let definition = index_definition(&metadata, name)?;
        if equal.len() > definition.fields.len()
            || (equal.len() == definition.fields.len() && (lower.is_some() || upper.is_some()))
        {
            return Err(key::invalid("query bounds exceed the index field count"));
        }
        let mut index_prefix = TupleBuf::new();
        write_index_prefix(&mut index_prefix, b'I', collection, &definition.name)?;
        let mut initial_after = TupleBuf::new();
        let mut before = TupleBuf::new();
        index_bounds_into(
            index_prefix.as_slice(),
            equal,
            lower,
            upper,
            &mut initial_after,
            &mut before,
        )?;
        if initial_after.len() > crate::record::MAX_KEY {
            initial_after.truncate(crate::record::MAX_KEY);
        }
        if before.len() > crate::record::MAX_KEY {
            before.truncate(crate::record::MAX_KEY);
            before.end_in_place();
        }
        if initial_after.as_slice() >= before.as_slice() {
            return Ok(Ok(()));
        }
        let mut doc_prefix = TupleBuf::new();
        write_collection_prefix(&mut doc_prefix, b'D', collection)?;
        let mut unique_prefix = TupleBuf::new();
        let unique_prefix_slice = if definition.unique {
            write_index_prefix(&mut unique_prefix, b'U', collection, &definition.name)?;
            Some(unique_prefix.as_slice())
        } else {
            None
        };
        let mut entries = IndexEntries::new();
        let mut tuple_buf = TupleBuf::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let limit = page_limit.min(MAX_QUERY_DOCUMENTS);
            let page_after = cursor.as_deref().unwrap_or(initial_after.as_slice());
            let outcome = match self.visit_index_page_stored(
                definition,
                &metadata,
                index_prefix.as_slice(),
                doc_prefix.as_slice(),
                unique_prefix_slice,
                page_after,
                before.as_slice(),
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
                |id, stored| visit(id, stored),
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
        let mut index_prefix = TupleBuf::new();
        write_index_prefix(&mut index_prefix, b'I', collection, &definition.name)?;
        let mut after_buf = TupleBuf::new();
        let mut before = TupleBuf::new();
        let after = if let Some(cursor) = options.after {
            let IndexBounds {
                equal_prefix,
                after: exact_after,
                before: exact_before,
            } = index_bounds(
                index_prefix.as_slice(),
                &query.equal,
                query.lower.as_ref(),
                query.upper.as_ref(),
            )?;
            if !cursor.starts_with(&equal_prefix)
                || cursor >= exact_before.as_slice()
                || cursor < exact_after.as_slice()
            {
                return Err(key::invalid("cursor is outside the query bounds"));
            }
            before.extend_from_slice(&exact_before);
            cursor
        } else {
            index_bounds_into(
                index_prefix.as_slice(),
                &query.equal,
                query.lower.as_ref(),
                query.upper.as_ref(),
                &mut after_buf,
                &mut before,
            )?;
            after_buf.as_slice()
        };
        // No stored key can extend a maximum-length prefix. An exclusive
        // lower bound can drop its remaining bytes. An exclusive upper bound
        // must also include a key equal to its truncated prefix.
        let after = &after[..after.len().min(crate::record::MAX_KEY)];
        if before.len() > crate::record::MAX_KEY {
            before.truncate(crate::record::MAX_KEY);
            before.end_in_place();
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
        let mut doc_prefix = TupleBuf::new();
        write_collection_prefix(&mut doc_prefix, b'D', collection)?;
        let mut unique_prefix = TupleBuf::new();
        let unique_prefix_slice = if definition.unique {
            write_index_prefix(&mut unique_prefix, b'U', collection, &definition.name)?;
            Some(unique_prefix.as_slice())
        } else {
            None
        };
        let mut entries = IndexEntries::new();
        let mut tuple_buf = TupleBuf::new();
        let outcome = self
            .visit_index_page_stored(
                definition,
                metadata,
                index_prefix.as_slice(),
                doc_prefix.as_slice(),
                unique_prefix_slice,
                after,
                before.as_slice(),
                query.limit,
                &options,
                &mut entries,
                &mut tuple_buf,
                |id, stored| {
                    let document = match stored {
                        StoredDocument::Flat(flat) => Value::Object(flat.to_map()),
                        StoredDocument::Full(document) => std::mem::take(document),
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
        mut visit: impl FnMut(&str, &mut StoredDocument<'_>) -> Result<ControlFlow<()>, E>,
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
            let mut doc_key_buf = TupleBuf::new();
            doc_key_buf.extend_from_slice(doc_prefix);
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
                    doc_key_buf.truncate(doc_prefix.len());
                    doc_key_buf.extend_from_slice(id.as_bytes());
                    let document_key = doc_key_buf.as_slice();
                    let doc_bytes_before = document_key_value_bytes;
                    let used_before = used;
                    let mut next_document_bytes = doc_bytes_before;
                    let upcoming_end = peek_limit.max(index + 1).min(entries_read);
                    let has_upcoming = index + 1 < upcoming_end;
                    let found = reader.visit_document_bytes_with_peek(
                        document_key,
                        has_upcoming,
                        |len| {
                            next_document_bytes = doc_bytes_before
                                .checked_add(document_key.len())
                                .and_then(|total| total.checked_add(len))
                                .ok_or_else(|| key::invalid("document scan byte count overflow"))?;
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
                                    used_before
                                        .checked_add(id.len())
                                        .and_then(|total| total.checked_add(len))
                                        .is_none_or(|total| total > limit)
                                })
                            {
                                return Err(key::invalid("lookup response exceeds its byte limit"));
                            }
                            Ok(())
                        },
                        |push_key| {
                            let mut peek_key_buf = TupleBuf::new();
                            peek_key_buf.extend_from_slice(doc_prefix);
                            for next_index in index + 1..upcoming_end {
                                let next_entry = entries.get(next_index);
                                let Some((_, next_id)) = peek_tuple_and_id(
                                    next_entry,
                                    index_prefix,
                                    definition.fields.len(),
                                ) else {
                                    break;
                                };
                                peek_key_buf.truncate(doc_prefix.len());
                                peek_key_buf.extend_from_slice(next_id);
                                if !push_key(peek_key_buf.as_slice()) {
                                    break;
                                }
                            }
                        },
                        |encoded, unique_verifier| {
                            let next_used = used_before
                                .checked_add(id.len())
                                .and_then(|n| n.checked_add(encoded.len()))
                                .ok_or_else(|| key::invalid("query response size overflow"))?;
                            if options
                                .response_byte_limit
                                .is_some_and(|limit| next_used > limit)
                            {
                                return Ok(None);
                            }
                            let mut stored = Self::decode_stored(encoded, &metadata.schema)?;
                            index::verify_entry_stored(
                                definition,
                                &stored,
                                index_prefix,
                                id,
                                entry,
                                tuple_buf,
                            )?;
                            if let Some(unique_prefix) = unique_prefix {
                                let mut unique_key_buf = TupleBuf::new();
                                unique_key_buf.extend_from_slice(unique_prefix);
                                unique_key_buf.extend_from_slice(tuple_buf.as_slice());
                                unique_verifier.verify_owner(
                                    unique_key_buf.as_slice(),
                                    id,
                                    has_upcoming,
                                    |push_key| {
                                        let mut peek_unique_buf = TupleBuf::new();
                                        peek_unique_buf.extend_from_slice(unique_prefix);
                                        for next_index in index + 1..upcoming_end {
                                            let next_entry = entries.get(next_index);
                                            let Some((next_tuple, _)) = peek_tuple_and_id(
                                                next_entry,
                                                index_prefix,
                                                definition.fields.len(),
                                            ) else {
                                                break;
                                            };
                                            if unique_prefix.len() + next_tuple.len()
                                                > crate::record::MAX_KEY
                                            {
                                                break;
                                            }
                                            peek_unique_buf.truncate(unique_prefix.len());
                                            peek_unique_buf.extend_from_slice(next_tuple);
                                            if !push_key(peek_unique_buf.as_slice()) {
                                                break;
                                            }
                                        }
                                    },
                                )?;
                            }
                            match visit(id, &mut stored) {
                                Ok(ControlFlow::Continue(())) => {}
                                Ok(ControlFlow::Break(())) => {
                                    stopped = true;
                                }
                                Err(err) => {
                                    visitor_error = Some(err);
                                }
                            }
                            Ok(Some(next_used))
                        },
                    )?;
                    let Some(outcome) = found else {
                        return Err(invalid_data("index references a missing document"));
                    };
                    let Some(next_used) = outcome else {
                        break;
                    };
                    document_key_value_bytes = next_document_bytes;
                    used = next_used;
                    returned += 1;
                    cursor_index = Some(index);
                    if stopped || visitor_error.is_some() {
                        break;
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

    #[test]
    fn stack_index_bounds_match_allocating_bounds_including_max_key_truncation() {
        let long_s = "a".repeat(256);
        let max_s = "\u{7f}".repeat(256);
        let cases: Vec<(
            &str,
            &str,
            Vec<IndexValue>,
            Option<IndexBound>,
            Option<IndexBound>,
        )> = vec![
            ("items", "idx", vec![], None, None),
            (
                "items",
                "idx",
                vec![IndexValue::Integer(42)],
                Some(IndexBound {
                    value: IndexValue::String("ab".into()),
                    inclusive: true,
                }),
                Some(IndexBound {
                    value: IndexValue::String("cd".into()),
                    inclusive: false,
                }),
            ),
            (
                "items",
                "idx",
                vec![IndexValue::Boolean(true)],
                Some(IndexBound {
                    value: IndexValue::Integer(i64::MAX),
                    inclusive: false,
                }),
                Some(IndexBound {
                    value: IndexValue::Integer(i64::MAX),
                    inclusive: true,
                }),
            ),
            (
                "items",
                "idx",
                vec![
                    IndexValue::String(long_s.clone()),
                    IndexValue::String(long_s.clone()),
                    IndexValue::String(long_s.clone()),
                ],
                Some(IndexBound {
                    value: IndexValue::String(max_s.clone()),
                    inclusive: false,
                }),
                Some(IndexBound {
                    value: IndexValue::String(max_s.clone()),
                    inclusive: true,
                }),
            ),
        ];
        for (collection, name, equal, lower, upper) in cases {
            let prefix = key::index(b'I', collection, name).unwrap();
            let mut prefix_buf = TupleBuf::new();
            write_index_prefix(&mut prefix_buf, b'I', collection, name).unwrap();
            assert_eq!(prefix_buf.as_slice(), prefix.as_slice());

            let IndexBounds {
                after, mut before, ..
            } = index_bounds(&prefix, &equal, lower.as_ref(), upper.as_ref()).unwrap();
            let expected_after = &after[..after.len().min(crate::record::MAX_KEY)];
            if before.len() > crate::record::MAX_KEY {
                before.truncate(crate::record::MAX_KEY);
                before = key::end(&before);
            }
            let mut fast_after = TupleBuf::new();
            let mut fast_before = TupleBuf::new();
            index_bounds_into(
                &prefix,
                &equal,
                lower.as_ref(),
                upper.as_ref(),
                &mut fast_after,
                &mut fast_before,
            )
            .unwrap();
            if fast_after.len() > crate::record::MAX_KEY {
                fast_after.truncate(crate::record::MAX_KEY);
            }
            if fast_before.len() > crate::record::MAX_KEY {
                fast_before.truncate(crate::record::MAX_KEY);
                fast_before.end_in_place();
            }
            assert_eq!(fast_after.as_slice(), expected_after);
            assert_eq!(fast_before.as_slice(), before.as_slice());
        }
    }

    #[test]
    fn visit_index_stored_handles_order_staging_unique_multipage_and_conflicts_on_both_backends() {
        use crate::{CheckpointStore, CollectionSchema, DocumentDatabase, Store};
        use serde_json::json;

        fn exercise(db: &DocumentDatabase<'_>) {
            db.create_collection("items", CollectionSchema::default())
                .unwrap();
            db.create_index(
                "items",
                IndexDefinition {
                    name: "by_code".into(),
                    fields: vec![vec!["code".into()]],
                    unique: false,
                    partial: None,
                },
            )
            .unwrap();
            db.create_index(
                "items",
                IndexDefinition {
                    name: "by_rev".into(),
                    fields: vec![vec!["rev".into()]],
                    unique: true,
                    partial: None,
                },
            )
            .unwrap();
            // Insert 300 rows (> 256 page size), with `code` ascending with ID
            // (exercises cursor fast path), `rev` descending with ID (exercises
            // out-of-order peek cache & unique owner coalescing), and mix flat
            // rows with escaped-string / nested rows (exercises Full fallback).
            for chunk in (0..300_i64).collect::<Vec<_>>().chunks(50) {
                db.transaction(|tx| {
                    for &i in chunk {
                        let id = format!("r:{i:04}");
                        let doc = if i % 29 == 0 {
                            json!({"id": i, "code": i, "rev": 1000 - i, "note": "line1\nline2", "nested": {"k": i}})
                        } else {
                            json!({"id": i, "code": i, "rev": 1000 - i, "note": format!("n{i}")})
                        };
                        tx.insert("items", &id, &doc)?;
                    }
                    Ok(())
                })
                .unwrap();
            }

            // Full multi-page scan (> 256 rows) on non-unique ascending index.
            let mut ids = Vec::new();
            db.transaction_with_read_set(|tx| {
                tx.visit_index_stored(
                    "items",
                    "by_code",
                    &[],
                    None,
                    None,
                    false,
                    || 256,
                    |id, stored| {
                        let got = match stored {
                            StoredDocument::Flat(f) => f.get("id").and_then(|v| v.as_i64()),
                            StoredDocument::Full(v) => v.get("id").and_then(Value::as_i64),
                        };
                        ids.push((id.to_owned(), got.unwrap()));
                        Ok::<ControlFlow<()>, ()>(ControlFlow::Continue(()))
                    },
                )?
                .unwrap();
                Ok(())
            })
            .unwrap();
            assert_eq!(ids.len(), 300);
            for (i, (id, val)) in ids.iter().enumerate() {
                assert_eq!(id, &format!("r:{i:04}"));
                assert_eq!(*val, i as i64);
            }

            // Out-of-order range scan on UNIQUE index + staged update & delete + early stop.
            let mut rev_ids = Vec::new();
            db.transaction_with_read_set(|tx| {
                tx.replace(
                    "items",
                    "r:0010",
                    &json!({"id": 10, "code": 10, "rev": 990, "note": "staged"}),
                    false,
                )?;
                tx.delete("items", "r:0012")?;
                tx.visit_index_stored(
                    "items",
                    "by_rev",
                    &[],
                    Some(&IndexBound {
                        value: IndexValue::Integer(985),
                        inclusive: true,
                    }),
                    Some(&IndexBound {
                        value: IndexValue::Integer(995),
                        inclusive: true,
                    }),
                    false,
                    || 256,
                    |id, stored| {
                        let note = match &stored {
                            StoredDocument::Flat(f) => f.get("note").map(|v| format!("{v:?}")),
                            StoredDocument::Full(v) => v.get("note").map(ToString::to_string),
                        };
                        rev_ids.push((id.to_owned(), note));
                        Ok::<ControlFlow<()>, ()>(ControlFlow::Continue(()))
                    },
                )?
                .unwrap();
                Ok(())
            })
            .unwrap();
            // rev 985..=995 corresponds to ids 15,14,13,(12 deleted),11,10,9,8,7,6,5.
            let expected_ids: Vec<String> = [15, 14, 13, 11, 10, 9, 8, 7, 6, 5]
                .into_iter()
                .map(|i| format!("r:{i:04}"))
                .collect();
            assert_eq!(
                rev_ids.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
                expected_ids
            );
            assert!(
                rev_ids
                    .iter()
                    .find(|(id, _)| id == "r:0010")
                    .unwrap()
                    .1
                    .as_deref()
                    .unwrap()
                    .contains("staged")
            );
        }

        // Ordinary Store
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("store")).unwrap();
        let db = DocumentDatabase::new(&store);
        exercise(&db);
        // Verify read-set conflict detection on ordinary Store.
        let conflict = db.transaction_with_read_set(|tx| {
            tx.visit_index_stored(
                "items",
                "by_code",
                &[IndexValue::Integer(5)],
                None,
                None,
                false,
                || 2,
                |_, _| Ok::<ControlFlow<()>, ()>(ControlFlow::Continue(())),
            )?
            .unwrap();
            // Concurrent modification to the document read through the index.
            db.replace(
                "items",
                "r:0005",
                &json!({"id": 5, "code": 5, "rev": 995, "note": "concurrent"}),
                false,
            )?;
            tx.replace(
                "items",
                "r:0006",
                &json!({"id": 6, "code": 6, "rev": 994, "note": "mine"}),
                false,
            )?;
            Ok(())
        });
        assert!(crate::is_transaction_conflict(&conflict.unwrap_err()));

        // CheckpointStore (with persisted checkpoint blocks + suffix overlay)
        let ckpt_log = dir.path().join("ckpt-log");
        let ckpt_dir = dir.path().join("ckpt");
        let initial = Store::open(&ckpt_log).unwrap();
        let digest = initial.checkpoint_to(&ckpt_dir).unwrap();
        drop(initial);
        let ckpt = CheckpointStore::open(&ckpt_log, &ckpt_dir, &digest).unwrap();
        let cdb = DocumentDatabase::with_checkpoint_store(&ckpt);
        exercise(&cdb);
        // Re-checkpoint so the 300 rows live inside verified checkpoint blocks, then query again!
        let ckpt_dir2 = dir.path().join("ckpt2");
        let digest2 = ckpt.checkpoint_to(&ckpt_dir2).unwrap();
        drop(ckpt);
        let ckpt2 = crate::test_support::retry_lock(|| {
            CheckpointStore::open(&ckpt_log, &ckpt_dir2, &digest2)
        })
        .unwrap();
        let cdb2 = DocumentDatabase::with_checkpoint_store(&ckpt2);
        let mut count = 0_usize;
        cdb2.transaction_with_read_set(|tx| {
            tx.visit_index_stored(
                "items",
                "by_code",
                &[],
                Some(&IndexBound {
                    value: IndexValue::Integer(20),
                    inclusive: true,
                }),
                Some(&IndexBound {
                    value: IndexValue::Integer(35),
                    inclusive: false,
                }),
                false,
                || 256,
                |_, _| {
                    count += 1;
                    Ok::<ControlFlow<()>, ()>(ControlFlow::Continue(()))
                },
            )?
            .unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 15);
        // Also verify read-set conflict detection on CheckpointStore.
        let conflict = cdb2.transaction_with_read_set(|tx| {
            tx.visit_index_stored(
                "items",
                "by_code",
                &[IndexValue::Integer(25)],
                None,
                None,
                false,
                || 2,
                |_, _| Ok::<ControlFlow<()>, ()>(ControlFlow::Continue(())),
            )?
            .unwrap();
            cdb2.replace(
                "items",
                "r:0025",
                &json!({"id": 25, "code": 25, "rev": 975, "note": "concurrent"}),
                false,
            )?;
            tx.replace(
                "items",
                "r:0026",
                &json!({"id": 26, "code": 26, "rev": 974, "note": "mine"}),
                false,
            )?;
            Ok(())
        });
        assert!(crate::is_transaction_conflict(&conflict.unwrap_err()));
    }
}
