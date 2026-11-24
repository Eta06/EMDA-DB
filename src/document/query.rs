//! Bounded indexed document queries with explicit ordering and pagination.
use super::{
    index::{self, IndexDefinition, IndexValue},
    key,
    transaction::{DocumentTransaction, Metadata},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io;

const MAX_QUERY_DOCUMENTS: usize = 256;
const MAX_QUERY_BYTES: usize = 16 * 1024 * 1024;
const CURSOR_MAGIC: &[u8; 8] = b"EMDAIC01";
const CURSOR_HEADER_BYTES: usize = CURSOR_MAGIC.len() + 32;
const CURSOR_CHECKSUM_BYTES: usize = 4;

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

// The planner needs the original encoded document and key lengths to enforce
// the collection scan's byte budget without serializing parsed JSON again.
pub(super) struct IndexScan {
    pub next: Option<Vec<u8>>,
    pub plan: IndexQueryPlan,
    pub document_key_value_bytes: usize,
}

pub(super) struct IndexScanOptions<'a> {
    pub after: Option<&'a [u8]>,
    pub byte_limit: Option<usize>,
    pub candidate_limit: Option<usize>,
    pub unique_equality: bool,
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
            let mut decoded = Vec::new();
            loop {
                let byte = *bytes
                    .get(offset)
                    .ok_or_else(|| invalid_data("truncated index string"))?;
                offset += 1;
                if byte != 0 {
                    decoded.push(byte);
                } else {
                    let escaped = *bytes
                        .get(offset)
                        .ok_or_else(|| invalid_data("truncated index escape"))?;
                    offset += 1;
                    match escaped {
                        0 => break,
                        0xff => decoded.push(0),
                        _ => return Err(invalid_data("invalid index escape")),
                    }
                }
                if decoded.len() > 256 {
                    return Err(invalid_data("index string is too long"));
                }
            }
            std::str::from_utf8(&decoded)
                .map_err(|_| invalid_data("invalid UTF-8 index string"))?;
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
        let mut scope = Sha256::new();
        scope.update(CURSOR_MAGIC);
        scope.update(serde_json::to_vec(&(collection, definition)).map_err(io::Error::other)?);
        let scope: [u8; 32] = scope.finalize().into();
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

    // The visitor's effects must remain private until the page passes all
    // index checks and admission limits.
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
        let mut equal_prefix = index_prefix.clone();
        for value in &query.equal {
            value.encode(&mut equal_prefix)?;
        }
        let mut after = equal_prefix.clone();
        let mut before = key::end(&equal_prefix);
        if let Some(lower) = &query.lower {
            let mut bound = equal_prefix.clone();
            lower.value.encode(&mut bound)?;
            after = if lower.inclusive {
                bound
            } else {
                key::end(&bound)
            };
        }
        if let Some(upper) = &query.upper {
            let mut bound = equal_prefix.clone();
            upper.value.encode(&mut bound)?;
            before = if upper.inclusive {
                key::end(&bound)
            } else {
                bound
            };
        }
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
            });
        }
        let mut nonempty_entry = false;
        let rows = self.scan_admitted(after, &before, query.limit, |_, len| {
            nonempty_entry = len != 0;
            !nonempty_entry
        })?;
        let entries_read = rows.len();
        let mut returned = 0;
        let mut used = 0_usize;
        let mut document_key_value_bytes = 0_usize;
        let mut cursor = None;
        for (entry, _) in rows {
            if options.unique_equality && returned != 0 {
                return Err(invalid_data("unique index contains multiple matches"));
            }
            if options
                .candidate_limit
                .is_some_and(|limit| returned >= limit)
            {
                return Err(key::invalid("document scan exceeds the operation limit"));
            }
            let id = entry_id(&entry, &index_prefix, definition.fields.len())?;
            let document_key = key::document(collection, id)?;
            let mut next_document_bytes = document_key_value_bytes;
            let encoded = self
                .read_document_bytes(&document_key, |len| {
                    next_document_bytes = document_key_value_bytes
                        .checked_add(document_key.len())
                        .and_then(|total| total.checked_add(len))
                        .ok_or_else(|| key::invalid("document scan byte count overflow"))?;
                    if options
                        .byte_limit
                        .is_some_and(|limit| next_document_bytes > limit)
                    {
                        return Err(key::invalid("document scan exceeds the operation limit"));
                    }
                    Ok(())
                })?
                .ok_or_else(|| invalid_data("index references a missing document"))?;
            let next_used = used
                .checked_add(id.len())
                .and_then(|n| n.checked_add(encoded.len()))
                .ok_or_else(|| key::invalid("query response size overflow"))?;
            if next_used > MAX_QUERY_BYTES {
                break;
            }
            document_key_value_bytes = next_document_bytes;
            let document = Self::decode_document(&encoded, &metadata.schema)?;
            let expected = index::keys(collection, definition, &document, id)
                .map_err(|_| invalid_data("document cannot be represented by its index"))?
                .ok_or_else(|| invalid_data("document does not belong to its index"))?;
            if expected.entry != entry {
                return Err(invalid_data(
                    "document index entry does not match the document",
                ));
            }
            if let Some(unique) = expected.unique {
                if self.read_unique_owner(&unique)?.as_deref() != Some(id.as_bytes()) {
                    return Err(invalid_data("invalid unique index entry"));
                }
            }
            used = next_used;
            visit(DocumentEntry {
                id: id.to_owned(),
                document,
            });
            returned += 1;
            cursor = Some(entry);
        }
        if nonempty_entry && returned == entries_read {
            return Err(invalid_data("invalid index entry value"));
        }
        Ok(IndexScan {
            next: cursor,
            plan: plan(entries_read, returned),
            document_key_value_bytes,
        })
    }
}
