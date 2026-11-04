//! Bounded indexed document queries with explicit ordering and pagination.
use super::{
    index::{self, IndexValue},
    key,
    transaction::DocumentTransaction,
};
use serde_json::Value;
use std::io;

const MAX_QUERY_DOCUMENTS: usize = 256;
const MAX_QUERY_BYTES: usize = 16 * 1024 * 1024;

/// An inclusive or exclusive bound on the first field after equality fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexBound {
    /// A typed scalar in the index's ordering.
    pub value: IndexValue,
    /// Whether the bound includes matching values.
    pub inclusive: bool,
}

/// An opaque resume position from an earlier indexed page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexCursor(Vec<u8>);

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
        let definition = metadata
            .indexes
            .iter()
            .find(|index| index.name == name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index does not exist"))?;
        if query.equal.len() > definition.fields.len()
            || (query.equal.len() == definition.fields.len()
                && (query.lower.is_some() || query.upper.is_some()))
        {
            return Err(key::invalid("query bounds exceed the index field count"));
        }
        let index_prefix = key::index(b'I', collection, name)?;
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
        crate::record::validate_key(&after)?;
        crate::record::validate_key(&before)?;
        if let Some(cursor) = &query.after {
            if !cursor.0.starts_with(&equal_prefix) || cursor.0 >= before || cursor.0 < after {
                return Err(key::invalid("cursor is outside the query bounds"));
            }
            after = cursor.0.clone();
        }
        let plan = |entries_read, returned| IndexQueryPlan {
            index: definition.name.clone(),
            equality_fields: query.equal.len(),
            has_range: query.lower.is_some() || query.upper.is_some(),
            partial: definition.partial.is_some(),
            entries_read,
            returned,
        };
        if after >= before {
            return Ok(DocumentPage {
                documents: Vec::new(),
                next: None,
                plan: plan(0, 0),
            });
        }
        let rows = self.scan(&after, &before, query.limit)?;
        let entries_read = rows.len();
        let mut documents = Vec::with_capacity(rows.len());
        let mut used = 0_usize;
        let mut cursor = None;
        for (entry, value) in rows {
            if !value.is_empty() {
                return Err(invalid_data("invalid index entry value"));
            }
            let id = entry_id(&entry, &index_prefix, definition.fields.len())?;
            let encoded = self
                .read(&key::document(collection, id)?)?
                .ok_or_else(|| invalid_data("index references a missing document"))?;
            let next_used = used
                .checked_add(id.len())
                .and_then(|n| n.checked_add(encoded.len()))
                .ok_or_else(|| key::invalid("query response size overflow"))?;
            if next_used > MAX_QUERY_BYTES {
                break;
            }
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
                if self.read(&unique)?.as_deref() != Some(id.as_bytes()) {
                    return Err(invalid_data("invalid unique index entry"));
                }
            }
            used = next_used;
            documents.push(DocumentEntry {
                id: id.to_owned(),
                document,
            });
            cursor = Some(IndexCursor(entry));
        }
        let plan = plan(entries_read, documents.len());
        Ok(DocumentPage {
            documents,
            next: cursor,
            plan,
        })
    }
}
