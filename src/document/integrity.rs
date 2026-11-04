//! Explicit bounded index verification and atomic reconstruction.
use super::{index, key, transaction::DocumentTransaction};
use std::collections::BTreeMap;
use std::io;

const MAX_DOCUMENTS: usize = 256;
const MAX_INDEX_KEYS: usize = 8192;
const MAX_BYTES: usize = 32 * 1024 * 1024;

/// Work completed by a successful bounded collection verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionIntegrity {
    /// Documents inspected.
    pub documents: usize,
    /// Index definitions inspected.
    pub indexes: usize,
    /// Secondary and unique index keys verified.
    pub index_keys: usize,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl DocumentTransaction<'_, '_> {
    /// Checks every document and index entry in a bounded collection. This
    /// detects missing, unexpected, duplicate and incorrect index mappings.
    /// Checksums are not authentication and this does not repair corruption.
    pub fn verify_collection(&self, collection: &str) -> io::Result<CollectionIntegrity> {
        let metadata = self.metadata(collection)?;
        let documents =
            self.document_rows(collection, &metadata.schema, MAX_DOCUMENTS, MAX_BYTES)?;
        let document_count = documents.len();
        let mut expected = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut expected_bytes = 0_usize;
        for (id, document) in &documents {
            for definition in &metadata.indexes {
                let keys = index::keys(collection, definition, document, id)
                    .map_err(|_| invalid("stored document cannot be represented by its index"))?;
                if let Some(keys) = keys {
                    for (key, value) in std::iter::once((keys.entry, Vec::new())).chain(
                        keys.unique
                            .into_iter()
                            .map(|key| (key, id.as_bytes().to_vec())),
                    ) {
                        expected_bytes = expected_bytes
                            .checked_add(key.len())
                            .and_then(|n| n.checked_add(value.len()))
                            .ok_or_else(|| key::invalid("verification size overflow"))?;
                        if expected.len() >= MAX_INDEX_KEYS || expected_bytes > MAX_BYTES {
                            return Err(key::invalid("verification exceeds index-key limits"));
                        }
                        if expected.insert(key, value).is_some() {
                            return Err(invalid("duplicate unique index value"));
                        }
                    }
                }
            }
        }
        let key_count = expected.len();
        let mut seen = 0_usize;
        let mut actual_bytes = 0_usize;
        for definition in &metadata.indexes {
            for kind in *b"IU" {
                let prefix = key::index(kind, collection, &definition.name)?;
                let end = key::end(&prefix);
                let mut cursor = prefix;
                loop {
                    let page = self.scan(&cursor, &end, 128)?;
                    if page.is_empty() {
                        break;
                    }
                    for (key, value) in page {
                        seen += 1;
                        actual_bytes = actual_bytes
                            .checked_add(key.len())
                            .and_then(|n| n.checked_add(value.len()))
                            .ok_or_else(|| key::invalid("verification size overflow"))?;
                        if seen > MAX_INDEX_KEYS || actual_bytes > MAX_BYTES {
                            return Err(key::invalid("verification exceeds index-key limits"));
                        }
                        cursor = key.clone();
                        if expected.remove(&key).as_deref() != Some(value.as_slice()) {
                            return Err(invalid("unexpected or incorrect index entry"));
                        }
                    }
                }
            }
        }
        if !expected.is_empty() {
            return Err(invalid("document index entries are missing"));
        }
        Ok(CollectionIntegrity {
            documents: document_count,
            indexes: metadata.indexes.len(),
            index_keys: key_count,
        })
    }

    /// Rebuilds a named index in one atomic transaction, within both drop and
    /// build limits. It requires valid documents and satisfiable uniqueness.
    pub fn rebuild_index(&mut self, collection: &str, name: &str) -> io::Result<()> {
        self.mutate(|this| {
            let definition = this
                .metadata(collection)?
                .indexes
                .into_iter()
                .find(|index| index.name == name)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index does not exist"))?;
            this.drop_index(collection, name)?;
            this.create_index(collection, definition)
        })
    }
}
