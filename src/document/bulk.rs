//! Atomic multi-document operations using one underlying DB02 commit.
use super::{
    DocumentTransaction, key,
    update::{DocumentUpdate, FieldCondition},
};
use serde_json::Value;
use std::io;

/// One operation in an atomic document batch.
pub enum BulkOperation<'a> {
    /// Insert an absent ID.
    Insert {
        collection: &'a str,
        id: &'a str,
        document: &'a Value,
    },
    /// Replace an ID, optionally inserting it.
    Replace {
        collection: &'a str,
        id: &'a str,
        document: &'a Value,
        upsert: bool,
    },
    /// Delete an ID when present.
    Delete { collection: &'a str, id: &'a str },
    /// Conditionally update an ID.
    Update {
        collection: &'a str,
        id: &'a str,
        conditions: &'a [FieldCondition],
        updates: &'a [DocumentUpdate],
        upsert: bool,
    },
}

/// The result of a successfully committed batch member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkResult {
    /// An insert or replacement was staged.
    Written,
    /// Whether a document existed and was deleted.
    Deleted(bool),
    /// Whether all conditions matched and an update was staged.
    Updated(bool),
}

impl DocumentTransaction<'_, '_> {
    /// Stages 1 to 128 operations in order. A failure aborts the entire
    /// transaction even if the caller handles the error. Normal transaction
    /// record-count and byte budgets also apply.
    pub fn bulk(&mut self, operations: &[BulkOperation<'_>]) -> io::Result<Vec<BulkResult>> {
        if operations.is_empty() || operations.len() > 128 {
            return self
                .mutate(|_| Err(key::invalid("bulk requires between 1 and 128 operations")));
        }
        let mut results = Vec::with_capacity(operations.len());
        for operation in operations {
            let result = match operation {
                BulkOperation::Insert {
                    collection,
                    id,
                    document,
                } => {
                    self.insert(collection, id, document)?;
                    BulkResult::Written
                }
                BulkOperation::Replace {
                    collection,
                    id,
                    document,
                    upsert,
                } => {
                    self.replace(collection, id, document, *upsert)?;
                    BulkResult::Written
                }
                BulkOperation::Delete { collection, id } => {
                    BulkResult::Deleted(self.delete(collection, id)?)
                }
                BulkOperation::Update {
                    collection,
                    id,
                    conditions,
                    updates,
                    upsert,
                } => BulkResult::Updated(
                    self.update_if(collection, id, conditions, updates, *upsert)?,
                ),
            };
            results.push(result);
        }
        Ok(results)
    }
}
