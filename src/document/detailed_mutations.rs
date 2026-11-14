//! Opt-in structured outcomes for common single-call document mutations.
use super::{
    BulkOperation, BulkResult, DocumentDatabase, DocumentTransaction, DocumentTransactionMode,
    DocumentUpdate, FieldCondition,
};
use crate::TransactionFailure;
use serde_json::Value;
use std::io;

impl DocumentDatabase<'_> {
    // Match the conservative, no-deadline transaction used by the existing
    // convenience methods; this does not add retries or relax commit checks.
    fn mutation_detailed<T>(
        &self,
        action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> io::Result<T>,
    ) -> Result<T, TransactionFailure> {
        self.transaction_detailed(DocumentTransactionMode::Conservative, None, None, action)
    }

    /// Inserts a new document, reporting whether its commit outcome is known.
    ///
    /// `Callback` and `Rejected` mean this call's staged document and index
    /// changes were not submitted. `OutcomeUnknown` needs reconciliation
    /// before retry. Independent checkpoint maintenance may have effects, and
    /// a rejection does not establish that the store is healthy.
    pub fn insert_detailed(
        &self,
        collection: &str,
        id: &str,
        document: &Value,
    ) -> Result<(), TransactionFailure> {
        self.mutation_detailed(|tx| tx.insert(collection, id, document))
    }

    /// Replaces a document, or inserts it when `upsert` is true, with the
    /// same failure and reconciliation semantics as [`Self::insert_detailed`].
    pub fn replace_detailed(
        &self,
        collection: &str,
        id: &str,
        document: &Value,
        upsert: bool,
    ) -> Result<(), TransactionFailure> {
        self.mutation_detailed(|tx| tx.replace(collection, id, document, upsert))
    }

    /// Applies conditional ordered updates with structured commit outcomes.
    /// `Ok(false)` means the document was absent without upsert or a condition
    /// did not match; an unknown outcome still requires reconciliation before
    /// a non-idempotent retry.
    pub fn update_if_detailed(
        &self,
        collection: &str,
        id: &str,
        conditions: &[FieldCondition],
        updates: &[DocumentUpdate],
        upsert: bool,
    ) -> Result<bool, TransactionFailure> {
        self.mutation_detailed(|tx| tx.update_if(collection, id, conditions, updates, upsert))
    }

    /// Deletes a document with structured commit outcomes. `Ok(false)` means
    /// the document was absent; reconcile unknown outcomes before retrying.
    pub fn delete_detailed(&self, collection: &str, id: &str) -> Result<bool, TransactionFailure> {
        self.mutation_detailed(|tx| tx.delete(collection, id))
    }

    /// Executes a bounded, atomic document batch with structured commit
    /// outcomes. A failed member aborts this call's staged batch; an unknown
    /// commit outcome must be reconciled before retrying the whole batch.
    pub fn bulk_detailed(
        &self,
        operations: &[BulkOperation<'_>],
    ) -> Result<Vec<BulkResult>, TransactionFailure> {
        self.mutation_detailed(|tx| tx.bulk(operations))
    }
}
