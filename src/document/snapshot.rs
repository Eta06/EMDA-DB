use super::{
    DocumentPage, DocumentQuery, DocumentQueryResult, DocumentTransaction, IndexQuery,
    PlannedDocumentQueryResult,
};
use crate::{CheckpointSnapshot, Snapshot};
use serde_json::Value;
use std::io;

/// Read-only document queries over an already captured storage snapshot.
/// All pages use its captured documents and indexes. The caller keeps the
/// underlying snapshot alive and controls its limits, deadline, and cancellation.
pub struct DocumentSnapshot<'a> {
    view: DocumentTransaction<'a, 'a>,
}

impl<'a> DocumentSnapshot<'a> {
    /// Borrows an ordinary-store snapshot.
    pub fn new(snapshot: &'a Snapshot<'a>) -> Self {
        Self {
            view: DocumentTransaction::ordinary_snapshot(snapshot),
        }
    }

    /// Borrows a checkpoint-store snapshot.
    pub fn with_checkpoint_snapshot(snapshot: &'a CheckpointSnapshot<'a>) -> Self {
        Self {
            view: DocumentTransaction::checkpoint_snapshot(snapshot),
        }
    }

    /// Reads one document from the captured view.
    pub fn get(&self, collection: &str, id: &str) -> io::Result<Option<Value>> {
        self.view.get(collection, id)
    }

    /// Executes a bounded collection scan over the captured view.
    pub fn query(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<DocumentQueryResult> {
        self.view.query(collection, query)
    }

    /// Executes a bounded, optionally indexed query over the captured view.
    pub fn query_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<PlannedDocumentQueryResult> {
        self.view.query_planned(collection, query)
    }

    /// Reads a named-index page from the captured view.
    pub fn query_index(
        &self,
        collection: &str,
        name: &str,
        query: &IndexQuery,
    ) -> io::Result<DocumentPage> {
        self.view.query_index(collection, name, query)
    }
}
