use super::{
    DocumentPage, DocumentQuery, DocumentQueryResult, DocumentTransaction, IndexQuery,
    PlannedDocumentQueryResult,
};
use crate::{CheckpointSnapshot, CheckpointView, Snapshot};
use serde_json::Value;
use std::io;

/// Read-only queries over an already captured storage snapshot or a verified
/// historical checkpoint. All pages use its captured documents and indexes.
/// The caller keeps the underlying view alive and controls its limits.
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

    /// Borrows a verified historical checkpoint. Reopening the same checkpoint
    /// with its independently trusted digest and original covered log preserves
    /// the view across processes. Later log records are outside the view.
    pub fn with_checkpoint_view(view: &'a CheckpointView) -> Self {
        Self {
            view: DocumentTransaction::historical_checkpoint(view),
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
