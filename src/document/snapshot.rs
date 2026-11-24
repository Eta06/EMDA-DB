use super::{
    AggregateRequest, AggregateResult, AggregateStatsRequest, AggregateStatsResult, DocumentPage,
    DocumentQuery, DocumentQueryExplanation, DocumentQueryResult, DocumentReadLimits,
    DocumentTransaction, IndexQuery, PlannedAggregateStatsResult, PlannedDocumentQueryResult,
    PlannedLookupResult,
};
use crate::retained_candidate::{AuthorizedRetainedStore, ManagedRetainedStore, RetainedAuthority};
use crate::{CheckpointSnapshot, CheckpointView, Snapshot};
use serde_json::Value;
use std::io;

/// Read-only queries over an already captured storage snapshot or a verified
/// historical checkpoint, or over a borrowed retained selection. All pages
/// use its captured documents and indexes. The caller keeps the underlying
/// view alive and controls its limits.
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

    /// Borrows the selected managed retained generation without copying its
    /// index or overlay. The borrow excludes writes, rotation and retirement
    /// on this handle while the view is in use.
    pub fn with_managed_retained_store(store: &'a ManagedRetainedStore) -> io::Result<Self> {
        store.verify_selection()?;
        Ok(Self {
            view: DocumentTransaction::retained_snapshot(store),
        })
    }

    /// Checks the independent authority and borrows the selected generation.
    /// Like a retained document transaction, this checks authority when
    /// capturing the view. The borrow excludes writes and maintenance on
    /// this handle. Create a fresh view to check authority again.
    pub fn with_authorized_retained_store<A: RetainedAuthority>(
        store: &'a mut AuthorizedRetainedStore<A>,
    ) -> io::Result<Self> {
        Ok(Self {
            view: DocumentTransaction::retained_snapshot(store.checked_document_reader()?),
        })
    }

    /// Sets a cumulative logical read budget for this wrapper, starting with
    /// the next read. The budget covers metadata, documents, and index reads
    /// across all pages. Exceeding either limit rejects the current request
    /// and subsequent reads through this wrapper. Calling this method again
    /// starts a new budget over the same captured view.
    pub fn with_read_limits(mut self, limits: DocumentReadLimits) -> Self {
        self.view.limit_reads(limits);
        self
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

    /// Explains a planned query using the captured collection metadata.
    pub fn explain_query(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<DocumentQueryExplanation> {
        self.view.explain_query(collection, query)
    }

    /// Executes a bounded, optionally indexed query over the captured view.
    pub fn query_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<PlannedDocumentQueryResult> {
        self.view.query_planned(collection, query)
    }

    /// Selects source rows with the bounded planner and follows their string
    /// IDs within the captured view.
    pub fn lookup_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_path: &[String],
        foreign_collection: &str,
    ) -> io::Result<PlannedLookupResult> {
        self.view
            .lookup_planned(collection, query, local_path, foreign_collection)
    }

    /// Selects source rows with the bounded planner and resolves their
    /// complete unique tuples within the captured view.
    pub fn lookup_unique_index_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_paths: &[Vec<String>],
        foreign_collection: &str,
        foreign_index: &str,
    ) -> io::Result<PlannedLookupResult> {
        self.view.lookup_unique_index_planned(
            collection,
            query,
            local_paths,
            foreign_collection,
            foreign_index,
        )
    }

    /// Computes bounded counts and optional sums over the captured view.
    pub fn aggregate(
        &self,
        collection: &str,
        request: &AggregateRequest,
    ) -> io::Result<AggregateResult> {
        self.view.aggregate(collection, request)
    }

    /// Computes bounded integer statistics over the captured view.
    pub fn aggregate_stats(
        &self,
        collection: &str,
        request: &AggregateStatsRequest,
    ) -> io::Result<AggregateStatsResult> {
        self.view.aggregate_stats(collection, request)
    }

    /// Computes bounded statistics using qualifying indexes captured with
    /// the documents. This path assumes those indexes are maintained.
    pub fn aggregate_stats_planned(
        &self,
        collection: &str,
        request: &AggregateStatsRequest,
    ) -> io::Result<PlannedAggregateStatsResult> {
        self.view.aggregate_stats_planned(collection, request)
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
