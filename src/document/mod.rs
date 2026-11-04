//! Experimental, transactional JSON documents over the shared storage engine.
mod aggregate;
mod bulk;
mod filter;
mod index;
mod integrity;
mod key;
mod lifecycle;
mod online_index;
mod query;
mod schema;
mod schema_migration;
mod transaction;
mod update;

pub use aggregate::{AggregateGroup, AggregateRequest, AggregateResult, LookupEntry, LookupResult};
pub use bulk::{BulkOperation, BulkResult};
pub use filter::{
    Comparison, DocumentFilter, DocumentQuery, DocumentQueryResult, DocumentSort, QueryPlan,
};
pub use index::{IndexDefinition, IndexValue, PartialFilter};
pub use integrity::CollectionIntegrity;
pub use online_index::{IndexDropPhase, IndexJobStatus, IndexStep};
pub use query::{DocumentEntry, DocumentPage, IndexBound, IndexCursor, IndexQuery, IndexQueryPlan};
pub use schema::{CollectionSchema, FieldRule, FieldType};
pub use schema_migration::{SchemaMigrationStatus, SchemaStep};
pub use transaction::DocumentTransaction;
pub use update::{DocumentUpdate, FieldCondition};

use crate::{
    CheckpointStore, CheckpointTransactionMode, Store, TimedTransactionError, TransactionMode,
};
use serde_json::Value;
use std::io;
use std::time::Instant;

/// Transaction validation and admission mode for deadline-limited document work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentTransactionMode {
    /// Reject when any write intervenes.
    Conservative,
    /// Validate observed point keys and complete scanned ranges.
    ReadSet,
    /// Read-set validation with FIFO admission among participating callers.
    ReadSetFifo,
}

/// A document view over one already-open store. The document namespace must
/// not be modified using the raw key/value API. This is not a security boundary.
pub struct DocumentDatabase<'a> {
    backend: Backend<'a>,
}

enum Backend<'a> {
    Store(&'a Store),
    Checkpoint(&'a CheckpointStore),
}

impl<'a> DocumentDatabase<'a> {
    /// Uses the ordinary in-memory-index store.
    pub fn new(store: &'a Store) -> Self {
        Self {
            backend: Backend::Store(store),
        }
    }

    /// Uses the checkpoint-backed writable store.
    pub fn with_checkpoint_store(store: &'a CheckpointStore) -> Self {
        Self {
            backend: Backend::Checkpoint(store),
        }
    }

    /// Runs a conservative serializable transaction containing document and
    /// unreserved raw key/value operations. No automatic retry is performed.
    /// Commit errors may have an unknown outcome, as with the underlying store.
    pub fn transaction<T>(
        &self,
        action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> io::Result<T>,
    ) -> io::Result<T> {
        match self.backend {
            Backend::Store(store) => store.transaction(|tx| {
                let mut transaction = DocumentTransaction::ordinary(tx);
                let result = action(&mut transaction);
                transaction.finish(result)
            }),
            Backend::Checkpoint(store) => store.transaction(|tx| {
                let mut transaction = DocumentTransaction::checkpoint(tx);
                let result = action(&mut transaction);
                transaction.finish(result)
            }),
        }
    }

    /// Runs a transaction with bounded point and complete-range validation.
    /// Unrelated intervening writes need not conflict. Callbacks are not
    /// retried; callers must handle conflicts and external side effects.
    pub fn transaction_with_read_set<T>(
        &self,
        action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> io::Result<T>,
    ) -> io::Result<T> {
        match self.backend {
            Backend::Store(store) => store.transaction_with_read_set(|tx| {
                let mut transaction = DocumentTransaction::ordinary(tx);
                let result = action(&mut transaction);
                transaction.finish(result)
            }),
            Backend::Checkpoint(store) => store.transaction_with_read_set(|tx| {
                let mut transaction = DocumentTransaction::checkpoint(tx);
                let result = action(&mut transaction);
                transaction.finish(result)
            }),
        }
    }

    /// Runs a transaction with an operation-boundary deadline. Expiry before
    /// commit means no staged writes were submitted. I/O errors can still have
    /// unknown outcomes; callbacks and I/O are not forcibly interrupted.
    pub fn transaction_until<T>(
        &self,
        deadline: Instant,
        mode: DocumentTransactionMode,
        action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> io::Result<T>,
    ) -> Result<T, TimedTransactionError> {
        match self.backend {
            Backend::Store(store) => {
                let mode = match mode {
                    DocumentTransactionMode::Conservative => TransactionMode::Conservative,
                    DocumentTransactionMode::ReadSet => TransactionMode::ReadSet,
                    DocumentTransactionMode::ReadSetFifo => TransactionMode::ReadSetFifo,
                };
                store.transaction_until(deadline, mode, |tx| {
                    let mut transaction = DocumentTransaction::ordinary(tx);
                    let result = action(&mut transaction);
                    transaction.finish(result)
                })
            }
            Backend::Checkpoint(store) => {
                let mode = match mode {
                    DocumentTransactionMode::Conservative => {
                        CheckpointTransactionMode::Conservative
                    }
                    DocumentTransactionMode::ReadSet => CheckpointTransactionMode::ReadSet,
                    DocumentTransactionMode::ReadSetFifo => CheckpointTransactionMode::ReadSetFifo,
                };
                store.transaction_until(deadline, mode, |tx| {
                    let mut transaction = DocumentTransaction::checkpoint(tx);
                    let result = action(&mut transaction);
                    transaction.finish(result)
                })
            }
        }
    }

    /// Creates an empty collection and persists its schema atomically.
    pub fn create_collection(&self, collection: &str, schema: CollectionSchema) -> io::Result<()> {
        self.transaction(|tx| tx.create_collection(collection, schema))
    }

    /// Starts a durable incremental schema migration.
    pub fn start_schema_migration(
        &self,
        collection: &str,
        schema: CollectionSchema,
    ) -> io::Result<()> {
        self.transaction(|tx| tx.start_schema_migration(collection, schema))
    }

    /// Advances a schema migration by a bounded number of documents.
    pub fn advance_schema_migration(
        &self,
        collection: &str,
        limit: usize,
    ) -> io::Result<SchemaStep> {
        self.transaction(|tx| tx.advance_schema_migration(collection, limit))
    }

    /// Cancels a pending schema migration without changing the active schema.
    pub fn cancel_schema_migration(&self, collection: &str) -> io::Result<()> {
        self.transaction(|tx| tx.cancel_schema_migration(collection))
    }

    /// Inspects the pending schema migration, if any.
    pub fn schema_migration(&self, collection: &str) -> io::Result<Option<SchemaMigrationStatus>> {
        self.transaction(|tx| tx.schema_migration(collection))
    }

    /// Validates existing documents and replaces a collection schema atomically.
    pub fn set_schema(&self, collection: &str, schema: CollectionSchema) -> io::Result<()> {
        self.transaction(|tx| tx.set_schema(collection, schema))
    }

    /// Removes a bounded collection and its indexes atomically.
    pub fn drop_collection(&self, collection: &str) -> io::Result<()> {
        self.transaction(|tx| tx.drop_collection(collection))
    }

    /// Runs a bounded atomic batch of document operations.
    pub fn bulk(&self, operations: &[BulkOperation<'_>]) -> io::Result<Vec<BulkResult>> {
        self.transaction(|tx| tx.bulk(operations))
    }

    /// Reads a document by ID. Missing documents return `None`.
    pub fn get(&self, collection: &str, id: &str) -> io::Result<Option<Value>> {
        self.transaction(|tx| tx.get(collection, id))
    }

    /// Inserts a new document, failing if its ID already exists.
    pub fn insert(&self, collection: &str, id: &str, document: &Value) -> io::Result<()> {
        self.transaction(|tx| tx.insert(collection, id, document))
    }

    /// Replaces an existing document, or inserts it when `upsert` is true.
    pub fn replace(
        &self,
        collection: &str,
        id: &str,
        document: &Value,
        upsert: bool,
    ) -> io::Result<()> {
        self.transaction(|tx| tx.replace(collection, id, document, upsert))
    }

    /// Conditionally applies ordered updates in one transaction.
    pub fn update_if(
        &self,
        collection: &str,
        id: &str,
        conditions: &[FieldCondition],
        updates: &[DocumentUpdate],
        upsert: bool,
    ) -> io::Result<bool> {
        self.transaction(|tx| tx.update_if(collection, id, conditions, updates, upsert))
    }

    /// Checks every document and index key within bounded verification limits.
    pub fn verify_collection(&self, collection: &str) -> io::Result<CollectionIntegrity> {
        self.transaction(|tx| tx.verify_collection(collection))
    }

    /// Atomically rebuilds a bounded index from valid collection documents.
    pub fn rebuild_index(&self, collection: &str, name: &str) -> io::Result<()> {
        self.transaction(|tx| tx.rebuild_index(collection, name))
    }

    /// Starts a durable incremental index build.
    pub fn start_index_build(
        &self,
        collection: &str,
        definition: IndexDefinition,
    ) -> io::Result<()> {
        self.transaction(|tx| tx.start_index_build(collection, definition))
    }

    /// Advances an incremental index build by a bounded number of documents.
    pub fn advance_index_build(
        &self,
        collection: &str,
        name: &str,
        limit: usize,
    ) -> io::Result<IndexStep> {
        self.transaction(|tx| tx.advance_index_build(collection, name, limit))
    }

    /// Cancels an incremental index build and starts cleanup.
    pub fn cancel_index_build(&self, collection: &str, name: &str) -> io::Result<()> {
        self.transaction(|tx| tx.cancel_index_build(collection, name))
    }

    /// Makes an index unavailable and starts incremental cleanup.
    pub fn start_index_drop(&self, collection: &str, name: &str) -> io::Result<()> {
        self.transaction(|tx| tx.start_index_drop(collection, name))
    }

    /// Advances an incremental index drop by a bounded number of keys.
    pub fn advance_index_drop(
        &self,
        collection: &str,
        name: &str,
        limit: usize,
    ) -> io::Result<IndexStep> {
        self.transaction(|tx| tx.advance_index_drop(collection, name, limit))
    }

    /// Lists durable, resumable index builds and drops.
    pub fn index_jobs(&self, collection: &str) -> io::Result<Vec<IndexJobStatus>> {
        self.transaction(|tx| tx.index_jobs(collection))
    }

    /// Builds a secondary index atomically within the documented limits.
    pub fn create_index(&self, collection: &str, definition: IndexDefinition) -> io::Result<()> {
        self.transaction(|tx| tx.create_index(collection, definition))
    }

    /// Drops a secondary index atomically within the documented limits.
    pub fn drop_index(&self, collection: &str, name: &str) -> io::Result<()> {
        self.transaction(|tx| tx.drop_index(collection, name))
    }

    /// Aggregates a bounded collection scan.
    pub fn aggregate(
        &self,
        collection: &str,
        request: &AggregateRequest,
    ) -> io::Result<AggregateResult> {
        self.transaction(|tx| tx.aggregate(collection, request))
    }

    /// Follows string IDs from a bounded source query into another collection.
    pub fn lookup(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_path: &[String],
        foreign_collection: &str,
    ) -> io::Result<LookupResult> {
        self.transaction(|tx| tx.lookup(collection, query, local_path, foreign_collection))
    }

    /// Executes a bounded collection scan with filter, sorting and projection.
    pub fn query(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<DocumentQueryResult> {
        self.transaction(|tx| tx.query(collection, query))
    }

    /// Reads a bounded page using a named secondary index.
    pub fn query_index(
        &self,
        collection: &str,
        name: &str,
        query: &IndexQuery,
    ) -> io::Result<DocumentPage> {
        self.transaction(|tx| tx.query_index(collection, name, query))
    }

    /// Lists a collection's index definitions.
    pub fn indexes(&self, collection: &str) -> io::Result<Vec<IndexDefinition>> {
        self.transaction(|tx| tx.indexes(collection))
    }

    /// Deletes an existing document and returns whether one existed.
    pub fn delete(&self, collection: &str, id: &str) -> io::Result<bool> {
        self.transaction(|tx| tx.delete(collection, id))
    }
}
