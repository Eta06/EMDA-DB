//! An experimental, disk-backed key/value storage engine.
//!
//! This library is not production-ready. See the README for limitations.

#[cfg(not(unix))]
compile_error!("this prototype currently supports Unix-like systems only");

mod backup;
mod checkpoint;
mod checkpoint_store;
mod checkpoint_view;
mod directory;
mod document;
mod log;
mod log_io;
mod metadata_witness;
mod record;
mod recovered_view;
mod recovery;
mod staging;
mod store;
#[cfg(test)]
mod test_support;
mod transaction_budget;
mod witness;

pub use checkpoint_store::{
    CheckpointSnapshot, CheckpointStore, CheckpointTransaction, CheckpointTransactionMode,
    CheckpointWorker, CheckpointWorkerStatus, MAX_CHECKPOINT_ACTIVE_SNAPSHOTS,
    MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS, MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES,
    MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
};
pub use checkpoint_view::{
    CheckpointView, MAX_CHECKPOINT_VIEW_BLOCK_BYTES, MAX_CHECKPOINT_VIEW_BLOCKS,
};
pub use document::{
    AggregateGroup, AggregateRequest, AggregateResult, BulkOperation, BulkResult,
    CollectionIntegrity, CollectionSchema, Comparison, DocumentDatabase, DocumentEntry,
    DocumentFilter, DocumentPage, DocumentQuery, DocumentQueryAccess, DocumentQueryResult,
    DocumentSort, DocumentTransaction, DocumentTransactionMode, DocumentUpdate, FieldCondition,
    FieldRule, FieldType, IndexBound, IndexCursor, IndexDefinition, IndexDropPhase, IndexJobStatus,
    IndexQuery, IndexQueryPlan, IndexStep, IndexValue, LookupEntry, LookupResult, PartialFilter,
    PlannedDocumentQueryResult, QueryPlan, SchemaMigrationStatus, SchemaStep,
};
pub use record::batch::{BatchOperation, MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS};
pub use recovered_view::{
    MAX_RECOVERED_VIEW_TAIL_BYTES, MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS,
    RecoveredCheckpointView,
};
pub use staging::{StagingCleanup, cleanup_abandoned_staging};
pub use store::{
    COMPACTED_CHECKPOINT_FILE, Entry, MAX_ACTIVE_SNAPSHOTS, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES,
    MAX_SNAPSHOT_KEY_BYTES, MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_TOTAL_KEY_BYTES,
    MAX_SNAPSHOT_TOTAL_KEYS, MAX_TRANSACTION_READ_CONSTRAINT_BYTES,
    MAX_TRANSACTION_READ_CONSTRAINTS, MAX_TRANSACTION_STAGED_TOTAL_BYTES,
    MAX_TRANSACTION_VALIDATION_ITEMS, OnlineCompactionOptions, OnlineCompactionReport, OnlineStore,
    Snapshot, Store, TimedTransactionError, Transaction, TransactionMode,
};
