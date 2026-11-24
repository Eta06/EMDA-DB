//! An experimental, disk-backed key/value storage engine.
//!
//! This library is not production-ready. See the README for limitations.

#[cfg(not(unix))]
compile_error!("this prototype currently supports Unix-like systems only");

mod backup;
mod cancellation;
mod checkpoint;
mod checkpoint_store;
mod checkpoint_view;
mod controlled_lock;
mod direct_read;
mod direct_write;
mod directory;
mod document;
mod log;
mod log_io;
mod metadata_witness;
mod publication_outcome;
mod record;
mod recovered_view;
mod recovery;
pub mod retained_base_v2;
pub mod retained_candidate;
mod snapshot_reaper;
mod staging;
mod store;
mod store_lock;
#[cfg(test)]
mod test_support;
mod transaction_budget;
mod transaction_conflict;
mod transaction_outcome;
mod witness;

pub use backup::{FormatUpgradeLimits, FullLogBackupLimits, FullLogRestoreLimits};
pub use cancellation::{CancellationToken, ControlledTransactionError};
pub use checkpoint_store::{
    BoundedFullRebuildLimits, BoundedRebuildReadLimits, BoundedSuffixAdmission,
    BoundedSuffixRebuildLimits, CheckpointRecoveryLimits, CheckpointSnapshot,
    CheckpointSnapshotBudgetStatus, CheckpointStore, CheckpointStoreLimits, CheckpointTransaction,
    CheckpointTransactionMode, CheckpointWorker, CheckpointWorkerStatus,
    MAX_CHECKPOINT_ACTIVE_SNAPSHOTS, MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS,
    MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES, MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
};
pub use checkpoint_view::{
    CheckpointView, CheckpointViewLimits, MAX_CHECKPOINT_VIEW_BLOCK_BYTES,
    MAX_CHECKPOINT_VIEW_BLOCKS, MAX_CHECKPOINT_VIEW_FILE_BYTES,
};
pub use direct_read::{DirectReadStatus, MAX_ACTIVE_DIRECT_READS};
pub use direct_write::{DirectWriteStatus, MAX_ACTIVE_DIRECT_WRITES};
pub use document::{
    AggregateGroup, AggregateRequest, AggregateResult, AggregateStatsGroup, AggregateStatsRequest,
    AggregateStatsResult, BulkOperation, BulkResult, CollectionDropStep, CollectionIntegrity,
    CollectionSchema, Comparison, DocumentDatabase, DocumentEntry, DocumentFilter, DocumentPage,
    DocumentQuery, DocumentQueryAccess, DocumentQueryExplanation, DocumentQueryProbe,
    DocumentQueryResult, DocumentReadLimits, DocumentRequestLimits, DocumentSnapshot, DocumentSort,
    DocumentTransaction, DocumentTransactionLimits, DocumentTransactionMode, DocumentUpdate,
    FieldCondition, FieldRule, FieldType, IndexBound, IndexCursor, IndexDefinition, IndexDropPhase,
    IndexJobStatus, IndexQuery, IndexQueryPlan, IndexStep, IndexValue, LookupEntry, LookupResult,
    MAX_COLLECTION_ARCHIVE_BYTES, MAX_COLLECTION_ARCHIVE_DOCUMENTS,
    MAX_DOCUMENT_UPDATE_INPUT_BYTES, MaintenanceKey, MaintenanceProgress, NumericFieldStats,
    PartialFilter, PlannedAggregateStatsResult, PlannedDocumentQueryResult, QueryPlan,
    SchemaMigrationStatus, SchemaStep,
};
pub use log::{PeakVerificationLimits, StoreRecoveryLimits};
pub use publication_outcome::{
    HandoffFailure, ManagedRebuildFailure, ManagedRolloverFailure, OnlineHandoffFailure,
    PublicationFailure, RecoveryOpenFailure, RetainedRotationFailure,
};
pub use record::batch::{BatchOperation, MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS};
pub use recovered_view::{
    MAX_RECOVERED_VIEW_TAIL_BYTES, MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS,
    RecoveredCheckpointView, RecoveredViewLimits,
};
pub use snapshot_reaper::{SnapshotReaper, SnapshotReaperStatus};
pub use staging::{
    StagingCleanup, StagingCleanupFailure, cleanup_abandoned_staging,
    cleanup_abandoned_staging_detailed,
};
pub use store::{
    COMPACTED_CHECKPOINT_FILE, Entry, GroupCommitStatus, MAX_ACTIVE_SNAPSHOTS,
    MAX_GROUP_COMMIT_BUFFERED_BYTES, MAX_GROUP_COMMIT_BUFFERED_RECORDS, MAX_SCAN_BYTES,
    MAX_SCAN_ENTRIES, MAX_SNAPSHOT_KEY_BYTES, MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_TOTAL_KEY_BYTES,
    MAX_SNAPSHOT_TOTAL_KEYS, MAX_TRANSACTION_READ_CONSTRAINT_BYTES,
    MAX_TRANSACTION_READ_CONSTRAINTS, MAX_TRANSACTION_STAGED_TOTAL_BYTES,
    MAX_TRANSACTION_VALIDATION_ITEMS, OnlineCompactionOptions, OnlineCompactionReport,
    OnlineReclaimError, OnlineReclaimFailure, OnlineReclamationFailure,
    OnlineReclamationFailureStage, OnlineReclamationWorker, OnlineReclamationWorkerStatus,
    OnlineSnapshot, OnlineStore, RetainedExportLimits, RetiredCleanupStatus, Snapshot,
    SnapshotBudgetStatus, Store, SuffixExportFailure, TimedTransactionError, Transaction,
    TransactionMode,
};
pub use transaction_budget::{MAX_ACTIVE_TRANSACTIONS, TransactionBudgetStatus};
pub use transaction_conflict::is_transaction_conflict;
pub use transaction_outcome::{MutationFailure, TransactionFailure};
