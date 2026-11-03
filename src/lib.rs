//! An experimental, disk-backed key/value storage engine.
//!
//! This library is not production-ready. See the README for limitations.

#[cfg(not(unix))]
compile_error!("this prototype currently supports Unix-like systems only");

mod backup;
mod checkpoint;
mod checkpoint_view;
mod directory;
mod log;
mod log_io;
mod metadata_witness;
mod record;
mod recovery;
mod store;
mod witness;

pub use checkpoint_view::{
    CheckpointView, MAX_CHECKPOINT_VIEW_BLOCK_BYTES, MAX_CHECKPOINT_VIEW_BLOCKS,
};
pub use record::batch::{BatchOperation, MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS};
pub use store::{
    Entry, MAX_ACTIVE_SNAPSHOTS, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES, MAX_SNAPSHOT_KEY_BYTES,
    MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_TOTAL_KEY_BYTES, MAX_SNAPSHOT_TOTAL_KEYS, Snapshot, Store,
    Transaction,
};
