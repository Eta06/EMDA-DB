//! An experimental, disk-backed key/value storage engine.
//!
//! This library is not production-ready. See the README for limitations.

#[cfg(not(unix))]
compile_error!("this prototype currently supports Unix-like systems only");

mod backup;
mod directory;
mod log;
mod log_io;
mod record;
mod recovery;
mod store;

pub use store::Store;
