//! An explicit marker for transactions rejected before commit by validation.
use std::fmt;
use std::io;

#[derive(Debug)]
struct Conflict(&'static str);

impl fmt::Display for Conflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for Conflict {}

pub(crate) fn conflict(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, Conflict(message))
}

/// Returns whether a directly returned transaction error carries the engine's
/// pre-commit conflict marker.
///
/// A `WouldBlock` kind alone does not identify a conflict: admission, callback
/// and I/O errors can use the same kind. A false result does not by itself
/// classify an outcome. Wrapping or replacing an error can discard the marker.
/// A conflict does not roll back independent writes or other callback effects.
pub fn is_transaction_conflict(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        && error.get_ref().is_some_and(|cause| cause.is::<Conflict>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_is_distinct_from_other_would_block_errors_and_same_text() {
        let marked = conflict("transaction conflict");
        assert_eq!(marked.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(marked.to_string(), "transaction conflict");
        assert!(is_transaction_conflict(&marked));
        assert!(!is_transaction_conflict(&io::Error::new(
            io::ErrorKind::WouldBlock,
            "transaction conflict"
        )));
        assert!(!is_transaction_conflict(&io::Error::from(
            io::ErrorKind::WouldBlock
        )));
        let reclassified = io::Error::other(marked.into_inner().unwrap());
        assert!(!is_transaction_conflict(&reclassified));
    }
}
