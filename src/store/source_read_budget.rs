//! A logical source-read ceiling shared by online maintenance operations.
use std::io;

/// Remaining logical source bytes an operation may read, or unlimited.
pub(crate) struct SourceReadBudget(pub(crate) Option<u64>);

impl SourceReadBudget {
    /// Charges `bytes` before they are read.
    pub(crate) fn charge(&mut self, bytes: u64) -> io::Result<()> {
        if let Some(remaining) = &mut self.0 {
            *remaining = remaining.checked_sub(bytes).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "online maintenance source read byte limit exceeded",
                )
            })?;
        }
        Ok(())
    }
}
