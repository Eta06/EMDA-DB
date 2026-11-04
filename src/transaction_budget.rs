//! Per-store reservations for staged transaction records.
use std::io;
use std::sync::Mutex;

/// Maximum combined encoded staged-record bytes across transactions on one open store.
/// This does not include allocator overhead, snapshots, returned values, or commit buffers.
/// Checkpoint read-set transactions also reserve retained point/range keys,
/// bounds and fixed observation metadata, so less space can be available for staged records.
pub const MAX_TRANSACTION_STAGED_TOTAL_BYTES: usize = 64 * 1024 * 1024;

/// Current process-local transaction reservation limit and usage for one open store.
///
/// The counters do not measure total memory or coordinate independent stores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransactionBudgetStatus {
    /// Configured logical reservation limit, no greater than the hard maximum.
    pub limit_bytes: usize,
    /// Bytes currently reserved by transactions, including checkpoint read sets.
    pub reserved_bytes: usize,
}

pub(crate) struct TransactionBudget {
    pub(crate) used: usize,
    pub(crate) limit: usize,
}

impl Default for TransactionBudget {
    fn default() -> Self {
        Self {
            used: 0,
            limit: MAX_TRANSACTION_STAGED_TOTAL_BYTES,
        }
    }
}

impl TransactionBudget {
    pub(crate) fn status(&self) -> TransactionBudgetStatus {
        TransactionBudgetStatus {
            limit_bytes: self.limit,
            reserved_bytes: self.used,
        }
    }

    pub(crate) fn set_limit(&mut self, limit: usize) -> io::Result<()> {
        if limit > MAX_TRANSACTION_STAGED_TOTAL_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction budget exceeds hard maximum",
            ));
        }
        if limit < self.used {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "transaction budget is below current reservations",
            ));
        }
        self.limit = limit;
        Ok(())
    }

    fn reserve(&mut self, bytes: usize) -> io::Result<()> {
        if bytes > self.limit - self.used {
            return Err(io::Error::other(
                "aggregate transaction staged byte limit exceeded",
            ));
        }
        self.used += bytes;
        Ok(())
    }

    pub(crate) fn release(&mut self, bytes: usize) {
        self.used -= bytes;
    }
}

// Releases an incremental reservation if constructing a staged entry unwinds.
pub(crate) struct PendingReservation<'a> {
    budget: &'a Mutex<TransactionBudget>,
    bytes: usize,
}

impl<'a> PendingReservation<'a> {
    pub(crate) fn acquire(budget: &'a Mutex<TransactionBudget>, bytes: usize) -> io::Result<Self> {
        if bytes > 0 {
            budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .reserve(bytes)?;
        }
        Ok(Self { budget, bytes })
    }

    pub(crate) fn finish(&mut self) {
        self.bytes = 0;
    }

    pub(crate) fn grow(&mut self, bytes: usize) -> io::Result<()> {
        let total = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("transaction reservation overflow"))?;
        if bytes > 0 {
            self.budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .reserve(bytes)?;
        }
        self.bytes = total;
        Ok(())
    }

    pub(crate) fn shrink(&mut self, bytes: usize) {
        assert!(bytes <= self.bytes, "reservation shrink exceeds its size");
        if bytes > 0 {
            self.budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(bytes);
            self.bytes -= bytes;
        }
    }
}

impl Drop for PendingReservation<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(self.bytes);
        }
    }
}
