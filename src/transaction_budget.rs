//! Per-store reservations for staged transaction records.
use std::io;
use std::sync::Mutex;

/// Maximum combined encoded staged-record bytes across transactions on one open store.
/// This does not include allocator overhead, snapshots, returned values, or commit buffers.
/// Checkpoint read-set transactions also reserve retained point/range keys,
/// bounds and fixed observation metadata, so less space can be available for staged records.
pub const MAX_TRANSACTION_STAGED_TOTAL_BYTES: usize = 64 * 1024 * 1024;

/// Maximum admitted transactions for one open store, including queued calls.
/// Independent writes and read-only snapshots do not use this allowance.
pub const MAX_ACTIVE_TRANSACTIONS: usize = 64;

/// Current process-local transaction byte and admission limits for one open store.
///
/// The counters do not measure total memory or coordinate independent stores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransactionBudgetStatus {
    /// Configured logical reservation limit, no greater than the hard maximum.
    pub limit_bytes: usize,
    /// Bytes currently reserved by transactions, including checkpoint read sets.
    pub reserved_bytes: usize,
    /// Configured admission limit, no greater than the hard maximum.
    pub active_limit: usize,
    /// Admitted transactions, including callbacks, queued calls and commits.
    pub active_transactions: usize,
}

pub(crate) struct TransactionBudget {
    pub(crate) used: usize,
    pub(crate) limit: usize,
    active: usize,
    active_limit: usize,
}

impl Default for TransactionBudget {
    fn default() -> Self {
        Self {
            used: 0,
            limit: MAX_TRANSACTION_STAGED_TOTAL_BYTES,
            active: 0,
            active_limit: MAX_ACTIVE_TRANSACTIONS,
        }
    }
}

impl TransactionBudget {
    pub(crate) fn status(&self) -> TransactionBudgetStatus {
        TransactionBudgetStatus {
            limit_bytes: self.limit,
            reserved_bytes: self.used,
            active_limit: self.active_limit,
            active_transactions: self.active,
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

    pub(crate) fn set_active_limit(&mut self, limit: usize) -> io::Result<()> {
        if limit > MAX_ACTIVE_TRANSACTIONS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "active transaction limit exceeds hard maximum",
            ));
        }
        if limit < self.active {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "active transaction limit is below current admissions",
            ));
        }
        self.active_limit = limit;
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

/// An admission covers the entire transaction, including waiting and unwind.
pub(crate) struct ActiveTransaction<'a> {
    budget: &'a Mutex<TransactionBudget>,
}

impl<'a> ActiveTransaction<'a> {
    pub(crate) fn acquire(budget: &'a Mutex<TransactionBudget>) -> io::Result<Self> {
        let mut state = budget
            .lock()
            .map_err(|_| io::Error::other("transaction budget poisoned"))?;
        if state.active >= state.active_limit {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "active transaction limit exceeded",
            ));
        }
        state.active += 1;
        Ok(Self { budget })
    }
}

impl Drop for ActiveTransaction<'_> {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.active -= 1;
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
