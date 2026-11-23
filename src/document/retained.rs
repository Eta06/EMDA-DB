use super::DocumentTransaction;
use crate::retained_candidate::{AuthorizedRetainedStore, ManagedRetainedStore, RetainedAuthority};
use crate::{
    BatchOperation, CancellationToken, ControlledTransactionError, Entry, MAX_ATOMIC_BATCH_BYTES,
    MAX_ATOMIC_BATCH_OPERATIONS, MutationFailure, TimedTransactionError, TransactionFailure,
    record::{self, Operation},
};
use std::{
    collections::BTreeMap,
    io,
    ops::Bound::Excluded,
    sync::{Mutex, TryLockError},
    time::Instant,
};

pub(super) struct RetainedTransaction<'store> {
    store: &'store ManagedRetainedStore,
    deadline: Option<Instant>,
    cancellation: Option<&'store CancellationToken>,
    staged: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    encoded_bytes: usize,
}

pub(super) trait Backend: Send {
    fn checked_reader(&mut self) -> io::Result<&ManagedRetainedStore>;
    fn commit(
        &mut self,
        operations: &[BatchOperation<'_>],
        admit: &dyn Fn() -> io::Result<()>,
    ) -> Result<(), MutationFailure>;
}

impl Backend for ManagedRetainedStore {
    fn checked_reader(&mut self) -> io::Result<&ManagedRetainedStore> {
        self.generation_id()?;
        Ok(self)
    }

    fn commit(
        &mut self,
        operations: &[BatchOperation<'_>],
        admit: &dyn Fn() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        self.write_batch_checking(operations, admit)
    }
}

impl<A: RetainedAuthority + Send> Backend for AuthorizedRetainedStore<A> {
    fn checked_reader(&mut self) -> io::Result<&ManagedRetainedStore> {
        self.checked_document_reader()
    }

    fn commit(
        &mut self,
        operations: &[BatchOperation<'_>],
        admit: &dyn Fn() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        self.write_batch_checking(operations, admit)
    }
}

impl RetainedTransaction<'_> {
    pub(super) fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_admitted(key, |_| Ok(()))
    }

    pub(super) fn get_admitted(
        &self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        check_admission(self.deadline, self.cancellation)?;
        match self.staged.get(key) {
            Some(value) => {
                if let Some(value) = value {
                    admit(value.len())?;
                }
                Ok(value.clone())
            }
            None => self.store.get_admitted(key, admit),
        }
    }

    pub(super) fn stage(&mut self, operation: BatchOperation<'_>) -> io::Result<()> {
        check_admission(self.deadline, self.cancellation)?;
        let (op, key, value) = operation.fields();
        record::validate(op, key, value)?;
        let previous = self.staged.get(key);
        if previous.is_none() && self.staged.len() >= MAX_ATOMIC_BATCH_OPERATIONS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction write count limit exceeded",
            ));
        }
        let framing = record::HEADER_SIZE + key.len() + record::CHECKSUM_SIZE;
        let old = previous.map_or(0, |value| framing + value.as_ref().map_or(0, Vec::len));
        let total = self.encoded_bytes - old + framing + value.len();
        if total > MAX_ATOMIC_BATCH_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction encoded write limit exceeded",
            ));
        }
        self.staged
            .insert(key.to_vec(), (op == Operation::Put).then(|| value.to_vec()));
        self.encoded_bytes = total;
        Ok(())
    }

    pub(super) fn scan(&self, after: &[u8], before: &[u8], limit: usize) -> io::Result<Vec<Entry>> {
        check_admission(self.deadline, self.cancellation)?;
        let staged = (after < before)
            .then(|| {
                self.staged
                    .range::<[u8], _>((Excluded(after), Excluded(before)))
            })
            .into_iter()
            .flatten()
            .map(|(key, value)| (key.as_slice(), value.as_deref()));
        self.store.scan_with_staged(after, before, limit, staged)
    }
}

pub(super) fn run<T>(
    store: &Mutex<&mut dyn Backend>,
    deadline: Option<Instant>,
    cancellation: Option<&CancellationToken>,
    action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> io::Result<T>,
) -> Result<T, TransactionFailure> {
    let admit = || check_admission(deadline, cancellation);
    admit().map_err(reject)?;
    let mut store = match store.try_lock() {
        Ok(store) => store,
        Err(TryLockError::Poisoned(error)) => error.into_inner(),
        Err(TryLockError::WouldBlock) => {
            return Err(TransactionFailure::Rejected(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retained document transaction is already active",
            )));
        }
    };
    let reader = store
        .checked_reader()
        .map_err(TransactionFailure::Rejected)?;
    admit().map_err(reject)?;
    let mut staged = RetainedTransaction {
        store: reader,
        deadline,
        cancellation,
        staged: BTreeMap::new(),
        encoded_bytes: 4,
    };
    let mut transaction = DocumentTransaction::retained(&mut staged);
    let result = action(&mut transaction);
    let value = transaction
        .finish(result)
        .map_err(TransactionFailure::Callback)?;
    admit().map_err(reject)?;
    let RetainedTransaction { staged, .. } = staged;
    if staged.is_empty() {
        store
            .checked_reader()
            .map_err(TransactionFailure::Rejected)?;
        admit().map_err(reject)?;
    } else {
        let operations = staged
            .iter()
            .map(|(key, value)| match value {
                Some(value) => BatchOperation::Put { key, value },
                None => BatchOperation::Delete { key },
            })
            .collect::<Vec<_>>();
        store
            .commit(&operations, &admit)
            .map_err(|failure| match failure {
                MutationFailure::Rejected(error) => reject(error),
                MutationFailure::OutcomeUnknown(error) => TransactionFailure::OutcomeUnknown(error),
            })?;
    }
    Ok(value)
}

fn check_admission(
    deadline: Option<Instant>,
    cancellation: Option<&CancellationToken>,
) -> io::Result<()> {
    crate::cancellation::check(cancellation)?;
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(TimedTransactionError::DeadlineExceeded.into_io());
    }
    Ok(())
}

fn reject(error: io::Error) -> TransactionFailure {
    if error
        .get_ref()
        .and_then(|source| source.downcast_ref::<TimedTransactionError>())
        .is_some_and(|error| matches!(error, TimedTransactionError::DeadlineExceeded))
    {
        return TransactionFailure::DeadlineExceeded;
    }
    match crate::cancellation::classify(TimedTransactionError::Operation(error)) {
        ControlledTransactionError::Cancelled => TransactionFailure::Cancelled,
        ControlledTransactionError::DeadlineExceeded => TransactionFailure::DeadlineExceeded,
        ControlledTransactionError::Operation(error) => TransactionFailure::Rejected(error),
    }
}

pub(super) fn into_controlled(error: TransactionFailure) -> ControlledTransactionError {
    match error {
        TransactionFailure::Cancelled => ControlledTransactionError::Cancelled,
        TransactionFailure::DeadlineExceeded => ControlledTransactionError::DeadlineExceeded,
        TransactionFailure::Callback(error) => {
            crate::cancellation::classify(TimedTransactionError::Operation(error))
        }
        TransactionFailure::Rejected(error) | TransactionFailure::OutcomeUnknown(error) => {
            ControlledTransactionError::Operation(error)
        }
    }
}

pub(super) fn into_timed(error: TransactionFailure) -> TimedTransactionError {
    match error {
        TransactionFailure::DeadlineExceeded => TimedTransactionError::DeadlineExceeded,
        TransactionFailure::Callback(error)
        | TransactionFailure::Rejected(error)
        | TransactionFailure::OutcomeUnknown(error) => TimedTransactionError::Operation(error),
        error @ TransactionFailure::Cancelled => {
            TimedTransactionError::Operation(io::Error::new(io::ErrorKind::Interrupted, error))
        }
    }
}
