//! Mixed transactional and direct operations checked against a serial model.
use database_engine::{
    BatchOperation, CancellationToken, CheckpointStore, CheckpointTransaction,
    CheckpointTransactionMode, ControlledTransactionError, Store, TimedTransactionError,
    Transaction, TransactionMode, is_transaction_conflict,
};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[path = "mixed_transaction_history/model.rs"]
mod model;
use model::{Event, INITIAL, OPERATIONS, Operation, Outcome, State, serializes};

type Entries = Vec<(Vec<u8>, Vec<u8>)>;
const KEYS: [&[u8]; 3] = [b"a", b"b", b"c"];

fn decode(value: Option<Vec<u8>>) -> Option<u8> {
    value.map(|value| {
        assert_eq!(value.len(), 1);
        value[0]
    })
}

fn decode_entries(entries: Entries) -> State {
    let mut state = [None; 3];
    let mut previous = None;
    for (key, value) in entries {
        let index = KEYS.iter().position(|candidate| *candidate == key).unwrap();
        assert!(
            previous.is_none_or(|old| old < index),
            "scan is not ordered"
        );
        assert_eq!(value.len(), 1);
        state[index] = Some(value[0]);
        previous = Some(index);
    }
    state
}

trait Tx {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    fn scan(&self) -> io::Result<Entries>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn delete(&mut self, key: &[u8]) -> io::Result<()>;
}

macro_rules! implement_tx {
    ($kind:ty) => {
        impl Tx for $kind {
            fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
                self.get(key)
            }
            fn scan(&self) -> io::Result<Entries> {
                self.scan(None, None, 10)
            }
            fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
                self.put(key, value)
            }
            fn delete(&mut self, key: &[u8]) -> io::Result<()> {
                self.delete(key)
            }
        }
    };
}
implement_tx!(Transaction<'_>);
implement_tx!(CheckpointTransaction<'_>);

fn transaction(tx: &mut impl Tx, operation: Operation) -> io::Result<State> {
    match operation {
        Operation::RangeTransaction => {
            let observed = decode_entries(tx.scan()?);
            thread::yield_now();
            if observed[0].is_some() {
                tx.delete(b"a")?;
            } else {
                tx.put(b"a", &[1])?;
            }
            match observed[1] {
                Some(value) => tx.put(b"c", &[value + 10])?,
                None => tx.delete(b"c")?,
            }
            Ok(observed)
        }
        Operation::PointTransaction => {
            let observed = [None, decode(tx.get(b"b")?), None];
            thread::yield_now();
            if observed[1].is_some() {
                tx.delete(b"c")?;
            } else {
                tx.put(b"c", &[9])?;
            }
            Ok(observed)
        }
        _ => unreachable!(),
    }
}

enum Engine {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

fn timed<T>(result: Result<T, TimedTransactionError>) -> io::Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(TimedTransactionError::Operation(error)) => Err(error),
        Err(other) => panic!("unexpected timed transaction result: {other:?}"),
    }
}

fn controlled<T>(result: Result<T, ControlledTransactionError>) -> io::Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(ControlledTransactionError::Operation(error)) => Err(error),
        Err(other) => panic!("unexpected controlled transaction result: {other:?}"),
    }
}

impl Engine {
    fn transact(&self, operation: Operation, mode: usize, api: usize) -> io::Result<State> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let cancellation = CancellationToken::new();
        match self {
            Self::Ordinary(store) => {
                let mode = match mode {
                    1 => TransactionMode::ReadSet,
                    2 => TransactionMode::ReadSetFifo,
                    _ => TransactionMode::Conservative,
                };
                match api {
                    0 => match mode {
                        TransactionMode::Conservative => {
                            store.transaction(|tx| transaction(tx, operation))
                        }
                        TransactionMode::ReadSet => {
                            store.transaction_with_read_set(|tx| transaction(tx, operation))
                        }
                        TransactionMode::ReadSetFifo => {
                            store.transaction_with_read_set_fifo(|tx| transaction(tx, operation))
                        }
                    },
                    1 => timed(
                        store.transaction_until(deadline, mode, |tx| transaction(tx, operation)),
                    ),
                    _ => controlled(store.transaction_controlled(
                        &cancellation,
                        Some(deadline),
                        mode,
                        |tx| transaction(tx, operation),
                    )),
                }
            }
            Self::Checkpoint(store) => {
                let mode = match mode {
                    0 => CheckpointTransactionMode::Conservative,
                    1 => CheckpointTransactionMode::ReadSet,
                    2 => CheckpointTransactionMode::ReadSetFifo,
                    _ => CheckpointTransactionMode::Fifo,
                };
                match api {
                    0 => match mode {
                        CheckpointTransactionMode::Conservative => {
                            store.transaction(|tx| transaction(tx, operation))
                        }
                        CheckpointTransactionMode::Fifo => {
                            store.transaction_with_fifo(|tx| transaction(tx, operation))
                        }
                        CheckpointTransactionMode::ReadSet => {
                            store.transaction_with_read_set(|tx| transaction(tx, operation))
                        }
                        CheckpointTransactionMode::ReadSetFifo => {
                            store.transaction_with_read_set_fifo(|tx| transaction(tx, operation))
                        }
                    },
                    1 => timed(
                        store.transaction_until(deadline, mode, |tx| transaction(tx, operation)),
                    ),
                    _ => controlled(store.transaction_controlled(
                        &cancellation,
                        Some(deadline),
                        mode,
                        |tx| transaction(tx, operation),
                    )),
                }
            }
        }
    }

    fn execute(&self, operation: Operation, iteration: usize) -> Outcome {
        match operation {
            Operation::RangeTransaction | Operation::PointTransaction => {
                let mode = (iteration / 4
                    + usize::from(matches!(operation, Operation::PointTransaction)))
                    % 4;
                match self.transact(operation, mode, iteration / 12 % 3) {
                    Ok(observed) => Outcome::Transaction(observed),
                    Err(error) if is_transaction_conflict(&error) => Outcome::Conflict,
                    Err(error) => panic!("unexpected transaction error: {error}"),
                }
            }
            Operation::Put => {
                match self {
                    Self::Ordinary(store) => store.put(b"b", &[1]),
                    Self::Checkpoint(store) => store.put(b"b", &[1]),
                }
                .unwrap();
                Outcome::Done
            }
            Operation::Delete => {
                match self {
                    Self::Ordinary(store) => store.delete(b"b"),
                    Self::Checkpoint(store) => store.delete(b"b"),
                }
                .unwrap();
                Outcome::Done
            }
            Operation::Compare => Outcome::Compared(
                match self {
                    Self::Ordinary(store) => store.compare_exchange(b"b", None, Some(&[2])),
                    Self::Checkpoint(store) => store.compare_exchange(b"b", None, Some(&[2])),
                }
                .unwrap(),
            ),
            Operation::Batch => {
                let batch = [
                    BatchOperation::Put {
                        key: b"a",
                        value: &[6],
                    },
                    BatchOperation::Delete { key: b"c" },
                ];
                match self {
                    Self::Ordinary(store) => store.write_batch(&batch),
                    Self::Checkpoint(store) => store.write_batch(&batch),
                }
                .unwrap();
                Outcome::Done
            }
            Operation::Scan => Outcome::Read(self.scan()),
            Operation::GetMany => {
                let values = match self {
                    Self::Ordinary(store) => store.get_many(&KEYS),
                    Self::Checkpoint(store) => store.get_many(&KEYS),
                }
                .unwrap();
                Outcome::Read(
                    values
                        .into_iter()
                        .map(decode)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                )
            }
        }
    }

    fn scan(&self) -> State {
        decode_entries(
            match self {
                Self::Ordinary(store) => store.scan(None, None, 10),
                Self::Checkpoint(store) => store.scan(None, None, 10),
            }
            .unwrap(),
        )
    }
}

#[test]
fn mixed_transactions_deletions_and_conditional_writes_serialize_and_recover() {
    let histories = std::env::var("EMDA_MIXED_TRANSACTION_HISTORIES")
        .map(|value| value.parse::<usize>().expect("invalid history count"))
        .unwrap_or(128);
    assert!((1..=4096).contains(&histories));
    let mut transactions = [0_usize; 2];
    let mut comparisons = [0_usize; 2];
    for iteration in 0..histories {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let v2 = !cfg!(target_os = "linux") || iteration % 4 < 2;
        if v2 {
            std::fs::create_dir(&path).unwrap();
            std::fs::write(
                path.join("records.log"),
                include_bytes!("fixtures/v2-empty-records.log"),
            )
            .unwrap();
            std::fs::write(
                path.join("commit.state"),
                include_bytes!("fixtures/v2-empty-commit.state"),
            )
            .unwrap();
        }
        let initial = Store::open(&path).unwrap();
        initial
            .write_batch(&[
                BatchOperation::Put {
                    key: b"a",
                    value: &[0],
                },
                BatchOperation::Put {
                    key: b"c",
                    value: &[0],
                },
            ])
            .unwrap();
        assert_eq!(
            &std::fs::read(path.join("records.log")).unwrap()[..8],
            if v2 { b"EMDALOG2" } else { b"EMDALOG3" }
        );
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        let store = if iteration % 2 == 0 {
            Engine::Ordinary(initial)
        } else {
            drop(initial);
            Engine::Checkpoint(CheckpointStore::open(&path, &checkpoint, &digest).unwrap())
        };
        let barrier = Barrier::new(OPERATIONS.len() + 1);
        let clock = AtomicUsize::new(0);
        let events = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for offset in 0..OPERATIONS.len() {
                let operation = OPERATIONS[(iteration + offset) % OPERATIONS.len()];
                let (barrier, clock, events, store) = (&barrier, &clock, &events, &store);
                scope.spawn(move || {
                    barrier.wait();
                    let start = clock.fetch_add(1, Ordering::SeqCst);
                    let outcome = store.execute(operation, iteration);
                    let finish = clock.fetch_add(1, Ordering::SeqCst);
                    events.lock().unwrap().push(Event {
                        operation,
                        outcome,
                        start,
                        finish,
                    });
                });
            }
            barrier.wait();
        });
        let recovered_state = store.scan();
        let mut events = events.into_inner().unwrap();
        for event in &events {
            match event.outcome {
                Outcome::Conflict => transactions[0] += 1,
                Outcome::Transaction(_) => transactions[1] += 1,
                Outcome::Compared(success) => comparisons[usize::from(success)] += 1,
                _ => {}
            }
        }
        events.push(Event {
            operation: Operation::Scan,
            outcome: Outcome::Read(recovered_state),
            start: clock.fetch_add(1, Ordering::SeqCst),
            finish: clock.fetch_add(1, Ordering::SeqCst),
        });
        assert!(
            serializes(&events, INITIAL),
            "iteration={iteration} events={events:?}"
        );
        drop(store);
        let ordinary = Engine::Ordinary(Store::open(&path).unwrap());
        assert_eq!(ordinary.scan(), recovered_state);
        drop(ordinary);
        let checkpoint_store =
            Engine::Checkpoint(CheckpointStore::open(&path, &checkpoint, &digest).unwrap());
        assert_eq!(checkpoint_store.scan(), recovered_state);
    }
    eprintln!(
        "histories={histories} transactions_conflict={} transactions_committed={} compare_mismatch={} compare_success={}",
        transactions[0], transactions[1], comparisons[0], comparisons[1]
    );
}
