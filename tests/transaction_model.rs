//! Concurrent transaction histories checked against a serial reference model.
use database_engine::{BatchOperation, Store, Transaction};
use std::io::{self, ErrorKind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};
use std::thread;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Transfer { from: usize, read_set: bool },
    SetBoth([u8; 2]),
    Put { key: usize, value: u8 },
    ReadBoth,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Committed([u8; 2]),
    Conflict,
    Done,
    Read([u8; 2]),
}

#[derive(Clone, Copy, Debug)]
struct Event {
    operation: Operation,
    outcome: Outcome,
    started: usize,
    finished: usize,
}

fn explain(events: &[Event], used: u64, values: [u8; 2]) -> bool {
    if used.count_ones() as usize == events.len() {
        return true;
    }
    for (index, event) in events.iter().enumerate() {
        let bit = 1_u64 << index;
        if used & bit != 0
            || events.iter().enumerate().any(|(other_index, other)| {
                used & (1_u64 << other_index) == 0 && other.finished < event.started
            })
        {
            continue;
        }
        let mut next = values;
        match (event.operation, event.outcome) {
            (Operation::Transfer { .. }, Outcome::Conflict) => {}
            (Operation::Transfer { from, .. }, Outcome::Committed(observed))
                if observed == values =>
            {
                let to = 1 - from;
                if next[from] > 0 {
                    next[from] -= 1;
                    next[to] += 1;
                }
            }
            (Operation::SetBoth(after), Outcome::Done) => next = after,
            (Operation::Put { key, value }, Outcome::Done) => next[key] = value,
            (Operation::ReadBoth, Outcome::Read(observed)) if observed == values => {}
            _ => continue,
        }
        if explain(events, used | bit, next) {
            return true;
        }
    }
    false
}

fn read_pair(store: &Store) -> [u8; 2] {
    let result = store.get_many(&[b"a", b"b"]).unwrap();
    [
        result[0].as_ref().unwrap()[0],
        result[1].as_ref().unwrap()[0],
    ]
}

fn transfer(tx: &mut Transaction<'_>, from: usize) -> io::Result<[u8; 2]> {
    let before = [tx.get(b"a")?.unwrap()[0], tx.get(b"b")?.unwrap()[0]];
    thread::yield_now();
    let to = 1 - from;
    if before[from] > 0 {
        let mut after = before;
        after[from] -= 1;
        after[to] += 1;
        tx.put(b"a", &[after[0]])?;
        tx.put(b"b", &[after[1]])?;
    }
    Ok(before)
}

fn execute(store: &Store, operation: Operation) -> Outcome {
    match operation {
        Operation::Transfer { from, read_set } => match if read_set {
            store.transaction_with_read_set(|tx| transfer(tx, from))
        } else {
            store.transaction(|tx| transfer(tx, from))
        } {
            Ok(before) => Outcome::Committed(before),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Outcome::Conflict,
            Err(error) => panic!("unexpected transaction error: {error}"),
        },
        Operation::SetBoth(after) => {
            store
                .write_batch(&[
                    BatchOperation::Put {
                        key: b"a",
                        value: &after[..1],
                    },
                    BatchOperation::Put {
                        key: b"b",
                        value: &after[1..],
                    },
                ])
                .unwrap();
            Outcome::Done
        }
        Operation::Put { key, value } => {
            store
                .put(if key == 0 { b"a" } else { b"b" }, &[value])
                .unwrap();
            Outcome::Done
        }
        Operation::ReadBoth => Outcome::Read(read_pair(store)),
    }
}

#[test]
fn reference_model_rejects_a_fractured_successful_transaction() {
    let history = [Event {
        operation: Operation::Transfer {
            from: 0,
            read_set: true,
        },
        outcome: Outcome::Committed([3, 9]),
        started: 0,
        finished: 1,
    }];
    assert!(!explain(&history, 0, [3, 2]));
}

#[test]
fn concurrent_transactions_batches_and_reads_match_a_serial_history_and_recovery() {
    let operations = [
        Operation::Transfer {
            from: 0,
            read_set: false,
        },
        Operation::Transfer {
            from: 1,
            read_set: true,
        },
        Operation::Transfer {
            from: 0,
            read_set: true,
        },
        Operation::SetBoth([5, 5]),
        Operation::Put { key: 0, value: 2 },
        Operation::ReadBoth,
        Operation::ReadBoth,
    ];
    for iteration in 0..128 {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put(b"a", &[3]).unwrap();
        store.put(b"b", &[2]).unwrap();
        let clock = AtomicUsize::new(0);
        let barrier = Barrier::new(operations.len() + 1);
        let events = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for offset in 0..operations.len() {
                let operation = operations[(iteration + offset) % operations.len()];
                let (store, barrier, clock, events) = (&store, &barrier, &clock, &events);
                scope.spawn(move || {
                    barrier.wait();
                    let started = clock.fetch_add(1, Ordering::SeqCst);
                    let outcome = execute(store, operation);
                    let finished = clock.fetch_add(1, Ordering::SeqCst);
                    events.lock().unwrap().push(Event {
                        operation,
                        outcome,
                        started,
                        finished,
                    });
                });
            }
            barrier.wait();
        });
        let expected = read_pair(&store);
        let mut events = events.into_inner().unwrap();
        events.push(Event {
            operation: Operation::ReadBoth,
            outcome: Outcome::Read(expected),
            started: clock.fetch_add(1, Ordering::SeqCst),
            finished: clock.fetch_add(1, Ordering::SeqCst),
        });
        assert!(explain(&events, 0, [3, 2]), "invalid history: {events:?}");
        drop(store);
        assert_eq!(read_pair(&Store::open(directory.path()).unwrap()), expected);
    }
}
