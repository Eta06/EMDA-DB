//! Concurrent transaction histories checked against a serial reference model.
use database_engine::{BatchOperation, Store, TimedTransactionError, Transaction, TransactionMode};
use std::io::{self, ErrorKind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
enum Operation {
    Transfer {
        from: usize,
        read_set: bool,
        fifo: bool,
        timed: bool,
    },
    SetBoth([u8; 2]),
    Put {
        key: usize,
        value: u8,
    },
    CompareExchange {
        key: usize,
        expected: u8,
        replacement: u8,
    },
    ReadBoth,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Committed([u8; 2]),
    Conflict,
    Done,
    Compared(bool),
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
            (
                Operation::CompareExchange {
                    key,
                    expected,
                    replacement,
                },
                Outcome::Compared(success),
            ) if success == (values[key] == expected) => {
                if success {
                    next[key] = replacement;
                }
            }
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
        Operation::Transfer {
            from,
            read_set,
            fifo,
            timed,
        } => match if timed {
            let mode = if fifo {
                TransactionMode::ReadSetFifo
            } else if read_set {
                TransactionMode::ReadSet
            } else {
                TransactionMode::Conservative
            };
            store
                .transaction_until(Instant::now() + Duration::from_secs(30), mode, |tx| {
                    transfer(tx, from)
                })
                .map_err(|error| match error {
                    TimedTransactionError::Operation(error) => error,
                    TimedTransactionError::DeadlineExceeded => {
                        panic!("model history exceeded its generous deadline")
                    }
                })
        } else if fifo {
            store.transaction_with_read_set_fifo(|tx| transfer(tx, from))
        } else if read_set {
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
        Operation::CompareExchange {
            key,
            expected,
            replacement,
        } => Outcome::Compared(
            store
                .compare_exchange(
                    if key == 0 { b"a" } else { b"b" },
                    Some(&[expected]),
                    Some(&[replacement]),
                )
                .unwrap(),
        ),
        Operation::ReadBoth => Outcome::Read(read_pair(store)),
    }
}

#[test]
fn reference_model_rejects_a_fractured_successful_transaction() {
    let history = [Event {
        operation: Operation::Transfer {
            from: 0,
            read_set: true,
            fifo: false,
            timed: false,
        },
        outcome: Outcome::Committed([3, 9]),
        started: 0,
        finished: 1,
    }];
    assert!(!explain(&history, 0, [3, 2]));
}

#[test]
fn reference_model_checks_compare_exchange_results() {
    for (expected, success, valid) in [
        (3, true, true),
        (3, false, false),
        (9, false, true),
        (9, true, false),
    ] {
        let history = [Event {
            operation: Operation::CompareExchange {
                key: 0,
                expected,
                replacement: 7,
            },
            outcome: Outcome::Compared(success),
            started: 0,
            finished: 1,
        }];
        assert_eq!(explain(&history, 0, [3, 2]), valid);
    }
}

#[test]
fn concurrent_transactions_batches_and_reads_match_a_serial_history_and_recovery() {
    let histories = std::env::var("EMDA_TRANSACTION_MODEL_HISTORIES")
        .map(|value| value.parse::<usize>().expect("invalid history count"))
        .unwrap_or(2048);
    assert!((1..=4096).contains(&histories));
    let operations = [
        Operation::Transfer {
            from: 0,
            read_set: false,
            fifo: false,
            timed: false,
        },
        Operation::Transfer {
            from: 1,
            read_set: true,
            fifo: false,
            timed: false,
        },
        Operation::Transfer {
            from: 0,
            read_set: true,
            fifo: true,
            timed: false,
        },
        Operation::Transfer {
            from: 1,
            read_set: false,
            fifo: false,
            timed: true,
        },
        Operation::Transfer {
            from: 0,
            read_set: true,
            fifo: false,
            timed: true,
        },
        Operation::Transfer {
            from: 1,
            read_set: true,
            fifo: true,
            timed: true,
        },
        Operation::SetBoth([5, 5]),
        Operation::Put { key: 0, value: 2 },
        Operation::CompareExchange {
            key: 0,
            expected: 3,
            replacement: 7,
        },
        Operation::ReadBoth,
        Operation::ReadBoth,
    ];
    let mut compared = [0_usize; 2];
    for iteration in 0..histories {
        let directory = tempfile::tempdir().unwrap();
        let capped = iteration % 2 == 0;
        let log_cap = (iteration % 4 == 0).then_some(1024 * 1024);
        let open = || {
            if capped {
                Store::open_with_resource_limits(directory.path(), log_cap, Some(2), Some(2))
            } else {
                Store::open(directory.path())
            }
        };
        let store = open().unwrap();
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
        for event in &events {
            if let Outcome::Compared(success) = event.outcome {
                compared[usize::from(success)] += 1;
            }
        }
        events.push(Event {
            operation: Operation::ReadBoth,
            outcome: Outcome::Read(expected),
            started: clock.fetch_add(1, Ordering::SeqCst),
            finished: clock.fetch_add(1, Ordering::SeqCst),
        });
        assert!(explain(&events, 0, [3, 2]), "invalid history: {events:?}");
        drop(store);
        assert_eq!(read_pair(&open().unwrap()), expected);
    }
    eprintln!(
        "compare-exchange outcomes: mismatch={} success={}",
        compared[0], compared[1]
    );
}
