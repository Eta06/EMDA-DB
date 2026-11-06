//! Check short concurrent histories against a real-time serial reference model.
use database_engine::{
    BatchOperation, CheckpointStore, CheckpointTransaction, CheckpointTransactionMode, Store,
    TimedTransactionError,
};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

#[derive(Debug)]
enum Outcome {
    Transaction(Option<[u8; 2]>),
    Blind,
    PutX,
    Read([u8; 2]),
}

#[derive(Debug)]
struct Event {
    operation: usize,
    start: u64,
    end: u64,
    outcome: Outcome,
}

fn serializes(events: &[Event], final_state: [u8; 2]) -> bool {
    fn search(events: &[Event], final_state: [u8; 2], chosen: u8, state: [u8; 2]) -> bool {
        if chosen.count_ones() as usize == events.len() {
            return state == final_state;
        }
        for (index, event) in events.iter().enumerate() {
            if chosen & (1 << index) != 0 {
                continue;
            }
            // Any operation that completed before this one began must precede it.
            if events.iter().enumerate().any(|(other, predecessor)| {
                chosen & (1 << other) == 0 && other != index && predecessor.end < event.start
            }) {
                continue;
            }
            let next = match &event.outcome {
                Outcome::Transaction(None) => state,
                Outcome::Transaction(Some(observed)) if *observed == state => match event.operation
                {
                    0 => [state[0] + 1, state[1] + 1],
                    1 => [state[1] + 2, state[0] + 3],
                    _ => unreachable!(),
                },
                Outcome::Blind => [7, 8],
                Outcome::PutX => [4, state[1]],
                Outcome::Read(observed) if *observed == state => state,
                _ => continue,
            };
            if search(events, final_state, chosen | (1 << index), next) {
                return true;
            }
        }
        false
    }
    search(events, final_state, 0, [0, 0])
}

fn pair(values: Vec<Option<Vec<u8>>>) -> [u8; 2] {
    [
        values[0].as_ref().unwrap()[0],
        values[1].as_ref().unwrap()[0],
    ]
}

#[test]
fn model_rejects_a_stale_read_after_a_completed_independent_write() {
    let events = [
        Event {
            operation: 4,
            start: 0,
            end: 1,
            outcome: Outcome::PutX,
        },
        Event {
            operation: 5,
            start: 2,
            end: 3,
            outcome: Outcome::Read([0, 0]),
        },
    ];
    assert!(!serializes(&events, [4, 0]));
}

#[test]
fn mixed_checkpoint_histories_match_a_serial_model_after_recovery() {
    let histories = std::env::var("EMDA_CHECKPOINT_MODEL_HISTORIES")
        .map(|value| value.parse::<usize>().expect("invalid history count"))
        .unwrap_or(256);
    assert!((1..=4096).contains(&histories));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let mut store = Arc::new(CheckpointStore::open(&path, &checkpoint, &digest).unwrap());
    for iteration in 0..histories {
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"x",
                    value: &[0],
                },
                BatchOperation::Put {
                    key: b"y",
                    value: &[0],
                },
            ])
            .unwrap();
        let barrier = Arc::new(Barrier::new(7));
        let clock = Arc::new(AtomicU64::new(0));
        let events = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for operation in 0..6 {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let clock = Arc::clone(&clock);
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    if (iteration + operation) % 4 == 0 {
                        std::thread::yield_now();
                    }
                    let start = clock.fetch_add(1, Ordering::SeqCst);
                    let outcome = match operation {
                        0 | 1 => {
                            let action = |tx: &mut CheckpointTransaction<'_>| {
                                let (x, y) = if operation == 0 {
                                    (tx.get(b"x")?.unwrap()[0], tx.get(b"y")?.unwrap()[0])
                                } else {
                                    let range = tx.scan(Some(b"w"), Some(b"z"), 10)?;
                                    assert_eq!(range.len(), 2);
                                    assert_eq!(range[0].0, b"x");
                                    assert_eq!(range[1].0, b"y");
                                    (range[0].1[0], range[1].1[0])
                                };
                                let next = if operation == 0 {
                                    [x + 1, y + 1]
                                } else {
                                    [y + 2, x + 3]
                                };
                                tx.put(b"x", &[next[0]])?;
                                tx.put(b"y", &[next[1]])?;
                                Ok([x, y])
                            };
                            let result = match (iteration + operation) % 8 {
                                0 => store.transaction(action),
                                1 => store.transaction_with_fifo(action),
                                2 => store.transaction_with_read_set(action),
                                3 => store.transaction_with_read_set_fifo(action),
                                mode => store
                                    .transaction_until(
                                        std::time::Instant::now()
                                            + std::time::Duration::from_secs(30),
                                        match mode {
                                            4 => CheckpointTransactionMode::Conservative,
                                            5 => CheckpointTransactionMode::Fifo,
                                            6 => CheckpointTransactionMode::ReadSet,
                                            _ => CheckpointTransactionMode::ReadSetFifo,
                                        },
                                        action,
                                    )
                                    .map_err(|error| match error {
                                        TimedTransactionError::Operation(error) => error,
                                        TimedTransactionError::DeadlineExceeded => {
                                            panic!(
                                                "model transaction exceeded its unexpired deadline"
                                            )
                                        }
                                    }),
                            };
                            match result {
                                Ok(observed) => Outcome::Transaction(Some(observed)),
                                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                    Outcome::Transaction(None)
                                }
                                Err(error) => panic!("unexpected transaction error: {error}"),
                            }
                        }
                        2 => {
                            store
                                .write_batch(&[
                                    BatchOperation::Put {
                                        key: b"x",
                                        value: &[7],
                                    },
                                    BatchOperation::Put {
                                        key: b"y",
                                        value: &[8],
                                    },
                                ])
                                .unwrap();
                            Outcome::Blind
                        }
                        3 | 5 => Outcome::Read(pair(store.get_many(&[b"x", b"y"]).unwrap())),
                        4 => {
                            store.put(b"x", &[4]).unwrap();
                            Outcome::PutX
                        }
                        _ => unreachable!(),
                    };
                    let end = clock.fetch_add(1, Ordering::SeqCst);
                    Event {
                        operation,
                        start,
                        end,
                        outcome,
                    }
                }));
            }
            barrier.wait();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        let final_state = pair(store.get_many(&[b"x", b"y"]).unwrap());
        assert!(
            serializes(&events, final_state),
            "iteration {iteration}: {events:?}, final={final_state:?}"
        );
        drop(store);
        let recovered = Store::open(&path).unwrap();
        assert_eq!(
            pair(recovered.get_many(&[b"x", b"y"]).unwrap()),
            final_state
        );
        drop(recovered);
        store = Arc::new(CheckpointStore::open(&path, &checkpoint, &digest).unwrap());
    }
}
