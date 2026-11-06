//! Concurrent deletion, reinsertion and conditional writes checked by model.
#![cfg(target_os = "linux")]

use database_engine::{BatchOperation, CheckpointStore, Store};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};
use std::thread;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Put(usize, u8),
    Delete(usize),
    Compare(usize, Option<u8>, Option<u8>),
    Batch,
    ReadOne(usize),
    ReadBoth,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Done,
    Compared(bool),
    One(Option<u8>),
    Both([Option<u8>; 2]),
}

#[derive(Clone, Copy, Debug)]
struct Event {
    operation: Operation,
    outcome: Outcome,
    start: usize,
    finish: usize,
}

fn serializes(events: &[Event], used: u64, state: [Option<u8>; 2]) -> bool {
    if used.count_ones() as usize == events.len() {
        return true;
    }
    for (index, event) in events.iter().enumerate() {
        let bit = 1_u64 << index;
        if used & bit != 0
            || events.iter().enumerate().any(|(other, candidate)| {
                used & (1_u64 << other) == 0 && candidate.finish < event.start
            })
        {
            continue;
        }
        let mut next = state;
        match (event.operation, event.outcome) {
            (Operation::Put(key, value), Outcome::Done) => next[key] = Some(value),
            (Operation::Delete(key), Outcome::Done) => next[key] = None,
            (Operation::Compare(key, expected, replacement), Outcome::Compared(success))
                if success == (state[key] == expected) =>
            {
                if success {
                    next[key] = replacement;
                }
            }
            (Operation::Batch, Outcome::Done) => next = [None, Some(3)],
            (Operation::ReadOne(key), Outcome::One(value)) if value == state[key] => {}
            (Operation::ReadBoth, Outcome::Both(values)) if values == state => {}
            _ => continue,
        }
        if serializes(events, used | bit, next) {
            return true;
        }
    }
    false
}

fn key(index: usize) -> &'static [u8] {
    if index == 0 { b"a" } else { b"b" }
}

fn decode(value: Option<Vec<u8>>) -> Option<u8> {
    value.map(|bytes| {
        assert_eq!(bytes.len(), 1);
        bytes[0]
    })
}

enum Engine {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl Engine {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        match self {
            Self::Ordinary(store) => store.put(key, value),
            Self::Checkpoint(store) => store.put(key, value),
        }
    }
    fn delete(&self, key: &[u8]) -> io::Result<()> {
        match self {
            Self::Ordinary(store) => store.delete(key),
            Self::Checkpoint(store) => store.delete(key),
        }
    }
    fn compare(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
    ) -> io::Result<bool> {
        match self {
            Self::Ordinary(store) => store.compare_exchange(key, expected, replacement),
            Self::Checkpoint(store) => store.compare_exchange(key, expected, replacement),
        }
    }
    fn batch(&self) -> io::Result<()> {
        let operations = [
            BatchOperation::Put {
                key: b"a",
                value: &[6],
            },
            BatchOperation::Put {
                key: b"b",
                value: &[3],
            },
            BatchOperation::Delete { key: b"a" },
        ];
        match self {
            Self::Ordinary(store) => store.write_batch(&operations),
            Self::Checkpoint(store) => store.write_batch(&operations),
        }
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(store) => store.get(key),
            Self::Checkpoint(store) => store.get(key),
        }
    }
    fn both(&self) -> [Option<u8>; 2] {
        let values = match self {
            Self::Ordinary(store) => store.get_many(&[b"a", b"b"]),
            Self::Checkpoint(store) => store.get_many(&[b"a", b"b"]),
        }
        .unwrap();
        [decode(values[0].clone()), decode(values[1].clone())]
    }
}

fn execute(store: &Engine, operation: Operation) -> Outcome {
    match operation {
        Operation::Put(index, value) => {
            store.put(key(index), &[value]).unwrap();
            Outcome::Done
        }
        Operation::Delete(index) => {
            store.delete(key(index)).unwrap();
            Outcome::Done
        }
        Operation::Compare(index, expected, replacement) => {
            let expected = expected.map(|value| [value]);
            let replacement = replacement.map(|value| [value]);
            Outcome::Compared(
                store
                    .compare(
                        key(index),
                        expected.as_ref().map(|v| &v[..]),
                        replacement.as_ref().map(|v| &v[..]),
                    )
                    .unwrap(),
            )
        }
        Operation::Batch => {
            store.batch().unwrap();
            Outcome::Done
        }
        Operation::ReadOne(index) => Outcome::One(decode(store.get(key(index)).unwrap())),
        Operation::ReadBoth => Outcome::Both(store.both()),
    }
}

#[test]
fn model_rejects_fractured_batch_and_false_conditional_success() {
    let fractured = [
        Event {
            operation: Operation::Batch,
            outcome: Outcome::Done,
            start: 0,
            finish: 1,
        },
        Event {
            operation: Operation::ReadBoth,
            outcome: Outcome::Both([Some(6), Some(3)]),
            start: 2,
            finish: 3,
        },
    ];
    assert!(!serializes(&fractured, 0, [Some(0), Some(0)]));
    let wrong = [Event {
        operation: Operation::Compare(0, None, Some(2)),
        outcome: Outcome::Compared(true),
        start: 0,
        finish: 1,
    }];
    assert!(!serializes(&wrong, 0, [Some(0), Some(0)]));
}

#[test]
fn deletion_reinsertion_and_reads_have_a_serial_history_after_recovery() {
    let histories = std::env::var("EMDA_DELETE_MODEL_HISTORIES")
        .map(|value| value.parse::<usize>().expect("invalid history count"))
        .unwrap_or(512);
    assert!((1..=4096).contains(&histories));
    let operations = [
        Operation::Put(0, 1),
        Operation::Delete(0),
        Operation::Compare(0, None, Some(2)),
        Operation::Compare(0, Some(1), None),
        Operation::Batch,
        Operation::ReadOne(0),
        Operation::ReadBoth,
        Operation::Put(1, 4),
        Operation::ReadBoth,
    ];
    let mut compared = [0_usize; 2];
    for iteration in 0..histories {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        std::fs::create_dir(&path).unwrap();
        if iteration % 4 < 2 {
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
                    key: b"b",
                    value: &[0],
                },
            ])
            .unwrap();
        let expected_magic: &[u8] = if iteration % 4 < 2 {
            b"EMDALOG2"
        } else {
            b"EMDALOG3"
        };
        assert_eq!(
            &std::fs::read(path.join("records.log")).unwrap()[..8],
            expected_magic
        );
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        drop(initial);
        let store = if iteration % 2 == 0 {
            Engine::Ordinary(Store::open(&path).unwrap())
        } else {
            Engine::Checkpoint(CheckpointStore::open(&path, &checkpoint, &digest).unwrap())
        };
        let clock = AtomicUsize::new(0);
        let barrier = Barrier::new(operations.len() + 1);
        let events = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for offset in 0..operations.len() {
                let operation = operations[(iteration + offset) % operations.len()];
                let (store, barrier, clock, events) = (&store, &barrier, &clock, &events);
                scope.spawn(move || {
                    barrier.wait();
                    let start = clock.fetch_add(1, Ordering::SeqCst);
                    let outcome = execute(store, operation);
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
        let expected = store.both();
        let mut events = events.into_inner().unwrap();
        for event in &events {
            if let Outcome::Compared(success) = event.outcome {
                compared[usize::from(success)] += 1;
            }
        }
        events.push(Event {
            operation: Operation::ReadBoth,
            outcome: Outcome::Both(expected),
            start: clock.fetch_add(1, Ordering::SeqCst),
            finish: clock.fetch_add(1, Ordering::SeqCst),
        });
        assert!(
            serializes(&events, 0, [Some(0), Some(0)]),
            "invalid history: {events:?}"
        );
        drop(store);
        let ordinary = Engine::Ordinary(Store::open(&path).unwrap());
        assert_eq!(ordinary.both(), expected);
        drop(ordinary);
        let checkpoint_store =
            Engine::Checkpoint(CheckpointStore::open(&path, &checkpoint, &digest).unwrap());
        assert_eq!(checkpoint_store.both(), expected);
    }
    eprintln!(
        "conditional results: mismatch={} success={}",
        compared[0], compared[1]
    );
}

#[path = "delete_reinsert_model/long_history.rs"]
mod long_history;
