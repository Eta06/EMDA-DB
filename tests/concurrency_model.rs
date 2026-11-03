//! Check short concurrent single-key histories against a sequential model.
use database_engine::Store;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Put(u8),
    Delete,
    Get,
}

#[derive(Debug)]
struct Event {
    operation: Operation,
    started: usize,
    finished: usize,
    observed: Option<u8>,
}

fn has_sequential_explanation(events: &[Event], used: u64, value: Option<u8>) -> bool {
    if used.count_ones() as usize == events.len() {
        return true;
    }
    for (index, event) in events.iter().enumerate() {
        let bit = 1_u64 << index;
        if used & bit != 0 {
            continue;
        }
        // Every operation that completed before this one started must come first.
        if events.iter().enumerate().any(|(other_index, other)| {
            used & (1_u64 << other_index) == 0 && other.finished < event.started
        }) {
            continue;
        }
        let next = match event.operation {
            Operation::Put(byte) => Some(byte),
            Operation::Delete => None,
            Operation::Get if event.observed == value => value,
            Operation::Get => continue,
        };
        if has_sequential_explanation(events, used | bit, next) {
            return true;
        }
    }
    false
}

#[test]
fn model_rejects_a_stale_read_after_a_completed_write() {
    let events = [
        Event {
            operation: Operation::Put(1),
            started: 0,
            finished: 1,
            observed: None,
        },
        Event {
            operation: Operation::Get,
            started: 2,
            finished: 3,
            observed: Some(0),
        },
    ];
    assert!(!has_sequential_explanation(&events, 0, Some(0)));
}

#[test]
fn overlapping_reads_writes_and_deletes_match_a_single_key_model() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let operations = [
        Operation::Put(1),
        Operation::Put(2),
        Operation::Delete,
        Operation::Get,
        Operation::Get,
        Operation::Get,
    ];
    let mut final_value = None;
    for _ in 0..40 {
        store.put(b"key", &[0]).unwrap();
        let clock = AtomicUsize::new(0);
        let barrier = Barrier::new(operations.len() + 1);
        let events = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for operation in operations {
                let store = Arc::clone(&store);
                let barrier = &barrier;
                let clock = &clock;
                let events = &events;
                scope.spawn(move || {
                    barrier.wait();
                    let started = clock.fetch_add(1, Ordering::SeqCst);
                    let observed = match operation {
                        Operation::Put(byte) => {
                            store.put(b"key", &[byte]).unwrap();
                            None
                        }
                        Operation::Delete => {
                            store.delete(b"key").unwrap();
                            None
                        }
                        Operation::Get => store.get(b"key").unwrap().map(|bytes| {
                            assert_eq!(bytes.len(), 1);
                            bytes[0]
                        }),
                    };
                    let finished = clock.fetch_add(1, Ordering::SeqCst);
                    events.lock().unwrap().push(Event {
                        operation,
                        started,
                        finished,
                        observed,
                    });
                });
            }
            barrier.wait();
        });
        final_value = store.get(b"key").unwrap().map(|bytes| bytes[0]);
        let mut events = events.into_inner().unwrap();
        events.push(Event {
            operation: Operation::Get,
            started: clock.fetch_add(1, Ordering::SeqCst),
            finished: clock.fetch_add(1, Ordering::SeqCst),
            observed: final_value,
        });
        assert!(
            has_sequential_explanation(&events, 0, Some(0)),
            "concurrent history has no valid sequential ordering: {events:?}"
        );
    }
    drop(store);
    let recovered = Store::open(dir.path()).unwrap().get(b"key").unwrap();
    assert_eq!(recovered.map(|bytes| bytes[0]), final_value);
}
