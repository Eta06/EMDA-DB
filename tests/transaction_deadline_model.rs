//! Model concurrent deadline outcomes and recovery on both writable backends.
use database_engine::{BatchOperation, Store, TimedTransactionError};
use std::cell::Cell;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Condvar, Mutex};
use std::time::{Duration, Instant};

mod support;
use support::retry_lock;
#[path = "transaction_deadline_model/backend.rs"]
mod backend;
#[path = "transaction_deadline_model/reference.rs"]
mod reference;
use backend::Backend;
use reference::serializes;

const MARKERS: [&[u8]; 4] = [b"m0", b"m1", b"m2", b"m3"];
const OPERATIONS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct State {
    pair: [u8; 2],
    marks: u8,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Committed([u8; 2]),
    Expired {
        staged: bool,
    },
    // Operation errors can have an unknown commit outcome. Only allow the
    // full atomic commit if the callback finished staging all three writes.
    Unknown {
        staged: Option<[u8; 2]>,
        timed_out: bool,
    },
    Batch,
    Put,
    Read([u8; 2]),
}

#[derive(Clone, Copy, Debug)]
struct Event {
    operation: usize,
    start: usize,
    end: usize,
    outcome: Outcome,
}

#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn wait(&self) {
        drop(
            self.changed
                .wait_while(self.open.lock().unwrap(), |open| !*open)
                .unwrap(),
        );
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

struct ReleaseOnDrop<'a>(&'a Gate);
impl Drop for ReleaseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

// Let the long-deadline writer start after the short-deadline writer and
// independent mutations complete. It still overlaps the staged-expiry writer
// when schedules permit, while guaranteeing a useful successful transaction.
struct FinishMutation<'a> {
    count: &'a AtomicUsize,
    gate: &'a Gate,
}
impl Drop for FinishMutation<'_> {
    fn drop(&mut self) {
        if self.count.fetch_add(1, Ordering::SeqCst) == 2 {
            self.gate.release();
        }
    }
}

fn execute(backend: &Backend, operation: usize, round: usize, gate: &Gate) -> Outcome {
    match operation {
        0..=3 => {
            let _release = (operation == 0).then(|| ReleaseOnDrop(gate));
            let budget = match operation {
                0 => Duration::from_millis(50),
                1 => Duration::ZERO,
                2 => Duration::from_millis(1),
                _ => Duration::from_secs(20),
            };
            let deadline = Instant::now() + budget;
            let staged = Cell::new(None);
            let result = backend.timed(deadline, round + operation, |tx| {
                let before = [tx.get(b"a")?.unwrap()[0], tx.get(b"b")?.unwrap()[0]];
                tx.put(b"a", &[before[0] + 1])?;
                std::thread::yield_now();
                tx.put(b"b", &[before[1] + 1])?;
                tx.put(MARKERS[operation], &[1])?;
                staged.set(Some(before));
                if operation == 0 {
                    gate.release();
                    std::thread::sleep(
                        deadline.saturating_duration_since(Instant::now())
                            + Duration::from_millis(1),
                    );
                }
                Ok(before)
            });
            match result {
                Ok(before) => Outcome::Committed(before),
                Err(TimedTransactionError::DeadlineExceeded) => Outcome::Expired {
                    staged: staged.get().is_some(),
                },
                Err(TimedTransactionError::Operation(error))
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    Outcome::Unknown {
                        staged: staged.get(),
                        timed_out: error.kind() == ErrorKind::TimedOut,
                    }
                }
                Err(error) => panic!("unexpected transaction error: {error}"),
            }
        }
        4 => {
            backend.batch(&[
                BatchOperation::Put {
                    key: b"a",
                    value: &[5],
                },
                BatchOperation::Put {
                    key: b"b",
                    value: &[5],
                },
            ]);
            Outcome::Batch
        }
        5 => {
            backend.put(b"a", &[2]);
            Outcome::Put
        }
        6 | 7 => Outcome::Read(backend.state().pair),
        _ => unreachable!(),
    }
}

#[test]
fn deadline_races_match_a_serial_history_after_both_recoveries() {
    let histories = std::env::var("EMDA_DEADLINE_MODEL_HISTORIES")
        .map(|value| value.parse::<usize>().expect("invalid history count"))
        .unwrap_or(8);
    assert!((1..=256).contains(&histories));
    let mut totals = [0_usize; 5];
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for capped in [false, true] {
            for cp in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("store");
                let checkpoint = root.path().join("checkpoint");
                std::fs::create_dir(&path).unwrap();
                if v2 {
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
                            value: &[3],
                        },
                        BatchOperation::Put {
                            key: b"b",
                            value: &[2],
                        },
                    ])
                    .unwrap();
                let digest = initial.checkpoint_to(&checkpoint).unwrap();
                drop(initial);
                let mut staged_expired = 0;
                let mut successful = 0;
                for round in 0..histories {
                    let backend = Backend::open(&path, &checkpoint, &digest, capped, cp);
                    backend.batch(&[
                        BatchOperation::Delete { key: MARKERS[0] },
                        BatchOperation::Delete { key: MARKERS[1] },
                        BatchOperation::Delete { key: MARKERS[2] },
                        BatchOperation::Delete { key: MARKERS[3] },
                        BatchOperation::Put {
                            key: b"a",
                            value: &[3],
                        },
                        BatchOperation::Put {
                            key: b"b",
                            value: &[2],
                        },
                    ]);
                    let before = State {
                        pair: [3, 2],
                        marks: 0,
                    };
                    assert_eq!(backend.state(), before);
                    let barrier = Barrier::new(OPERATIONS + 1);
                    let gate = Gate::default();
                    let mutations_done = Gate::default();
                    let mutations_finished = AtomicUsize::new(0);
                    let clock = AtomicUsize::new(0);
                    let events = std::thread::scope(|scope| {
                        let mut handles = Vec::new();
                        for offset in 0..OPERATIONS {
                            let operation = (round * 3 + offset) % OPERATIONS;
                            let (backend, barrier, gate, clock) =
                                (&backend, &barrier, &gate, &clock);
                            let (mutations_done, mutations_finished) =
                                (&mutations_done, &mutations_finished);
                            handles.push(scope.spawn(move || {
                                let _finished =
                                    matches!(operation, 2 | 4 | 5).then(|| FinishMutation {
                                        count: mutations_finished,
                                        gate: mutations_done,
                                    });
                                barrier.wait();
                                if operation != 0 {
                                    gate.wait();
                                }
                                if operation == 3 {
                                    mutations_done.wait();
                                }
                                let start = clock.fetch_add(1, Ordering::SeqCst);
                                let outcome = execute(backend, operation, round, gate);
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
                    let observed = backend.state();
                    assert!(
                        serializes(&events, before, observed),
                        "v2={v2} capped={capped} checkpoint={cp} round={round} events={events:?} final={observed:?}"
                    );
                    for event in &events {
                        match event.outcome {
                            Outcome::Committed(_) => {
                                totals[0] += 1;
                                successful += 1;
                            }
                            Outcome::Expired { staged } => {
                                totals[1] += 1;
                                if staged {
                                    totals[2] += 1;
                                    staged_expired += usize::from(event.operation == 0);
                                }
                            }
                            Outcome::Unknown {
                                timed_out: false, ..
                            } => totals[3] += 1,
                            Outcome::Unknown {
                                timed_out: true, ..
                            } => totals[4] += 1,
                            _ => {}
                        }
                    }
                    drop(backend);
                    for recovery_cp in [false, true] {
                        let recovered =
                            Backend::open(&path, &checkpoint, &digest, capped, recovery_cp);
                        assert_eq!(
                            recovered.state(),
                            observed,
                            "v2={v2} capped={capped} checkpoint={cp} round={round} recovery_cp={recovery_cp}"
                        );
                    }
                }
                assert!(
                    staged_expired > 0,
                    "v2={v2} capped={capped} checkpoint={cp}: no staged expiry was observed"
                );
                assert!(
                    successful > 0,
                    "v2={v2} capped={capped} checkpoint={cp}: no transaction committed"
                );
            }
        }
    }
    eprintln!(
        "histories_per_configuration={histories} committed={} expired={} staged_expired={} operation_would_block={} operation_timed_out={}",
        totals[0], totals[1], totals[2], totals[3], totals[4]
    );
}
