//! Serial-check successive concurrent windows without resetting the store.
use super::*;

fn open_writer(
    path: &std::path::Path,
    checkpoint: &std::path::Path,
    digest: &[u8; 32],
    backed: bool,
) -> Engine {
    if backed {
        Engine::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Engine::Ordinary(Store::open(path).unwrap())
    }
}

#[test]
fn concurrent_windows_form_a_long_serial_history_across_reopens() {
    let windows = std::env::var("EMDA_LONG_SERIAL_WINDOWS")
        .map(|value| value.parse::<usize>().expect("invalid window count"))
        .unwrap_or(64);
    assert!((1..=256).contains(&windows));
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
    let mut total_compared = [0_usize; 2];
    let mut recovery_checks = 0;
    for v2 in [true, false] {
        for checkpoint_writer in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            let checkpoint = root.path().join("checkpoint");
            std::fs::create_dir(&path).unwrap();
            if v2 {
                std::fs::write(
                    path.join("records.log"),
                    include_bytes!("../fixtures/v2-empty-records.log"),
                )
                .unwrap();
                std::fs::write(
                    path.join("commit.state"),
                    include_bytes!("../fixtures/v2-empty-commit.state"),
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
            assert_eq!(
                &std::fs::read(path.join("records.log")).unwrap()[..8],
                if v2 { b"EMDALOG2" } else { b"EMDALOG3" }
            );
            let digest = initial.checkpoint_to(&checkpoint).unwrap();
            drop(initial);
            let mut store = Some(open_writer(&path, &checkpoint, &digest, checkpoint_writer));
            let mut state = [Some(0), Some(0)];
            let clock = AtomicUsize::new(0);
            for window in 0..windows {
                let barrier = Barrier::new(operations.len() + 1);
                let events = Mutex::new(Vec::new());
                thread::scope(|scope| {
                    for offset in 0..operations.len() {
                        let operation = operations[(window * 5 + offset) % operations.len()];
                        let (store, barrier, clock, events) =
                            (store.as_ref().unwrap(), &barrier, &clock, &events);
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
                let expected = store.as_ref().unwrap().both();
                let mut events = events.into_inner().unwrap();
                for event in &events {
                    if let Outcome::Compared(success) = event.outcome {
                        total_compared[usize::from(success)] += 1;
                    }
                }
                events.push(Event {
                    operation: Operation::ReadBoth,
                    outcome: Outcome::Both(expected),
                    start: clock.fetch_add(1, Ordering::SeqCst),
                    finish: clock.fetch_add(1, Ordering::SeqCst),
                });
                assert!(
                    serializes(&events, 0, state),
                    "window={window} v2={v2} checkpoint={checkpoint_writer} start={state:?} events={events:?}"
                );
                state = expected;
                if (window + 1) % 8 == 0 || window + 1 == windows {
                    drop(store.take());
                    let ordinary = Engine::Ordinary(Store::open(&path).unwrap());
                    assert_eq!(ordinary.both(), state);
                    drop(ordinary);
                    let checkpoint_store = Engine::Checkpoint(
                        CheckpointStore::open(&path, &checkpoint, &digest).unwrap(),
                    );
                    assert_eq!(checkpoint_store.both(), state);
                    drop(checkpoint_store);
                    recovery_checks += 2;
                    if window + 1 < windows {
                        store = Some(open_writer(&path, &checkpoint, &digest, checkpoint_writer));
                    }
                }
            }
        }
    }
    eprintln!(
        "windows={} operations={} recovery_checks={} compare_mismatch={} compare_success={}",
        windows * 4,
        windows * 4 * operations.len(),
        recovery_checks,
        total_compared[0],
        total_compared[1]
    );
}
