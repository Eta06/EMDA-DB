//! Compose checkpoint-writer transaction histories across recovery.
use super::*;
use std::path::Path;

fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], capped: bool) -> CheckpointStore {
    if capped {
        CheckpointStore::open_with_limits(path, checkpoint, digest, 16 * 1024 * 1024, 2, 2).unwrap()
    } else {
        CheckpointStore::open(path, checkpoint, digest).unwrap()
    }
}

#[test]
fn checkpoint_transaction_windows_compose_across_reopens() {
    let windows = std::env::var("EMDA_LONG_CHECKPOINT_TRANSACTION_WINDOWS")
        .map(|value| value.parse::<usize>().expect("invalid window count"))
        .unwrap_or(64);
    assert!((1..=256).contains(&windows));
    let mut committed = 0;
    let mut conflicts = 0;
    let mut recovery_checks = 0;
    for v2 in [true, false] {
        for capped in [false, true] {
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
                        key: b"x",
                        value: &[0],
                    },
                    BatchOperation::Put {
                        key: b"y",
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
            let mut store = Some(open(&path, &checkpoint, &digest, capped));
            let mut state = [0, 0];
            let clock = AtomicU64::new(0);
            for window in 0..windows {
                let barrier = Barrier::new(7);
                let events = std::thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for offset in 0..6 {
                        let operation = (window * 5 + offset) % 6;
                        let (store, barrier, clock) = (store.as_ref().unwrap(), &barrier, &clock);
                        handles.push(scope.spawn(move || {
                            barrier.wait();
                            if (window + operation) % 4 == 0 {
                                std::thread::yield_now();
                            }
                            let start = clock.fetch_add(1, Ordering::SeqCst);
                            let outcome = execute(store, operation, window);
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
                let expected = pair(store.as_ref().unwrap().get_many(&[b"x", b"y"]).unwrap());
                for event in &events {
                    match &event.outcome {
                        Outcome::Transaction(Some(_)) => committed += 1,
                        Outcome::Transaction(None) => conflicts += 1,
                        _ => {}
                    }
                }
                assert!(
                    serializes_from(&events, state, expected),
                    "window={window} v2={v2} capped={capped} start={state:?} events={events:?} final={expected:?}"
                );
                state = expected;
                if (window + 1) % 8 == 0 || window + 1 == windows {
                    drop(store.take());
                    let ordinary = Store::open(&path).unwrap();
                    assert_eq!(pair(ordinary.get_many(&[b"x", b"y"]).unwrap()), state);
                    drop(ordinary);
                    let checkpoint_store = open(&path, &checkpoint, &digest, capped);
                    assert_eq!(
                        pair(checkpoint_store.get_many(&[b"x", b"y"]).unwrap()),
                        state
                    );
                    recovery_checks += 2;
                    if window + 1 < windows {
                        store = Some(checkpoint_store);
                    }
                }
            }
        }
    }
    eprintln!(
        "windows={} operations={} recovery_checks={} committed={} conflicts={}",
        windows * 4,
        windows * 4 * 6,
        recovery_checks,
        committed,
        conflicts
    );
}
