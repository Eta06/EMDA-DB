//! Compose transaction/batch histories across repeated recovery.
use super::*;
use database_engine::CheckpointStore;
use std::path::Path;

fn open(path: &Path, capped: bool) -> Store {
    if capped {
        Store::open_with_resource_limits(path, None, Some(2), Some(2)).unwrap()
    } else {
        Store::open(path).unwrap()
    }
}

fn checkpoint_pair(store: &CheckpointStore) -> [u8; 2] {
    let result = store.get_many(&[b"a", b"b"]).unwrap();
    [
        result[0].as_ref().unwrap()[0],
        result[1].as_ref().unwrap()[0],
    ]
}

#[test]
fn transaction_windows_compose_across_reopens() {
    let windows = std::env::var("EMDA_LONG_TRANSACTION_WINDOWS")
        .map(|value| value.parse::<usize>().expect("invalid window count"))
        .unwrap_or(64);
    assert!((1..=256).contains(&windows));
    let mut committed = 0;
    let mut conflicts = 0;
    let mut comparisons = [0_usize; 2];
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
            let initial = open(&path, capped);
            initial.put(b"a", &[3]).unwrap();
            initial.put(b"b", &[2]).unwrap();
            assert_eq!(
                &std::fs::read(path.join("records.log")).unwrap()[..8],
                if v2 { b"EMDALOG2" } else { b"EMDALOG3" }
            );
            let digest = initial.checkpoint_to(&checkpoint).unwrap();
            let mut store = Some(initial);
            let mut state = [3, 2];
            let clock = AtomicUsize::new(0);
            for window in 0..windows {
                let barrier = Barrier::new(MODEL_OPERATIONS.len() + 1);
                let events = Mutex::new(Vec::new());
                thread::scope(|scope| {
                    for offset in 0..MODEL_OPERATIONS.len() {
                        let operation =
                            MODEL_OPERATIONS[(window * 7 + offset) % MODEL_OPERATIONS.len()];
                        let (store, barrier, clock, events) =
                            (store.as_ref().unwrap(), &barrier, &clock, &events);
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
                let expected = read_pair(store.as_ref().unwrap());
                let mut events = events.into_inner().unwrap();
                for event in &events {
                    match event.outcome {
                        Outcome::Committed(_) => committed += 1,
                        Outcome::Conflict => conflicts += 1,
                        Outcome::Compared(success) => comparisons[usize::from(success)] += 1,
                        _ => {}
                    }
                }
                events.push(Event {
                    operation: Operation::ReadBoth,
                    outcome: Outcome::Read(expected),
                    started: clock.fetch_add(1, Ordering::SeqCst),
                    finished: clock.fetch_add(1, Ordering::SeqCst),
                });
                assert!(
                    explain(&events, 0, state),
                    "window={window} v2={v2} capped={capped} start={state:?} events={events:?}"
                );
                state = expected;
                if (window + 1) % 8 == 0 || window + 1 == windows {
                    drop(store.take());
                    let ordinary = open(&path, capped);
                    assert_eq!(read_pair(&ordinary), state);
                    drop(ordinary);
                    let checkpoint_store =
                        CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
                    assert_eq!(checkpoint_pair(&checkpoint_store), state);
                    drop(checkpoint_store);
                    recovery_checks += 2;
                    if window + 1 < windows {
                        store = Some(open(&path, capped));
                    }
                }
            }
        }
    }
    eprintln!(
        "windows={} operations={} recovery_checks={} committed={} conflicts={} compare_mismatch={} compare_success={}",
        windows * 4,
        windows * 4 * MODEL_OPERATIONS.len(),
        recovery_checks,
        committed,
        conflicts,
        comparisons[0],
        comparisons[1]
    );
}
