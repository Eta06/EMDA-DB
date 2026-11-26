//! Independent writing transactions share group commits, while overlapping
//! queued writes and exclusive writers keep their ordering.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

struct Held {
    store: Arc<Store>,
    armed: Arc<AtomicBool>,
    started: mpsc::Receiver<()>,
    resume: mpsc::Sender<()>,
}

// A store whose next group synchronization can be held open.
fn held(dir: &Path) -> Held {
    let armed = Arc::new(AtomicBool::new(false));
    let (started_tx, started) = mpsc::channel();
    let (resume, resume_rx) = mpsc::channel::<()>();
    let store = Store::open(dir).unwrap();
    let hook_armed = Arc::clone(&armed);
    let resume_rx = Mutex::new(resume_rx);
    store.commits.set_sync_hook(Some(Arc::new(move || {
        if hook_armed.swap(false, Ordering::SeqCst) {
            started_tx.send(()).unwrap();
            let _ = resume_rx.lock().unwrap().recv();
        }
    })));
    Held {
        store: Arc::new(store),
        armed,
        started,
        resume,
    }
}

fn reserved(store: &Store) -> usize {
    store.group_commit_status().unwrap().reserved_records
}

fn wait_for_reserved(store: &Store, records: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while reserved(store) < records {
        assert!(
            Instant::now() < deadline,
            "{records} records never reserved"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

// Starts a read-set transaction that writes `key` and holds its sync open.
fn hold_first(held: &Held, key: &'static [u8]) -> thread::JoinHandle<io::Result<()>> {
    held.armed.store(true, Ordering::SeqCst);
    let store = Arc::clone(&held.store);
    let first = thread::spawn(move || {
        store.transaction_with_read_set(|tx| {
            tx.get(key)?;
            tx.put(key, b"first")
        })
    });
    held.started.recv_timeout(Duration::from_secs(10)).unwrap();
    first
}

#[test]
fn a_disjoint_transaction_shares_the_pending_sync() {
    let dir = tempfile::tempdir().unwrap();
    let held = held(dir.path());
    let first = hold_first(&held, b"a");
    let store = Arc::clone(&held.store);
    let second = thread::spawn(move || {
        store.transaction_with_read_set(|tx| {
            tx.get(b"b")?;
            tx.scan(Some(b"m"), Some(b"z"), 16)?;
            tx.put(b"b", b"second")
        })
    });
    // The disjoint transaction validates and queues behind the first.
    wait_for_reserved(&held.store, 2);
    assert_eq!(
        held.store.get(b"b").unwrap(),
        None,
        "not visible before sync"
    );
    held.resume.send(()).unwrap();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    drop(held);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(reopened.get(b"a").unwrap().as_deref(), Some(&b"first"[..]));
    assert_eq!(reopened.get(b"b").unwrap().as_deref(), Some(&b"second"[..]));
}

#[test]
fn an_overlapping_point_or_range_conflicts_after_the_pending_write_publishes() {
    for overlap in ["point", "range", "conservative"] {
        let dir = tempfile::tempdir().unwrap();
        let held = held(dir.path());
        let first = hold_first(&held, b"k");
        let store = Arc::clone(&held.store);
        let finished = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&finished);
        let second = thread::spawn(move || {
            let result = match overlap {
                "point" => store.transaction_with_read_set(|tx| {
                    tx.get(b"k")?;
                    tx.put(b"other", b"x")
                }),
                "range" => store.transaction_with_read_set(|tx| {
                    tx.scan(Some(b"j"), Some(b"l"), 16)?;
                    tx.put(b"other", b"x")
                }),
                _ => store.transaction(|tx| {
                    tx.get(b"unrelated")?;
                    tx.put(b"other", b"x")
                }),
            };
            done.store(true, Ordering::SeqCst);
            result
        });
        // It neither queues nor returns while the overlapping write syncs.
        thread::sleep(Duration::from_millis(50));
        assert_eq!(reserved(&held.store), 1, "{overlap}");
        assert!(!finished.load(Ordering::SeqCst), "{overlap}");
        held.resume.send(()).unwrap();
        first.join().unwrap().unwrap();
        let error = second.join().unwrap().unwrap_err();
        assert!(crate::is_transaction_conflict(&error), "{overlap}: {error}");
        assert_eq!(held.store.get(b"other").unwrap(), None, "{overlap}");
        // A retry sees the published write and commits.
        held.store
            .transaction_with_read_set(|tx| {
                assert_eq!(tx.get(b"k")?.as_deref(), Some(&b"first"[..]));
                tx.put(b"other", b"x")
            })
            .unwrap();
    }
}

#[test]
fn exclusive_writers_wait_for_a_pending_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let held = held(dir.path());
    held.store.put(b"counter", &0_i64.to_le_bytes()).unwrap();
    let first = hold_first(&held, b"a");
    let store = Arc::clone(&held.store);
    let increment = thread::spawn(move || store.increment_i64(b"counter", 1));
    let store = Arc::clone(&held.store);
    let exchange =
        thread::spawn(move || store.compare_exchange(b"a", Some(b"first"), Some(b"exchanged")));
    // Neither exclusive writer reaches the queue while the transaction syncs.
    thread::sleep(Duration::from_millis(50));
    assert_eq!(reserved(&held.store), 1);
    held.resume.send(()).unwrap();
    first.join().unwrap().unwrap();
    assert_eq!(increment.join().unwrap().unwrap(), 1);
    // The exchange compared against the acknowledged transaction write.
    assert!(exchange.join().unwrap().unwrap());
    assert_eq!(
        held.store.get(b"a").unwrap().as_deref(),
        Some(&b"exchanged"[..])
    );
}

#[test]
fn concurrent_disjoint_and_contended_transfers_keep_every_acknowledged_result() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    for account in 0..8_u8 {
        store.put(&[b'a', account], &100_i64.to_le_bytes()).unwrap();
    }
    let committed = std::sync::atomic::AtomicUsize::new(0);
    thread::scope(|scope| {
        for client in 0..8_u8 {
            let (store, committed) = (&store, &committed);
            scope.spawn(move || {
                for round in 0..30_u8 {
                    // Half the clients use their own account pair; half share one.
                    let (from, to) = if client % 2 == 0 {
                        (client, (client + 1) % 8)
                    } else {
                        (0, 1 + round % 7)
                    };
                    loop {
                        let result = store.transaction_with_read_set(|tx| {
                            let read = |tx: &Transaction<'_>, account: u8| -> io::Result<i64> {
                                Ok(i64::from_le_bytes(
                                    tx.get(&[b'a', account])?
                                        .unwrap()
                                        .as_slice()
                                        .try_into()
                                        .unwrap(),
                                ))
                            };
                            let (a, b) = (read(tx, from)?, read(tx, to)?);
                            tx.put(&[b'a', from], &(a - 1).to_le_bytes())?;
                            tx.put(&[b'a', to], &(b + 1).to_le_bytes())
                        });
                        match result {
                            Ok(()) => break,
                            Err(error) if crate::is_transaction_conflict(&error) => {}
                            Err(error) => panic!("{error}"),
                        }
                    }
                    committed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(committed.into_inner(), 240);
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    let total: i64 = (0..8_u8)
        .map(|account| {
            i64::from_le_bytes(
                reopened
                    .get(&[b'a', account])
                    .unwrap()
                    .unwrap()
                    .as_slice()
                    .try_into()
                    .unwrap(),
            )
        })
        .sum();
    assert_eq!(total, 800);
}
