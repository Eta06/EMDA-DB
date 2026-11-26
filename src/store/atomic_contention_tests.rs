//! Compare-exchange and increment keep shared barrier access while their
//! record is synchronized, so ordinary writers to other keys can queue behind
//! them, while every exclusive writer still waits for the acknowledgement.
use super::fault_tests::BlockingSyncIo;
use super::*;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

fn blocking_store(
    dir: &Path,
) -> (
    Arc<Store>,
    Arc<AtomicBool>,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let armed = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    // A real witnessed log whose file I/O can block in one sync.
    let store = Store::open_using(dir, |path, initialize| {
        let (mut log, index) = Log::open(path, initialize)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))?;
        log.replace_io_for_test(Box::new(BlockingSyncIo {
            file,
            armed: Arc::clone(&armed),
            started: started_tx,
            resume: Mutex::new(resume_rx),
        }));
        Ok((log, index))
    })
    .unwrap();
    (Arc::new(store), armed, started_rx, resume_tx)
}

fn limited_blocking_store(
    dir: &Path,
    max_live_keys: usize,
) -> (
    Arc<Store>,
    Arc<AtomicBool>,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let (store, armed, started, resume) = blocking_store(dir);
    let mut store = Arc::into_inner(store).expect("only owner");
    store.max_live_keys = Some(max_live_keys);
    (Arc::new(store), armed, started, resume)
}

// Waits until `records` group-commit reservations are held: the syncing
// record plus any queued behind it.
fn wait_for_reserved(store: &Store, records: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while store.group_commit_status().unwrap().reserved_records < records {
        assert!(
            Instant::now() < deadline,
            "{records} records were never reserved"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn an_ordinary_write_queues_behind_a_syncing_compare_exchange() {
    let dir = tempfile::tempdir().unwrap();
    let (store, armed, started, resume) = blocking_store(dir.path());
    store.put(b"cas", b"old").unwrap();
    armed.store(true, Ordering::SeqCst);
    let exchanging = Arc::clone(&store);
    let exchange =
        thread::spawn(move || exchanging.compare_exchange(b"cas", Some(b"old"), Some(b"new")));
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    // A writer to another key reaches the queue while the exchange syncs.
    let writing = Arc::clone(&store);
    let other = thread::spawn(move || writing.put(b"other", b"queued"));
    wait_for_reserved(&store, 2);
    // A second exchange of the same key needs exclusive access, so it waits
    // for the first acknowledgement and then sees the new value.
    let racing = Arc::clone(&store);
    let second =
        thread::spawn(move || racing.compare_exchange(b"cas", Some(b"old"), Some(b"lost")));
    thread::sleep(Duration::from_millis(50));
    assert_eq!(store.group_commit_status().unwrap().reserved_records, 2);
    // Nothing is visible before the synchronization completes.
    assert_eq!(store.get(b"cas").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(store.get(b"other").unwrap(), None);
    resume.send(()).unwrap();
    assert!(exchange.join().unwrap().unwrap());
    other.join().unwrap().unwrap();
    assert!(!second.join().unwrap().unwrap());
    assert_eq!(store.get(b"cas").unwrap().as_deref(), Some(&b"new"[..]));
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(reopened.get(b"cas").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(
        reopened.get(b"other").unwrap().as_deref(),
        Some(&b"queued"[..])
    );
}

#[test]
fn increments_stay_serialized_while_other_writers_queue() {
    let dir = tempfile::tempdir().unwrap();
    let (store, armed, started, resume) = blocking_store(dir.path());
    armed.store(true, Ordering::SeqCst);
    let incrementing = Arc::clone(&store);
    let first = thread::spawn(move || incrementing.increment_i64(b"counter", 5));
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let writing = Arc::clone(&store);
    let other = thread::spawn(move || writing.put(b"other", b"queued"));
    wait_for_reserved(&store, 2);
    let racing = Arc::clone(&store);
    let second = thread::spawn(move || racing.increment_i64(b"counter", 7));
    thread::sleep(Duration::from_millis(50));
    assert_eq!(store.group_commit_status().unwrap().reserved_records, 2);
    resume.send(()).unwrap();
    assert_eq!(first.join().unwrap().unwrap(), 5);
    other.join().unwrap().unwrap();
    // The second increment read the acknowledged first result.
    assert_eq!(second.join().unwrap().unwrap(), 12);
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"counter").unwrap().as_deref(),
        Some(&12_i64.to_le_bytes()[..])
    );
}

#[test]
fn concurrent_compare_exchange_and_writers_keep_every_acknowledged_result() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    store.put(b"counter", &0_u64.to_le_bytes()).unwrap();
    // Each exchanger retries until it advances the counter by one; writers
    // to other keys run alongside. Every success must be counted exactly once.
    thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..25 {
                    loop {
                        let current = store.get(b"counter").unwrap().unwrap();
                        let next = u64::from_le_bytes(current.as_slice().try_into().unwrap()) + 1;
                        if store
                            .compare_exchange(b"counter", Some(&current), Some(&next.to_le_bytes()))
                            .unwrap()
                        {
                            break;
                        }
                    }
                }
            });
        }
        for writer in 0..4 {
            let store = &store;
            scope.spawn(move || {
                for index in 0..25 {
                    store
                        .put(format!("w{writer}-{index}").as_bytes(), b"v")
                        .unwrap();
                }
            });
        }
    });
    assert_eq!(
        store.get(b"counter").unwrap().as_deref(),
        Some(&100_u64.to_le_bytes()[..])
    );
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"counter").unwrap().as_deref(),
        Some(&100_u64.to_le_bytes()[..])
    );
    assert_eq!(reopened.scan(Some(b"w"), None, 1000).unwrap().len(), 100);
}

fn batch() -> [crate::BatchOperation<'static>; 2] {
    [
        crate::BatchOperation::Put {
            key: b"batch-a",
            value: b"1",
        },
        crate::BatchOperation::Put {
            key: b"batch-b",
            value: b"2",
        },
    ]
}

#[test]
fn a_blind_batch_queues_behind_a_syncing_write_unless_key_limits_apply() {
    for limited in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (store, armed, started, resume) = if limited {
            limited_blocking_store(dir.path(), 16)
        } else {
            blocking_store(dir.path())
        };
        armed.store(true, Ordering::SeqCst);
        let writing = Arc::clone(&store);
        let first = thread::spawn(move || writing.put(b"first", b"syncing"));
        started.recv_timeout(Duration::from_secs(10)).unwrap();
        let batching = Arc::clone(&store);
        let batch = thread::spawn(move || batching.write_batch(&batch()));
        if limited {
            // Live-key limits need stable membership: the batch waits for
            // the new key's acknowledgement before it is checked or queued.
            thread::sleep(Duration::from_millis(50));
            assert_eq!(store.group_commit_status().unwrap().reserved_records, 1);
        } else {
            wait_for_reserved(&store, 2);
        }
        resume.send(()).unwrap();
        first.join().unwrap().unwrap();
        batch.join().unwrap().unwrap();
        drop(store);
        let reopened = Store::open(dir.path()).unwrap();
        assert_eq!(
            reopened.scan(None, None, 10).unwrap().len(),
            3,
            "limited={limited}"
        );
    }
}
