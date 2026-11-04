use super::*;

#[test]
fn staged_budget_is_shared_and_released_on_abort_and_panic() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let value = [7; 48];
    let cost = HEADER_SIZE + 1 + value.len() + CHECKSUM_SIZE;
    store.transaction_budget.lock().unwrap().limit = cost * 2 - 1;
    let result: io::Result<()> = store.transaction(|outer| {
        outer.put(b"a", &value)?;
        outer.put(b"a", b"x")?;
        assert_eq!(
            store.transaction_budget.lock().unwrap().used,
            HEADER_SIZE + 2 + CHECKSUM_SIZE
        );
        outer.put(b"a", &value)?;
        let rejected: io::Result<()> = store.transaction(|inner| inner.put(b"b", &value));
        assert!(rejected.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, cost);
        let aborted: io::Result<()> = store.transaction(|inner| {
            inner.put(b"b", b"x")?;
            Err(io::Error::other("abort nested transaction"))
        });
        assert!(aborted.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, cost);
        Err(io::Error::other("abort outer transaction"))
    });
    assert!(result.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    assert_eq!(store.get(b"a").unwrap(), None);
    assert_eq!(store.get(b"b").unwrap(), None);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: io::Result<()> = store.transaction(|tx| {
            tx.put(b"a", &value)?;
            panic!("abort transaction callback")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    store.transaction(|tx| tx.put(b"a", b"ok")).unwrap();
}

#[test]
fn generation_exhaustion_rejects_a_write_before_append() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.write().unwrap().generation = u64::MAX;
    let before = fs::read(path.join("records.log")).unwrap();
    assert!(store.put(b"new", b"value").is_err());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(store.get(b"new").unwrap(), None);
}

fn wait_for_queue(store: &CheckpointStore, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while store.transaction_queue.queued_for_test() != expected {
        assert!(
            std::time::Instant::now() < deadline,
            "transaction did not enter queue"
        );
        std::thread::yield_now();
    }
}

#[test]
fn fifo_runs_callbacks_and_commits_in_admission_order() {
    use std::sync::{Arc, mpsc};
    let (_dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let first_turn = store.transaction_queue.enter().unwrap();
    let (send, receive) = mpsc::channel();
    let mut handles = Vec::new();
    for index in 0..3_u8 {
        let waiting = Arc::clone(&store);
        let send = send.clone();
        handles.push(std::thread::spawn(move || {
            waiting.transaction_with_fifo(|tx| {
                send.send(index).unwrap();
                tx.put(b"winner", &[index])
            })
        }));
        wait_for_queue(&store, usize::from(index) + 2);
    }
    drop(first_turn);
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    for result in results {
        result.unwrap();
    }
    assert_eq!(receive.try_iter().collect::<Vec<_>>(), vec![0, 1, 2]);
    assert_eq!(store.get(b"winner").unwrap().as_deref(), Some(&[2][..]));
    assert_eq!(store.transaction_queue.queued_for_test(), 0);
    // Same-thread nested FIFO admission is rejected before it can deadlock.
    store
        .transaction_with_fifo(|outer| {
            let nested = store.transaction_with_fifo(|_| Ok(()));
            assert_eq!(nested.unwrap_err().kind(), io::ErrorKind::Other);
            outer.put(b"outer", b"ok")
        })
        .unwrap();
}

#[test]
fn fifo_waiters_wake_after_failed_or_panicking_commit() {
    use std::sync::Arc;
    for fault in [LogFault::SyncBefore, LogFault::Panic(1)] {
        let (_dir, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let store = Arc::new(store);
        let first_turn = store.transaction_queue.enter().unwrap();
        let first_store = Arc::clone(&store);
        let first = std::thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                first_store.transaction_with_fifo(|tx| {
                    tx.put(b"left", b"one")?;
                    tx.put(b"right", b"two")
                })
            }))
        });
        wait_for_queue(&store, 2);
        let second_store = Arc::clone(&store);
        let second = std::thread::spawn(move || {
            second_store.transaction_with_fifo(|tx| tx.put(b"other", b"must-not-commit"))
        });
        wait_for_queue(&store, 3);
        drop(first_turn);
        let first_result = first.join().unwrap();
        assert!(first_result.is_err() || first_result.unwrap().is_err());
        assert!(second.join().unwrap().is_err());
        assert_eq!(store.transaction_queue.queued_for_test(), 0);
        failed_instance_rejects_all_operations(&store, &path);
        drop(store);
        let recovered = retry_lock(|| Store::open(&path));
        if matches!(fault, LogFault::Panic(_)) {
            assert!(matches!(
                recovered.err().unwrap().kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
        } else {
            let recovered = recovered.unwrap();
            assert_eq!(
                recovered.get_many(&[b"left", b"right"]).unwrap(),
                vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
            );
            assert_eq!(recovered.get(b"other").unwrap(), None);
        }
    }
}

#[test]
fn fifo_other_writes_still_conflict_and_callback_panic_releases_turn() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let error = store
        .transaction_with_fifo(|tx| {
            tx.put(b"staged", b"discarded")?;
            store.put(b"independent", b"committed")?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(store.get(b"staged").unwrap(), None);
    assert_eq!(
        store.get(b"independent").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: io::Result<()> = store.transaction_with_fifo(|tx| {
            tx.put(b"staged", b"discarded")?;
            panic!("injected callback panic")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(store.transaction_queue.queued_for_test(), 0);
    assert_eq!(store.get(b"staged").unwrap(), None);
    store
        .transaction_with_fifo(|tx| tx.put(b"later", b"committed"))
        .unwrap();
}
