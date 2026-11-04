use super::*;

#[test]
fn reads_continue_while_mutations_wait_and_the_new_previous_path_is_safe() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let worker_store = Arc::clone(&store);
    let old_path = previous.clone();
    let worker = thread::spawn(move || {
        worker_store.compact_reclaim_with_hooks(
            &old_path,
            &Default::default(),
            |_| Ok(()),
            crate::directory::sync_existing,
            |point| {
                if point == reclaim::Point::AfterMove {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    {
        let state = store.state.read().unwrap();
        assert!(matches!(
            state.store.writes.try_read(),
            Err(TryLockError::WouldBlock)
        ));
    }
    let (done_tx, done_rx) = mpsc::channel();
    let read_store = Arc::clone(&store);
    let reader = thread::spawn(move || {
        done_tx
            .send(read_store.with_store(|db| db.get(b"overwritten")))
            .unwrap()
    });
    let callback = done_rx.recv_timeout(WAIT);
    let (started_tx, started_rx) = mpsc::channel();
    let (write_tx, write_rx) = mpsc::channel();
    let write_store = Arc::clone(&store);
    let writer = thread::spawn(move || {
        started_tx.send(()).unwrap();
        write_tx
            .send(write_store.with_store(|db| db.put(b"live", b"still active")))
            .unwrap();
    });
    started_rx.recv_timeout(WAIT).unwrap();
    let write_before_cleanup = write_rx.recv_timeout(Duration::from_millis(100));
    let replacement = Store::open(&previous).unwrap();
    replacement.put(b"new", b"independent").unwrap();
    drop(replacement);
    resume_tx.send(()).unwrap();
    assert_eq!(callback.unwrap().unwrap(), Some(b"39".to_vec()));
    assert!(matches!(
        write_before_cleanup,
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    reader.join().unwrap();
    worker.join().unwrap().unwrap();
    write_rx.recv_timeout(WAIT).unwrap().unwrap();
    writer.join().unwrap();
    let replacement = retry_lock(|| Store::open(&previous)).unwrap();
    assert_eq!(
        replacement.get(b"new").unwrap(),
        Some(b"independent".to_vec())
    );
    assert_eq!(
        store.with_store(|db| db.get(b"live")).unwrap(),
        Some(b"still active".to_vec())
    );
}

#[test]
fn acknowledged_concurrent_increments_survive_repeated_reclamation_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let (source, _) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    let start = Arc::new(Barrier::new(5));
    let values = Arc::new(Mutex::new(Vec::new()));
    thread::scope(|scope| {
        for _ in 0..4 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            let values = Arc::clone(&values);
            scope.spawn(move || {
                start.wait();
                for _ in 0..64 {
                    let value = store
                        .with_store(|db| db.increment_i64(b"count", 1))
                        .unwrap();
                    values.lock().unwrap().push(value);
                }
            });
        }
        start.wait();
        for n in 0..4 {
            store
                .compact_and_reclaim(
                    root.path().join(format!("previous-{n}")),
                    &Default::default(),
                )
                .unwrap();
        }
    });
    let mut actual = values.lock().unwrap().clone();
    actual.sort_unstable();
    assert_eq!(actual, (1..=256).collect::<Vec<_>>());
    assert_eq!(
        store.with_store(|db| db.get(b"count")).unwrap(),
        Some(256_i64.to_le_bytes().to_vec())
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"count")
            .unwrap(),
        Some(256_i64.to_le_bytes().to_vec())
    );
}

#[test]
fn waiting_writer_resumes_after_a_reported_cleanup_failure() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let compact_store = Arc::clone(&store);
    let compact = thread::spawn(move || {
        compact_store.compact_reclaim_with_hooks(
            &previous,
            &Default::default(),
            |_| Ok(()),
            crate::directory::sync_existing,
            |point| {
                if point == reclaim::Point::AfterMove {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                    return Err(io::Error::other("injected cleanup error"));
                }
                Ok(())
            },
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (written_tx, written_rx) = mpsc::channel();
    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || {
        started_tx.send(()).unwrap();
        written_tx
            .send(writer_store.with_store(|db| db.put(b"after", b"acknowledged")))
            .unwrap();
    });
    started_rx.recv_timeout(WAIT).unwrap();
    let early = written_rx.recv_timeout(Duration::from_millis(100));
    resume_tx.send(()).unwrap();
    assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
    assert!(matches!(
        compact.join().unwrap(),
        Err(OnlineReclaimError::Reclamation { .. })
    ));
    written_rx.recv_timeout(WAIT).unwrap().unwrap();
    writer.join().unwrap();
    crate::cleanup_abandoned_staging(root.path()).unwrap();
    drop(store);
    let reopened = retry_lock(|| Store::open(&source)).unwrap();
    assert_eq!(reopened.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
    assert_eq!(
        reopened.get(b"after").unwrap(),
        Some(b"acknowledged".to_vec())
    );
}
