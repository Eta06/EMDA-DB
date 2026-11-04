use super::*;

#[test]
fn streamed_checkpoint_parent_sync_failure_reports_uncertain_publication() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"new", b"value").unwrap();
    let destination = dir.path().join("published");
    let error = store
        .checkpoint_to_with_sync(&destination, |_| {
            Err(io::Error::other("injected parent sync failure"))
        })
        .unwrap_err();
    assert!(error.to_string().contains("checkpoint may exist"));
    assert!(destination.exists());
    assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
    drop(store);
    // The checksum is read from the fixture only to inspect this uncertain
    // publication; real callers must protect the digest separately.
    let bytes = fs::read(&destination).unwrap();
    let published_digest = bytes[bytes.len() - 32..].try_into().unwrap();
    let reopened =
        retry_lock(|| CheckpointStore::open(&path, &destination, &published_digest)).unwrap();
    assert_eq!(
        reopened.get(b"base").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(
        reopened.get(b"new").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn streamed_checkpoint_prepublication_failure_leaves_source_usable() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"new", b"value").unwrap();
    let destination = dir.path().join("not-published");
    let state = store.write().unwrap();
    let (count, peak) = store.verify_for_checkpoint(&state).unwrap();
    let result = crate::checkpoint::create_streaming_with_hooks(
        &store.log,
        count,
        peak,
        &destination,
        |emit| {
            state.view.visit_entries(None, None, |key, location| {
                emit(key, location)?;
                Ok(std::ops::ControlFlow::Continue(()))
            })
        },
        |parent| File::open(parent)?.sync_all(),
        || Err(io::Error::other("injected before publication")),
    );
    assert!(result.is_err());
    assert!(!destination.exists());
    drop(state);
    assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
    store.put(b"later", b"still-writable").unwrap();
    drop(store);
    let reopened = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        reopened.get(b"later").unwrap().as_deref(),
        Some(&b"still-writable"[..])
    );
}

#[test]
fn streamed_checkpoint_blocks_writes_until_verified_publication() {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    let (dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    store.put(b"before", b"value").unwrap();
    let destination = dir.path().join("next");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let new_digest = std::thread::scope(|scope| {
        let checkpoint_store = Arc::clone(&store);
        let destination = &destination;
        let checkpoint_thread = scope.spawn(move || {
            let state = checkpoint_store.write().unwrap();
            let (count, peak) = checkpoint_store.verify_for_checkpoint(&state).unwrap();
            crate::checkpoint::create_streaming_with_hooks(
                &checkpoint_store.log,
                count,
                peak,
                destination,
                |emit| {
                    state.view.visit_entries(None, None, |key, location| {
                        emit(key, location)?;
                        Ok(std::ops::ControlFlow::Continue(()))
                    })
                },
                |parent| File::open(parent)?.sync_all(),
                || {
                    entered_tx.send(()).unwrap();
                    resume_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|_| io::Error::other("checkpoint test release timed out"))?;
                    Ok(())
                },
            )
            .unwrap()
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let writer = Arc::clone(&store);
        let writer_thread = scope.spawn(move || {
            started_tx.send(()).unwrap();
            let result = writer.put(b"after", b"value");
            done_tx.send(result).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(50));
        resume_tx.send(()).unwrap();
        let digest = checkpoint_thread.join().unwrap();
        writer_thread.join().unwrap();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        digest
    });
    drop(store);
    let historical =
        retry_lock(|| crate::CheckpointView::open(&path, &destination, &new_digest)).unwrap();
    assert_eq!(
        historical.get(b"before").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    assert_eq!(historical.get(b"after").unwrap(), None);
    drop(historical);
    let reopened = retry_lock(|| CheckpointStore::open(&path, &destination, &new_digest)).unwrap();
    assert_eq!(
        reopened.get(b"after").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn checkpoint_backup_blocks_writes_but_allows_readers() {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    let (dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let destination = dir.path().join("backup");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let backing = Arc::clone(&store);
        let destination = &destination;
        let backup = scope.spawn(move || {
            backing
                .backup_using(destination, |log, destination| {
                    crate::backup::create_with_hooks(
                        log,
                        destination,
                        |parent| File::open(parent)?.sync_all(),
                        || {
                            entered_tx.send(()).unwrap();
                            resume_rx
                                .recv_timeout(Duration::from_secs(10))
                                .map_err(|_| io::Error::other("backup test release timed out"))?;
                            Ok(())
                        },
                    )
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(
            store.get(b"base").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        let writer = Arc::clone(&store);
        let writer = scope.spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(writer.put(b"base", b"later")).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(50));
        resume_tx.send(()).unwrap();
        backup.join().unwrap();
        writer.join().unwrap();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
    });
    assert_eq!(store.get(b"base").unwrap().as_deref(), Some(&b"later"[..]));
    drop(store);
    let restored = dir.path().join("restored");
    Store::restore(&destination, &restored).unwrap();
    let copy = retry_lock(|| Store::open(&restored)).unwrap();
    assert_eq!(
        copy.get(b"base").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}

#[test]
fn live_rollover_sync_and_switch_failures_leave_the_old_view_usable() {
    for failure in ["sync", "switch"] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| {
            CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 1000, 1, 3)
        })
        .unwrap();
        store.put(b"new", b"first").unwrap();
        let original_tail = store.tail_bytes().unwrap();
        let destination = dir.path().join("next");
        let result = store.rollover_to_with_sync(&destination, |parent| {
            if failure == "sync" {
                Err(io::Error::other("injected directory sync error"))
            } else {
                fs::remove_file(&destination)?;
                File::open(parent)?.sync_all()
            }
        });
        let error = result.unwrap_err().to_string();
        if failure == "sync" {
            assert!(error.contains("checkpoint may exist"));
            assert!(destination.exists());
        } else {
            assert!(error.contains("checkpoint was published but switching failed"));
            assert!(!destination.exists());
        }
        assert_eq!(store.tail_bytes().unwrap(), original_tail);
        assert_eq!(store.overlay_keys().unwrap(), 1);
        assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"first"[..]));
        store.put(b"new", b"second").unwrap();
        assert!(store.put(b"other", b"full").is_err());
        drop(store);
        let reopened = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(
            reopened.get(b"new").unwrap().as_deref(),
            Some(&b"second"[..])
        );
    }
}

#[test]
fn every_streamed_checkpoint_ancestor_sync_failure_keeps_the_old_view() {
    let (probe, _, _, _) = fixture();
    let count = fs::canonicalize(probe.path()).unwrap().ancestors().count();
    drop(probe);
    for rollover in [false, true] {
        for fail_at in 0..count {
            let (dir, path, checkpoint, digest) = fixture();
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            store.put(b"before", b"value").unwrap();
            let old_tail = store.tail_bytes().unwrap();
            let next = dir.path().join("next");
            let mut seen = 0;
            let sync = |parent: &Path| {
                crate::directory::sync_existing_with(parent, |directory| {
                    let index = seen;
                    seen += 1;
                    if index == fail_at {
                        Err(io::Error::other("injected ancestor sync"))
                    } else {
                        File::open(directory)?.sync_all()
                    }
                })
            };
            let result = if rollover {
                store.rollover_to_with_sync(&next, sync)
            } else {
                store.checkpoint_to_with_sync(&next, sync)
            };
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("checkpoint may exist")
            );
            assert_eq!(seen, fail_at + 1);
            assert_eq!(store.tail_bytes().unwrap(), old_tail);
            store.put(b"after", b"acknowledged").unwrap();
            drop(store);
            let reopened =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            assert_eq!(
                reopened.get(b"after").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
        }
    }
}
