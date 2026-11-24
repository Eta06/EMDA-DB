use super::*;

#[test]
fn explicit_checkpoint_scan_limits_preserve_prior_state_and_admit_exact_reads() {
    for mode in ["copy", "rollover", "managed"] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let selection = super::super::selection::path(&path);
        if mode == "managed" {
            store.rollover_managed(dir.path().join("selected")).unwrap();
        }
        store.put(b"pending", b"value").unwrap();
        let snapshot = store.snapshot().unwrap();
        let destination = dir.path().join("next");
        let files = [
            path.join("records.log"),
            path.join("commit.state"),
            selection,
        ];
        let before = files.each_ref().map(|file| fs::read(file).ok());
        let tail = store.tail_bytes().unwrap();
        let exact = store.log.len().unwrap() * if mode == "copy" { 2 } else { 3 };

        let rejected = match mode {
            "copy" => store
                .checkpoint_to_with_scan_limit(&destination, exact - 1)
                .unwrap_err(),
            "rollover" => store
                .rollover_to_with_scan_limit(&destination, exact - 1)
                .unwrap_err(),
            "managed" => match store
                .rollover_managed_with_scan_limit(&destination, exact - 1)
                .unwrap_err()
            {
                crate::ManagedRolloverFailure::NotPublished(error) => {
                    crate::PublicationFailure::NotPublished(error)
                }
                error => panic!("unexpected managed outcome: {error}"),
            },
            _ => unreachable!(),
        };
        assert!(
            matches!(rejected, crate::PublicationFailure::NotPublished(error)
            if error.kind() == io::ErrorKind::InvalidInput
                && error.to_string() == "checkpoint log scan byte limit exceeded")
        );
        assert!(!destination.exists());
        assert_eq!(files.each_ref().map(|path| fs::read(path).ok()), before);
        assert_eq!(store.tail_bytes().unwrap(), tail);

        let digest = match mode {
            "copy" => store
                .checkpoint_to_with_scan_limit(&destination, exact)
                .unwrap(),
            "rollover" => store
                .rollover_to_with_scan_limit(&destination, exact)
                .unwrap(),
            "managed" => store
                .rollover_managed_with_scan_limit(&destination, exact)
                .unwrap(),
            _ => unreachable!(),
        };
        assert_eq!(
            store.tail_bytes().unwrap(),
            if mode == "copy" { tail } else { 0 }
        );
        assert_eq!(
            snapshot.get(b"pending").unwrap().as_deref(),
            Some(&b"value"[..])
        );
        drop(snapshot);
        drop(store);
        let reopened = retry_lock(|| {
            if mode == "managed" {
                CheckpointStore::open_managed(&path)
            } else {
                CheckpointStore::open(&path, &destination, &digest)
            }
        })
        .unwrap();
        assert_eq!(reopened.overlay_keys().unwrap(), 0);
        assert_eq!(
            reopened.get(b"pending").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}

#[test]
fn checkpoint_paths_reject_a_same_length_log_change_after_replay_reads_it() {
    use crate::log_io::replay_fault::ReplaceAfterRead;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    for mode in ["copy", "rollover", "managed", "automatic"] {
        let (dir, path, checkpoint, digest) = fixture();
        let mut store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
        let destination = dir.path().join("next");
        let selection_path = super::super::selection::path(&path);
        if mode == "automatic" {
            store.rollover_managed(dir.path().join("selected")).unwrap();
            store.enable_automatic_checkpoints().unwrap();
        }
        let previous_selection = fs::read(&selection_path).ok();
        let start = store.log.len().unwrap();
        store.put(b"new", b"value").unwrap();
        let end = store.log.len().unwrap();
        let log_path = path.join("records.log");
        let original = fs::read(&log_path).unwrap();
        let replacement = record::encode(Operation::Put, b"bad", b"value").unwrap();
        assert_eq!(replacement.len() as u64, end - start);
        let fired = Arc::new(AtomicBool::new(false));
        store.log.replace_io_for_test(Box::new(ReplaceAfterRead {
            file: fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&log_path)
                .unwrap(),
            observed: start..end,
            replace_at: start,
            replacement,
            fired: Arc::clone(&fired),
        }));

        let error = match mode {
            "copy" => store.checkpoint_to(&destination).map(|_| ()),
            "rollover" => store.rollover_to(&destination).map(|_| ()),
            "managed" => store.rollover_managed(&destination).map(|_| ()),
            _ => store
                .automatic_rollover(store.write().unwrap())
                .map(|_| ())
                .map_err(crate::ManagedRolloverFailure::into_io_error),
        }
        .expect_err(mode);
        assert!(fired.load(Ordering::Relaxed), "{mode}");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{mode}: {error}");
        assert_eq!(error.to_string(), "checkpoint does not match log prefix");
        assert_eq!(fs::read(&selection_path).ok(), previous_selection, "{mode}");
        assert!(!destination.exists(), "{mode}");
        assert_eq!(store.tail_bytes().unwrap(), end - start, "{mode}");
        if mode == "automatic" {
            let root = super::super::automatic::root(&path);
            assert!(!root.join("slot-0").exists());
            assert!(!root.join("slot-1").exists());
        }
        fs::write(log_path, original).unwrap();
        drop(store);
        let reopened = if mode == "automatic" {
            CheckpointStore::open_managed(&path).unwrap()
        } else {
            CheckpointStore::open(&path, &checkpoint, &digest).unwrap()
        };
        assert_eq!(
            reopened.get(b"new").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        assert_eq!(reopened.get(b"bad").unwrap(), None);
    }
}

#[test]
fn detailed_rollover_keeps_old_view_after_sync_or_switch_failure() {
    for failure in ["sync", "switch"] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.put(b"new", b"value").unwrap();
        let old_tail = store.tail_bytes().unwrap();
        let destination = dir.path().join("next");
        let result = store.rollover_to_detailed_with_sync(&destination, |parent| {
            if failure == "sync" {
                Err(io::Error::other("injected parent sync failure"))
            } else {
                fs::write(&destination, b"corrupt published checkpoint")?;
                File::open(parent)?.sync_all()
            }
        });
        assert!(matches!(
            result,
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert!(destination.exists());
        assert_eq!(store.tail_bytes().unwrap(), old_tail);
        assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
        store.put(b"later", b"safe").unwrap();
        drop(store);
        let recovered = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        assert_eq!(
            recovered.get(b"new").unwrap().as_deref(),
            Some(&b"value"[..])
        );
        assert_eq!(
            recovered.get(b"later").unwrap().as_deref(),
            Some(&b"safe"[..])
        );
    }
}

#[test]
fn detailed_streamed_checkpoint_sync_failure_is_unknown_and_source_remains_usable() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let target = dir.path().join("target");
    let result = store.checkpoint_to_detailed_with_sync(&target, |_| {
        Err(io::Error::other("injected parent sync failure"))
    });
    assert!(matches!(
        result,
        Err(crate::PublicationFailure::OutcomeUnknown(_))
    ));
    assert!(target.exists());
    assert_eq!(
        store.get(b"base").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    drop(store);
    let view = retry_lock(|| CheckpointView::open(&path, &target, &digest)).unwrap();
    assert_eq!(
        view.get(b"base").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}

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
    let (count, peak, verified_log) = store.verify_for_checkpoint(&state).unwrap();
    let result = crate::checkpoint::create_streaming_with_hooks(
        &store.log,
        count,
        peak,
        (&destination, verified_log),
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
fn manual_checkpoint_publication_allows_readers_and_excludes_writers() {
    use std::sync::{TryLockError, mpsc};
    use std::time::Duration;

    for mode in ["copy", "rollover", "managed"] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.put(b"before", b"value").unwrap();
        let destination = dir.path().join("next");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let (read_tx, read_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let new_digest = std::thread::scope(|scope| {
            let store = &store;
            let destination = &destination;
            let checkpoint_thread = scope.spawn(move || {
                let sync = |parent: &Path| {
                    entered_tx.send(()).unwrap();
                    resume_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|_| io::Error::other("checkpoint test release timed out"))?;
                    File::open(parent)?.sync_all()
                };
                match mode {
                    "copy" => store.checkpoint_to_with_sync(destination, sync),
                    "rollover" => store.rollover_to_with_sync(destination, sync),
                    "managed" => store.rollover_managed_with_hooks(
                        destination,
                        sync,
                        || {
                            assert!(store.state.try_read().is_ok());
                            assert_eq!(
                                store.get_many(&[b"before", b"after"])?,
                                [Some(b"value".to_vec()), None]
                            );
                            Ok(())
                        },
                        crate::checkpoint_store::selection::sync_ancestors,
                    ),
                    _ => unreachable!(),
                }
                .unwrap()
            });
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            assert!(destination.exists());
            assert!(matches!(
                store.writers.try_read(),
                Err(TryLockError::WouldBlock)
            ));
            let writer = scope.spawn(move || {
                started_tx.send(()).unwrap();
                done_tx.send(store.put(b"after", b"value")).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let reader = scope.spawn(move || {
                read_tx
                    .send(store.get_many(&[b"before", b"after"]))
                    .unwrap();
            });
            let observed = read_rx.recv_timeout(Duration::from_secs(2));
            let early = done_rx.recv_timeout(Duration::from_millis(50));
            resume_tx.send(()).unwrap();
            let digest = checkpoint_thread.join().unwrap();
            writer.join().unwrap();
            reader.join().unwrap();
            assert_eq!(
                observed.unwrap().unwrap(),
                [Some(b"value".to_vec()), None],
                "{mode}"
            );
            assert!(
                matches!(early, Err(mpsc::RecvTimeoutError::Timeout)),
                "{mode}"
            );
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap();
            digest
        });
        assert_eq!(
            store.overlay_keys().unwrap(),
            if mode == "copy" { 2 } else { 1 }
        );
        drop(store);
        let historical =
            retry_lock(|| crate::CheckpointView::open(&path, &destination, &new_digest)).unwrap();
        assert_eq!(
            historical.get(b"before").unwrap().as_deref(),
            Some(&b"value"[..])
        );
        assert_eq!(historical.get(b"after").unwrap(), None);
        drop(historical);
        let reopened = if mode == "managed" {
            retry_lock(|| CheckpointStore::open_managed(&path)).unwrap()
        } else {
            retry_lock(|| CheckpointStore::open(&path, &destination, &new_digest)).unwrap()
        };
        assert_eq!(
            reopened.get(b"after").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}

#[test]
fn manual_rollover_preparation_panic_still_disables_readers() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"before", b"committed").unwrap();
    let destination = dir.path().join("next");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.rollover_to_with_sync(&destination, |_| panic!("injected checkpoint sync panic"))
    }));
    assert!(result.is_err());
    assert!(store.get(b"before").is_err());
    assert!(store.put(b"after", b"value").is_err());
    drop(store);
    let recovered = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        recovered.get(b"before").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
    assert_eq!(recovered.get(b"after").unwrap(), None);
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
                        crate::FullLogBackupLimits::default(),
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
