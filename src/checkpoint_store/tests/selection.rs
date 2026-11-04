use super::*;
use std::os::unix::fs::symlink;

#[test]
fn managed_rollovers_reopen_the_recorded_checkpoint_and_preserve_snapshots() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"new", b"first").unwrap();
    let snapshot = store.snapshot().unwrap();
    let next = dir.path().join("managed-first");
    store.rollover_managed(&next).unwrap();
    assert_eq!(store.overlay_keys().unwrap(), 0);
    store.put(b"new", b"second").unwrap();
    assert_eq!(
        snapshot.get(b"new").unwrap().as_deref(),
        Some(&b"first"[..])
    );
    assert!(store.rollover_to(dir.path().join("unmanaged")).is_err());
    assert!(!dir.path().join("unmanaged").exists());
    drop(snapshot);
    drop(store);

    assert!(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).is_err());
    let store = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"second"[..]));
    assert_eq!(store.overlay_keys().unwrap(), 1);
    store
        .rollover_managed(dir.path().join("managed-second"))
        .unwrap();
    store.put(b"third", b"value").unwrap();
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(
        reopened.get(b"new").unwrap().as_deref(),
        Some(&b"second"[..])
    );
    assert_eq!(
        reopened.get(b"third").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn managed_selection_publication_failures_poison_the_writer_and_recover_visible_selection() {
    for already_managed in [false, true] {
        for after_rename in [false, true] {
            let (dir, path, checkpoint, digest) = fixture();
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            if already_managed {
                store.rollover_managed(dir.path().join("first")).unwrap();
            }
            store.put(b"before", b"acknowledged").unwrap();
            let snapshot = store.snapshot().unwrap();
            let selection_file = super::super::selection::path(&path);
            let old_selection = fs::read(&selection_file).ok();
            let destination = dir.path().join("next");
            let error = store
                .rollover_managed_with_hooks(
                    &destination,
                    |parent| File::open(parent)?.sync_all(),
                    || {
                        if after_rename {
                            Ok(())
                        } else {
                            Err(io::Error::other("before rename"))
                        }
                    },
                    |parent| {
                        if after_rename {
                            Err(io::Error::other("directory sync"))
                        } else {
                            File::open(parent)?.sync_all()
                        }
                    },
                )
                .unwrap_err();
            assert!(error.to_string().contains("reopen required"));
            assert!(destination.exists());
            assert!(store.get(b"before").is_err());
            assert!(snapshot.get(b"before").is_err());
            assert!(store.put(b"after", b"not committed").is_err());
            let observed = fs::read(&selection_file).ok();
            if after_rename {
                assert_ne!(observed, old_selection);
            } else {
                assert_eq!(observed, old_selection);
            }
            drop(snapshot);
            drop(store);
            let reopened = if observed.is_some() {
                retry_lock(|| CheckpointStore::open_managed(&path)).unwrap()
            } else {
                assert_eq!(
                    retry_lock(|| CheckpointStore::open_managed(&path))
                        .err()
                        .unwrap()
                        .kind(),
                    io::ErrorKind::NotFound
                );
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap()
            };
            assert_eq!(
                reopened.get(b"before").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            assert_eq!(reopened.get(b"after").unwrap(), None);
            reopened.put(b"resumed", b"value").unwrap();
        }
    }
}

#[test]
fn managed_selection_missing_corrupt_symlink_and_wrong_store_fail_closed() {
    let (dir, path, checkpoint, digest) = fixture();
    assert_eq!(
        retry_lock(|| CheckpointStore::open_managed(&path))
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::NotFound
    );
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"unique", b"in-first-store").unwrap();
    store.rollover_managed(dir.path().join("selected")).unwrap();
    drop(store);
    let selection_file = super::super::selection::path(&path);
    let original = fs::read(&selection_file).unwrap();
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    fs::write(&selection_file, &corrupt).unwrap();
    assert!(retry_lock(|| CheckpointStore::open_managed(&path)).is_err());
    assert!(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).is_err());
    fs::remove_file(&selection_file).unwrap();
    let unrelated = dir.path().join("unrelated");
    fs::write(&unrelated, &original).unwrap();
    symlink(&unrelated, &selection_file).unwrap();
    assert!(retry_lock(|| CheckpointStore::open_managed(&path)).is_err());
    assert_eq!(fs::read(&unrelated).unwrap(), original);
    fs::remove_file(&selection_file).unwrap();
    fs::create_dir(&selection_file).unwrap();
    assert!(retry_lock(|| CheckpointStore::open_managed(&path)).is_err());
    fs::remove_dir(&selection_file).unwrap();
    fs::write(&selection_file, &original).unwrap();

    let (_other_dir, other_path, other_checkpoint, other_digest) = fixture();
    fs::write(super::super::selection::path(&other_path), &original).unwrap();
    assert!(retry_lock(|| CheckpointStore::open_managed(&other_path)).is_err());
    assert!(
        retry_lock(|| CheckpointStore::open(&other_path, &other_checkpoint, &other_digest))
            .is_err()
    );
    assert_eq!(
        fs::read(super::super::selection::path(&other_path)).unwrap(),
        original
    );
    let restored = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(
        restored.get(b"unique").unwrap().as_deref(),
        Some(&b"in-first-store"[..])
    );
}

#[test]
fn managed_rollover_detects_changed_selection_and_never_overwrites_reserved_path() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let selected = super::super::selection::path(&path);
    assert!(store.checkpoint_to(&selected).is_err());
    assert!(store.rollover_to(&selected).is_err());
    assert!(store.backup(&selected).is_err());
    assert!(store.compact_to(&selected).is_err());
    assert!(store.rollover_managed(&selected).is_err());
    assert!(!selected.exists());
    store.rollover_managed(dir.path().join("first")).unwrap();
    let original = fs::read(&selected).unwrap();
    fs::write(&selected, b"corrupt").unwrap();
    assert!(store.rollover_managed(dir.path().join("second")).is_err());
    assert!(!dir.path().join("second").exists());
    assert!(store.put(b"blocked", b"value").is_err());
    drop(store);
    fs::write(&selected, original).unwrap();
    assert!(
        retry_lock(|| CheckpointStore::open_managed(&path))
            .unwrap()
            .get(b"blocked")
            .unwrap()
            .is_none()
    );
}

#[test]
fn managed_open_applies_tail_and_log_limits_before_recovery() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.rollover_managed(dir.path().join("first")).unwrap();
    store.put(b"one", b"value").unwrap();
    store.put(b"two", b"value").unwrap();
    let length = fs::metadata(path.join("records.log")).unwrap().len();
    drop(store);
    assert!(
        retry_lock(|| CheckpointStore::open_managed_with_resource_limits(&path, 0, 0, 0, None))
            .is_err()
    );
    assert!(
        retry_lock(|| CheckpointStore::open_managed_with_resource_limits(
            &path,
            MAX_RECOVERED_VIEW_TAIL_BYTES,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            Some(length - 1)
        ))
        .is_err()
    );
    let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(
        reopened.get(b"two").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

fn pause_selection() -> io::Result<()> {
    println!("MANAGED_SELECTION_BOUNDARY_REACHED");
    io::stdout().flush()?;
    loop {
        std::thread::park();
    }
}

#[test]
fn managed_selection_crash_child() {
    let Ok(parent) = std::env::var("EMDA_SELECTION_CRASH_PATH") else {
        return;
    };
    let parent = Path::new(&parent);
    let mode = std::env::var("EMDA_SELECTION_CRASH_STAGE").unwrap();
    let digest = fs::read(parent.join("digest")).unwrap().try_into().unwrap();
    let store_path = parent.join("store");
    let store = if super::super::selection::path(&store_path).exists() {
        retry_lock(|| CheckpointStore::open_managed(&store_path)).unwrap()
    } else {
        retry_lock(|| CheckpointStore::open(&store_path, parent.join("checkpoint"), &digest))
            .unwrap()
    };
    store
        .rollover_managed_with_hooks(
            &parent.join("next"),
            |directory| File::open(directory)?.sync_all(),
            || {
                if mode == "staged" {
                    pause_selection()
                } else {
                    Ok(())
                }
            },
            |directory| {
                if mode == "published" {
                    pause_selection()
                } else {
                    File::open(directory)?.sync_all()
                }
            },
        )
        .unwrap();
}

#[test]
fn managed_selection_process_kill_recovers_the_visible_selection() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    for already_managed in [false, true] {
        for mode in ["staged", "published"] {
            let (dir, path, checkpoint, digest) = fixture();
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            if already_managed {
                store.rollover_managed(dir.path().join("first")).unwrap();
            }
            store.put(b"before", b"acknowledged").unwrap();
            let original_log = fs::read(path.join("records.log")).unwrap();
            let previous = fs::read(super::super::selection::path(&path)).ok();
            drop(store);
            fs::write(dir.path().join("digest"), digest).unwrap();
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "checkpoint_store::tests::selection::managed_selection_crash_child",
                    "--nocapture",
                ])
                .env("EMDA_SELECTION_CRASH_PATH", dir.path())
                .env("EMDA_SELECTION_CRASH_STAGE", mode)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (send, receive) = mpsc::channel();
            let reader = std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    if line.contains("MANAGED_SELECTION_BOUNDARY_REACHED") {
                        let _ = send.send(());
                        break;
                    }
                }
            });
            let reached = receive.recv_timeout(Duration::from_secs(20));
            let busy = crate::cleanup_abandoned_staging(&path);
            let _ = child.kill();
            let _ = child.wait();
            reader.join().unwrap();
            reached.expect("child did not reach managed selection boundary");
            assert_eq!(busy.unwrap_err().kind(), io::ErrorKind::WouldBlock);
            let cleaned = retry_lock(|| crate::cleanup_abandoned_staging(&path)).unwrap();
            assert_eq!(cleaned.removed_markers, 1);
            assert_eq!(cleaned.removed_payloads, usize::from(mode == "staged"));
            assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
            let visible = fs::read(super::super::selection::path(&path)).ok();
            if mode == "staged" {
                assert_eq!(visible, previous);
            } else {
                assert_ne!(visible, previous);
            }
            let reopened = if visible.is_some() {
                retry_lock(|| CheckpointStore::open_managed(&path)).unwrap()
            } else {
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap()
            };
            assert_eq!(
                reopened.get(b"before").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            reopened.put(b"after", b"recovered").unwrap();
        }
    }
}

#[test]
fn managed_selection_publication_blocks_writers_until_directory_sync() {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    let (dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let rolling = Arc::clone(&store);
        let destination = dir.path().join("next");
        let roller = scope.spawn(move || {
            rolling
                .rollover_managed_with_hooks(
                    &destination,
                    |parent| File::open(parent)?.sync_all(),
                    || Ok(()),
                    |parent| {
                        entered_tx.send(()).unwrap();
                        resume_rx
                            .recv_timeout(Duration::from_secs(10))
                            .map_err(|_| io::Error::other("release timed out"))?;
                        File::open(parent)?.sync_all()
                    },
                )
                .unwrap()
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let writing = Arc::clone(&store);
        let writer = scope.spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(writing.put(b"after", b"value")).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(50));
        resume_tx.send(()).unwrap();
        roller.join().unwrap();
        writer.join().unwrap();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
    });
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(
        reopened.get(b"after").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn failed_checkpoint_before_selection_publication_keeps_previous_view_usable() {
    for failure in ["checkpoint-sync", "checkpoint-open"] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.rollover_managed(dir.path().join("first")).unwrap();
        store.put(b"before", b"value").unwrap();
        let old_selection = fs::read(super::super::selection::path(&path)).unwrap();
        let destination = dir.path().join("next");
        let error = store
            .rollover_managed_with_hooks(
                &destination,
                |parent| {
                    if failure == "checkpoint-sync" {
                        Err(io::Error::other("injected checkpoint directory sync"))
                    } else {
                        fs::remove_file(&destination)?;
                        File::open(parent)?.sync_all()
                    }
                },
                || panic!("selection publication must not begin"),
                |_| panic!("selection publication must not begin"),
            )
            .unwrap_err();
        assert!(error.to_string().contains(if failure == "checkpoint-sync" {
            "checkpoint may exist"
        } else {
            "switching failed"
        }));
        assert_eq!(
            fs::read(super::super::selection::path(&path)).unwrap(),
            old_selection
        );
        store.put(b"after", b"acknowledged").unwrap();
        drop(store);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"after").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
    }
}

#[test]
fn panic_during_selection_publication_rejects_further_writes_and_recovers() {
    for after_rename in [false, true] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.rollover_managed(dir.path().join("first")).unwrap();
        store.put(b"before", b"acknowledged").unwrap();
        let original = fs::read(super::super::selection::path(&path)).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = store.rollover_managed_with_hooks(
                &dir.path().join("next"),
                |parent| File::open(parent)?.sync_all(),
                || {
                    if !after_rename {
                        panic!("before rename");
                    }
                    Ok(())
                },
                |_| panic!("after rename"),
            );
        }));
        assert!(result.is_err());
        assert!(store.get(b"before").is_err());
        assert!(store.put(b"after", b"must not commit").is_err());
        let visible = fs::read(super::super::selection::path(&path)).unwrap();
        assert_eq!(visible == original, !after_rename);
        drop(store);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"before").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        assert_eq!(reopened.get(b"after").unwrap(), None);
    }
}

#[test]
fn managed_open_rejects_selection_and_directory_sync_failures_before_return() {
    for fault in ["file", "directory-before", "directory-after"] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.rollover_managed(dir.path().join("first")).unwrap();
        store.put(b"before", b"acknowledged").unwrap();
        drop(store);
        let original_selection = fs::read(super::super::selection::path(&path)).unwrap();
        let result = retry_lock(|| {
            CheckpointStore::open_source_using(
                &path,
                OpenSource::Managed,
                OpenLimits {
                    max_tail_bytes: MAX_RECOVERED_VIEW_TAIL_BYTES,
                    max_tail_keys: MAX_RECOVERED_VIEW_TAIL_KEYS,
                    max_tail_key_bytes: MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                    max_log_bytes: None,
                    ..Default::default()
                },
                Log::open_checkpoint_writer,
                |store| {
                    super::super::selection::synchronize_with(
                        store,
                        |file| {
                            if fault == "file" {
                                Err(io::Error::other("selection sync"))
                            } else {
                                file.sync_all()
                            }
                        },
                        |parent| {
                            if fault == "directory-before" {
                                return Err(io::Error::other("directory sync"));
                            }
                            File::open(parent)?.sync_all()?;
                            if fault == "directory-after" {
                                return Err(io::Error::other("post-sync error"));
                            }
                            Ok(())
                        },
                    )
                },
            )
        });
        assert!(result.is_err());
        assert_eq!(
            fs::read(super::super::selection::path(&path)).unwrap(),
            original_selection
        );
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"before").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        reopened.put(b"after", b"value").unwrap();
    }
}

#[test]
fn managed_rollover_stops_if_any_checkpoint_ancestor_sync_fails() {
    let (probe, _, _, _) = fixture();
    let count = fs::canonicalize(probe.path()).unwrap().ancestors().count();
    drop(probe);
    for fail_at in 0..count {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.rollover_managed(dir.path().join("first")).unwrap();
        store.put(b"before", b"acknowledged").unwrap();
        let selection = super::super::selection::path(&path);
        let old_selection = fs::read(&selection).unwrap();
        let mut seen = 0;
        let result = store.rollover_managed_with_hooks(
            &dir.path().join("next"),
            |parent| {
                super::super::selection::sync_ancestors_with(parent, |ancestor| {
                    let index = seen;
                    seen += 1;
                    if index == fail_at {
                        Err(io::Error::other("injected ancestor sync"))
                    } else {
                        File::open(ancestor)?.sync_all()
                    }
                })
            },
            || panic!("selection publication must not begin"),
            |_| panic!("selection publication must not begin"),
        );
        assert!(result.is_err());
        assert_eq!(seen, fail_at + 1);
        assert_eq!(fs::read(&selection).unwrap(), old_selection);
        store.put(b"after", b"accepted").unwrap();
        drop(store);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"after").unwrap().as_deref(),
            Some(&b"accepted"[..])
        );
    }
}

#[test]
fn managed_rollover_poisoned_if_any_selection_ancestor_sync_fails() {
    let (probe, probe_path, _, _) = fixture();
    let count = fs::canonicalize(&probe_path).unwrap().ancestors().count();
    drop(probe);
    for fail_at in 0..count {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.rollover_managed(dir.path().join("first")).unwrap();
        store.put(b"before", b"acknowledged").unwrap();
        let mut seen = 0;
        let result = store.rollover_managed_with_hooks(
            &dir.path().join("next"),
            |parent| File::open(parent)?.sync_all(),
            || Ok(()),
            |parent| {
                super::super::selection::sync_ancestors_with(parent, |ancestor| {
                    let index = seen;
                    seen += 1;
                    if index == fail_at {
                        Err(io::Error::other("injected ancestor sync"))
                    } else {
                        File::open(ancestor)?.sync_all()
                    }
                })
            },
        );
        assert!(result.unwrap_err().to_string().contains("reopen required"));
        assert_eq!(seen, fail_at + 1);
        assert!(store.get(b"before").is_err());
        assert!(store.put(b"after", b"not committed").is_err());
        drop(store);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"before").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        assert_eq!(reopened.get(b"after").unwrap(), None);
    }
}

#[test]
fn managed_open_refuses_each_failed_selection_ancestor_sync() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.rollover_managed(dir.path().join("first")).unwrap();
    store.put(b"before", b"acknowledged").unwrap();
    drop(store);
    let count = fs::canonicalize(&path).unwrap().ancestors().count();
    for fail_at in 0..count {
        let mut seen = 0;
        let result = retry_lock(|| {
            CheckpointStore::open_source_using(
                &path,
                OpenSource::Managed,
                OpenLimits {
                    max_tail_bytes: MAX_RECOVERED_VIEW_TAIL_BYTES,
                    max_tail_keys: MAX_RECOVERED_VIEW_TAIL_KEYS,
                    max_tail_key_bytes: MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                    max_log_bytes: None,
                    ..Default::default()
                },
                Log::open_checkpoint_writer,
                |store| {
                    super::super::selection::synchronize_with(store, File::sync_all, |parent| {
                        super::super::selection::sync_ancestors_with(parent, |ancestor| {
                            let index = seen;
                            seen += 1;
                            if index == fail_at {
                                Err(io::Error::other("injected ancestor sync"))
                            } else {
                                File::open(ancestor)?.sync_all()
                            }
                        })
                    })
                },
            )
        });
        assert!(result.is_err());
        assert_eq!(seen, fail_at + 1);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"before").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
    }
}
