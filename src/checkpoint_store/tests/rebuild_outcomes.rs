use super::*;
use crate::ManagedRebuildFailure;

// Observe the exact lock held by the rebuild across success and unwinding.
fn fixture_rebuild(
    path: &Path,
    destination: &Path,
    lock: &mut Option<crate::store_lock::StoreLock>,
    before_selection: impl FnOnce() -> io::Result<()>,
) -> io::Result<[u8; 32]> {
    let digest = CheckpointStore::rebuild_managed_using(
        path,
        destination,
        |path| {
            let store = Store::open(path)?;
            *lock = Some(store.share_store_lock());
            Ok(store)
        },
        super::super::selection::sync_ancestors,
        before_selection,
        |parent| File::open(parent)?.sync_all(),
        None,
    )?;
    drop(lock.take());
    Ok(digest)
}

#[test]
fn fixture_rebuild_errors_and_panics_preserve_other_lock_owners() {
    for panic in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        drop(Store::open(&path).unwrap());
        let mut lock = None;
        let mut reached = false;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fixture_rebuild(&path, &dir.path().join("checkpoint"), &mut lock, || {
                reached = true;
                if panic {
                    panic!("injected panic");
                }
                Err(io::Error::other("injected error"))
            })
        }));
        assert!(reached, "injected hook was not reached");
        if panic {
            assert!(result.is_err());
        } else {
            let error = result.unwrap().unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert!(error.to_string().ends_with("injected error"));
        }
        assert_eq!(
            Store::open(&path).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(lock);
        drop(Store::open(&path).unwrap());
    }
}

#[test]
fn fixture_rebuild_success_releases_its_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"value").unwrap();
    drop(store);
    let mut lock = None;
    fixture_rebuild(&path, &dir.path().join("checkpoint"), &mut lock, || Ok(())).unwrap();
    assert!(lock.is_none());
    let reopened = CheckpointStore::open_managed(&path).unwrap();
    assert_eq!(
        reopened.get(b"safe").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

fn rebuild_fixture(
    v2: bool,
    managed: bool,
) -> (tempfile::TempDir, PathBuf, PathBuf, [u8; 32], PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    if v2 {
        fs::create_dir(&path).unwrap();
        fs::write(
            path.join("records.log"),
            include_bytes!("../../../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            path.join("commit.state"),
            include_bytes!("../../../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let base = dir.path().join("base");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"acknowledged").unwrap();
    let digest = store.checkpoint_to(&base).unwrap();
    drop(store);
    if managed {
        let mut lock = None;
        fixture_rebuild(&path, &dir.path().join("old"), &mut lock, || Ok(())).unwrap();
        let store = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        store.put(b"tail", b"acknowledged").unwrap();
        drop(store);
    } else {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        store.put(b"tail", b"acknowledged").unwrap();
        drop(store);
    }
    let backup = dir.path().join("backup");
    let store = retry_lock(|| Store::open(&path)).unwrap();
    store.backup(&backup).unwrap();
    drop(store);
    (dir, path, base, digest, backup)
}

#[test]
fn detailed_rebuild_classifies_publication_boundaries_and_preserves_recovery() {
    for v2 in [true, false] {
        for managed in [false, true] {
            for failure in [
                "reject",
                "checkpoint-sync",
                "checkpoint-open",
                "selection-before",
                "selection-after",
            ] {
                let (dir, path, base, digest, backup) = rebuild_fixture(v2, managed);
                let selection_path = super::super::selection::path(&path);
                let previous = fs::read(&selection_path).ok();
                let destination = if failure == "reject" {
                    dir.path().join("missing/next")
                } else {
                    dir.path().join("next")
                };
                let result = crate::publication_outcome::track_managed_rebuild(|tracker| {
                    CheckpointStore::rebuild_managed_using(
                        &path,
                        &destination,
                        |path| {
                            crate::test_support::diagnose_lock(
                                path,
                                "rebuild classification source",
                                Store::open(path),
                            )
                        },
                        |parent| {
                            if failure == "checkpoint-sync" {
                                Err(io::Error::other("injected checkpoint sync failure"))
                            } else {
                                if failure == "checkpoint-open" {
                                    fs::write(&destination, b"corrupted checkpoint")?;
                                }
                                File::open(parent)?.sync_all()
                            }
                        },
                        || {
                            if failure == "selection-before" {
                                Err(io::Error::other("injected before selection rename"))
                            } else {
                                Ok(())
                            }
                        },
                        |parent| {
                            if failure == "selection-after" {
                                Err(io::Error::other("injected selection sync failure"))
                            } else {
                                File::open(parent)?.sync_all()
                            }
                        },
                        Some(tracker),
                    )
                });
                let classified = match failure {
                    "reject" => matches!(&result, Err(ManagedRebuildFailure::NotPublished(_))),
                    "checkpoint-sync" | "checkpoint-open" => matches!(
                        &result,
                        Err(ManagedRebuildFailure::CheckpointOutcomeUnknown(_))
                    ),
                    _ => matches!(
                        &result,
                        Err(ManagedRebuildFailure::SelectionOutcomeUnknown(_))
                    ),
                };
                assert!(
                    classified,
                    "v2={v2} managed={managed} failure={failure} result={result:?}"
                );
                assert_eq!(destination.exists(), failure != "reject");
                let visible = fs::read(&selection_path).ok();
                assert_eq!(visible == previous, failure != "selection-after");

                let recovered = retry_lock(|| Store::open(&path)).unwrap();
                for key in [b"safe".as_slice(), b"tail".as_slice()] {
                    assert_eq!(
                        recovered.get(key).unwrap().as_deref(),
                        Some(&b"acknowledged"[..])
                    );
                }
                drop(recovered);
                let recovered = if visible.is_some() {
                    retry_lock(|| CheckpointStore::open_managed(&path)).unwrap()
                } else {
                    retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap()
                };
                assert_eq!(
                    recovered.get(b"tail").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                drop(recovered);
                let restored_path = dir.path().join("restored");
                Store::restore(&backup, &restored_path).unwrap();
                let restored = retry_lock(|| Store::open(&restored_path)).unwrap();
                assert_eq!(
                    restored.get(b"tail").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
            }
        }
    }
}

#[test]
fn detailed_rebuild_public_api_preserves_limits_locks_and_existing_files() {
    for v2 in [true, false] {
        let (dir, path, _, _, _) = rebuild_fixture(v2, true);
        let previous = fs::read(super::super::selection::path(&path)).unwrap();
        let destination = dir.path().join("new");
        let active = retry_lock(|| Store::open(&path)).unwrap();
        let result = CheckpointStore::rebuild_managed_selection_detailed(&path, &destination);
        if let Err(ManagedRebuildFailure::NotPublished(error)) = &result {
            crate::test_support::report_lock_error(&path, "public rebuild rejection", error);
        }
        assert!(
            matches!(&result, Err(ManagedRebuildFailure::NotPublished(error)) if error.kind() == io::ErrorKind::WouldBlock),
            "{result:?}"
        );
        drop(active);
        let result = CheckpointStore::rebuild_managed_selection_with_limits_detailed(
            &path,
            &destination,
            Some(1),
            None,
            None,
        );
        assert!(
            matches!(&result, Err(ManagedRebuildFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidInput),
            "{result:?}"
        );
        assert!(!destination.exists());
        assert_eq!(
            fs::read(super::super::selection::path(&path)).unwrap(),
            previous
        );
        let existing = dir.path().join("existing");
        fs::write(&existing, b"preserve").unwrap();
        assert!(matches!(
            CheckpointStore::rebuild_managed_selection_detailed(&path, &existing),
            Err(ManagedRebuildFailure::CheckpointOutcomeUnknown(_))
        ));
        assert_eq!(fs::read(&existing).unwrap(), b"preserve");
        CheckpointStore::rebuild_managed_selection_with_limits_detailed(
            &path,
            &destination,
            None,
            None,
            None,
        )
        .unwrap();
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"tail").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
    }
}

#[test]
fn detailed_rebuild_holds_the_lock_through_success_or_uncertain_selection_sync() {
    use std::sync::mpsc;
    use std::time::Duration;

    for v2 in [true, false] {
        for fail_sync in [false, true] {
            let (dir, path, _, _, _) = rebuild_fixture(v2, true);
            let destination = dir.path().join("next");
            let (ready_tx, ready_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let result = std::thread::scope(|scope| {
                let path_ref = path.as_path();
                let destination_ref = destination.as_path();
                let rebuilding = scope.spawn(move || {
                    crate::publication_outcome::track_managed_rebuild(|tracker| {
                        CheckpointStore::rebuild_managed_using(
                            path_ref,
                            destination_ref,
                            |path| {
                                crate::test_support::diagnose_lock(
                                    path,
                                    "rebuild lock-hold source",
                                    Store::open(path),
                                )
                            },
                            super::super::selection::sync_ancestors,
                            || Ok(()),
                            |parent| {
                                ready_tx.send(()).unwrap();
                                resume_rx
                                    .recv_timeout(Duration::from_secs(10))
                                    .map_err(|_| io::Error::other("rebuild release timed out"))?;
                                if fail_sync {
                                    Err(io::Error::other("injected selection sync failure"))
                                } else {
                                    super::super::selection::sync_ancestors(parent)
                                }
                            },
                            Some(tracker),
                        )
                    })
                });
                if let Err(error) = ready_rx.recv_timeout(Duration::from_secs(10)) {
                    drop(resume_tx);
                    let result = rebuilding.join();
                    panic!("rebuild did not reach selection sync: {error:?}; worker={result:?}");
                }
                let ordinary = Store::open(&path).err().map(|error| error.kind());
                let checkpoint = CheckpointStore::open_managed(&path)
                    .err()
                    .map(|error| error.kind());
                resume_tx.send(()).unwrap();
                let result = rebuilding.join().unwrap();
                assert_eq!(ordinary, Some(io::ErrorKind::WouldBlock));
                assert_eq!(checkpoint, Some(io::ErrorKind::WouldBlock));
                result
            });
            if fail_sync {
                assert!(matches!(
                    result,
                    Err(ManagedRebuildFailure::SelectionOutcomeUnknown(_))
                ));
            } else {
                result.unwrap();
            }
            let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
            assert_eq!(
                reopened.get(b"tail").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            reopened.put(b"after", b"value").unwrap();
            drop(reopened);
            let recovered = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                recovered.get(b"after").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn not_published_does_not_claim_full_log_recovery_left_the_source_unchanged() {
    use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};

    for v2 in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        if v2 {
            fs::create_dir(&path).unwrap();
            fs::write(
                path.join("records.log"),
                include_bytes!("../../../tests/fixtures/v2-empty-records.log"),
            )
            .unwrap();
            fs::write(
                path.join("commit.state"),
                include_bytes!("../../../tests/fixtures/v2-empty-commit.state"),
            )
            .unwrap();
        }
        let store = Store::open(&path).unwrap();
        if !v2
            && !fs::read(path.join("records.log"))
                .unwrap()
                .starts_with(b"EMDALOG3")
        {
            continue;
        }
        store.put(b"safe", b"acknowledged").unwrap();
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("records.log"))
            .unwrap();
        let before_witness = if v2 {
            fs::read(path.join("commit.state")).unwrap()
        } else {
            let mut marker = [0; witness::WITNESS_SIZE];
            assert_eq!(
                fgetxattr(&log, "user.db.commit.v3", &mut marker[..]).unwrap(),
                marker.len()
            );
            marker.to_vec()
        };
        store.put(b"pending", b"complete").unwrap();
        drop(store);
        let complete_log = fs::read(path.join("records.log")).unwrap();
        // Model a complete but unacknowledged tail whose durable witness was
        // not advanced. This deliberate rollback is only a test fixture.
        if v2 {
            let mut witness_file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path.join("commit.state"))
                .unwrap();
            witness_file.write_all(&before_witness).unwrap();
            witness_file.sync_all().unwrap();
        } else {
            fsetxattr(
                &log,
                "user.db.commit.v3",
                &before_witness,
                XattrFlags::REPLACE,
            )
            .unwrap();
            log.sync_all().unwrap();
        }
        let selection = super::super::selection::path(&path);
        fs::write(&selection, b"invalid selection").unwrap();
        let destination = dir.path().join("never-published");
        let result = CheckpointStore::rebuild_managed_selection_detailed(&path, &destination);
        if let Err(ManagedRebuildFailure::NotPublished(error)) = &result {
            crate::test_support::report_lock_error(&path, "not-published recovery", error);
        }
        assert!(
            matches!(&result, Err(ManagedRebuildFailure::NotPublished(_))),
            "{result:?}"
        );
        assert!(!destination.exists());
        assert_eq!(fs::read(&selection).unwrap(), b"invalid selection");
        assert_eq!(fs::read(path.join("records.log")).unwrap(), complete_log);
        let after_witness = if v2 {
            fs::read(path.join("commit.state")).unwrap()
        } else {
            let mut marker = [0; witness::WITNESS_SIZE];
            assert_eq!(
                fgetxattr(&log, "user.db.commit.v3", &mut marker[..]).unwrap(),
                marker.len()
            );
            marker.to_vec()
        };
        assert_ne!(after_witness, before_witness, "v2={v2} rebuild={result:?}");
        let recovered = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(
            recovered.get(b"pending").unwrap().as_deref(),
            Some(&b"complete"[..])
        );
    }
}
