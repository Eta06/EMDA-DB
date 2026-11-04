use super::*;
use std::os::unix::fs::symlink;

#[test]
fn rebuild_bootstraps_and_recovers_a_missing_checkpoint_or_oversized_suffix() {
    for missing in [false, true] {
        let (dir, path, _checkpoint, _digest) = fixture();
        let first = dir.path().join("first-managed");
        CheckpointStore::rebuild_managed_selection(&path, &first).unwrap();
        let store = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        store.put(b"one", b"value-1").unwrap();
        store.put(b"two", b"value-2").unwrap();
        drop(store);
        if missing {
            fs::remove_file(&first).unwrap();
        }
        assert!(
            retry_lock(|| CheckpointStore::open_managed_with_resource_limits(
                &path,
                64 * 1024,
                1,
                32,
                None
            ))
            .is_err()
        );
        let replacement = dir.path().join("rebuilt");
        CheckpointStore::rebuild_managed_selection(&path, &replacement).unwrap();
        let recovered = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(&path, 64 * 1024, 1, 32, None)
        })
        .unwrap();
        assert_eq!(recovered.overlay_keys().unwrap(), 0);
        assert_eq!(
            recovered.get(b"one").unwrap().as_deref(),
            Some(&b"value-1"[..])
        );
        assert_eq!(
            recovered.get(b"two").unwrap().as_deref(),
            Some(&b"value-2"[..])
        );
        recovered.put(b"new", b"accepted").unwrap();
    }
}

#[test]
fn rebuild_rejects_untrusted_selection_existing_destinations_and_active_store() {
    let (dir, path, checkpoint, digest) = fixture();
    let active = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let destination = dir.path().join("new");
    assert!(CheckpointStore::rebuild_managed_selection(&path, &destination).is_err());
    assert!(!destination.exists());
    drop(active);
    let selection = super::super::selection::path(&path);
    fs::write(&selection, b"unrelated").unwrap();
    assert!(CheckpointStore::rebuild_managed_selection(&path, &destination).is_err());
    assert_eq!(fs::read(&selection).unwrap(), b"unrelated");
    assert!(!destination.exists());
    fs::remove_file(&selection).unwrap();
    let unrelated = dir.path().join("unrelated");
    fs::write(&unrelated, b"keep").unwrap();
    symlink(&unrelated, &selection).unwrap();
    assert!(CheckpointStore::rebuild_managed_selection(&path, &destination).is_err());
    assert_eq!(fs::read(&unrelated).unwrap(), b"keep");
    fs::remove_file(&selection).unwrap();
    assert!(CheckpointStore::rebuild_managed_selection(&path, &selection).is_err());
    assert!(CheckpointStore::rebuild_managed_selection(&path, &unrelated).is_err());
    assert_eq!(fs::read(&unrelated).unwrap(), b"keep");
    assert!(
        CheckpointStore::rebuild_managed_selection(dir.path().join("missing-store"), &destination)
            .is_err()
    );
    assert!(!dir.path().join("missing-store").exists());
}

#[test]
fn rebuild_publication_errors_and_panics_leave_a_recoverable_visible_selection() {
    for panic in [false, true] {
        for after_rename in [false, true] {
            let (dir, path, checkpoint, digest) = fixture();
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            store.rollover_managed(dir.path().join("first")).unwrap();
            store.put(b"before", b"acknowledged").unwrap();
            drop(store);
            let selection = super::super::selection::path(&path);
            let previous = fs::read(&selection).unwrap();
            let outcome = std::panic::catch_unwind(|| {
                CheckpointStore::rebuild_managed_with_hooks(
                    &path,
                    &dir.path().join("rebuilt"),
                    || {
                        if !after_rename {
                            if panic {
                                panic!("injected before rename");
                            }
                            return Err(io::Error::other("injected before rename"));
                        }
                        Ok(())
                    },
                    |_| {
                        if panic {
                            panic!("injected after rename");
                        }
                        Err(io::Error::other("injected after rename"))
                    },
                )
            });
            if panic {
                assert!(outcome.is_err());
            } else {
                assert!(
                    outcome
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("recovery required")
                );
            }
            let visible = fs::read(&selection).unwrap();
            assert_eq!(visible == previous, !after_rename);
            let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
            assert_eq!(
                reopened.get(b"before").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            reopened.put(b"after", b"value").unwrap();
        }
    }
}

#[test]
fn rebuild_rejects_corrupt_log_without_replacing_the_selection() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.rollover_managed(dir.path().join("first")).unwrap();
    drop(store);
    let selection = super::super::selection::path(&path);
    let previous = fs::read(&selection).unwrap();
    let log_path = path.join("records.log");
    let log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)
        .unwrap();
    let mut byte = [0];
    FileExt::read_exact_at(&log, &mut byte, 28).unwrap();
    FileExt::write_all_at(&log, &[byte[0] ^ 1], 28).unwrap();
    assert!(CheckpointStore::rebuild_managed_selection(&path, dir.path().join("rebuilt")).is_err());
    assert_eq!(fs::read(selection).unwrap(), previous);
    assert!(!dir.path().join("rebuilt").exists());
}

#[test]
fn rebuild_holds_the_store_lock_through_selection_synchronization() {
    use std::sync::mpsc;
    use std::time::Duration;
    let (dir, path, _checkpoint, _digest) = fixture();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let path = &path;
        let destination = dir.path().join("rebuilt");
        let rebuilding = scope.spawn(move || {
            CheckpointStore::rebuild_managed_with_hooks(
                path,
                &destination,
                || Ok(()),
                |parent| {
                    ready_tx.send(()).unwrap();
                    resume_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|_| io::Error::other("release timed out"))?;
                    File::open(parent)?.sync_all()
                },
            )
            .unwrap()
        });
        ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(Store::open(path).is_err());
        assert_eq!(
            crate::cleanup_abandoned_staging(path).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        resume_tx.send(()).unwrap();
        rebuilding.join().unwrap();
    });
    assert!(retry_lock(|| CheckpointStore::open_managed(&path)).is_ok());
}

fn pause_rebuild() -> io::Result<()> {
    println!("MANAGED_REBUILD_BOUNDARY_REACHED");
    io::stdout().flush()?;
    loop {
        std::thread::park();
    }
}

#[test]
fn managed_rebuild_crash_child() {
    let Ok(parent) = std::env::var("EMDA_REBUILD_CRASH_PATH") else {
        return;
    };
    let parent = Path::new(&parent);
    let mode = std::env::var("EMDA_REBUILD_CRASH_STAGE").unwrap();
    CheckpointStore::rebuild_managed_with_hooks(
        &parent.join("store"),
        &parent.join("rebuilt"),
        || {
            if mode == "staged" {
                pause_rebuild()
            } else {
                Ok(())
            }
        },
        |directory| {
            if mode == "published" {
                pause_rebuild()
            } else {
                File::open(directory)?.sync_all()
            }
        },
    )
    .unwrap();
}

#[test]
fn managed_rebuild_process_kill_preserves_a_recoverable_selection() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    for mode in ["staged", "published"] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.rollover_managed(dir.path().join("first")).unwrap();
        store.put(b"before", b"acknowledged").unwrap();
        drop(store);
        let selection = super::super::selection::path(&path);
        let previous = fs::read(&selection).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "checkpoint_store::tests::rebuild::managed_rebuild_crash_child",
                "--nocapture",
            ])
            .env("EMDA_REBUILD_CRASH_PATH", dir.path())
            .env("EMDA_REBUILD_CRASH_STAGE", mode)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line.contains("MANAGED_REBUILD_BOUNDARY_REACHED") {
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
        reached.expect("child did not reach managed rebuild boundary");
        assert_eq!(busy.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        let cleaned = retry_lock(|| crate::cleanup_abandoned_staging(&path)).unwrap();
        assert_eq!(cleaned.removed_markers, 1);
        assert_eq!(cleaned.removed_payloads, usize::from(mode == "staged"));
        let visible = fs::read(selection).unwrap();
        assert_eq!(visible == previous, mode == "staged");
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"before").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        reopened.put(b"after", b"recovered").unwrap();
    }
}

#[test]
fn rebuild_preserves_selection_if_any_checkpoint_ancestor_sync_fails() {
    let (probe, _, _, _) = fixture();
    let count = fs::canonicalize(probe.path()).unwrap().ancestors().count();
    drop(probe);
    for fail_at in 0..count {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.rollover_managed(dir.path().join("first")).unwrap();
        store.put(b"before", b"acknowledged").unwrap();
        drop(store);
        let selection = super::super::selection::path(&path);
        let previous = fs::read(&selection).unwrap();
        let mut seen = 0;
        let result = CheckpointStore::rebuild_managed_with_sync_hooks(
            &path,
            &dir.path().join("rebuilt"),
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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("ancestry sync failed")
        );
        assert_eq!(seen, fail_at + 1);
        assert_eq!(fs::read(selection).unwrap(), previous);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"before").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
    }
}
