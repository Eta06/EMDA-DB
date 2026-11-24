use database_engine::{CheckpointStore, HandoffFailure, Store};
use std::fs;

mod support;
use support::retry_lock;

fn create_source(path: &std::path::Path, v2: bool) -> Store {
    if v2 {
        fs::create_dir(path).unwrap();
        fs::write(
            path.join("records.log"),
            include_bytes!("fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            path.join("commit.state"),
            include_bytes!("fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let store = Store::open(path).unwrap();
    store.put(b"key", b"old").unwrap();
    store.put(b"key", b"latest").unwrap();
    store
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn detailed_handoffs_preserve_rejected_and_successful_copies_in_both_formats() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("ordinary");
        let previous = root.path().join("ordinary-previous");
        let store = create_source(&path, v2);
        let log = fs::read(path.join("records.log")).unwrap();
        if !v2 && !log.starts_with(b"EMDALOG3") {
            eprintln!("V3 handoff case skipped: filesystem fell back to V2");
            continue;
        }
        assert!(log.starts_with(if v2 { b"EMDALOG2" } else { b"EMDALOG3" }));
        fs::write(&previous, b"preserve").unwrap();
        assert!(matches!(
            store.compact_and_replace_detailed(&path, &previous),
            Err(HandoffFailure::NotPublished(_))
        ));
        assert_eq!(fs::read(&previous).unwrap(), b"preserve");
        fs::remove_file(&previous).unwrap();
        let store = retry_lock(|| Store::open(&path)).unwrap();
        let store = store
            .compact_and_replace_detailed(&path, &previous)
            .unwrap();
        assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"latest"[..]));
        drop(store);
        for directory in [&path, &previous] {
            let reopened = retry_lock(|| Store::open(directory)).unwrap();
            assert_eq!(
                reopened.get(b"key").unwrap().as_deref(),
                Some(&b"latest"[..])
            );
        }

        for managed in [false, true] {
            let source = root
                .path()
                .join(if managed { "managed" } else { "checkpoint" });
            let previous = root.path().join(if managed {
                "managed-previous"
            } else {
                "checkpoint-previous"
            });
            let checkpoint = root.path().join(if managed {
                "managed-base"
            } else {
                "checkpoint-base"
            });
            let store = create_source(&source, v2);
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let cp = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            if managed {
                cp.rollover_managed(root.path().join("selected")).unwrap();
            }
            cp.put(b"tail", b"acknowledged").unwrap();
            fs::write(&previous, b"preserve").unwrap();
            assert!(matches!(
                cp.compact_and_replace_detailed(&previous),
                Err(HandoffFailure::NotPublished(_))
            ));
            assert_eq!(fs::read(&previous).unwrap(), b"preserve");
            fs::remove_file(&previous).unwrap();
            let cp = if managed {
                retry_lock(|| CheckpointStore::open_managed(&source)).unwrap()
            } else {
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap()
            };
            let (cp, new_digest) = cp.compact_and_replace_detailed(&previous).unwrap();
            assert_eq!(
                cp.get(b"tail").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            drop(cp);
            let reopened = if managed {
                retry_lock(|| CheckpointStore::open_managed(&source)).unwrap()
            } else {
                retry_lock(|| {
                    CheckpointStore::open(
                        &source,
                        source.join(database_engine::COMPACTED_CHECKPOINT_FILE),
                        &new_digest,
                    )
                })
                .unwrap()
            };
            assert_eq!(
                reopened.get(b"tail").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            drop(reopened);
            let old = retry_lock(|| Store::open(&previous)).unwrap();
            assert_eq!(
                old.get(b"tail").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[test]
fn unsupported_detailed_handoffs_are_rejected_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let previous = root.path().join("previous");
    let store = create_source(&source, true);
    assert!(matches!(
        store.compact_and_replace_detailed(&source, &previous),
        Err(HandoffFailure::NotPublished(error)) if error.kind() == std::io::ErrorKind::Unsupported
    ));
    assert!(!previous.exists());
    let store = retry_lock(|| Store::open(&source)).unwrap();
    let checkpoint = root.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let cp = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    assert!(matches!(
        cp.compact_and_replace_detailed(&previous),
        Err(HandoffFailure::NotPublished(error)) if error.kind() == std::io::ErrorKind::Unsupported
    ));
    assert!(!previous.exists());
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"latest"[..])
    );
}
