#[cfg(target_os = "linux")]
use database_engine::Store;
use database_engine::{OnlineCompactionOptions, OnlineHandoffFailure, OnlineStore};
use std::fs;

#[cfg(target_os = "linux")]
mod support;
#[cfg(target_os = "linux")]
use support::retry_lock;

fn create_store(path: &std::path::Path, v2: bool) -> OnlineStore {
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
    let store = OnlineStore::open(path).unwrap();
    store
        .with_store(|db| {
            db.put(b"key", b"old")?;
            db.put(b"key", b"latest")
        })
        .unwrap();
    store
}

#[cfg(target_os = "linux")]
#[test]
fn detailed_online_handoff_preserves_rejected_and_successful_copies() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let previous = root.path().join("previous");
        let store = create_store(&source, v2);
        let log = fs::read(source.join("records.log")).unwrap();
        if !v2 && !log.starts_with(b"EMDALOG3") {
            eprintln!("V3 online handoff case skipped: filesystem fell back to V2");
            continue;
        }
        assert!(log.starts_with(if v2 { b"EMDALOG2" } else { b"EMDALOG3" }));
        fs::write(&previous, b"preserve").unwrap();
        assert!(matches!(
            store.compact_and_replace_detailed(&previous, &OnlineCompactionOptions::default()),
            Err(OnlineHandoffFailure::NotPublished(_))
        ));
        assert_eq!(fs::read(&previous).unwrap(), b"preserve");
        assert_eq!(
            store.with_store(|db| db.get(b"key")).unwrap().as_deref(),
            Some(&b"latest"[..])
        );
        fs::remove_file(&previous).unwrap();
        store
            .compact_and_replace_detailed(&previous, &OnlineCompactionOptions::default())
            .unwrap();
        assert_eq!(
            store.with_store(|db| db.get(b"key")).unwrap().as_deref(),
            Some(&b"latest"[..])
        );
        drop(store);
        for path in [&source, &previous] {
            assert_eq!(
                retry_lock(|| Store::open(path))
                    .unwrap()
                    .get(b"key")
                    .unwrap()
                    .as_deref(),
                Some(&b"latest"[..])
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[test]
fn unsupported_detailed_online_handoff_keeps_handle_usable() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let previous = root.path().join("previous");
    let store = create_store(&source, true);
    assert!(matches!(
        store.compact_and_replace_detailed(&previous, &OnlineCompactionOptions::default()),
        Err(OnlineHandoffFailure::NotPublished(error)) if error.kind() == std::io::ErrorKind::Unsupported
    ));
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|db| db.get(b"key")).unwrap().as_deref(),
        Some(&b"latest"[..])
    );
}
