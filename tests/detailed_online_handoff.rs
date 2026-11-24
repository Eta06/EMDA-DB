use database_engine::Store;
use database_engine::{
    OnlineCompactionOptions, OnlineHandoffFailure, OnlineReclaimFailure, OnlineStore,
};
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

#[test]
fn owned_snapshot_keeps_a_captured_store_readable_until_cancelled() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let online = create_store(&source, true);
    let mut snapshot = online.snapshot(1, 3).unwrap();
    online.with_store(|db| db.put(b"key", b"new")).unwrap();
    drop(online);
    assert_eq!(
        snapshot
            .as_snapshot()
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(b"latest".as_slice())
    );
    assert_eq!(
        Store::open(&source).err().unwrap().kind(),
        std::io::ErrorKind::WouldBlock
    );
    snapshot.cancel();
    snapshot.cancel();
    assert!(snapshot.as_snapshot().is_err());
    assert_eq!(
        Store::open(&source)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(b"new".as_slice())
    );
}

#[cfg(target_os = "linux")]
#[test]
fn owned_snapshots_read_reclaimed_logs_and_share_capacity_across_generations() {
    use database_engine::{CollectionSchema, DocumentDatabase, DocumentSnapshot};
    use serde_json::json;

    for v2 in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let previous = root.path().join("previous");
        let mut online = create_store(&source, v2);
        online.enable_direct_and_snapshot_read_limit(1).unwrap();
        online
            .with_store(|store| {
                store.set_snapshot_budget_limits(3, 64, 8192)?;
                let db = DocumentDatabase::new(store);
                db.create_collection("c", CollectionSchema::default())?;
                db.insert("c", "id", &json!({"generation": 0}))
            })
            .unwrap();
        let mut first = online.snapshot(32, 4096).unwrap();
        let mut middle = None;
        for value in 1..=2 {
            online
                .compact_and_reclaim_detailed(&previous, &OnlineCompactionOptions::default())
                .unwrap();
            assert!(!previous.exists());
            assert_eq!(
                online.enable_direct_read_limit(4).unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
            online
                .with_store(|store| store.set_active_direct_read_limit(0))
                .unwrap();
            assert_eq!(
                first.as_snapshot().unwrap().get(b"key").unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
            online
                .with_store(|store| store.set_active_direct_read_limit(1))
                .unwrap();
            online
                .with_store(|store| {
                    store.put(b"key", &[value])?;
                    DocumentDatabase::new(store).replace(
                        "c",
                        "id",
                        &json!({"generation": value}),
                        false,
                    )
                })
                .unwrap();
            if value == 1 {
                assert_eq!(
                    online
                        .with_store(|store| Ok(store.snapshot_budget_status()?.active))
                        .unwrap(),
                    1
                );
                middle = Some(online.snapshot(32, 4096).unwrap());
            }
        }
        let second = online.snapshot(32, 4096).unwrap();
        assert!(online.snapshot(32, 4096).is_err());
        assert_eq!(
            first
                .as_snapshot()
                .unwrap()
                .scan(Some(b"j"), None, 1)
                .unwrap(),
            vec![(b"key".to_vec(), b"latest".to_vec())]
        );
        let documents = DocumentSnapshot::new(first.as_snapshot().unwrap());
        assert_eq!(
            documents.get("c", "id").unwrap(),
            Some(json!({"generation": 0}))
        );
        assert_eq!(
            second.as_snapshot().unwrap().get(b"key").unwrap(),
            Some(vec![2])
        );
        let middle = middle.unwrap();
        assert_eq!(
            middle.as_snapshot().unwrap().get(b"key").unwrap(),
            Some(vec![1])
        );
        first.cancel();
        drop(middle);
        assert_eq!(
            online
                .with_store(|store| Ok(store.snapshot_budget_status()?.active))
                .unwrap(),
            1
        );
        let another = online.snapshot(32, 4096).unwrap();
        assert_eq!(
            another.as_snapshot().unwrap().get(b"key").unwrap(),
            Some(vec![2])
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn detailed_online_reclamation_preserves_data_after_rejection_and_success() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let previous = root.path().join("previous");
        let store = create_store(&source, v2);
        let log = fs::read(source.join("records.log")).unwrap();
        if !v2 && !log.starts_with(b"EMDALOG3") {
            eprintln!("V3 online reclamation case skipped: filesystem fell back to V2");
            continue;
        }
        fs::write(&previous, b"preserve").unwrap();
        assert!(matches!(
            store.compact_and_reclaim_detailed(&previous, &OnlineCompactionOptions::default()),
            Err(OnlineReclaimFailure::Compaction(
                OnlineHandoffFailure::NotPublished(_)
            ))
        ));
        assert_eq!(fs::read(&previous).unwrap(), b"preserve");
        fs::remove_file(&previous).unwrap();
        store
            .compact_and_reclaim_detailed(&previous, &OnlineCompactionOptions::default())
            .unwrap();
        assert!(!previous.exists());
        drop(store);
        assert_eq!(
            retry_lock(|| Store::open(&source))
                .unwrap()
                .get(b"key")
                .unwrap()
                .as_deref(),
            Some(&b"latest"[..])
        );
    }
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
    assert!(matches!(
        store.compact_and_reclaim_detailed(&previous, &OnlineCompactionOptions::default()),
        Err(OnlineReclaimFailure::Compaction(OnlineHandoffFailure::NotPublished(error)))
            if error.kind() == std::io::ErrorKind::Unsupported
    ));
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|db| db.get(b"key")).unwrap().as_deref(),
        Some(&b"latest"[..])
    );
}
