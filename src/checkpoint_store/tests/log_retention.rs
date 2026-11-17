use super::*;

/// Rollover is not live-log reclamation. The same file bytes stay addressable
/// for an existing snapshot, and an unchanged transaction generation remains
/// equivalent to unchanged log locations.
#[test]
fn rollover_preserves_log_bytes_snapshots_and_read_set_locations() {
    let (dir, path, checkpoint, original_digest) = fixture();
    let log = path.join("records.log");
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &original_digest)).unwrap();
    store.put(b"a", b"v1").unwrap();
    let snapshot = store.snapshot().unwrap();
    assert_eq!(store.snapshot_budget_status().unwrap().active, 1);
    let before_overwrite = fs::read(&log).unwrap();

    store.put(b"a", b"v2").unwrap();
    let after_overwrite = fs::read(&log).unwrap();
    assert!(after_overwrite.len() > before_overwrite.len());
    assert!(after_overwrite.starts_with(&before_overwrite));
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"v2"[..]));

    let next = dir.path().join("rolled");
    let mut first_rollover = None;
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"v2"[..]));
            let pinned = fs::read(&log)?;
            let digest = store.rollover_to(&next)?;
            assert_eq!(fs::read(&log)?, pinned);
            assert_eq!(store.tail_bytes()?, 0);
            first_rollover = Some(digest);
            tx.put(b"b", b"from-read-set")?;
            Ok(())
        })
        .unwrap();
    let first_digest = first_rollover.expect("read-set rollover digest");
    assert_eq!(
        store.get(b"b").unwrap().as_deref(),
        Some(&b"from-read-set"[..])
    );

    let later = dir.path().join("rolled-again");
    let mut second_rollover = None;
    store
        .transaction(|tx| {
            let pinned = fs::read(&log)?;
            let digest = store.rollover_to(&later)?;
            assert_eq!(fs::read(&log)?, pinned);
            assert_eq!(store.tail_bytes()?, 0);
            second_rollover = Some(digest);
            tx.put(b"c", b"from-conservative")?;
            Ok(())
        })
        .unwrap();
    let second_digest = second_rollover.expect("conservative rollover digest");
    assert_eq!(
        store.get(b"c").unwrap().as_deref(),
        Some(&b"from-conservative"[..])
    );

    let conflict = store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"v2"[..]));
            store.put(b"a", b"temporary")?;
            store.put(b"a", b"v2")?;
            tx.put(b"e", b"must-not-commit")?;
            Ok(())
        })
        .unwrap_err();
    assert!(crate::is_transaction_conflict(&conflict));
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"v2"[..]));
    assert!(store.get(b"e").unwrap().is_none());
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(snapshot.get(b"b").unwrap(), None);

    let retained = fs::read(&log).unwrap();
    assert!(retained.starts_with(&after_overwrite));
    assert!(retained.len() > after_overwrite.len());
    let retained_len = retained.len() as u64;
    drop(snapshot);
    assert_eq!(store.snapshot_budget_status().unwrap().active, 0);
    assert_eq!(fs::metadata(&log).unwrap().len(), retained_len);
    drop(store);

    let from_original =
        retry_lock(|| CheckpointStore::open(&path, &checkpoint, &original_digest)).unwrap();
    assert!(from_original.tail_bytes().unwrap() > 0);
    assert_eq!(
        from_original.get(b"a").unwrap().as_deref(),
        Some(&b"v2"[..])
    );
    assert_eq!(
        from_original.get(b"b").unwrap().as_deref(),
        Some(&b"from-read-set"[..])
    );
    assert_eq!(
        from_original.get(b"c").unwrap().as_deref(),
        Some(&b"from-conservative"[..])
    );
    assert!(from_original.get(b"e").unwrap().is_none());
    let original_tail = from_original.tail_bytes().unwrap();
    drop(from_original);

    let from_first = retry_lock(|| CheckpointStore::open(&path, &next, &first_digest)).unwrap();
    assert_eq!(from_first.get(b"a").unwrap().as_deref(), Some(&b"v2"[..]));
    assert!(from_first.get(b"e").unwrap().is_none());
    drop(from_first);

    let from_second = retry_lock(|| CheckpointStore::open(&path, &later, &second_digest)).unwrap();
    assert!(from_second.tail_bytes().unwrap() > 0);
    assert!(from_second.tail_bytes().unwrap() < original_tail);
    assert_eq!(from_second.get(b"a").unwrap().as_deref(), Some(&b"v2"[..]));
    assert_eq!(
        from_second.get(b"c").unwrap().as_deref(),
        Some(&b"from-conservative"[..])
    );
    assert!(from_second.get(b"e").unwrap().is_none());
    assert_eq!(fs::metadata(&log).unwrap().len(), retained_len);
}

#[test]
fn automatic_rollover_appends_without_reclaiming_a_live_snapshot() {
    let (dir, path, checkpoint, digest) = fixture();
    let log = path.join("records.log");
    let store =
        retry_lock(|| CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 1024, 1, 64))
            .unwrap();
    store
        .rollover_managed(dir.path().join("initial-managed"))
        .unwrap();
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"v1").unwrap();
    let snapshot = store.snapshot().unwrap();
    let before = fs::read(&log).unwrap();

    store.put(b"b", b"v2").unwrap();
    let after_first = fs::read(&log).unwrap();
    assert!(after_first.len() > before.len());
    assert!(after_first.starts_with(&before));
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(snapshot.get(b"b").unwrap(), None);

    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"v1"[..]));
            let pinned = fs::read(&log)?;
            store.put(b"c", b"v3")?;
            let grown = fs::read(&log)?;
            assert!(grown.len() > pinned.len());
            assert!(grown.starts_with(&pinned));
            tx.put(b"d", b"v4")?;
            Ok(())
        })
        .unwrap();

    let final_log = fs::read(&log).unwrap();
    assert!(final_log.starts_with(&before));
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(snapshot.get(b"c").unwrap(), None);
    assert_eq!(snapshot.get(b"d").unwrap(), None);
    assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"v2"[..]));
    assert_eq!(store.get(b"c").unwrap().as_deref(), Some(&b"v3"[..]));
    assert_eq!(store.get(b"d").unwrap().as_deref(), Some(&b"v4"[..]));
    drop(snapshot);
    drop(store);

    let recovered = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(recovered.get(b"a").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(recovered.get(b"d").unwrap().as_deref(), Some(&b"v4"[..]));
    assert_eq!(fs::metadata(&log).unwrap().len(), final_log.len() as u64);
}
