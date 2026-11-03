use database_engine::{MAX_SNAPSHOT_KEY_BYTES, MAX_SNAPSHOT_KEYS, Store};
use std::io::ErrorKind;

#[test]
fn snapshot_pages_remain_stable_across_updates_deletions_and_insertions() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    for (k, v) in [
        (b"a".as_slice(), b"old-a".as_slice()),
        (b"b", b"old-b"),
        (b"c", b"old-c"),
    ] {
        store.put(k, v).unwrap();
    }
    let snapshot = store.snapshot(3, 3).unwrap();
    let first = snapshot.scan(None, None, 1).unwrap();
    store.put(b"a", b"new-a").unwrap();
    store.delete(b"b").unwrap();
    store.put(b"d", b"new-d").unwrap();
    let second = snapshot.scan(Some(&first[0].0), None, 1).unwrap();
    let third = snapshot.scan(Some(&second[0].0), None, 1).unwrap();
    assert_eq!(first, vec![(b"a".to_vec(), b"old-a".to_vec())]);
    assert_eq!(second, vec![(b"b".to_vec(), b"old-b".to_vec())]);
    assert_eq!(third, vec![(b"c".to_vec(), b"old-c".to_vec())]);
    assert!(
        snapshot
            .scan(Some(&third[0].0), None, 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"old-a"[..]));
    assert_eq!(snapshot.get(b"d").unwrap(), None);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"new-a"[..]));
    drop(snapshot);
    store.put(b"after", b"drop").unwrap();
}

#[test]
fn snapshot_enforces_caller_and_hard_limits_without_partial_views() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    assert!(
        store
            .snapshot(0, 0)
            .unwrap()
            .scan(None, None, 1)
            .unwrap()
            .is_empty()
    );
    store.put(b"ab", b"v").unwrap();
    store.put(b"c", b"v").unwrap();
    assert!(store.snapshot(1, 3).is_err());
    assert!(store.snapshot(2, 2).is_err());
    assert_eq!(
        store
            .snapshot(MAX_SNAPSHOT_KEYS + 1, 3)
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store
            .snapshot(2, MAX_SNAPSHOT_KEY_BYTES + 1)
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
    let snapshot = store.snapshot(2, 3).unwrap();
    assert_eq!(
        snapshot.scan(None, Some(b"c"), 5).unwrap(),
        vec![(b"ab".to_vec(), b"v".to_vec())]
    );
    assert!(snapshot.scan(None, None, 0).is_err());
}

#[test]
fn concurrent_snapshot_limit_releases_capacity_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"k", b"value").unwrap();
    let mut snapshots = Vec::new();
    for _ in 0..database_engine::MAX_ACTIVE_SNAPSHOTS {
        snapshots.push(store.snapshot(1, 1).unwrap());
    }
    assert!(store.snapshot(1, 1).is_err());
    drop(snapshots.pop().unwrap());
    snapshots.push(store.snapshot(1, 1).unwrap());
    assert!(store.snapshot(1, 1).is_err());
    drop(snapshots);
    assert!(store.snapshot(1, 1).is_ok());
}

#[test]
fn racing_snapshot_creation_cannot_exceed_active_limit() {
    use std::sync::Barrier;
    use std::thread;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"k", b"v").unwrap();
    let start = Barrier::new(33);
    let acquired = Barrier::new(33);
    let winners = thread::scope(|scope| {
        let workers = (0..32)
            .map(|_| {
                let (store, start, acquired) = (&store, &start, &acquired);
                scope.spawn(move || {
                    start.wait();
                    let snapshot = store.snapshot(1, 1);
                    let success = snapshot.is_ok();
                    acquired.wait();
                    drop(snapshot);
                    success
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        acquired.wait();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|won| *won)
            .count()
    });
    assert_eq!(winners, database_engine::MAX_ACTIVE_SNAPSHOTS);
    assert!(store.snapshot(1, 1).is_ok());
}
