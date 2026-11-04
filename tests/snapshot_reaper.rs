use database_engine::{CheckpointStore, SnapshotReaper, Store};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod support;
use support::retry_lock;

fn wait_until(mut condition: impl FnMut() -> bool) {
    let timeout = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            Instant::now() < timeout,
            "snapshot reaper did not make progress"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn invalid_interval(result: std::io::Result<SnapshotReaper>) {
    assert!(matches!(result, Err(error) if error.kind() == ErrorKind::InvalidInput));
}

macro_rules! exercise_worker {
    ($store:expr, $expired:expr, $permanent:expr) => {{
        let store = &$store;
        invalid_interval(store.start_snapshot_reaper(Duration::ZERO));
        invalid_interval(store.start_snapshot_reaper(Duration::from_secs(24 * 60 * 60 + 1)));
        let expired = $expired;
        let permanent = $permanent;
        let worker = store.start_snapshot_reaper(Duration::from_millis(20)).unwrap();
        assert!(matches!(store.start_snapshot_reaper(Duration::from_secs(1)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists));
        wait_until(|| worker.status().reaped == 1);
        assert_eq!(expired.get(b"base").unwrap_err().kind(), ErrorKind::TimedOut);
        assert_eq!(permanent.get(b"base").unwrap().as_deref(), Some(&b"value"[..]));
        store.put(b"later", b"write").unwrap();
        assert_eq!(permanent.get(b"later").unwrap(), None);
        let stopped = worker.stop().unwrap();
        assert!(stopped.stopped);
        assert!(stopped.last_error.is_none());
        assert_eq!(stopped.reaped, 1);
        drop(expired);
        drop(permanent);
        let replacement = store.start_snapshot_reaper(Duration::from_secs(3600)).unwrap();
        replacement.request_check().unwrap();
        wait_until(|| replacement.status().checks >= 1);
        drop(replacement);
        assert_eq!(Arc::strong_count(store), 1);
    }};
}

#[test]
fn ordinary_worker_reaps_only_expired_views_and_can_restart() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    store.put(b"base", b"value").unwrap();
    let deadline = Instant::now() + Duration::from_millis(500);
    exercise_worker!(
        store,
        store.snapshot_until(4, 64, deadline).unwrap(),
        store.snapshot(4, 64).unwrap()
    );
    drop(store);
    assert_eq!(
        Store::open(root.path())
            .unwrap()
            .get(b"later")
            .unwrap()
            .as_deref(),
        Some(&b"write"[..])
    );
}

#[test]
fn checkpoint_worker_reaps_only_expired_views_and_can_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    initial.put(b"base", b"value").unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let deadline = Instant::now() + Duration::from_millis(500);
    exercise_worker!(
        store,
        store.snapshot_until(deadline).unwrap(),
        store.snapshot().unwrap()
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&path))
            .unwrap()
            .get(b"later")
            .unwrap()
            .as_deref(),
        Some(&b"write"[..])
    );
}
