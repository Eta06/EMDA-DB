use super::*;
use crate::Store;
use crate::log_io::LogIo;
use crate::test_support::retry_lock;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Instant;

fn fixture() -> (tempfile::TempDir, PathBuf, Arc<CheckpointStore>) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("base");
    let store = retry_lock(|| Store::open(&path)).unwrap();
    store.put(b"base", b"safe").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = retry_lock(|| {
        CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 4096, 16, 128)
    })
    .unwrap();
    store.rollover_managed(root.path().join("managed")).unwrap();
    store.enable_automatic_checkpoints().unwrap();
    (root, path, Arc::new(store))
}

fn wait_for(
    worker: &CheckpointWorker,
    predicate: impl Fn(&CheckpointWorkerStatus) -> bool,
) -> CheckpointWorkerStatus {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = worker.status();
        if predicate(&status) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "checkpoint worker timed out: {status:?}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn requested_checks_respect_threshold_and_keep_snapshots_and_recovery_valid() {
    let (_root, path, store) = fixture();
    let worker = store
        .start_checkpoint_worker(Duration::from_secs(3600), 1)
        .unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |status| status.checks >= 1);
    assert_eq!(status.checkpoints, 0);
    store.put(b"key", b"old").unwrap();
    let snapshot = store.snapshot().unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |status| status.checkpoints >= 1);
    assert!(status.last_error.is_none());
    assert_eq!(store.overlay_keys().unwrap(), 0);
    store.put(b"key", b"new").unwrap();
    assert_eq!(snapshot.get(b"key").unwrap(), Some(b"old".to_vec()));
    drop(snapshot);
    let status = worker.stop().unwrap();
    assert!(status.stopped);
    drop(store);
    let reopened = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(&path, 4096, 16, 128, None)
    })
    .unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn periodic_checks_run_and_stopping_releases_the_worker_lease() {
    let (_root, _path, store) = fixture();
    let worker = store
        .start_checkpoint_worker(Duration::from_millis(30), 1)
        .unwrap();
    store.put(b"a", b"value").unwrap();
    wait_for(&worker, |status| status.checkpoints >= 1);
    assert!(
        matches!(store.start_checkpoint_worker(Duration::from_secs(1), 1), Err(ref error) if error.kind() == io::ErrorKind::AlreadyExists)
    );
    assert!(worker.stop().unwrap().stopped);
    let replacement = store
        .start_checkpoint_worker(Duration::from_secs(3600), 1)
        .unwrap();
    drop(replacement);
    assert_eq!(Arc::strong_count(&store), 1);
}

#[test]
fn invalid_configuration_and_disabled_automatic_checkpoints_are_rejected() {
    let (_root, _path, store) = fixture();
    for (interval, threshold) in [
        (Duration::ZERO, 1),
        (Duration::from_micros(999), 1),
        (Duration::from_secs(86_401), 1),
        (Duration::from_secs(1), 0),
        (Duration::from_secs(1), 4097),
    ] {
        assert!(
            matches!(store.start_checkpoint_worker(interval, threshold), Err(ref error) if error.kind() == io::ErrorKind::InvalidInput)
        );
    }
    store.disable_automatic_checkpoints().unwrap();
    assert!(
        matches!(store.start_checkpoint_worker(Duration::from_secs(1), 1), Err(ref error) if error.kind() == io::ErrorKind::InvalidInput)
    );
}

#[test]
fn disabled_automatic_mode_suspends_checks_until_reenabled() {
    let (_root, _path, store) = fixture();
    let worker = store
        .start_checkpoint_worker(Duration::from_secs(3600), 1)
        .unwrap();
    store.put(b"key", b"value").unwrap();
    store.disable_automatic_checkpoints().unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |status| status.checks >= 1);
    assert_eq!(status.checkpoints, 0);
    assert!(store.tail_bytes().unwrap() > 0);
    store.enable_automatic_checkpoints().unwrap();
    worker.request_check().unwrap();
    wait_for(&worker, |status| status.checkpoints >= 1);
    assert_eq!(store.overlay_keys().unwrap(), 0);
    worker.stop().unwrap();
}

#[test]
fn worker_error_is_reported_and_does_not_submit_a_user_write() {
    let (_root, path, store) = fixture();
    store.put(b"key", b"safe").unwrap();
    let worker = store
        .start_checkpoint_worker(Duration::from_secs(3600), 1)
        .unwrap();
    fs::write(
        path.join(".emda-auto-checkpoints-v1").join("owner"),
        b"invalid",
    )
    .unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |status| status.stopped);
    assert!(status.last_error.is_some());
    assert!(worker.request_check().is_err());
    assert!(store.get(b"key").is_err());
    assert!(worker.stop().unwrap().last_error.is_some());
    drop(store);
    let recovered = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(recovered.get(b"key").unwrap(), Some(b"safe".to_vec()));
}

#[test]
fn stopping_waits_for_a_check_already_blocked_on_the_store() {
    let (_root, _path, store) = fixture();
    let worker = store
        .start_checkpoint_worker(Duration::from_secs(3600), 1)
        .unwrap();
    let held = store.write().unwrap();
    worker.request_check().unwrap();
    wait_for(&worker, |status| status.in_flight);
    let (done, receiver) = mpsc::channel();
    let stopper = thread::spawn(move || done.send(worker.stop()).unwrap());
    assert!(matches!(
        receiver.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    drop(held);
    assert!(
        receiver
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .stopped
    );
    stopper.join().unwrap();
}

struct PanicRead(File);
impl LogIo for PanicRead {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
    fn read_exact_at(&self, _buffer: &mut [u8], _offset: u64) -> io::Result<()> {
        panic!("injected background checkpoint panic")
    }
    fn append_all(&self, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::other("unexpected append"))
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_all()
    }
}

#[test]
fn worker_panic_is_reported_and_recovery_preserves_acknowledged_data() {
    let (_root, path, shared) = fixture();
    let mut store = Arc::try_unwrap(shared).ok().unwrap();
    store.put(b"key", b"safe").unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    store.log.replace_io_for_test(Box::new(PanicRead(file)));
    let store = Arc::new(store);
    let worker = store
        .start_checkpoint_worker(Duration::from_secs(3600), 1)
        .unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |status| status.stopped);
    assert!(status.last_error.unwrap().contains("panicked"));
    assert!(store.get(b"key").is_err());
    worker.stop().unwrap();
    drop(store);
    let recovered = retry_lock(|| Store::open(Path::new(&path))).unwrap();
    assert_eq!(recovered.get(b"key").unwrap(), Some(b"safe".to_vec()));
}

#[test]
fn concurrent_starts_admit_exactly_one_worker() {
    let (_root, _path, store) = fixture();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            barrier.wait();
            store.start_checkpoint_worker(Duration::from_secs(3600), 1)
        }));
    }
    let mut started = Vec::new();
    for thread in threads {
        match thread.join().unwrap() {
            Ok(worker) => started.push(worker),
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::AlreadyExists),
        }
    }
    assert_eq!(started.len(), 1);
    started.pop().unwrap().stop().unwrap();
}

#[test]
fn worker_rollover_does_not_conflict_with_a_transaction_by_itself() {
    let (_root, _path, store) = fixture();
    store.put(b"key", b"before").unwrap();
    let worker = store
        .start_checkpoint_worker(Duration::from_secs(3600), 1)
        .unwrap();
    store
        .transaction(|transaction| {
            assert_eq!(transaction.get(b"key")?, Some(b"before".to_vec()));
            worker.request_check()?;
            wait_for(&worker, |status| status.checkpoints >= 1);
            transaction.put(b"other", b"committed")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(store.get(b"other").unwrap(), Some(b"committed".to_vec()));
    worker.stop().unwrap();
}
