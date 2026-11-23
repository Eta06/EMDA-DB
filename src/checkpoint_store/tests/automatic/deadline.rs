use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;

struct PausingLog {
    file: File,
    armed: AtomicBool,
    entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}
impl LogIo for PausingLog {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .map_err(|_| io::Error::other("release timed out"))?;
        }
        FileExt::read_exact_at(&self.file, bytes, offset)
    }
    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        (&self.file).write_all(bytes)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

#[test]
fn automatic_rollover_keeps_committed_reads_available_during_verification() {
    use std::sync::{Arc, TryLockError};

    for background in [false, true] {
        let (_dir, path, mut store) = managed_default_fixture(1024, 1, 64);
        store.enable_automatic_checkpoints().unwrap();
        store.put(b"a", b"acknowledged").unwrap();
        let selection = crate::checkpoint_store::selection::path(&path);
        let old_selection = fs::read(&selection).unwrap();
        let log = path.join("records.log");
        let old_log = fs::read(&log).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        store.log.replace_io_for_test(Box::new(PausingLog {
            file: OpenOptions::new()
                .read(true)
                .append(true)
                .open(&log)
                .unwrap(),
            armed: AtomicBool::new(true),
            entered: entered_tx,
            resume: Mutex::new(resume_rx),
        }));
        let store = Arc::new(store);
        let worker = background.then(|| {
            let worker = store
                .start_checkpoint_worker(Duration::from_secs(3600), 1)
                .unwrap();
            worker.request_check().unwrap();
            worker
        });
        let snapshot = std::thread::scope(|scope| {
            let store = store.as_ref();
            let writer = (!background).then(|| scope.spawn(|| store.put(b"b", b"after")));
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            assert!(matches!(
                store.writers.try_read(),
                Err(TryLockError::WouldBlock)
            ));
            assert_eq!(fs::read(&log).unwrap(), old_log);
            assert_eq!(fs::read(&selection).unwrap(), old_selection);

            let (read_tx, read_rx) = mpsc::channel();
            let reader = scope.spawn(move || {
                let row = store.get_many(&[b"a", b"b"]).unwrap();
                read_tx.send((row, store.snapshot().unwrap())).unwrap();
            });
            let observed = read_rx.recv_timeout(Duration::from_secs(3));
            resume_tx.send(()).unwrap();
            if let Some(writer) = writer {
                writer.join().unwrap().unwrap();
            }
            reader.join().unwrap();
            let (row, snapshot) = observed.expect("committed reads stalled during rollover");
            assert_eq!(row, [Some(b"acknowledged".to_vec()), None]);
            snapshot
        });
        if let Some(worker) = worker {
            let status = worker.stop().unwrap();
            assert_eq!(status.checkpoints, 1);
            assert!(status.last_error.is_none());
            assert_eq!(store.tail_bytes().unwrap(), 0);
            assert_eq!(fs::read(&log).unwrap(), old_log);
            store.put(b"b", b"after").unwrap();
        }
        assert_ne!(fs::read(&selection).unwrap(), old_selection);
        assert_eq!(snapshot.get(b"a").unwrap(), Some(b"acknowledged".to_vec()));
        assert_eq!(snapshot.get(b"b").unwrap(), None);
        drop(snapshot);
        drop(store);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(reopened.get(b"a").unwrap(), Some(b"acknowledged".to_vec()));
        assert_eq!(reopened.get(b"b").unwrap(), Some(b"after".to_vec()));
    }
}

#[test]
fn transaction_deadline_expiring_during_automatic_rollover_prevents_append() {
    use std::sync::Arc;
    use std::time::Instant;

    let (_dir, path, mut store) = managed_fixture(1024, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"acknowledged").unwrap();
    let original_log = fs::read(path.join("records.log")).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    store.log.replace_io_for_test(Box::new(PausingLog {
        file,
        armed: AtomicBool::new(true),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    }));
    let store = Arc::new(store);
    let deadline = Instant::now() + Duration::from_secs(2);
    let result = std::thread::scope(|scope| {
        let store = Arc::clone(&store);
        let worker = scope.spawn(move || {
            store.transaction_until(deadline, CheckpointTransactionMode::Conservative, |tx| {
                tx.put(b"b", b"must-not-commit")?;
                Ok(())
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(10),
        );
        resume_tx.send(()).unwrap();
        worker.join().unwrap()
    });
    assert!(matches!(
        result,
        Err(crate::TimedTransactionError::DeadlineExceeded)
    ));
    assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
    assert_eq!(
        store.get(b"a").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(store.get(b"b").unwrap(), None);
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(
        reopened.get(b"a").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(reopened.get(b"b").unwrap(), None);
}

#[test]
fn transaction_cancellation_during_automatic_rollover_prevents_append() {
    use crate::{CancellationToken, ControlledTransactionError};
    use std::sync::Arc;
    let (_dir, path, mut store) = managed_fixture(1024, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"acknowledged").unwrap();
    let original_log = fs::read(path.join("records.log")).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    store.log.replace_io_for_test(Box::new(PausingLog {
        file,
        armed: AtomicBool::new(true),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    }));
    let store = Arc::new(store);
    let token = CancellationToken::new();
    let result = std::thread::scope(|scope| {
        let store = Arc::clone(&store);
        let worker_token = token.clone();
        let worker = scope.spawn(move || {
            store.transaction_controlled(
                &worker_token,
                None,
                CheckpointTransactionMode::Conservative,
                |tx| tx.put(b"b", b"must-not-commit"),
            )
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(10));
        token.cancel();
        let _ = resume_tx.send(());
        let result = worker.join().unwrap();
        entered.expect("automatic rollover did not reach the pause");
        result
    });
    assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
    assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
    assert_eq!(
        store.get(b"a").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(store.get(b"b").unwrap(), None);
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(
        reopened.get(b"a").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(reopened.get(b"b").unwrap(), None);
}

#[test]
fn detailed_cancellation_during_automatic_rollover_is_a_confirmed_abort() {
    use crate::{CancellationToken, TransactionFailure};
    use std::sync::Arc;
    let (_dir, path, mut store) = managed_fixture(1024, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"acknowledged").unwrap();
    let original_log = fs::read(path.join("records.log")).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    store.log.replace_io_for_test(Box::new(PausingLog {
        file,
        armed: AtomicBool::new(true),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    }));
    let store = Arc::new(store);
    let token = CancellationToken::new();
    let result = std::thread::scope(|scope| {
        let store = Arc::clone(&store);
        let worker_token = token.clone();
        let worker = scope.spawn(move || {
            store.transaction_detailed(
                CheckpointTransactionMode::Conservative,
                None,
                Some(&worker_token),
                |tx| tx.put(b"b", b"must-not-commit"),
            )
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(10));
        token.cancel();
        let _ = resume_tx.send(());
        let result = worker.join().unwrap();
        entered.expect("automatic rollover did not reach the pause");
        result
    });
    assert!(matches!(result, Err(TransactionFailure::Cancelled)));
    assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
    assert_eq!(store.get(b"b").unwrap(), None);
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(
        reopened.get(b"a").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(reopened.get(b"b").unwrap(), None);
}
