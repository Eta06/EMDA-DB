//! Concurrency tests for reads of immutable log records.
use super::*;
use crate::log_io::LogIo;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

struct BlockingReadIo {
    file: File,
    armed: Arc<AtomicBool>,
    started: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl LogIo for BlockingReadIo {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.started.send(()).map_err(io::Error::other)?;
            self.resume
                .lock()
                .unwrap()
                .recv()
                .map_err(io::Error::other)?;
        }
        FileExt::read_exact_at(&self.file, buffer, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        let mut file = &self.file;
        file.write_all(bytes)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

#[test]
fn blocked_read_does_not_block_a_write_and_observes_its_original_value() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(
        Store::open_using(dir.path(), |path, _| {
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.join("records.log"))?;
            File::open(path)?.sync_all()?;
            Log::from_io(Box::new(BlockingReadIo {
                file,
                armed: Arc::clone(&armed),
                started: started_tx,
                resume: Mutex::new(resume_rx),
            }))
        })
        .unwrap(),
    );
    store.put(b"key", b"old").unwrap();
    armed.store(true, Ordering::SeqCst);
    let reading = Arc::clone(&store);
    let reader = thread::spawn(move || reading.get(b"key"));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    let writing = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let _ = done_tx.send(writing.put(b"key", b"new"));
    });
    let completed_while_read_blocked = done_rx.recv_timeout(Duration::from_secs(3));
    // Always release both workers before asserting, including on the old path.
    resume_tx.send(()).unwrap();
    let old_value = reader.join().unwrap().unwrap();
    writer.join().unwrap();
    completed_while_read_blocked
        .expect("a blocked value read held up the writer")
        .unwrap();
    assert_eq!(old_value.as_deref(), Some(&b"old"[..]));
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
}

#[test]
fn expired_snapshot_reaping_does_not_interrupt_a_blocked_read() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Store::open_using(dir.path(), |path, _| {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join("records.log"))?;
        File::open(path)?.sync_all()?;
        Log::from_io(Box::new(BlockingReadIo {
            file,
            armed: Arc::clone(&armed),
            started: started_tx,
            resume: Mutex::new(resume_rx),
        }))
    })
    .unwrap();
    store.put(b"key", b"old").unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let snapshot = store.snapshot_until(1, 3, deadline).unwrap();
    armed.store(true, Ordering::SeqCst);
    thread::scope(|scope| {
        let reader = scope.spawn(|| snapshot.get(b"key"));
        let started = started_rx.recv_timeout(Duration::from_secs(10));
        if started.is_err() {
            let _ = resume_tx.send(());
            reader.join().unwrap().unwrap();
            panic!("snapshot read did not reach its I/O boundary");
        }
        thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
        );
        let (reaped_tx, reaped_rx) = mpsc::channel();
        let store_ref = &store;
        let reaper = scope.spawn(move || {
            let _ = reaped_tx.send(store_ref.reap_expired_snapshots());
        });
        let before_resume = reaped_rx.recv_timeout(Duration::from_secs(1));
        resume_tx.send(()).unwrap();
        let value = reader.join().unwrap().unwrap();
        reaper.join().unwrap();
        assert_eq!(
            before_resume
                .expect("reaper waited for an active read")
                .unwrap(),
            0
        );
        assert_eq!(value.as_deref(), Some(&b"old"[..]));
    });
    assert_eq!(store.reap_expired_snapshots().unwrap(), 1);
    assert_eq!(
        snapshot.get(b"key").unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert!(store.snapshot(1, 3).is_ok());
}

#[test]
fn scan_captures_locations_before_concurrent_updates_without_blocking_writes() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(
        Store::open_using(dir.path(), |path, _| {
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.join("records.log"))?;
            File::open(path)?.sync_all()?;
            Log::from_io(Box::new(BlockingReadIo {
                file,
                armed: Arc::clone(&armed),
                started: started_tx,
                resume: Mutex::new(resume_rx),
            }))
        })
        .unwrap(),
    );
    store.put(b"a", b"old-a").unwrap();
    store.put(b"b", b"old-b").unwrap();
    armed.store(true, Ordering::SeqCst);
    let reading = Arc::clone(&store);
    let reader = thread::spawn(move || reading.scan(None, None, 10));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writing = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let result = writing
            .put(b"a", b"new-a")
            .and_then(|()| writing.delete(b"b"))
            .and_then(|()| writing.put(b"c", b"new-c"));
        let _ = done_tx.send(result);
    });
    let completed = done_rx.recv_timeout(Duration::from_secs(3));
    resume_tx.send(()).unwrap();
    let original = reader.join().unwrap().unwrap();
    writer.join().unwrap();
    completed
        .expect("blocked scan held the index lock")
        .unwrap();
    assert_eq!(
        original,
        vec![
            (b"a".to_vec(), b"old-a".to_vec()),
            (b"b".to_vec(), b"old-b".to_vec())
        ]
    );
    assert_eq!(
        store.scan(None, None, 10).unwrap(),
        vec![
            (b"a".to_vec(), b"new-a".to_vec()),
            (b"c".to_vec(), b"new-c".to_vec())
        ]
    );
}

#[test]
fn compaction_keeps_readers_available_and_blocks_writes_until_copy_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(
        Store::open_using(&dir.path().join("live"), |path, _| {
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.join("records.log"))?;
            File::open(path)?.sync_all()?;
            Log::from_io(Box::new(BlockingReadIo {
                file,
                armed: Arc::clone(&armed),
                started: started_tx,
                resume: Mutex::new(resume_rx),
            }))
        })
        .unwrap(),
    );
    store.put(b"key", b"old").unwrap();
    let destination = dir.path().join("copy");
    armed.store(true, Ordering::SeqCst);
    let copying = Arc::clone(&store);
    let copy_path = destination.clone();
    let copier = thread::spawn(move || copying.compact_to(copy_path));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writing = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let _ = done_tx.send(writing.put(b"key", b"new"));
    });
    let reading = Arc::clone(&store);
    let (read_tx, read_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let _ = read_tx.send(reading.get(b"key"));
    });
    let read = read_rx.recv_timeout(Duration::from_secs(3));
    let wrote_early = done_rx.recv_timeout(Duration::from_millis(100));
    resume_tx.send(()).unwrap();
    copier.join().unwrap().unwrap();
    writer.join().unwrap();
    reader.join().unwrap();
    assert_eq!(read.unwrap().unwrap().as_deref(), Some(&b"old"[..]));
    assert!(matches!(wrote_early, Err(mpsc::RecvTimeoutError::Timeout)));
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(
        Store::open(destination)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"old"[..])
    );
}

#[test]
fn checkpoint_keeps_readers_available_and_blocks_writes_until_publication_sync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    let checkpoint = dir.path().join("index");
    let store = Arc::new(Store::open(&path).unwrap());
    store.put(b"key", b"old").unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let checkpoint_store = Arc::clone(&store);
    let checkpoint_path = checkpoint.clone();
    let worker = thread::spawn(move || {
        checkpoint_store.checkpoint_to_with_sync(&checkpoint_path, |_| {
            started_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            Ok(())
        })
    });
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writer_store = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let _ = done_tx.send(writer_store.put(b"key", b"new"));
    });
    let reader_store = Arc::clone(&store);
    let (read_tx, read_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let _ = read_tx.send(reader_store.get(b"key"));
    });
    let read = read_rx.recv_timeout(Duration::from_secs(3));
    let wrote_early = done_rx.recv_timeout(Duration::from_millis(100));
    resume_tx.send(()).unwrap();
    let token = worker.join().unwrap().unwrap();
    writer.join().unwrap();
    reader.join().unwrap();
    assert_eq!(read.unwrap().unwrap().as_deref(), Some(&b"old"[..]));
    assert!(matches!(wrote_early, Err(mpsc::RecvTimeoutError::Timeout)));
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
    drop(store);
    // The new record is in the tail and must be replayed from the checkpoint.
    assert_eq!(
        Store::open_with_checkpoint(&path, &checkpoint, &token)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
}
