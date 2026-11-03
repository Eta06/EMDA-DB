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
