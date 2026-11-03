//! Failure and publication checks through the real V3 coordinator path.
use super::*;
use crate::witness::{Witness, WitnessIo};
use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

const NAME: &str = "user.db.commit.v3";

#[derive(Clone, Copy)]
enum Fault {
    Read,
    WriteBefore,
    WriteAfter,
    SyncBefore,
    SyncAfter,
    Panic,
    Block,
}

struct Controlled {
    file: File,
    armed: Arc<AtomicBool>,
    fault: Fault,
    entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl WitnessIo for Controlled {
    fn len(&self) -> io::Result<u64> {
        Ok(36)
    }
    fn read_exact_at(&self, bytes: &mut [u8], _: u64) -> io::Result<()> {
        if self.armed.load(Ordering::Acquire) && matches!(self.fault, Fault::Read) {
            return Err(io::Error::other("injected xattr read error"));
        }
        assert_eq!(fgetxattr(&self.file, NAME, bytes)?, 36);
        Ok(())
    }
    fn write_all_at(&self, bytes: &[u8], _: u64) -> io::Result<()> {
        let armed = self.armed.load(Ordering::Acquire);
        if armed && matches!(self.fault, Fault::WriteBefore) {
            return Err(io::Error::other("injected xattr write error"));
        }
        fsetxattr(&self.file, NAME, bytes, XattrFlags::REPLACE)?;
        if armed && matches!(self.fault, Fault::WriteAfter) {
            return Err(io::Error::other("injected error after xattr write"));
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        let armed = self.armed.load(Ordering::Acquire);
        if armed {
            match self.fault {
                Fault::SyncBefore => return Err(io::Error::other("injected fsync error")),
                Fault::Panic => panic!("injected sync panic"),
                Fault::Block => {
                    self.entered.send(()).unwrap();
                    self.resume.lock().unwrap().recv().unwrap();
                }
                _ => {}
            }
        }
        self.file.sync_all()?;
        if armed && matches!(self.fault, Fault::SyncAfter) {
            return Err(io::Error::other("injected error after fsync"));
        }
        Ok(())
    }
}

fn store(
    path: &Path,
    fault: Fault,
    armed: Arc<AtomicBool>,
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
) -> Store {
    Store::open_using(path, |path, initialize| {
        let (mut log, index) = Log::open(path, initialize)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("records.log"))?;
        let bytes = std::fs::read(path.join("records.log"))?;
        assert_eq!(&bytes[..8], b"EMDALOG3");
        let id = bytes[8..24].try_into().unwrap();
        let (witness, _) = Witness::from_io(
            Box::new(Controlled {
                file,
                armed,
                fault,
                entered,
                resume: Mutex::new(resume),
            }),
            id,
        )?;
        log.attach_test_witness(witness);
        Ok((log, index))
    })
    .unwrap()
}

#[test]
fn metadata_failures_and_panics_never_acknowledge_and_poison_the_store() {
    for fault in [
        Fault::Read,
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncAfter,
        Fault::Panic,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered, _) = mpsc::channel();
        let (_, resume) = mpsc::channel();
        let store = store(dir.path(), fault, Arc::clone(&armed), entered, resume);
        store.put(b"key", b"old").unwrap();
        armed.store(true, Ordering::Release);
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.put(b"key", b"new")));
        assert!(!matches!(outcome, Ok(Ok(()))));
        assert!(store.get(b"key").is_err());
        assert!(store.delete(b"key").is_err());
        drop(store);
        let reopened = Store::open(dir.path()).unwrap();
        let value = reopened.get(b"key").unwrap().unwrap();
        assert!(value == b"old" || value == b"new");
    }
}

#[test]
fn metadata_fsync_must_complete_before_publication_or_acknowledgement() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(store(
        dir.path(),
        Fault::Block,
        Arc::clone(&armed),
        entered_tx,
        resume_rx,
    ));
    store.put(b"key", b"old").unwrap();
    armed.store(true, Ordering::Release);
    let writer_store = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || done_tx.send(writer_store.put(b"key", b"new")).unwrap());
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let value_before_sync = store.get(b"key");
    let completion_before_sync = done_rx.try_recv();
    resume_tx.send(()).unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer.join().unwrap();
    assert_eq!(value_before_sync.unwrap().as_deref(), Some(&b"old"[..]));
    assert!(matches!(
        completion_before_sync,
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
}
