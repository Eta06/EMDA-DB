//! Fault injection against the checkpoint writer's real append and witness paths.
use super::*;
use crate::Store;
use crate::log_io::LogIo;
use crate::test_support::retry_lock;
use crate::witness::{self, Witness, WitnessIo};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::FileExt;

fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join("records.log"),
        include_bytes!("../../tests/fixtures/v2-empty-records.log"),
    )
    .unwrap();
    fs::write(
        path.join("commit.state"),
        include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
    )
    .unwrap();
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"acknowledged").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    (dir, path, checkpoint, digest)
}

#[derive(Clone, Copy)]
enum LogFault {
    Partial(usize),
    Panic(usize),
    SyncBefore,
    SyncAfter,
    SyncTimeout,
}

struct FaultLog {
    file: File,
    fault: LogFault,
}

impl LogIo for FaultLog {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, bytes, offset)
    }
    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        match self.fault {
            LogFault::Partial(count) | LogFault::Panic(count) => {
                (&self.file).write_all(&bytes[..count.min(bytes.len())])?;
                if matches!(self.fault, LogFault::Panic(_)) {
                    panic!("injected append panic");
                }
                Err(io::Error::other("injected partial append"))
            }
            _ => (&self.file).write_all(bytes),
        }
    }
    fn sync_data(&self) -> io::Result<()> {
        match self.fault {
            LogFault::SyncBefore => Err(io::Error::other("injected log sync failure")),
            LogFault::SyncTimeout => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "injected log sync timeout",
            )),
            LogFault::SyncAfter => {
                self.file.sync_data()?;
                Err(io::Error::other("injected post-sync failure"))
            }
            _ => self.file.sync_data(),
        }
    }
}

fn failed_instance_rejects_all_operations(store: &CheckpointStore, path: &Path) {
    let before = fs::metadata(path.join("records.log")).unwrap().len();
    assert!(store.get(b"base").is_err());
    assert!(store.scan(None, None, 10).is_err());
    assert!(store.put(b"other", b"must-not-write").is_err());
    let backup = path.parent().unwrap().join("must-not-backup");
    assert!(store.backup(&backup).is_err());
    assert!(!backup.exists());
    assert_eq!(
        fs::metadata(path.join("records.log")).unwrap().len(),
        before
    );
}

#[derive(Clone, Copy)]
enum WitnessFault {
    Partial(usize),
    Panic(usize),
    SyncBefore,
    SyncAfter,
}

struct FaultWitness {
    file: File,
    fault: WitnessFault,
}

impl WitnessIo for FaultWitness {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, bytes, offset)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        match self.fault {
            WitnessFault::Partial(count) | WitnessFault::Panic(count) => {
                FileExt::write_all_at(&self.file, &bytes[..count.min(bytes.len())], offset)?;
                if matches!(self.fault, WitnessFault::Panic(_)) {
                    panic!("injected witness panic");
                }
                Err(io::Error::other("injected partial witness"))
            }
            _ => FileExt::write_all_at(&self.file, bytes, offset),
        }
    }
    fn sync_data(&self) -> io::Result<()> {
        match self.fault {
            WitnessFault::SyncBefore => Err(io::Error::other("injected witness sync failure")),
            WitnessFault::SyncAfter => {
                self.file.sync_data()?;
                Err(io::Error::other("injected post-sync witness failure"))
            }
            _ => self.file.sync_data(),
        }
    }
}

mod automatic;
mod commit_faults;
mod compaction;
mod direct_admission;
mod document_faults;
#[cfg(target_os = "linux")]
mod metadata_faults;
mod online_index_faults;
mod rebuild;
mod recovery;
mod rollover;
mod schema_migration_faults;
mod selection;

mod transaction;

#[test]
fn direct_read_reservation_rejects_competing_calls_without_blocking_writes() {
    let (_root, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store.set_active_direct_read_limit(1).unwrap();
    let admission = store.direct_reads.acquire().unwrap();
    assert_eq!(store.direct_read_status().active, 1);
    assert_eq!(
        store.set_active_direct_read_limit(0).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        store.get(b"base").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        store.get_many(&[b"base"]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        store.scan(None, None, 1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(
        store
            .compare_exchange(b"base", Some(b"acknowledged"), Some(b"updated"))
            .unwrap()
    );
    drop(admission);
    assert_eq!(store.direct_read_status().active, 0);
    assert_eq!(
        store.get(b"base").unwrap().as_deref(),
        Some(&b"updated"[..])
    );
}

#[test]
fn direct_read_admission_counts_checkpoint_state_lock_waiters() {
    use std::sync::Barrier;
    use std::time::{Duration, Instant};
    let (_root, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store.set_active_direct_read_limit(1).unwrap();
    let entered = Barrier::new(2);
    std::thread::scope(|scope| {
        let lock = store.state.write().unwrap();
        let worker = scope.spawn(|| {
            entered.wait();
            store.get(b"base")
        });
        entered.wait();
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.direct_read_status().active == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let status = store.direct_read_status();
        if status.active != 1 {
            drop(lock);
            let _ = worker.join();
            panic!("checkpoint reader did not enter admission");
        }
        let rejected = store.get(b"base");
        let reduction = store.set_active_direct_read_limit(0);
        drop(lock);
        let result = worker.join().unwrap();
        assert_eq!((status.limit, status.active), (1, 1));
        assert_eq!(rejected.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(reduction.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(result.unwrap().as_deref(), Some(&b"acknowledged"[..]));
    });
    assert_eq!(store.direct_read_status().active, 0);
}
