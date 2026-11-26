//! Fault injection against the checkpoint writer's real append and witness paths.
use super::*;
use crate::Store;
use crate::log_io::LogIo;
use crate::test_support::retry_lock;
use crate::witness::{self, Witness, WitnessIo};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::FileExt;

#[cfg(target_os = "linux")]
impl CheckpointStore {
    pub(crate) fn commit_process_test_log(&mut self) -> &mut Log {
        &mut self.log
    }
}

struct PausingLog {
    file: File,
    armed: std::sync::atomic::AtomicBool,
    entered: std::sync::mpsc::Sender<()>,
    resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
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
                .recv_timeout(std::time::Duration::from_secs(10))
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

fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
) {
    fixture_before_drop(|| {})
}

fn fixture_before_drop(
    before_drop: impl FnOnce(),
) -> (
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
    before_drop();
    drop(store);
    (dir, path, checkpoint, digest)
}

#[cfg(target_os = "linux")]
#[test]
fn fixture_drop_releases_lock_inherited_by_fork() {
    use crate::test_support::fork_lock::{Child, check_reopened, isolated};

    if isolated(
        concat!(
            module_path!(),
            "::fixture_drop_releases_lock_inherited_by_fork"
        ),
        "EMDA_FIXTURE_FORK_REGRESSION_CHILD",
    ) {
        return;
    }
    let mut child = None;
    let (_dir, path, _, _) = fixture_before_drop(|| child = Some(Child::spawn()));
    check_reopened(&path, child.unwrap());
}

#[cfg(target_os = "linux")]
#[test]
fn checkpoint_owners_release_locks_before_inheriting_children_exit() {
    use crate::test_support::fork_lock::{Child, check_reopened, isolated};

    if isolated(
        concat!(
            module_path!(),
            "::checkpoint_owners_release_locks_before_inheriting_children_exit"
        ),
        "EMDA_CHECKPOINT_OWNER_FORK",
    ) {
        return;
    }
    let (_dir, path, checkpoint, digest) = fixture();
    let historical = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
    let recovered = crate::RecoveredCheckpointView::open(&path, &checkpoint, &digest).unwrap();
    let child = Child::spawn();
    drop(historical);
    assert_eq!(
        Store::open(&path).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(recovered);
    check_reopened(&path, child);

    let writable = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let child = Child::spawn();
    assert_eq!(
        Store::open(&path).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(writable);
    check_reopened(&path, child);
}

#[derive(Clone, Copy)]
enum LogFault {
    Partial(usize),
    Panic(usize),
    SyncBefore,
    SyncAfter,
    SyncTimeout,
    SyncWouldBlock,
}

struct FaultLog {
    file: File,
    fault: LogFault,
}

impl LogIo for FaultLog {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn file_identity(&self) -> io::Result<Option<(u64, u64)>> {
        LogIo::file_identity(&self.file)
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
            LogFault::SyncWouldBlock => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "injected log sync failure",
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
mod deferred_prefix;
mod detailed_document_fault_tests;
mod direct_admission;
mod document_faults;
mod log_retention;
#[cfg(target_os = "linux")]
mod metadata_faults;
mod online_index_faults;
mod rebuild;
mod rebuild_crash_detailed;
mod rebuild_outcomes;
mod recovery;
mod rollover;
mod schema_migration_faults;
mod selection;
mod shared_view_file;

mod transaction;

#[test]
fn direct_read_reservation_rejects_competing_calls_without_blocking_writes() {
    let (_root, path, checkpoint, digest) = fixture();
    let mut store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store.enable_direct_read_limit(1).unwrap();
    let admission = store.direct_reads.acquire().unwrap();
    assert_eq!(store.direct_read_status().active, Some(1));
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
    assert_eq!(store.direct_read_status().active, Some(0));
    assert_eq!(
        store.get(b"base").unwrap().as_deref(),
        Some(&b"updated"[..])
    );
}

#[test]
fn snapshot_reads_can_share_direct_admission() {
    let (_root, path, checkpoint, digest) = fixture();
    let mut store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store.enable_direct_read_limit(0).unwrap();
    assert_eq!(
        store.snapshot().unwrap().get(b"base").unwrap(),
        Some(b"acknowledged".to_vec())
    );

    store.enable_direct_and_snapshot_read_limit(1).unwrap();
    let mut snapshot = store.snapshot().unwrap();
    store.put(b"base", b"new").unwrap();
    let guard = store.direct_reads.acquire_snapshot().unwrap();
    assert!(store.direct_read_status().includes_snapshots);
    assert_eq!(
        store.get(b"base").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        snapshot.get(b"base").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        snapshot.get_many(&[b"base"]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        snapshot.scan(None, None, 1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    store
        .transaction(|tx| {
            assert_eq!(tx.get(b"base")?, Some(b"new".to_vec()));
            Ok(())
        })
        .unwrap();
    drop(guard);
    assert_eq!(
        snapshot.get_many(&[b"base", b"missing"]).unwrap(),
        vec![Some(b"acknowledged".to_vec()), None]
    );
    snapshot.cancel();
    assert!(snapshot.scan(None, None, 1).is_err());
    assert_eq!(store.direct_read_status().active, Some(0));
    assert_eq!(store.get(b"base").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn direct_read_admission_counts_checkpoint_state_lock_waiters() {
    use std::sync::Barrier;
    use std::time::{Duration, Instant};
    let (_root, path, checkpoint, digest) = fixture();
    let mut store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store.enable_direct_read_limit(1).unwrap();
    let entered = Barrier::new(2);
    std::thread::scope(|scope| {
        let lock = store.state.write().unwrap();
        let worker = scope.spawn(|| {
            entered.wait();
            store.get(b"base")
        });
        entered.wait();
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.direct_read_status().active == Some(0) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let status = store.direct_read_status();
        if status.active != Some(1) {
            drop(lock);
            let _ = worker.join();
            panic!("checkpoint reader did not enter admission");
        }
        let rejected = store.get(b"base");
        let reduction = store.set_active_direct_read_limit(0);
        drop(lock);
        let result = worker.join().unwrap();
        assert_eq!((status.limit, status.active), (Some(1), Some(1)));
        assert_eq!(rejected.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(reduction.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(result.unwrap().as_deref(), Some(&b"acknowledged"[..]));
    });
    assert_eq!(store.direct_read_status().active, Some(0));
}

#[test]
fn read_set_qualifies_checkpoint_and_overlay_observations() {
    for range in [false, true] {
        for overlay in [false, true] {
            let (_dir, path, checkpoint, digest) = fixture();
            let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
            let key: &[u8] = if overlay { b"overlay" } else { b"base" };
            if overlay {
                store.put(key, b"value").unwrap();
            }
            let observed = std::sync::Barrier::new(2);
            let published = std::sync::Barrier::new(2);
            let result = std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    store.transaction_with_read_set(|tx| {
                        if range {
                            let (start, end): (&[u8], &[u8]) =
                                if overlay { (b"n", b"p") } else { (b"a", b"c") };
                            let rows = tx.scan(Some(start), Some(end), 100)?;
                            assert_eq!(rows.len(), 1);
                            assert_eq!(rows[0].0, key);
                        } else {
                            assert!(tx.get(key)?.is_some());
                        }
                        tx.put(b"staged", b"must not commit")?;
                        observed.wait();
                        published.wait();
                        Ok(())
                    })
                });
                observed.wait();
                // Model publication with unchanged locations and mutation counter.
                store.state.write().unwrap().log_generation = Some(GenerationId([42; 16]));
                published.wait();
                worker.join().unwrap()
            });
            let error = result.unwrap_err();
            assert!(crate::is_transaction_conflict(&error), "{error}");
            assert_eq!(store.get(b"staged").unwrap(), None);
        }
    }
}

#[test]
fn read_set_identity_conflict_at_observation_is_sticky() {
    for range in [false, true] {
        let (_dir, path, checkpoint, digest) = fixture();
        let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
        let original = store.state.read().unwrap().log_generation;
        let result = store.transaction_with_read_set(|tx| {
            if range {
                assert_eq!(tx.scan(Some(b"a"), Some(b"c"), 10)?.len(), 1);
            } else {
                assert!(tx.get(b"base")?.is_some());
            }
            tx.put(b"staged", b"no")?;
            store.state.write().unwrap().log_generation = Some(GenerationId([42; 16]));
            let error = tx.get(b"another").unwrap_err();
            assert!(crate::is_transaction_conflict(&error));
            // Even restoring the original identity cannot clear a swallowed conflict.
            store.state.write().unwrap().log_generation = original;
            assert!(crate::is_transaction_conflict(
                &tx.get(b"another").unwrap_err()
            ));
            Ok(())
        });
        assert!(crate::is_transaction_conflict(&result.unwrap_err()));
        assert_eq!(store.get(b"staged").unwrap(), None);
    }
}

#[test]
fn read_set_absence_and_empty_range_keep_membership_semantics_across_identity_change() {
    for tombstone in [false, true] {
        for insertion in [None, Some(&b"missing"[..]), Some(&b"middle"[..])] {
            let (_dir, path, checkpoint, digest) = fixture();
            let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
            if tombstone {
                store.put(b"missing", b"old").unwrap();
                store.delete(b"missing").unwrap();
            }
            let result = store.transaction_with_read_set(|tx| {
                assert_eq!(tx.get(b"missing")?, None);
                assert!(tx.scan(Some(b"l"), Some(b"n"), 10)?.is_empty());
                store.state.write().unwrap().log_generation = Some(GenerationId([42; 16]));
                // Revalidation of absent observations succeeds with unchanged membership.
                assert_eq!(tx.get(b"missing")?, None);
                assert!(tx.scan(Some(b"l"), Some(b"n"), 10)?.is_empty());
                if let Some(key) = insertion {
                    store.put(key, b"new")?;
                }
                tx.put(b"staged", b"ok")?;
                Ok(())
            });
            if insertion.is_some() {
                assert!(crate::is_transaction_conflict(&result.unwrap_err()));
                assert_eq!(store.get(b"staged").unwrap(), None);
            } else {
                result.unwrap();
                assert_eq!(store.get(b"staged").unwrap().as_deref(), Some(&b"ok"[..]));
            }
        }
    }
}
