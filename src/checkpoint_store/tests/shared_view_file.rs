use super::*;

#[test]
fn v2_keeps_the_independent_view_and_recovers_writes() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert!(store.log.shared_file_for_test().is_none());
    assert!(
        store
            .read()
            .unwrap()
            .view
            .base
            .log
            .shared_file_for_test()
            .is_none()
    );
    store.put(b"later", b"value").unwrap();
    drop(store);
    let reopened = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        reopened.get(b"later").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[cfg(target_os = "linux")]
mod v3 {
    use super::*;
    use std::sync::Arc;

    fn default_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, [u8; 32]) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let checkpoint = dir.path().join("checkpoint");
        let initial = Store::open(&path).unwrap();
        initial.put(b"base", b"old").unwrap();
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        drop(initial);
        (dir, path, checkpoint, digest)
    }

    fn has_v3(path: &Path) -> bool {
        fs::read(path.join("records.log"))
            .unwrap()
            .starts_with(b"EMDALOG3")
    }

    #[test]
    fn shared_view_stays_read_only_and_outlives_rollover_and_writer() {
        let (dir, path, checkpoint, digest) = default_fixture();
        if !has_v3(&path) {
            eprintln!("V3 is unavailable on this filesystem");
            return;
        }
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let old_view = Arc::clone(&store.read().unwrap().view.base.log);
        let writer_file = store.log.shared_file_for_test().unwrap();
        let view_file = old_view.shared_file_for_test().unwrap();
        assert!(Arc::ptr_eq(&writer_file, &view_file));
        let weak = Arc::downgrade(&writer_file);
        drop(writer_file);
        drop(view_file);
        assert!(old_view.sync_data().is_err());
        assert!(old_view.verify_witness().is_err());
        assert!(
            old_view
                .append_batch_unsynced(std::iter::empty::<crate::log::AppendRecord<'_>>())
                .is_err()
        );

        let snapshot = store.snapshot().unwrap();
        store.put(b"base", b"new").unwrap();
        store.put(b"later", b"value").unwrap();
        store.rollover_managed(dir.path().join("next")).unwrap();
        assert_eq!(snapshot.get(b"base").unwrap().as_deref(), Some(&b"old"[..]));
        assert_eq!(snapshot.get(b"later").unwrap(), None);
        assert_eq!(store.get(b"base").unwrap().as_deref(), Some(&b"new"[..]));
        let new_view = Arc::clone(&store.read().unwrap().view.base.log);
        assert!(Arc::ptr_eq(
            &store.log.shared_file_for_test().unwrap(),
            &new_view.shared_file_for_test().unwrap(),
        ));
        drop(new_view);
        drop(snapshot);
        drop(store);
        assert!(weak.upgrade().is_some());
        let mut header = [0; 8];
        old_view.read_bytes(&mut header, 0).unwrap();
        assert_eq!(&header, b"EMDALOG3");
        drop(old_view);
        assert!(weak.upgrade().is_none());
        let reopened = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(reopened.get(b"base").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(
            reopened.get(b"later").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }

    struct IdentityFault {
        file: File,
        fail: bool,
    }

    impl LogIo for IdentityFault {
        fn len(&self) -> io::Result<u64> {
            LogIo::len(&self.file)
        }
        fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            LogIo::read_exact_at(&self.file, bytes, offset)
        }
        fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
            LogIo::append_all(&self.file, bytes)
        }
        fn sync_data(&self) -> io::Result<()> {
            LogIo::sync_data(&self.file)
        }
        fn file_identity(&self) -> io::Result<Option<(u64, u64)>> {
            if self.fail {
                Err(io::Error::other("injected stat failure"))
            } else {
                Ok(None)
            }
        }
    }

    struct SharedAdapter {
        file: File,
        shared: Arc<File>,
        fail_identity: bool,
    }

    impl LogIo for SharedAdapter {
        fn len(&self) -> io::Result<u64> {
            LogIo::len(&self.file)
        }
        fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            LogIo::read_exact_at(&self.file, bytes, offset)
        }
        fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
            LogIo::append_all(&self.file, bytes)
        }
        fn sync_data(&self) -> io::Result<()> {
            LogIo::sync_data(&self.file)
        }
        fn file_identity(&self) -> io::Result<Option<(u64, u64)>> {
            if self.fail_identity {
                Err(io::Error::other("injected writer stat failure"))
            } else {
                LogIo::file_identity(&self.file)
            }
        }
        fn shared_file(&self) -> Option<Arc<File>> {
            Some(Arc::clone(&self.shared))
        }
    }

    #[test]
    fn unavailable_or_failed_identity_keeps_the_validated_view() {
        let (_dir, path, checkpoint, digest) = default_fixture();
        if !has_v3(&path) {
            return;
        }
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        for fail in [false, true] {
            let (mut view, _) = Log::open_checkpoint_view(&path).unwrap();
            view.release_view_witness().unwrap();
            view.replace_io_for_test(Box::new(IdentityFault {
                file: File::open(path.join("records.log")).unwrap(),
                fail,
            }));
            assert!(!view.try_share_checkpoint_view_file(&store.log));
            assert!(view.shared_file_for_test().is_none());
            let mut header = [0; 8];
            view.read_bytes(&mut header, 0).unwrap();
            assert_eq!(&header, b"EMDALOG3");
        }
        store.put(b"later", b"value").unwrap();
        drop(store);
        let reopened = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(
            reopened.get(b"later").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }

    #[test]
    fn same_log_identity_on_a_different_inode_is_not_shared() {
        let (dir, path, checkpoint, digest) = default_fixture();
        if !has_v3(&path) {
            return;
        }
        let backup = dir.path().join("backup");
        let source = retry_lock(|| Store::open(&path)).unwrap();
        source.backup(&backup).unwrap();
        drop(source);
        let restored_path = dir.path().join("restored");
        Store::restore(&backup, &restored_path).unwrap();
        let first = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let second =
            retry_lock(|| CheckpointStore::open(&restored_path, &checkpoint, &digest)).unwrap();
        let (mut view, _) = Log::open_checkpoint_view(&path).unwrap();
        view.release_view_witness().unwrap();
        assert!(view.id() == second.log.id());
        assert!(!view.try_share_checkpoint_view_file(&second.log));
        assert!(view.shared_file_for_test().is_none());
        let mut header = [0; 8];
        view.read_bytes(&mut header, 0).unwrap();
        assert_eq!(&header, b"EMDALOG3");
        drop(view);
        drop(second);
        drop(first);
    }

    #[test]
    fn writer_identity_failure_or_unrelated_shared_file_keeps_the_original_view() {
        let (dir, path, checkpoint, digest) = default_fixture();
        if !has_v3(&path) {
            return;
        }
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let writer_file = store.log.shared_file_for_test().unwrap();
        let unrelated_path = dir.path().join("unrelated");
        fs::write(&unrelated_path, b"not the validated log").unwrap();
        for (fail_identity, shared) in [
            (true, Arc::clone(&writer_file)),
            (false, Arc::new(File::open(&unrelated_path).unwrap())),
        ] {
            let (mut view, _) = Log::open_checkpoint_view(&path).unwrap();
            view.release_view_witness().unwrap();
            store.log.replace_io_for_test(Box::new(SharedAdapter {
                file: OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(path.join("records.log"))
                    .unwrap(),
                shared,
                fail_identity,
            }));
            assert!(!view.try_share_checkpoint_view_file(&store.log));
            let mut header = [0; 8];
            view.read_bytes(&mut header, 0).unwrap();
            assert_eq!(&header, b"EMDALOG3");
        }
        store.put(b"later", b"value").unwrap();
        drop(writer_file);
        drop(store);
        let reopened = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(
            reopened.get(b"later").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}
