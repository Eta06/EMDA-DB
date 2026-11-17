//! Compatibility guard for a possible future, non-legacy log format.
use database_engine::{CheckpointStore, CheckpointView, Store};
use std::fs;

#[test]
fn unknown_log_header_never_replays_the_suffix_as_a_legacy_store() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        if v2 {
            fs::create_dir(&path).unwrap();
            fs::write(
                path.join("records.log"),
                include_bytes!("fixtures/v2-empty-records.log"),
            )
            .unwrap();
            fs::write(
                path.join("commit.state"),
                include_bytes!("fixtures/v2-empty-commit.state"),
            )
            .unwrap();
        }
        let store = Store::open(&path).unwrap();
        store.put(b"suffix", b"not-a-complete-store").unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);
        let mut log = fs::read(path.join("records.log")).unwrap();
        log[..8].copy_from_slice(b"EMDALOG4");
        fs::write(path.join("records.log"), &log).unwrap();
        let witness = fs::read(path.join("commit.state")).unwrap();
        assert!(Store::open(&path).is_err());
        assert!(CheckpointStore::open(&path, &checkpoint, &digest).is_err());
        assert!(CheckpointView::open(&path, &checkpoint, &digest).is_err());
        assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness);
        // Even without the sidecar, the unknown header must not become a
        // legacy stream beginning at the first valid suffix record.
        fs::remove_file(path.join("commit.state")).unwrap();
        assert!(Store::open(&path).is_err());
        assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
        assert!(!path.join("commit.state").exists());
    }
}
