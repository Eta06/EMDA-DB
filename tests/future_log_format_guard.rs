//! Current-reader guards for a proposed byte-zero marker, not old-reader tests.
use database_engine::{CheckpointStore, CheckpointView, RecoveredCheckpointView, Store};
use sha2::{Digest, Sha256};
use std::fs;

#[test]
fn future_marker_and_truncated_headers_fail_closed() {
    // The default format is V3 only on supported Linux filesystems; the fixture
    // guarantees V2 coverage on every supported test platform.
    for fixture_v2 in [true, false] {
        for with_witness in [true, false] {
            for marker_len in 1..=8 {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("store");
                let checkpoint = root.path().join("checkpoint");
                if fixture_v2 {
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
                let original = fs::read(path.join("records.log")).unwrap();
                let log = if marker_len == 8 {
                    let mut bytes = original;
                    bytes[..8].copy_from_slice(b"EMDALOG4");
                    bytes
                } else {
                    b"EMDALOG4"[..marker_len].to_vec()
                };
                fs::write(path.join("records.log"), &log).unwrap();
                if !with_witness {
                    fs::remove_file(path.join("commit.state")).unwrap();
                }
                let witness = fs::read(path.join("commit.state")).ok();
                let checkpoint_bytes = fs::read(&checkpoint).unwrap();
                assert!(
                    Store::open(&path).is_err(),
                    "Store: {fixture_v2} {with_witness} {marker_len}"
                );
                assert!(CheckpointStore::open(&path, &checkpoint, &digest).is_err());
                assert!(CheckpointView::open(&path, &checkpoint, &digest).is_err());
                assert!(RecoveredCheckpointView::open(&path, &checkpoint, &digest).is_err());
                assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
                assert_eq!(fs::read(path.join("commit.state")).ok(), witness);
                assert_eq!(fs::read(&checkpoint).unwrap(), checkpoint_bytes);
            }
        }
    }
}

#[test]
fn restore_rejects_checksum_valid_backup_with_future_log_marker() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("source")).unwrap();
    store.put(b"suffix", b"value").unwrap();
    let backup = root.path().join("backup");
    store.backup(&backup).unwrap();
    let original = fs::read(&backup).unwrap();
    let footer = original.len() - 48;
    for marker_len in 1..=8 {
        let mut bytes = original.clone();
        if marker_len == 8 {
            bytes[8..16].copy_from_slice(b"EMDALOG4");
        } else {
            // Construct a structurally valid backup containing a truncated log.
            bytes.truncate(8);
            bytes.extend_from_slice(&b"EMDALOG4"[..marker_len]);
            bytes.extend_from_slice(&original[footer..]);
        }
        let end = bytes.len() - 48;
        let digest = Sha256::digest(&bytes[8..end]);
        bytes[end + 8..end + 16].copy_from_slice(&((end - 8) as u64).to_le_bytes());
        bytes[end + 16..].copy_from_slice(&digest);
        fs::write(&backup, &bytes).unwrap();
        let destination = root.path().join(format!("restore-{marker_len}"));
        assert!(Store::restore(&backup, &destination).is_err());
        assert!(!destination.exists());
        assert_eq!(fs::read(&backup).unwrap(), bytes);
    }
}

#[test]
fn live_backup_rejects_future_marker_without_publishing() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    let mut log = fs::read(path.join("records.log")).unwrap();
    log[..8].copy_from_slice(b"EMDALOG4");
    fs::write(path.join("records.log"), &log).unwrap();
    let witness = fs::read(path.join("commit.state")).unwrap();
    let destination = root.path().join("backup");
    assert!(store.backup(&destination).is_err());
    assert!(!destination.exists());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness);
}

#[test]
fn empty_future_log_is_rejected_without_mutating_candidate_files() {
    // This exercises only current readers. The marker and companion files are
    // deliberately not a LOG4 encoding or evidence about historical binaries.
    for companions in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("candidate");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("records.log"), b"EMDALOG4").unwrap();
        if companions {
            fs::write(path.join("commit.state"), b"unrecognized witness").unwrap();
            fs::write(path.join("base.rb"), b"unrecognized base").unwrap();
        }
        assert!(Store::open(&path).is_err());
        assert_eq!(fs::read(path.join("records.log")).unwrap(), b"EMDALOG4");
        assert_eq!(
            fs::read(path.join("commit.state")).ok(),
            companions.then(|| b"unrecognized witness".to_vec())
        );
        assert_eq!(
            fs::read(path.join("base.rb")).ok(),
            companions.then(|| b"unrecognized base".to_vec())
        );
    }
}
