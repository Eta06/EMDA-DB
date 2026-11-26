//! Opening a managed checkpoint store without hashing its covered prefix,
//! and verifying that prefix later.
use super::*;

const OBSOLETE: &[u8] = b"obsolete-value-marker";
const LIVE: &[u8] = b"current-value-marker";

// A managed store whose covered prefix holds an overwritten (obsolete)
// record, plus a suffix written after the checkpoint.
fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"k", OBSOLETE).unwrap();
    ordinary.put(b"k", LIVE).unwrap();
    for index in 0..32_u32 {
        ordinary
            .put(format!("key-{index:02}").as_bytes(), &[index as u8; 64])
            .unwrap();
    }
    let digest = ordinary.checkpoint_to(dir.path().join("base")).unwrap();
    drop(ordinary);
    let store =
        retry_lock(|| CheckpointStore::open(&path, dir.path().join("base"), &digest)).unwrap();
    store.rollover_managed(dir.path().join("managed")).unwrap();
    store.put(b"suffix", b"after the checkpoint").unwrap();
    drop(store);
    (dir, path)
}

// (checkpoint file bytes, log bytes, covered prefix bytes).
fn sizes(path: &Path) -> (u64, u64, u64) {
    let checkpoint = crate::checkpoint_store::selection::load(path)
        .unwrap()
        .unwrap()
        .checkpoint;
    let header = fs::read(&checkpoint).unwrap();
    let prefix = u64::from_le_bytes(header[8..16].try_into().unwrap());
    (
        fs::metadata(&checkpoint).unwrap().len(),
        fs::metadata(path.join("records.log")).unwrap().len(),
        prefix,
    )
}

fn deferred(path: &Path, total: Option<u64>) -> Result<CheckpointStore, RecoveryOpenFailure> {
    CheckpointStore::open_managed_deferring_prefix_verification(
        path,
        CheckpointStoreLimits::default(),
        CheckpointRecoveryLimits::default(),
        total,
    )
}

fn flip(path: &Path, marker: &[u8]) {
    let log = path.join("records.log");
    let mut bytes = fs::read(&log).unwrap();
    let at = bytes
        .windows(marker.len())
        .position(|window| window == marker)
        .unwrap();
    bytes[at + 3] ^= 0x20;
    fs::write(&log, bytes).unwrap();
}

#[test]
fn a_deferred_open_reads_only_the_checkpoint_and_suffix() {
    let (_dir, path) = fixture();
    let (checkpoint, log, prefix) = sizes(&path);
    let bounded = |total| {
        CheckpointStore::open_managed_with_total_recovery_scan_limit(
            &path,
            CheckpointStoreLimits::default(),
            CheckpointRecoveryLimits::default(),
            total,
        )
    };
    // A full open needs the checkpoint and the whole log.
    assert!(
        retry_lock(|| match bounded(checkpoint + log - 1) {
            Err(RecoveryOpenFailure::NotAdopted(error))
                if error.kind() == io::ErrorKind::WouldBlock =>
            {
                Err(error)
            }
            other => Ok(other),
        })
        .unwrap()
        .is_err()
    );
    // A deferred open needs only the checkpoint and the suffix.
    let short = retry_lock(
        || match deferred(&path, Some(checkpoint + log - prefix - 1)) {
            Err(RecoveryOpenFailure::NotAdopted(error))
                if error.kind() == io::ErrorKind::WouldBlock =>
            {
                Err(error)
            }
            other => Ok(other),
        },
    )
    .unwrap();
    assert!(
        matches!(&short, Err(RecoveryOpenFailure::NotAdopted(error))
            if error.kind() == io::ErrorKind::InvalidInput),
        "{:?}",
        short.as_ref().err()
    );
    let store = retry_lock(|| match deferred(&path, Some(checkpoint + log - prefix)) {
        Err(RecoveryOpenFailure::NotAdopted(error))
            if error.kind() == io::ErrorKind::WouldBlock =>
        {
            Err(error)
        }
        other => Ok(other),
    })
    .unwrap()
    .unwrap();
    assert!(!store.covered_prefix_verified().unwrap());
    assert_eq!(store.get(b"k").unwrap().as_deref(), Some(LIVE));
    assert_eq!(
        store.get(b"suffix").unwrap().as_deref(),
        Some(&b"after the checkpoint"[..])
    );
    // Verification is bounded too, and refuses before reading.
    let error = store.verify_covered_prefix(Some(prefix - 1)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(!store.covered_prefix_verified().unwrap());
    store.verify_covered_prefix(Some(prefix)).unwrap();
    assert!(store.covered_prefix_verified().unwrap());
    store.put(b"after", b"verification").unwrap();
    // Rollover replays the whole log as before.
    store
        .rollover_managed(path.parent().unwrap().join("next"))
        .unwrap();
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert!(reopened.covered_prefix_verified().unwrap());
    assert_eq!(
        reopened.get(b"after").unwrap().as_deref(),
        Some(&b"verification"[..])
    );
}

#[test]
fn damage_to_an_obsolete_covered_record_is_found_by_verification() {
    let (_dir, path) = fixture();
    flip(&path, OBSOLETE);
    // The full open still rejects it.
    let error = retry_lock(|| CheckpointStore::open_managed(&path))
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    // A deferred open serves live values; verification finds the damage and
    // fails the store closed.
    let store = retry_lock(|| match deferred(&path, None) {
        Err(RecoveryOpenFailure::NotAdopted(error))
            if error.kind() == io::ErrorKind::WouldBlock =>
        {
            Err(error)
        }
        other => Ok(other),
    })
    .unwrap()
    .unwrap();
    assert_eq!(store.get(b"k").unwrap().as_deref(), Some(LIVE));
    let error = store.verify_covered_prefix(None).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert!(!store.covered_prefix_verified().unwrap());
    assert!(store.get(b"k").is_err());
    assert!(store.put(b"later", b"refused").is_err());
}

#[test]
fn a_damaged_live_value_fails_its_read_after_a_deferred_open() {
    let (_dir, path) = fixture();
    flip(&path, LIVE);
    let store = retry_lock(|| match deferred(&path, None) {
        Err(RecoveryOpenFailure::NotAdopted(error))
            if error.kind() == io::ErrorKind::WouldBlock =>
        {
            Err(error)
        }
        other => Ok(other),
    })
    .unwrap()
    .unwrap();
    let error = store.get(b"k").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    // Other values are unaffected; verification also reports the damage.
    assert_eq!(
        store.get(b"key-05").unwrap().as_deref(),
        Some(&[5_u8; 64][..])
    );
    assert!(store.verify_covered_prefix(None).is_err());
}

#[test]
fn a_pending_tail_is_adopted_with_the_full_prefix_hash() {
    let (_dir, path) = fixture();
    // A complete record past the witnessed boundary.
    let tail = crate::record::encode(crate::record::Operation::Put, b"tail", b"value").unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(&tail)
        .unwrap();
    let store = retry_lock(|| match deferred(&path, None) {
        Err(RecoveryOpenFailure::NotAdopted(error))
            if error.kind() == io::ErrorKind::WouldBlock =>
        {
            Err(error)
        }
        other => Ok(other),
    })
    .unwrap()
    .unwrap();
    assert!(store.covered_prefix_verified().unwrap());
    assert_eq!(store.get(b"tail").unwrap().as_deref(), Some(&b"value"[..]));
}
