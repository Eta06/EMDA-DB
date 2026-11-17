use super::*;
use crate::log::PeakVerificationLimits;
use crate::record::batch::BatchOperation;

#[test]
fn reconstructs_verified_prefix_and_enforces_limits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"a", b"old").unwrap();
    store.put(b"a", b"new").unwrap();
    let before_batch = store.log.len().unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"bbb",
                value: b"v",
            },
            BatchOperation::Delete { key: b"a" },
        ])
        .unwrap();
    let boundary = store.log.len().unwrap();
    store.put(b"c", b"old").unwrap();
    store.delete(b"c").unwrap();
    let total = store.log.len().unwrap();
    let bytes = std::fs::read(dir.path().join("records.log")).unwrap();
    let inner = store.read().unwrap();
    let limits = PeakVerificationLimits {
        max_log_bytes: total,
        max_live_keys: 2,
        max_live_key_bytes: 4,
    };
    let (index, count, key_bytes) = store
        .log
        .reconstruct_prefix_index_peaks(&inner.index, boundary, limits)
        .unwrap();
    assert_eq!((count, key_bytes), (2, 4));
    assert_eq!(index.len(), 1);
    assert_eq!(store.log.read(index[b"bbb".as_slice()]).unwrap(), b"v");
    assert_eq!(index[b"bbb".as_slice()], inner.index[b"bbb".as_slice()]);
    let (earlier, count, key_bytes) = store
        .log
        .reconstruct_prefix_index_peaks(&inner.index, before_batch, limits)
        .unwrap();
    assert_eq!((count, key_bytes), (1, 1));
    assert_eq!(store.log.read(earlier[b"a".as_slice()]).unwrap(), b"new");
    assert_eq!(earlier[b"a".as_slice()].offset, before_batch - 4 - 3);
    let (empty, _, _) = store
        .log
        .reconstruct_prefix_index_peaks(
            &inner.index,
            crate::witness::LOG_HEADER_SIZE as u64,
            limits,
        )
        .unwrap();
    assert!(empty.is_empty());
    let (end, _, _) = store
        .log
        .reconstruct_prefix_index_peaks(&inner.index, total, limits)
        .unwrap();
    assert_eq!(end, *inner.index);
    let child_boundary = before_batch
        + crate::record::HEADER_SIZE as u64
        + 4
        + (crate::record::HEADER_SIZE + 3 + 1 + crate::record::CHECKSUM_SIZE) as u64;
    let err = store
        .log
        .reconstruct_prefix_index_peaks(&inner.index, child_boundary, limits)
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "log does not contain its acknowledged commit boundary"
    );
    let mut wrong = (*inner.index).clone();
    wrong.clear();
    let err = store
        .log
        .reconstruct_prefix_index_peaks(&wrong, boundary, limits)
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(err.to_string(), "live index does not match verified log");
    for invalid in [before_batch + 1, boundary - 1, total + 1] {
        assert!(
            store
                .log
                .reconstruct_prefix_index_peaks(&inner.index, invalid, limits)
                .is_err()
        );
    }
    for limit in [
        PeakVerificationLimits {
            max_log_bytes: total - 1,
            ..limits
        },
        PeakVerificationLimits {
            max_live_keys: 1,
            ..limits
        },
        PeakVerificationLimits {
            max_live_key_bytes: 3,
            ..limits
        },
    ] {
        assert_eq!(
            store
                .log
                .reconstruct_prefix_index_peaks(&inner.index, boundary, limit)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
    assert_eq!(
        std::fs::read(dir.path().join("records.log")).unwrap(),
        bytes
    );
}

#[test]
fn rejects_corrupt_obsolete_suffix() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"a", b"v").unwrap();
    let boundary = store.log.len().unwrap();
    store.put(b"obsolete", b"value").unwrap();
    store.delete(b"obsolete").unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.path().join("records.log"))
        .unwrap();
    file.seek(SeekFrom::Start(boundary + 1)).unwrap();
    file.write_all(&[0xff]).unwrap();
    let corrupted = std::fs::read(dir.path().join("records.log")).unwrap();
    let inner = store.read().unwrap();
    let limits = PeakVerificationLimits {
        max_log_bytes: u64::MAX,
        max_live_keys: usize::MAX,
        max_live_key_bytes: usize::MAX,
    };
    assert!(
        store
            .log
            .reconstruct_prefix_index_peaks(&inner.index, boundary, limits)
            .is_err()
    );
    assert_eq!(
        std::fs::read(dir.path().join("records.log")).unwrap(),
        corrupted
    );
}

#[test]
fn suffix_alone_exceeds_logical_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"a", b"v").unwrap();
    let boundary = store.log.len().unwrap();
    store.put(b"suffix", b"v").unwrap();
    store.delete(b"suffix").unwrap();
    let original = std::fs::read(dir.path().join("records.log")).unwrap();
    let inner = store.read().unwrap();
    let limits = PeakVerificationLimits {
        max_log_bytes: u64::MAX,
        max_live_keys: 1,
        max_live_key_bytes: usize::MAX,
    };
    let err = store
        .log
        .reconstruct_prefix_index_peaks(&inner.index, boundary, limits)
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(err.to_string(), "verification live key limit exceeded");
    assert_eq!(
        std::fs::read(dir.path().join("records.log")).unwrap(),
        original
    );
}

#[test]
fn rejects_modified_attached_v2_witness() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("records.log"),
        include_bytes!("../../tests/fixtures/v2-empty-records.log"),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("commit.state"),
        include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
    )
    .unwrap();
    let store = Store::open(dir.path()).unwrap();
    let boundary = store.log.len().unwrap();
    let original = std::fs::read(dir.path().join("records.log")).unwrap();
    let marker = std::fs::read(dir.path().join("commit.state")).unwrap();
    let inner = store.read().unwrap();
    let limits = PeakVerificationLimits {
        max_log_bytes: u64::MAX,
        max_live_keys: usize::MAX,
        max_live_key_bytes: usize::MAX,
    };
    for mismatched in [false, true] {
        let mut changed = marker.clone();
        if mismatched {
            changed[8] ^= 1;
            let crc = crc32fast::hash(&changed[..32]);
            changed[32..].copy_from_slice(&crc.to_le_bytes());
        } else {
            changed[32] ^= 1;
        }
        std::fs::write(dir.path().join("commit.state"), &changed).unwrap();
        let err = store
            .log
            .reconstruct_prefix_index_peaks(&inner.index, boundary, limits)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "invalid commit witness or log identity");
        assert_eq!(
            std::fs::read(dir.path().join("commit.state")).unwrap(),
            changed
        );
        assert_eq!(
            std::fs::read(dir.path().join("records.log")).unwrap(),
            original
        );
    }
}
