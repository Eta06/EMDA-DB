//! Disk-backed access to a fully verified, non-authoritative retained base.
use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{Expected, IndexLimits, IndexedCandidate};
use database_engine::{RetainedExportLimits, Store};
use std::fs::{self, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

const MAX_VALUE: usize = 16 * 1024 * 1024;
const BASE_LIMITS: Limits = Limits {
    file_bytes: 40 * 1024 * 1024,
    entries: 16,
    key_bytes: 64,
    work_bytes: 40 * 1024 * 1024 + 1,
};

fn candidate(root: &Path, entries: &[(&[u8], &[u8])]) -> Expected {
    let store = Store::open(root.join("source")).unwrap();
    for (key, value) in entries {
        store.put(key, value).unwrap();
    }
    store
        .build_retained_candidate(
            root.join("candidate"),
            [3; 16],
            [4; 16],
            RetainedExportLimits {
                max_log_bytes: BASE_LIMITS.file_bytes,
                max_live_keys: BASE_LIMITS.entries as usize,
                max_live_key_bytes: BASE_LIMITS.key_bytes as usize,
                max_output_bytes: BASE_LIMITS.file_bytes,
            },
        )
        .unwrap()
}

#[test]
fn indexed_pages_and_points_match_the_candidate_under_exact_limits() {
    let root = tempfile::tempdir().unwrap();
    let chunk = vec![1; 64 * 1024];
    let max = vec![2; MAX_VALUE];
    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"a", b""),
        (b"b", &chunk),
        (b"c", &chunk),
        (b"d", b""),
        (b"e", &max),
        (b"f", &max),
        (b"z", b"tail"),
    ];
    let expected = candidate(root.path(), &entries);
    let path = root.path().join("candidate");
    let limits = IndexLimits {
        blocks: 5,
        anchor_bytes: 5,
    };
    for (smaller, message) in [
        (
            IndexLimits {
                blocks: 4,
                ..limits
            },
            "block limit",
        ),
        (
            IndexLimits {
                anchor_bytes: 4,
                ..limits
            },
            "anchor byte limit",
        ),
    ] {
        let error = IndexedCandidate::open(&path, BASE_LIMITS, smaller, &expected)
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains(message), "{error}");
    }
    let reader = IndexedCandidate::open(&path, BASE_LIMITS, limits, &expected).unwrap();
    assert_eq!(reader.metadata().entries, entries.len() as u64);
    assert_eq!(reader.metadata().digest, expected.base_digest);
    assert_eq!(reader.get(b"a", 0).unwrap(), Some(vec![]));
    assert_eq!(
        reader.get(b"b", chunk.len()).unwrap().as_deref(),
        Some(chunk.as_slice())
    );
    assert_eq!(
        reader.get(b"e", MAX_VALUE).unwrap().as_deref(),
        Some(max.as_slice())
    );
    assert_eq!(
        reader.get(b"c", 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    for absent in [b"0".as_slice(), b"bb", b"g", b"zz"] {
        assert_eq!(reader.get(absent, 0).unwrap(), None);
    }
    assert_eq!(
        reader.get(b"", 0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        reader.scan(None, None, 0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(reader.scan(Some(b"d"), Some(b"d"), 1).unwrap().is_empty());
    let middle = reader.scan(Some(b"b"), Some(b"e"), 10).unwrap();
    assert_eq!(
        middle,
        vec![(b"c".to_vec(), chunk), (b"d".to_vec(), vec![])]
    );
    let page = reader.scan(Some(b"d"), Some(b"z"), 10).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].0, b"e");
    assert_eq!(page[0].1, max);
    let next = reader.scan(Some(&page[0].0), None, 2).unwrap();
    assert_eq!(
        next,
        vec![(b"f".to_vec(), max), (b"z".to_vec(), b"tail".to_vec())]
    );
}

#[test]
fn indexed_reads_authenticate_touched_blocks_from_the_captured_file() {
    let root = tempfile::tempdir().unwrap();
    let large = vec![42; 128 * 1024];
    let expected = candidate(
        root.path(),
        &[(b"a", b"first"), (b"b", &large), (b"c", b"end")],
    );
    let path = root.path().join("candidate");
    let limits = IndexLimits {
        blocks: 3,
        anchor_bytes: 3,
    };
    let wrong = Expected {
        base_digest: [0; 32],
        ..expected.clone()
    };
    assert!(IndexedCandidate::open(&path, BASE_LIMITS, limits, &wrong).is_err());
    let reader = IndexedCandidate::open(&path, BASE_LIMITS, limits, &expected).unwrap();
    let base = path.join("base");
    let original = root.path().join("captured-base");
    fs::rename(&base, &original).unwrap();
    fs::write(&base, b"different inode").unwrap();
    assert!(IndexedCandidate::open(&path, BASE_LIMITS, limits, &expected).is_err());
    assert_eq!(reader.get(b"b", large.len()).unwrap(), Some(large));

    let file = OpenOptions::new().write(true).open(&original).unwrap();
    // First entry starts at 112 and consumes 8 length bytes, one key byte,
    // and five value bytes. Change the last byte of the second value.
    let offset = 112 + 14 + 9 + 128 * 1024 - 1;
    file.write_all_at(&[0], offset).unwrap();
    assert_eq!(
        reader.get(b"b", MAX_VALUE).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        reader.scan(None, None, 2).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(reader.get(b"a", 5).unwrap(), Some(b"first".to_vec()));
    assert_eq!(
        reader.scan(Some(b"b"), Some(b"c"), 1).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    file.set_len(112).unwrap();
    assert_eq!(
        reader.get(b"a", 5).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn empty_candidate_needs_no_anchors() {
    let root = tempfile::tempdir().unwrap();
    let expected = candidate(root.path(), &[]);
    let reader = IndexedCandidate::open(
        root.path().join("candidate"),
        BASE_LIMITS,
        IndexLimits {
            blocks: 0,
            anchor_bytes: 0,
        },
        &expected,
    )
    .unwrap();
    assert_eq!(reader.metadata().entries, 0);
    assert_eq!(reader.get(b"absent", 0).unwrap(), None);
    assert!(reader.scan(None, None, 1).unwrap().is_empty());
}
