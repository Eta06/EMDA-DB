#![no_main]
use database_engine::{
    BatchOperation, CheckpointView, CheckpointViewLimits, RecoveredCheckpointView,
    RecoveredViewLimits, Store,
};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::path::PathBuf;

struct Fixture {
    _root: tempfile::TempDir,
    path: PathBuf,
    checkpoint: PathBuf,
    digest: [u8; 32],
    base: Vec<(Vec<u8>, Vec<u8>)>,
    recovered: Vec<(Vec<u8>, Vec<u8>)>,
    log: Vec<u8>,
    checkpoint_bytes: Vec<u8>,
    tail_bytes: u64,
}

fn fixture(data: &[u8]) -> Fixture {
    let count = 2 + usize::from(data.get(1).copied().unwrap_or(0) % 15);
    let value_length = usize::from(data.get(2).copied().unwrap_or(0) % 65);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    let mut expected = BTreeMap::new();
    for key in b'a'..b'a' + count as u8 {
        expected.insert(vec![key], vec![key + 1; value_length]);
    }
    let operations = expected
        .iter()
        .map(|(key, value)| BatchOperation::Put { key, value })
        .collect::<Vec<_>>();
    store.write_batch(&operations).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let base = expected
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let prefix = std::fs::metadata(path.join("records.log")).unwrap().len();
    let updated = vec![data.get(3).copied().unwrap_or(7); value_length];
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: &updated,
            },
            BatchOperation::Delete { key: b"b" },
            BatchOperation::Put {
                key: b"z",
                value: b"added",
            },
        ])
        .unwrap();
    expected.insert(b"a".to_vec(), updated);
    expected.remove(&b"b".to_vec());
    expected.insert(b"z".to_vec(), b"added".to_vec());
    drop(store);
    let log = std::fs::read(path.join("records.log")).unwrap();
    let checkpoint_bytes = std::fs::read(&checkpoint).unwrap();
    Fixture {
        _root: root,
        path,
        checkpoint,
        digest,
        base,
        recovered: expected.into_iter().collect(),
        tail_bytes: log.len() as u64 - prefix,
        log,
        checkpoint_bytes,
    }
}

fuzz_target!(|data: &[u8]| {
    let Some(&flags) = data.first() else { return };
    let f = fixture(data);
    let log_length = f.log.len() as u64;
    let checkpoint_length = f.checkpoint_bytes.len() as u64;
    let view_limits = CheckpointViewLimits {
        max_file_bytes: checkpoint_length - u64::from(flags & 2 != 0),
        max_blocks: usize::from(flags & 4 == 0),
    };
    let log_limit = (flags & 64 == 0).then_some(log_length - u64::from(flags & 1 != 0));
    let historical_ok = flags & 6 == 0 && (flags & 1 == 0 || log_limit.is_none());
    let historical = CheckpointView::open_with_recovery_limits(
        &f.path,
        &f.checkpoint,
        &f.digest,
        view_limits,
        log_limit,
    );
    if historical_ok {
        assert_eq!(historical.unwrap().scan(None, None, 128).unwrap(), f.base);
    } else {
        assert_eq!(
            historical.err().unwrap().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
    let limits = RecoveredViewLimits {
        max_tail_bytes: f.tail_bytes - u64::from(flags & 8 != 0),
        max_tail_keys: 3 - usize::from(flags & 16 != 0),
        max_tail_key_bytes: 3 - usize::from(flags & 32 != 0),
        max_log_bytes: log_limit,
        checkpoint_view: view_limits,
    };
    let recovered =
        RecoveredCheckpointView::open_with_options(&f.path, &f.checkpoint, &f.digest, limits);
    if historical_ok && flags & 56 == 0 {
        assert_eq!(
            recovered.unwrap().scan(None, None, 128).unwrap(),
            f.recovered
        );
    } else {
        assert_eq!(
            recovered.err().unwrap().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
    assert_eq!(std::fs::read(f.path.join("records.log")).unwrap(), f.log);
    assert_eq!(std::fs::read(&f.checkpoint).unwrap(), f.checkpoint_bytes);
});
