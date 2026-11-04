#![no_main]
use database_engine::{BatchOperation, CheckpointView, RecoveredCheckpointView, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::sync::OnceLock;

struct Fixture {
    _directory: tempfile::TempDir,
    view: RecoveredCheckpointView,
    expected: BTreeMap<Vec<u8>, Vec<u8>>,
}

fn fixture() -> &'static Fixture {
    static INSTANCE: OnceLock<Fixture> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("store");
        let checkpoint = directory.path().join("index");
        let store = Store::open(&path).expect("open store");
        let mut expected = BTreeMap::new();
        for fill in [b'a', b'q', b'z'] {
            for len in 1..=16 {
                expected.insert(vec![fill; len], b"short".to_vec());
            }
            for number in 0..1200_u32 {
                let mut key = vec![fill; 16];
                key.extend_from_slice(&(number * 2).to_be_bytes());
                expected.insert(key, b"base".to_vec());
            }
        }
        let entries = expected.iter().collect::<Vec<_>>();
        for chunk in entries.chunks(512) {
            let operations = chunk
                .iter()
                .map(|(key, value)| BatchOperation::Put { key, value })
                .collect::<Vec<_>>();
            store.write_batch(&operations).expect("write base fixture");
        }
        let digest = store.checkpoint_to(&checkpoint).expect("checkpoint");
        let mut changes = Vec::<(Vec<u8>, Option<Vec<u8>>)>::new();
        for fill in [b'a', b'q', b'z'] {
            for number in (0..1200_u32).step_by(8) {
                let mut key = vec![fill; 16];
                key.extend_from_slice(&(number * 2).to_be_bytes());
                changes.push((key, Some(b"updated".to_vec())));
            }
            for number in (0..1200_u32).step_by(9) {
                let mut key = vec![fill; 16];
                key.extend_from_slice(&(number * 2).to_be_bytes());
                changes.push((key, None));
            }
            for number in (0..1200_u32).step_by(5) {
                let mut key = vec![fill; 16];
                key.extend_from_slice(&(number * 2 + 1).to_be_bytes());
                changes.push((key, Some(b"inserted".to_vec())));
            }
        }
        for chunk in changes.chunks(512) {
            let operations = chunk
                .iter()
                .map(|(key, value)| match value {
                    Some(value) => BatchOperation::Put { key, value },
                    None => BatchOperation::Delete { key },
                })
                .collect::<Vec<_>>();
            store
                .write_batch(&operations)
                .expect("write suffix fixture");
            for (key, value) in chunk {
                match value {
                    Some(value) => {
                        expected.insert(key.clone(), value.clone());
                    }
                    None => {
                        expected.remove(key);
                    }
                }
            }
        }
        drop(store);
        let historical =
            CheckpointView::open(&path, &checkpoint, &digest).expect("historical view");
        assert!(historical.block_count() > 1);
        drop(historical);
        let view =
            RecoveredCheckpointView::open(&path, &checkpoint, &digest).expect("recovered view");
        assert!(view.overlay_keys() > 0);
        Fixture {
            _directory: directory,
            view,
            expected,
        }
    })
}

fn key(data: &[u8], variant: u8) -> Vec<u8> {
    let fill = [b'a', b'q', b'z'][usize::from(data[1]) % 3];
    match variant % 3 {
        0 => data[1..].to_vec(),
        1 if data.len() >= 6 => {
            let mut key = vec![fill; 16];
            let number = u32::from_be_bytes(data[2..6].try_into().expect("four bytes")) % 2402;
            key.extend_from_slice(&number.to_be_bytes());
            key.extend_from_slice(&data[6..]);
            key.truncate(1024);
            key
        }
        _ => vec![fill; usize::from(data[1] % 16) + 1],
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 || data.len() > 1025 {
        return;
    }
    let first = key(data, data[0]);
    let second = key(data, data[0] >> 4);
    let after = (data[0] & 4 != 0).then_some(first.as_slice());
    let before = (data[0] & 8 != 0).then_some(second.as_slice());
    let limit = usize::from(data[0] % 8) + 1;
    let fixture = fixture();
    assert_eq!(
        fixture.view.get(&first).expect("point read"),
        fixture.expected.get(&first).cloned()
    );
    let expected = fixture
        .expected
        .iter()
        .filter(|(key, _)| after.is_none_or(|bound| key.as_slice() > bound))
        .filter(|(key, _)| before.is_none_or(|bound| key.as_slice() < bound))
        .take(limit)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        fixture.view.scan(after, before, limit).expect("scan"),
        expected
    );
});
