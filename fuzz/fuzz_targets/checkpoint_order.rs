#![no_main]
use database_engine::{BatchOperation, CheckpointView, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::OnceLock;

struct Fixture {
    _directory: tempfile::TempDir,
    view: CheckpointView,
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
            for number in 0..2000_u32 {
                let mut key = vec![fill; 16];
                key.extend_from_slice(&(number * 2).to_be_bytes());
                expected.insert(key, b"value".to_vec());
            }
        }
        let entries = expected.iter().collect::<Vec<_>>();
        for chunk in entries.chunks(512) {
            let operations = chunk
                .iter()
                .map(|(key, value)| BatchOperation::Put { key, value })
                .collect::<Vec<_>>();
            store.write_batch(&operations).expect("write fixture");
        }
        let token = store.checkpoint_to(&checkpoint).expect("checkpoint");
        drop(store);
        let view = CheckpointView::open(&path, &checkpoint, &token).expect("open view");
        assert!(view.block_count() > 1);
        Fixture {
            _directory: directory,
            view,
            expected,
        }
    })
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 || data.len() > 1025 {
        return;
    }
    let fill = [b'a', b'q', b'z'][usize::from(data[1]) % 3];
    let key = match data[0] % 3 {
        0 => data[1..].to_vec(),
        1 if data.len() >= 6 => {
            let mut key = vec![fill; 16];
            let number = u32::from_be_bytes(data[2..6].try_into().expect("four bytes")) % 4002;
            key.extend_from_slice(&number.to_be_bytes());
            key.extend_from_slice(&data[6..]);
            if key.len() > 1024 {
                return;
            }
            key
        }
        _ => vec![fill; usize::from(data[1] % 16) + 1],
    };
    let fixture = fixture();
    assert_eq!(
        fixture.view.get(&key).expect("point read"),
        fixture.expected.get(&key).cloned()
    );
    let limit = usize::from(data[0] % 8) + 1;
    let expected = fixture
        .expected
        .range::<[u8], _>((Excluded(key.as_slice()), Unbounded))
        .take(limit)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        fixture.view.scan(Some(&key), None, limit).expect("scan"),
        expected
    );
});
