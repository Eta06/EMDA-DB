//! Compatibility with frozen bytes from the first standalone retained format.
use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    Expected, IndexLimits, OverlayLimits, RetainedStore, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{BatchOperation, Store};
use std::{fs, io};

const BASE: &[u8] = include_bytes!("fixtures/retained-v1-base");
const LOG: &[u8] = include_bytes!("fixtures/retained-v1-records.log");
const WITNESS: &[u8] = include_bytes!("fixtures/retained-v1-commit.state");
const BINDING: &[u8; 176] = include_bytes!("fixtures/retained-v1-binding.bin");
const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 4096,
        entries: 8,
        key_bytes: 128,
        work_bytes: 4097,
    },
    index: IndexLimits {
        blocks: 4,
        anchor_bytes: 128,
    },
    suffix: SuffixLimits {
        bytes: 4096,
        operations: 16,
        work: 1024,
    },
    overlay: OverlayLimits {
        entries: 8,
        key_bytes: 128,
    },
};

#[test]
fn retained_v1_fixture_reopens_and_accepts_a_new_atomic_batch() {
    let fixture = tempfile::tempdir().unwrap();
    let path = fixture.path();
    let files = [
        ("base", BASE),
        ("records.log", LOG),
        ("commit.state", WITNESS),
        ("store.lock", &[]),
    ];
    for (name, bytes) in files {
        fs::write(path.join(name), bytes).unwrap();
    }
    let binding = Expected {
        header: BINDING[..112].try_into().unwrap(),
        witness: BINDING[112..144].try_into().unwrap(),
        base_digest: BINDING[144..].try_into().unwrap(),
    };
    assert_eq!(
        Store::open(path).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
    let mut store = RetainedStore::open(path, LIMITS, &binding).unwrap();
    assert_eq!(
        store.scan(None, None, 8).unwrap(),
        [
            (vec![0, 255], vec![0, 2, 255]),
            (b"a".to_vec(), b"new".to_vec()),
            (b"empty".to_vec(), vec![]),
        ]
    );
    assert!(store.get(b"retired", 3).unwrap().is_none());
    for (name, bytes) in files {
        assert_eq!(fs::read(path.join(name)).unwrap(), bytes);
    }
    store
        .write_batch(&[
            BatchOperation::Delete { key: &[0, 255] },
            BatchOperation::Put {
                key: b"next",
                value: b"value",
            },
        ])
        .unwrap();
    drop(store);
    let reopened = RetainedStore::open(path, LIMITS, &binding).unwrap();
    assert_eq!(
        reopened.scan(None, None, 8).unwrap(),
        [
            (b"a".to_vec(), b"new".to_vec()),
            (b"empty".to_vec(), vec![]),
            (b"next".to_vec(), b"value".to_vec()),
        ]
    );
    assert!(fs::read(path.join("records.log")).unwrap().starts_with(LOG));
    assert_eq!(fs::read(path.join("base")).unwrap(), BASE);
}
