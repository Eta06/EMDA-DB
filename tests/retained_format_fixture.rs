//! Compatibility with frozen bytes from the first standalone retained format.
use database_engine::retained_candidate::RetainedStore;
use database_engine::{BatchOperation, Store};
use std::{fs, io};

#[path = "fixtures/retained_v1.rs"]
mod fixture;
use fixture::{BASE, LIMITS, LOG, WITNESS};

#[path = "../fuzz/fuzz_targets/retained_recovery/model.rs"]
mod model;

#[test]
fn retained_recovery_corpus_and_known_boundaries() {
    let corpus =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/retained-recovery-seeds");
    for entry in fs::read_dir(corpus).unwrap() {
        model::check(&fs::read(entry.unwrap().path()).unwrap());
    }
    for operation in 0..3 {
        for physical_end in 0..4 {
            for witness_end in 0..4 {
                model::check(&[operation, physical_end, 0, witness_end, 0, 0, 0, 42]);
            }
        }
    }
}

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
    let binding = fixture::binding();
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
