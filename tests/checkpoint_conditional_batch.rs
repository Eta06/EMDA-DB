use database_engine::{BatchOperation, CheckpointStore, MAX_SCAN_ENTRIES, Store};
use std::fs;
use std::io;
use std::sync::{Arc, Barrier};

fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"a", b"old").unwrap();
    ordinary.put(b"empty", b"").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    (dir, path, checkpoint, digest)
}

#[test]
fn conditional_batch_checks_absence_duplicates_and_validation_before_append() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let operations = [
        BatchOperation::Put {
            key: b"a",
            value: b"new",
        },
        BatchOperation::Put {
            key: b"b",
            value: b"value",
        },
    ];
    let log = path.join("records.log");
    let before = fs::read(&log).unwrap();
    assert!(
        !store
            .compare_exchange_batch(&[(b"a", Some(b"wrong")), (b"b", None)], &operations)
            .unwrap()
    );
    assert!(
        !store
            .compare_exchange_batch(&[(b"empty", None)], &operations)
            .unwrap()
    );
    assert!(
        !store
            .compare_exchange_batch(&[(b"a", Some(b"old")), (b"a", Some(b"wrong"))], &operations)
            .unwrap()
    );
    assert_eq!(fs::read(&log).unwrap(), before);
    assert_eq!(
        store
            .compare_exchange_batch(&[(b"a", Some(b"wrong"))], &[])
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        store
            .compare_exchange_batch(&[(b"", None)], &operations)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let many = vec![(b"a".as_slice(), None); MAX_SCAN_ENTRIES + 1];
    assert_eq!(
        store
            .compare_exchange_batch(&many, &operations)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let oversized = vec![0; 32 * 1024 * 1024];
    assert_eq!(
        store
            .compare_exchange_batch(&[(b"a", Some(&oversized))], &operations)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(fs::read(&log).unwrap(), before);
    assert!(
        store
            .compare_exchange_batch(
                &[(b"a", Some(b"old")), (b"empty", Some(b"")), (b"b", None)],
                &operations
            )
            .unwrap()
    );
    assert_eq!(
        store.get_many(&[b"a", b"b"]).unwrap(),
        vec![Some(b"new".to_vec()), Some(b"value".to_vec())]
    );
    let next = dir.path().join("next");
    let next_digest = store.rollover_to(&next).unwrap();
    drop(store);
    let reopened = CheckpointStore::open(&path, &next, &next_digest).unwrap();
    assert_eq!(
        reopened.get_many(&[b"a", b"b"]).unwrap(),
        vec![Some(b"new".to_vec()), Some(b"value".to_vec())]
    );
}

#[test]
fn concurrent_conditional_transfers_preserve_both_counters() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = Arc::new(CheckpointStore::open(&path, &checkpoint, &digest).unwrap());
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"left",
                value: &500_i64.to_le_bytes(),
            },
            BatchOperation::Put {
                key: b"right",
                value: &500_i64.to_le_bytes(),
            },
        ])
        .unwrap();
    let start = Arc::new(Barrier::new(5));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            scope.spawn(move || {
                start.wait();
                for _ in 0..40 {
                    let mut committed = false;
                    for _ in 0..10_000 {
                        let values = store.get_many(&[b"left", b"right"]).unwrap();
                        let left = values[0].as_ref().unwrap();
                        let right = values[1].as_ref().unwrap();
                        let l = i64::from_le_bytes(left.as_slice().try_into().unwrap());
                        let r = i64::from_le_bytes(right.as_slice().try_into().unwrap());
                        let next_l = (l - 1).to_le_bytes();
                        let next_r = (r + 1).to_le_bytes();
                        if store
                            .compare_exchange_batch(
                                &[(b"left", Some(left)), (b"right", Some(right))],
                                &[
                                    BatchOperation::Put {
                                        key: b"left",
                                        value: &next_l,
                                    },
                                    BatchOperation::Put {
                                        key: b"right",
                                        value: &next_r,
                                    },
                                ],
                            )
                            .unwrap()
                        {
                            committed = true;
                            break;
                        }
                    }
                    assert!(committed, "conditional transfer retry bound exceeded");
                }
            });
        }
        start.wait();
    });
    let expected = vec![
        Some(340_i64.to_le_bytes().to_vec()),
        Some(660_i64.to_le_bytes().to_vec()),
    ];
    assert_eq!(store.get_many(&[b"left", b"right"]).unwrap(), expected);
    drop(store);
    assert_eq!(
        Store::open(&path)
            .unwrap()
            .get_many(&[b"left", b"right"])
            .unwrap(),
        expected
    );
}
