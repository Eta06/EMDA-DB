//! Validate atomic counter/marker histories and observed receipts.
use super::*;
use std::collections::BTreeSet;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Recovered {
    pub(super) count: u64,
    markers: Vec<Option<u64>>,
}

pub(super) fn check_recovery<S: TestStore>(
    opened: io::Result<S>,
    receipts: &[Receipt],
) -> Option<Recovered> {
    let store = match opened {
        Ok(store) => store,
        Err(error) => {
            assert!(
                matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ),
                "unexpected recovery error: {error}"
            );
            return None;
        }
    };
    assert_eq!(store.read(b"safe").unwrap().as_deref(), Some(&b"guard"[..]));
    assert_eq!(
        store.read(BLOCKED).unwrap(),
        None,
        "unsubmitted transaction became visible"
    );
    let count = number(store.read(COUNTER).unwrap()).unwrap();
    assert!(count <= (WORKERS * PER_WORKER) as u64);
    let mut markers = Vec::with_capacity(WORKERS * PER_WORKER);
    let mut committed_numbers = BTreeSet::new();
    for worker in 0..WORKERS {
        let mut absent = false;
        for sequence in 0..PER_WORKER {
            let actual = store.read(&marker_key(worker, sequence)).unwrap();
            let value = if actual.is_some() {
                assert!(!absent, "worker {worker} has a gap before {sequence}");
                let value = number(actual).unwrap();
                assert!(
                    committed_numbers.insert(value),
                    "duplicate committed counter {value}"
                );
                Some(value)
            } else {
                absent = true;
                None
            };
            markers.push(value);
        }
    }
    assert_eq!(committed_numbers.len() as u64, count);
    assert!(
        committed_numbers.iter().copied().eq(1..=count),
        "counter history has a gap"
    );
    for receipt in receipts {
        assert_eq!(
            markers[receipt.worker * PER_WORKER + receipt.sequence],
            Some(receipt.counter),
            "acknowledged transaction was lost or changed: {receipt:?}"
        );
    }
    Some(Recovered { count, markers })
}

#[test]
fn model_rejects_lost_receipts_fractured_commits_and_staged_data() {
    use std::collections::BTreeMap;

    struct Memory(BTreeMap<Vec<u8>, Vec<u8>>);
    impl TestStore for Memory {
        fn write(&self, _key: &[u8], _value: &[u8]) -> io::Result<()> {
            Err(io::Error::other("read-only model"))
        }
        fn remove(&self, _key: &[u8]) -> io::Result<()> {
            Err(io::Error::other("read-only model"))
        }
        fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
            Ok(self.0.get(key).cloned())
        }
    }

    let mut valid = BTreeMap::new();
    valid.insert(b"safe".to_vec(), b"guard".to_vec());
    valid.insert(COUNTER.to_vec(), 2_u64.to_le_bytes().to_vec());
    valid.insert(marker_key(0, 0), 1_u64.to_le_bytes().to_vec());
    valid.insert(marker_key(1, 0), 2_u64.to_le_bytes().to_vec());
    let receipts = [Receipt {
        worker: 0,
        sequence: 0,
        counter: 1,
    }];
    assert!(check_recovery(Ok(Memory(valid.clone())), &receipts).is_some());

    let mut lost_receipt = valid.clone();
    lost_receipt.remove(&marker_key(0, 0));
    lost_receipt.insert(COUNTER.to_vec(), 1_u64.to_le_bytes().to_vec());
    lost_receipt.insert(marker_key(1, 0), 1_u64.to_le_bytes().to_vec());
    let mut fractured = valid.clone();
    fractured.insert(COUNTER.to_vec(), 3_u64.to_le_bytes().to_vec());
    let mut duplicate = valid.clone();
    duplicate.insert(marker_key(1, 0), 1_u64.to_le_bytes().to_vec());
    let mut staged = valid;
    staged.insert(BLOCKED.to_vec(), 3_u64.to_le_bytes().to_vec());
    for invalid in [lost_receipt, fractured, duplicate, staged] {
        assert!(
            std::panic::catch_unwind(|| check_recovery(Ok(Memory(invalid)), &receipts)).is_err()
        );
    }
}
