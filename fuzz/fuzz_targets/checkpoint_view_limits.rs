#![no_main]
use database_engine::{
    CheckpointStore, CheckpointStoreLimits, CheckpointViewLimits, MAX_CHECKPOINT_VIEW_BLOCK_BYTES,
    Store,
};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::io::ErrorKind;

fn key(byte: u8) -> Vec<u8> {
    format!("{:02}{}", byte % 48, "k".repeat(900)).into_bytes()
}

fn checkpoint_size(model: &BTreeMap<Vec<u8>, Vec<u8>>) -> u64 {
    100 + model.keys().map(|key| (18 + key.len()) as u64).sum::<u64>()
}

fn checkpoint_blocks(model: &BTreeMap<Vec<u8>, Vec<u8>>) -> usize {
    let mut blocks = 0;
    let mut used = 0;
    for key in model.keys() {
        let entry = 18 + key.len();
        if used != 0 && used + entry > MAX_CHECKPOINT_VIEW_BLOCK_BYTES {
            used = 0;
        }
        if used == 0 {
            blocks += 1;
        }
        used += entry;
    }
    blocks
}

fn check(store: &CheckpointStore, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    assert_eq!(
        store.scan(None, None, 128).unwrap(),
        model
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    );
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 || data.len() > 128 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut checkpoint = root.path().join("initial");
    let initial = Store::open(&path).unwrap();
    let mut model = BTreeMap::new();
    let first = key(0);
    initial.put(&first, b"initial").unwrap();
    model.insert(first, b"initial".to_vec());
    let mut digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let initial_bytes = std::fs::metadata(&checkpoint).unwrap().len();
    assert_eq!(initial_bytes, checkpoint_size(&model));
    let limits = CheckpointStoreLimits {
        checkpoint_view: CheckpointViewLimits {
            max_file_bytes: initial_bytes + u64::from(data[0]) * 256,
            max_blocks: 1 + usize::from(data[1] & 1),
        },
        ..Default::default()
    };
    let mut store =
        CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits).unwrap();
    let mut generation = 0;
    for bytes in data[2..].chunks_exact(3) {
        let key = key(bytes[1]);
        match bytes[0] % 5 {
            0 => {
                let value = vec![bytes[2]; usize::from(bytes[2] % 8)];
                store.put(&key, &value).unwrap();
                model.insert(key, value);
            }
            1 => {
                store.delete(&key).unwrap();
                model.remove(&key);
            }
            2 | 3 => {
                let snapshot = store.snapshot().unwrap();
                generation += 1;
                let next = root.path().join(format!("index-{generation}"));
                let before = std::fs::read(path.join("records.log")).unwrap();
                let result = store.rollover_to(&next);
                let eligible = checkpoint_size(&model) <= limits.checkpoint_view.max_file_bytes
                    && checkpoint_blocks(&model) <= limits.checkpoint_view.max_blocks;
                match result {
                    Ok(next_digest) => {
                        assert!(eligible);
                        assert_eq!(
                            std::fs::metadata(&next).unwrap().len(),
                            checkpoint_size(&model)
                        );
                        checkpoint = next;
                        digest = next_digest;
                    }
                    Err(error) => {
                        assert!(!eligible, "unexpected rollover error: {error}");
                        assert_eq!(error.kind(), ErrorKind::InvalidInput);
                        assert!(!next.exists());
                    }
                }
                assert_eq!(std::fs::read(path.join("records.log")).unwrap(), before);
                assert_eq!(
                    snapshot.scan(None, None, 128).unwrap(),
                    model
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Vec<_>>()
                );
            }
            _ => {
                check(&store, &model);
                drop(store);
                let too_small = CheckpointStoreLimits {
                    checkpoint_view: CheckpointViewLimits {
                        max_file_bytes: std::fs::metadata(&checkpoint).unwrap().len() - 1,
                        ..limits.checkpoint_view
                    },
                    ..limits
                };
                let before = std::fs::read(path.join("records.log")).unwrap();
                assert_eq!(
                    CheckpointStore::open_with_options(&path, &checkpoint, &digest, too_small)
                        .err()
                        .unwrap()
                        .kind(),
                    ErrorKind::InvalidInput
                );
                assert_eq!(std::fs::read(path.join("records.log")).unwrap(), before);
                store = CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits)
                    .unwrap();
                assert_eq!(store.limits(), limits);
            }
        }
        check(&store, &model);
    }
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.scan(None, None, 128).unwrap(),
        model.into_iter().collect::<Vec<_>>()
    );
});
