#![no_main]
use database_engine::{CheckpointStore, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

fn check(store: &CheckpointStore, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let expected = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(store.scan(None, None, 100).expect("scan"), expected);
    for index in 0..8 {
        let key = vec![b'a' + index];
        assert_eq!(store.get(&key).expect("read"), model.get(&key).cloned());
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 128 {
        return;
    }
    let root = tempfile::tempdir().expect("temporary directory");
    let path = root.path().join("store");
    let initial = root.path().join("initial");
    let ordinary = Store::open(&path).expect("create");
    let mut model = BTreeMap::new();
    ordinary.put(b"a", b"base").expect("base");
    model.insert(b"a".to_vec(), b"base".to_vec());
    let digest = ordinary.checkpoint_to(&initial).expect("checkpoint");
    drop(ordinary);
    let mut store = CheckpointStore::open(&path, &initial, &digest).expect("open");
    let mut generation = 0_usize;
    store
        .rollover_managed(root.path().join("managed-0"))
        .expect("first managed selection");
    let selection = path.join("checkpoint.selection");
    for bytes in data.chunks_exact(3) {
        let key = vec![b'a' + bytes[1] % 8];
        match bytes[0] % 6 {
            0 => {
                let value = vec![bytes[2]; usize::from(bytes[2] % 5)];
                store.put(&key, &value).expect("put");
                model.insert(key, value);
            }
            1 => {
                store.delete(&key).expect("delete");
                model.remove(&key);
            }
            2 => {
                generation += 1;
                let before = model.clone();
                let snapshot = store.snapshot().expect("snapshot");
                store
                    .rollover_managed(root.path().join(format!("managed-{generation}")))
                    .expect("rollover");
                assert_eq!(
                    snapshot.scan(None, None, 100).expect("snapshot scan"),
                    before.into_iter().collect::<Vec<_>>()
                );
            }
            3 => {
                check(&store, &model);
                drop(store);
                store = CheckpointStore::open_managed(&path).expect("reopen");
            }
            4 => {
                check(&store, &model);
                drop(store);
                let original = std::fs::read(&selection).expect("selection");
                let mut corrupt = original.clone();
                let index = usize::from(bytes[1]) % corrupt.len();
                corrupt[index] ^= (bytes[2] | 1).max(1);
                std::fs::write(&selection, &corrupt).expect("corrupt selection");
                assert!(CheckpointStore::open_managed(&path).is_err());
                assert!(CheckpointStore::open(&path, &initial, &digest).is_err());
                std::fs::write(&selection, original).expect("restore selection");
                store = CheckpointStore::open_managed(&path).expect("reopen valid selection");
            }
            _ => {
                assert!(store.rollover_to(root.path().join("unmanaged")).is_err());
            }
        }
        check(&store, &model);
    }
    drop(store);
    let reopened = CheckpointStore::open_managed(&path).expect("final recovery");
    check(&reopened, &model);
});
