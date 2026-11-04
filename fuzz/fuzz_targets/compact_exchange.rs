#![no_main]
use database_engine::Store;
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

fn check(store: &Store, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let expected = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(store.scan(None, None, 100).expect("scan"), expected);
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let source = root.path().join("live");
    let mut store = Store::open(&source).expect("open");
    let mut model = BTreeMap::new();
    let mut old = Vec::new();
    for chunk in data.chunks_exact(3) {
        let key = vec![b'a' + chunk[1] % 8];
        let value = vec![chunk[2]; usize::from(chunk[2] % 8)];
        match chunk[0] % 4 {
            0 => {
                store.put(&key, &value).expect("put");
                model.insert(key, value);
            }
            1 => {
                store.delete(&key).expect("delete");
                model.remove(&key);
            }
            2 => {
                let path = root.path().join(format!("previous-{}", old.len()));
                store = store
                    .compact_and_replace(&source, &path)
                    .expect("compact exchange");
                let previous = Store::open(&path).expect("open previous");
                check(&previous, &model);
                drop(previous);
                old.push((path, model.clone()));
            }
            _ => {
                drop(store);
                store = Store::open(&source).expect("reopen");
            }
        }
        check(&store, &model);
    }
    drop(store);
    check(&Store::open(&source).expect("final open"), &model);
    for (path, expected) in old {
        check(&Store::open(path).expect("open old"), &expected);
    }
});
