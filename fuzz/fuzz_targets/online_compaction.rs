#![no_main]
use database_engine::{
    BatchOperation, OnlineCompactionOptions, OnlineHandoffFailure, OnlineStore, Store,
};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

fn check(store: &Store, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let expected = model
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    assert_eq!(store.scan(None, None, 100).expect("scan"), expected);
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let source = root.path().join("live");
    if data.first().is_some_and(|byte| byte & 0x80 != 0) {
        std::fs::create_dir(&source).expect("V2 fixture directory");
        std::fs::write(
            source.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .expect("V2 log fixture");
        std::fs::write(
            source.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .expect("V2 witness fixture");
    }
    let mut store = OnlineStore::open(&source).expect("open");
    let mut model = BTreeMap::new();
    let mut old = Vec::new();
    for (iteration, chunk) in data.chunks_exact(3).enumerate() {
        let key = vec![b'a' + chunk[1] % 8];
        let value = vec![chunk[2]; usize::from(chunk[2] % 8)];
        match chunk[0] % 9 {
            0 => {
                store.with_store(|s| s.put(&key, &value)).expect("put");
                model.insert(key, value);
            }
            1 => {
                store.with_store(|s| s.delete(&key)).expect("delete");
                model.remove(&key);
            }
            2 => {
                let other = vec![b'a' + chunk[1].wrapping_add(1) % 8];
                store
                    .with_store(|s| {
                        s.write_batch(&[
                            BatchOperation::Delete { key: &other },
                            BatchOperation::Put {
                                key: &key,
                                value: &value,
                            },
                        ])
                    })
                    .expect("batch");
                model.remove(&other);
                model.insert(key, value);
            }
            3 => {
                let path = root.path().join(format!("previous-{}", old.len()));
                if chunk[2] & 1 != 0 {
                    store
                        .compact_and_replace_detailed(&path, &OnlineCompactionOptions::default())
                        .expect("detailed compact");
                } else {
                    store
                        .compact_and_replace(&path, &OnlineCompactionOptions::default())
                        .expect("compact");
                }
                check(&Store::open(&path).expect("old"), &model);
                old.push((path, model.clone()));
            }
            4 => {
                drop(store);
                store = OnlineStore::open(&source).expect("reopen");
            }
            5 => {
                store
                    .with_store(|s| {
                        let snapshot = s.snapshot(100, 1024)?;
                        for (k, v) in &model {
                            assert_eq!(snapshot.get(k)?, Some(v.clone()));
                        }
                        Ok(())
                    })
                    .expect("snapshot");
            }
            6 => {
                let path = root.path().join(format!("reclaimed-{iteration}"));
                store
                    .compact_and_reclaim(&path, &OnlineCompactionOptions::default())
                    .expect("compact and reclaim");
                assert!(!path.exists());
                database_engine::cleanup_abandoned_staging(root.path()).expect("cleanup");
            }
            7 => {
                let path = root.path().join(format!("blocked-{iteration}"));
                std::fs::write(&path, b"pre-existing destination").expect("sentinel");
                assert!(matches!(
                    store.compact_and_replace_detailed(&path, &OnlineCompactionOptions::default()),
                    Err(OnlineHandoffFailure::NotPublished(_))
                ));
                assert_eq!(
                    std::fs::read(&path).expect("sentinel after"),
                    b"pre-existing destination"
                );
                if chunk[1] & 1 != 0 {
                    drop(store);
                    store = OnlineStore::open(&source).expect("reopen after collision");
                }
            }
            _ => {
                let path = root.path().join(format!("invalid-{iteration}"));
                let options = OnlineCompactionOptions {
                    catchup_rounds: 17,
                    ..Default::default()
                };
                assert!(matches!(
                    store.compact_and_replace_detailed(&path, &options),
                    Err(OnlineHandoffFailure::NotPublished(_))
                ));
                assert!(!path.exists());
            }
        }
        store
            .with_store(|s| {
                check(s, &model);
                Ok(())
            })
            .expect("check");
    }
    drop(store);
    check(&Store::open(&source).expect("final open"), &model);
    for (path, expected) in old {
        check(&Store::open(path).expect("old reopen"), &expected);
    }
});
