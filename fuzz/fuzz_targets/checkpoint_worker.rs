#![no_main]
use database_engine::{CheckpointStore, CheckpointWorker, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn request(worker: &CheckpointWorker) {
    let previous = worker.status().checks;
    worker.request_check().expect("request");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = worker.status();
        assert!(status.last_error.is_none() && !status.stopped, "{status:?}");
        if status.checks > previous {
            break;
        }
        assert!(Instant::now() < deadline, "worker did not complete a check");
        std::thread::yield_now();
    }
}

fn check(store: &CheckpointStore, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let expected = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(store.scan(None, None, 100).expect("scan"), expected);
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 64 {
        return;
    }
    let root = tempfile::tempdir().expect("temporary directory");
    let path = root.path().join("store");
    let checkpoint = root.path().join("base");
    let ordinary = Store::open(&path).expect("create");
    ordinary.put(b"z", b"base").expect("base");
    let digest = ordinary.checkpoint_to(&checkpoint).expect("checkpoint");
    drop(ordinary);
    let initial =
        CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 4096, 4, 64).expect("open");
    initial
        .rollover_managed(root.path().join("managed"))
        .expect("managed");
    initial.enable_automatic_checkpoints().expect("enable");
    let mut store = Arc::new(initial);
    let mut worker = store
        .start_checkpoint_worker(Duration::from_secs(3600), 1)
        .expect("worker");
    let mut enabled = true;
    let mut model = BTreeMap::new();
    model.insert(b"z".to_vec(), b"base".to_vec());
    for bytes in data.chunks_exact(3) {
        let key = vec![b'a' + bytes[1] % 6];
        let value = vec![bytes[2]; usize::from(bytes[2] % 5)];
        match bytes[0] % 7 {
            0 | 1 => {
                let before = std::fs::read(path.join("records.log")).expect("log");
                let result = if bytes[0] % 7 == 0 {
                    store.put(&key, &value)
                } else {
                    store.delete(&key)
                };
                if result.is_ok() {
                    if bytes[0] % 7 == 0 {
                        model.insert(key, value);
                    } else {
                        model.remove(&key);
                    }
                } else {
                    assert!(!enabled);
                    assert_eq!(
                        std::fs::read(path.join("records.log")).expect("log"),
                        before
                    );
                }
            }
            2 => request(&worker),
            3 => {
                if enabled {
                    store.disable_automatic_checkpoints().expect("disable");
                } else {
                    store.enable_automatic_checkpoints().expect("enable");
                }
                enabled = !enabled;
            }
            4 => {
                let snapshot = store.snapshot().expect("snapshot");
                let old = model.clone();
                request(&worker);
                assert_eq!(
                    snapshot.scan(None, None, 100).expect("snapshot scan"),
                    old.into_iter().collect::<Vec<_>>()
                );
            }
            5 => {
                worker.stop().expect("stop");
                drop(store);
                let reopened =
                    CheckpointStore::open_managed_with_resource_limits(&path, 4096, 4, 64, None)
                        .expect("reopen");
                store = Arc::new(reopened);
                if !enabled {
                    store
                        .enable_automatic_checkpoints()
                        .expect("enable for worker restart");
                    enabled = true;
                }
                worker = store
                    .start_checkpoint_worker(Duration::from_secs(3600), 1)
                    .expect("restart");
            }
            _ => {
                let before = std::fs::read(path.join("records.log")).expect("log");
                let result = store.transaction_with_read_set(|tx| {
                    let _ = tx.get(&key)?;
                    tx.put(&key, &value)
                });
                if result.is_ok() {
                    model.insert(key, value);
                } else {
                    assert!(!enabled);
                    assert_eq!(
                        std::fs::read(path.join("records.log")).expect("log"),
                        before
                    );
                }
            }
        }
        check(&store, &model);
    }
    let status = worker.stop().expect("stop");
    assert!(status.stopped && status.last_error.is_none());
    drop(store);
    let reopened = CheckpointStore::open_managed_with_resource_limits(&path, 4096, 4, 64, None)
        .expect("final reopen");
    check(&reopened, &model);
});
