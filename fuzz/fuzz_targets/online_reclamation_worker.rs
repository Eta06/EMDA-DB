#![no_main]
use database_engine::{
    BatchOperation, OnlineCompactionOptions, OnlineReclamationWorker, OnlineStore, Store,
};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn request(worker: &OnlineReclamationWorker) {
    let previous = worker.status().checks;
    worker.request_check().expect("request");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = worker.status();
        assert!(status.last_error.is_none() && !status.stopped, "{status:?}");
        if status.checks > previous {
            return;
        }
        assert!(Instant::now() < deadline, "worker did not complete a check");
        std::thread::yield_now();
    }
}

fn check(store: &Store, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let expected = model
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    assert_eq!(store.scan(None, None, 100).expect("scan"), expected);
}

fn start(
    store: &Arc<OnlineStore>,
    previous: &std::path::Path,
    growth: u64,
    cleanup: bool,
) -> OnlineReclamationWorker {
    let worker = if cleanup {
        store.start_reclamation_worker_with_retired_cleanup(
            previous,
            Duration::from_secs(3600),
            1,
            growth,
            OnlineCompactionOptions::default(),
        )
    } else {
        store.start_reclamation_worker(
            previous,
            Duration::from_secs(3600),
            1,
            growth,
            OnlineCompactionOptions::default(),
        )
    }
    .expect("worker");
    assert_eq!(worker.retired_cleanup_status().is_some(), cleanup);
    worker
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 64 {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let source = root.path().join("live");
    let previous = root.path().join("previous");
    let mut store = Arc::new(OnlineStore::open(&source).expect("open"));
    let growth = u64::from(data.first().copied().unwrap_or(0)) + 1;
    let cleanup = data.first().is_some_and(|byte| byte & 1 != 0);
    let mut worker = start(&store, &previous, growth, cleanup);
    let mut model = BTreeMap::new();
    let mut old = Vec::new();
    for chunk in data.chunks_exact(3) {
        let key = vec![b'a' + chunk[1] % 6];
        let value = vec![chunk[2]; usize::from(chunk[2] % 8)];
        match chunk[0] % 7 {
            0 => {
                store.with_store(|s| s.put(&key, &value)).expect("put");
                model.insert(key, value);
            }
            1 => {
                store.with_store(|s| s.delete(&key)).expect("delete");
                model.remove(&key);
            }
            2 => {
                let other = vec![b'a' + chunk[1].wrapping_add(1) % 6];
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
            3 | 4 => {
                request(&worker);
                assert!(!previous.exists());
                if cleanup {
                    let status = worker.retired_cleanup_status().expect("cleanup status");
                    assert!(status.checks > 0 && status.skipped == 0 && status.deferred == 0);
                    assert_eq!(
                        store
                            .cleanup_retired_staging(&Default::default())
                            .expect("empty cleanup")
                            .skipped,
                        0
                    );
                }
            }
            5 => {
                let status = worker.stop().expect("stop");
                assert!(status.stopped && status.last_error.is_none());
                drop(store);
                store = Arc::new(OnlineStore::open(&source).expect("reopen"));
                worker = start(&store, &previous, growth, cleanup);
            }
            _ => {
                let path = root.path().join(format!("retained-{}", old.len()));
                store
                    .compact_and_replace(&path, &Default::default())
                    .expect("manual compaction");
                check(&Store::open(&path).expect("retained"), &model);
                old.push((path, model.clone()));
            }
        }
        store
            .with_store(|s| {
                check(s, &model);
                Ok(())
            })
            .expect("check");
    }
    let status = worker.stop().expect("stop");
    assert!(status.stopped && status.last_error.is_none());
    drop(store);
    check(&Store::open(&source).expect("final reopen"), &model);
    for (path, expected) in old {
        check(&Store::open(path).expect("old reopen"), &expected);
    }
});
