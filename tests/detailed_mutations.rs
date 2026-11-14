use database_engine::{
    BatchOperation, CheckpointStore, MAX_ACTIVE_DIRECT_WRITES, MAX_RECOVERED_VIEW_TAIL_BYTES,
    MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS, MutationFailure, Store,
};

macro_rules! exercise {
    ($store:expr) => {{
        let store = &$store;
        store.put_detailed(b"a", b"old").unwrap();
        assert!(
            !store
                .compare_exchange_detailed(b"a", Some(b"missing"), Some(b"wrong"))
                .unwrap()
        );
        assert!(
            store
                .compare_exchange_detailed(b"a", Some(b"old"), Some(b"new"))
                .unwrap()
        );
        store
            .put_detailed(b"counter", &0_i64.to_le_bytes())
            .unwrap();
        assert_eq!(store.increment_i64_detailed(b"counter", 2).unwrap(), 2);
        assert_eq!(store.increment_i64_detailed(b"counter", -1).unwrap(), 1);
        store
            .write_batch_detailed(&[
                BatchOperation::Put {
                    key: b"b",
                    value: b"yes",
                },
                BatchOperation::Put {
                    key: b"c",
                    value: b"yes",
                },
            ])
            .unwrap();
        store.delete_detailed(b"a").unwrap();
        assert_eq!(store.get(b"a").unwrap(), None);
        assert_eq!(store.get(b"counter").unwrap().unwrap(), 1_i64.to_le_bytes());

        assert!(matches!(
            store.put_detailed(b"", b"value"),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.delete_detailed(b""),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.compare_exchange_detailed(b"", None, None),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.increment_i64_detailed(b"b", 1),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.write_batch_detailed(&[BatchOperation::Put {
                key: b"",
                value: b"no"
            }]),
            Err(MutationFailure::Rejected(_))
        ));
        store.set_active_direct_write_limit(0).unwrap();
        assert!(matches!(
            store.put_detailed(b"blocked", b"no"),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.delete_detailed(b"b"),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.compare_exchange_detailed(b"b", Some(b"yes"), None),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.increment_i64_detailed(b"counter", 1),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.write_batch_detailed(&[BatchOperation::Delete { key: b"b" }]),
            Err(MutationFailure::Rejected(_))
        ));
        store
            .set_active_direct_write_limit(MAX_ACTIVE_DIRECT_WRITES)
            .unwrap();
        assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"yes"[..]));
        assert_eq!(store.get(b"blocked").unwrap(), None);
    }};
}

#[test]
fn direct_outcomes_preserve_success_mismatch_and_pre_submission_rejection() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    exercise!(store);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    exercise!(store);
    assert!(
        !store
            .compare_exchange_batch_detailed(
                &[(b"b", Some(b"wrong"))],
                &[BatchOperation::Delete { key: b"b" }]
            )
            .unwrap()
    );
    assert!(
        store
            .compare_exchange_batch_detailed(
                &[(b"b", Some(b"yes"))],
                &[BatchOperation::Put {
                    key: b"batch",
                    value: b"success"
                }]
            )
            .unwrap()
    );
    assert!(matches!(
        store.compare_exchange_batch_detailed(&[(b"", None)], &[]),
        Err(MutationFailure::Rejected(_))
    ));
    drop(store);
    let recovered = Store::open(&path).unwrap();
    assert_eq!(recovered.get(b"b").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(
        recovered.get(b"batch").unwrap().as_deref(),
        Some(&b"success"[..])
    );
}

macro_rules! quota_rejections {
    ($store:expr) => {{
        let store = &$store;
        assert!(matches!(
            store.put_detailed(b"new", b"no"),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.delete_detailed(b"safe"),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.compare_exchange_detailed(b"safe", Some(b"yes"), Some(b"no")),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(
            !store
                .compare_exchange_detailed(b"safe", Some(b"other"), Some(b"no"))
                .unwrap()
        );
        assert!(matches!(
            store.increment_i64_detailed(b"counter", 1),
            Err(MutationFailure::Rejected(_))
        ));
        assert!(matches!(
            store.write_batch_detailed(&[BatchOperation::Put {
                key: b"new",
                value: b"no"
            }]),
            Err(MutationFailure::Rejected(_))
        ));
        assert_eq!(store.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
        assert_eq!(store.get(b"new").unwrap(), None);
    }};
}

#[test]
fn log_quota_rejections_do_not_submit_direct_mutations() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"yes").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let original = std::fs::read(path.join("records.log")).unwrap();
    let cap = original.len() as u64;
    let store = Store::open_with_log_limit(&path, cap).unwrap();
    quota_rejections!(store);
    drop(store);
    let store = CheckpointStore::open_with_resource_limits(
        &path,
        &checkpoint,
        &digest,
        MAX_RECOVERED_VIEW_TAIL_BYTES,
        MAX_RECOVERED_VIEW_TAIL_KEYS,
        MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        Some(cap),
    )
    .unwrap();
    quota_rejections!(store);
    assert!(matches!(
        store.compare_exchange_batch_detailed(
            &[(b"safe", Some(b"yes"))],
            &[BatchOperation::Delete { key: b"safe" }]
        ),
        Err(MutationFailure::Rejected(_))
    ));
    drop(store);
    assert_eq!(std::fs::read(path.join("records.log")).unwrap(), original);
}

#[test]
fn ordinary_coordinator_rejection_is_conservatively_unknown() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.set_group_commit_limits(0, 0).unwrap();
    assert!(matches!(
        store.put_detailed(b"blocked", b"no"),
        Err(MutationFailure::OutcomeUnknown(_))
    ));
    assert_eq!(store.get(b"blocked").unwrap(), None);
}

macro_rules! race_for_one_key {
    ($store:expr) => {{
        let store = &$store;
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let results = std::thread::scope(|scope| {
            let mut releases = Vec::new();
            let workers = [b"first".as_slice(), b"second".as_slice()].map(|key| {
                let ready = ready_tx.clone();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                releases.push(release_tx);
                scope.spawn(move || {
                    ready.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .unwrap();
                    store.put_detailed(key, b"winner")
                })
            });
            let ready = [
                ready_rx.recv_timeout(std::time::Duration::from_secs(10)),
                ready_rx.recv_timeout(std::time::Duration::from_secs(10)),
            ];
            for release in releases {
                let _ = release.send(());
            }
            let results = workers.map(|worker| worker.join().unwrap());
            assert!(ready.into_iter().all(|item| item.is_ok()));
            results
        });
        assert_eq!(results.iter().filter(|item| item.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|item| matches!(item, Err(MutationFailure::Rejected(_))))
                .count(),
            1
        );
        assert_eq!(
            [b"first".as_slice(), b"second".as_slice()]
                .iter()
                .filter(|key| store.get(key).unwrap().is_some())
                .count(),
            1
        );
    }};
}

#[test]
fn concurrent_direct_writes_cannot_both_pass_a_one_key_quota() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("ordinary");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"yes").unwrap();
    drop(store);
    let store = Store::open_with_limits(&path, None, Some(2)).unwrap();
    race_for_one_key!(store);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        [b"first".as_slice(), b"second".as_slice()]
            .iter()
            .filter(|key| reopened.get(key).unwrap().is_some())
            .count(),
        1
    );

    let path = root.path().join("checkpoint-store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"yes").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store =
        CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 1024, 1, 64).unwrap();
    race_for_one_key!(store);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        [b"first".as_slice(), b"second".as_slice()]
            .iter()
            .filter(|key| reopened.get(key).unwrap().is_some())
            .count(),
        1
    );
}
