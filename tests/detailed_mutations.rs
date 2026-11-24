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
fn streamed_conditions_verify_large_values_even_after_a_mismatch() {
    use std::fs::{self, OpenOptions};
    use std::io::ErrorKind;
    use std::os::unix::fs::FileExt;

    fn checksum_rejection<T: std::fmt::Debug>(result: Result<T, MutationFailure>) {
        assert!(matches!(
            result.unwrap_err(),
            MutationFailure::Rejected(error)
                if error.kind() == ErrorKind::InvalidData
                    && error.to_string() == "stored value checksum mismatch"
        ));
    }

    macro_rules! exercise_large {
        ($store:expr, $key:expr, $value:expr, $log:expr) => {{
            let store = &$store;
            let key = $key;
            let value = $value;
            let log = $log;
            let raw = fs::read(log).unwrap();
            let start = raw.windows(32).position(|part| part == &value[..32]).unwrap();
            assert_eq!(&raw[start..start + value.len()], value);
            let mut incorrect = value.to_vec();
            incorrect[0] ^= 1;
            for expected in [None, Some(b"short".as_slice()), Some(incorrect.as_slice())] {
                assert!(!store.compare_exchange_detailed(key, expected, Some(b"unexpected")).unwrap());
            }
            assert!(matches!(
                store.increment_i64_detailed(key, 1),
                Err(MutationFailure::Rejected(error))
                    if error.to_string() == "increment requires an eight-byte signed integer"
            ));
            assert_eq!(fs::read(log).unwrap(), raw);

            let file = OpenOptions::new().write(true).open(log).unwrap();
            let last_byte = (start + value.len() - 1) as u64;
            file.write_all_at(&[value[value.len() - 1] ^ 1], last_byte).unwrap();
            let damaged = fs::read(log).unwrap();
            for expected in [None, Some(b"short".as_slice()), Some(incorrect.as_slice())] {
                checksum_rejection(store.compare_exchange_detailed(key, expected, Some(b"unexpected")));
            }
            checksum_rejection(store.increment_i64_detailed(key, 1));
            assert_eq!(fs::read(log).unwrap(), damaged);

            file.write_all_at(&value[value.len() - 1..], last_byte).unwrap();
            assert!(store.compare_exchange_detailed(key, Some(value), Some(b"verified")).unwrap());
            assert_eq!(store.get(key).unwrap().unwrap(), b"verified");
        }};
    }

    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let log = source.join("records.log");
    let store = Store::open(&source).unwrap();
    let base = vec![0xa1; 128 * 1024 + 1];
    let suffix = vec![0xb2; 128 * 1024 + 1];
    store.put(b"ordinary", &base).unwrap();
    exercise_large!(store, b"ordinary", &base, &log);
    let base = vec![0xc3; 128 * 1024 + 1];
    store.put(b"base", &base).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    store.put(b"suffix", &suffix).unwrap();
    drop(store);

    let store = CheckpointStore::open(&source, &checkpoint, &digest).unwrap();
    let mut incorrect = suffix.clone();
    *incorrect.last_mut().unwrap() ^= 1;
    let operations = [BatchOperation::Put {
        key: b"marked",
        value: b"yes",
    }];
    assert!(
        !store
            .compare_exchange_batch_detailed(&[(b"suffix", Some(&incorrect))], &operations)
            .unwrap()
    );
    assert_eq!(store.get(b"marked").unwrap(), None);
    assert!(
        store
            .compare_exchange_batch_detailed(
                &[(b"base", Some(&base)), (b"suffix", Some(&suffix))],
                &operations
            )
            .unwrap()
    );
    assert_eq!(store.get(b"marked").unwrap().unwrap(), b"yes");
    exercise_large!(store, b"base", &base, &log);
    exercise_large!(store, b"suffix", &suffix, &log);
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
fn ordinary_group_admission_rejection_is_confirmed_before_submission() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.put(b"counter", &0_i64.to_le_bytes()).unwrap();
    let before = std::fs::read(root.path().join("records.log")).unwrap();
    store.set_group_commit_limits(0, 0).unwrap();
    assert!(matches!(
        store.put_detailed(b"blocked", b"no"),
        Err(MutationFailure::Rejected(_))
    ));
    assert!(matches!(
        store.delete_detailed(b"counter"),
        Err(MutationFailure::Rejected(_))
    ));
    assert!(matches!(
        store.compare_exchange_detailed(b"counter", Some(&0_i64.to_le_bytes()), Some(b"new")),
        Err(MutationFailure::Rejected(_))
    ));
    assert!(matches!(
        store.increment_i64_detailed(b"counter", 1),
        Err(MutationFailure::Rejected(_))
    ));
    assert!(matches!(
        store.write_batch_detailed(&[BatchOperation::Put {
            key: b"batch",
            value: b"no"
        }]),
        Err(MutationFailure::Rejected(_))
    ));
    assert_eq!(store.get(b"blocked").unwrap(), None);
    assert_eq!(
        store.get(b"counter").unwrap(),
        Some(0_i64.to_le_bytes().to_vec())
    );
    assert_eq!(
        std::fs::read(root.path().join("records.log")).unwrap(),
        before
    );
}

#[test]
fn group_admission_rejections_release_log_quota_reservations() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open_with_log_limit(root.path(), 4096).unwrap();
    let before = std::fs::read(root.path().join("records.log")).unwrap();
    store.set_group_commit_limits(0, 0).unwrap();
    for _ in 0..128 {
        match store.put_detailed(b"reserved", b"value") {
            Err(MutationFailure::Rejected(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock)
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
    assert_eq!(
        std::fs::read(root.path().join("records.log")).unwrap(),
        before
    );
    store.set_group_commit_limits(1, 1).unwrap();
    assert!(
        matches!(store.put_detailed(b"reserved", b"value"), Err(MutationFailure::Rejected(ref error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    store
        .set_group_commit_limits(
            database_engine::MAX_GROUP_COMMIT_BUFFERED_BYTES,
            database_engine::MAX_GROUP_COMMIT_BUFFERED_RECORDS,
        )
        .unwrap();
    store.put_detailed(b"reserved", b"value").unwrap();
    assert_eq!(
        store.get(b"reserved").unwrap().as_deref(),
        Some(&b"value"[..])
    );
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
