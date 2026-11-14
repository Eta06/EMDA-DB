use super::*;

#[test]
fn automatic_configuration_races_preserve_atomic_writes_and_recovery() {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Duration;

    for use_default_format in [false, true] {
        let (_dir, path, store) = if use_default_format {
            managed_default_fixture(1_048_576, 256, 65_536)
        } else {
            managed_fixture(1_048_576, 256, 65_536)
        };
        let store = Arc::new(store);
        let barrier = Arc::new(Barrier::new(6));
        let (ready_tx, ready_rx) = mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = mpsc::sync_channel(0);
        let mut first_writer_sync = Some((ready_tx, resume_rx));
        std::thread::scope(|scope| {
            for worker in 0..3 {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let sync = if worker == 0 {
                    first_writer_sync.take()
                } else {
                    None
                };
                scope.spawn(move || {
                    barrier.wait();
                    for sequence in 0..8 {
                        let first = format!("w{worker}-{sequence:02}-a");
                        let second = format!("w{worker}-{sequence:02}-b");
                        store
                            .write_batch_detailed(&[
                                BatchOperation::Put {
                                    key: first.as_bytes(),
                                    value: b"pair",
                                },
                                BatchOperation::Put {
                                    key: second.as_bytes(),
                                    value: b"pair",
                                },
                            ])
                            .unwrap();
                        if sequence == 0 {
                            if let Some((ready, resume)) = &sync {
                                ready.send(()).unwrap();
                                resume.recv_timeout(Duration::from_secs(10)).unwrap();
                            }
                        }
                        std::thread::yield_now();
                    }
                });
            }
            for worker in 0..2 {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for sequence in 0..16 {
                        if (worker + sequence) % 2 == 0 {
                            store.enable_automatic_checkpoints_detailed().unwrap();
                        } else {
                            store.disable_automatic_checkpoints_detailed().unwrap();
                        }
                        std::thread::yield_now();
                    }
                });
            }
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                let interim: BTreeMap<_, _> =
                    store.scan(None, None, 256).unwrap().into_iter().collect();
                assert_eq!(
                    interim.get(b"w0-00-a".as_slice()).map(Vec::as_slice),
                    Some(&b"pair"[..])
                );
                assert_eq!(
                    interim.get(b"w0-00-b".as_slice()).map(Vec::as_slice),
                    Some(&b"pair"[..])
                );
                assert!(!interim.contains_key(b"w0-01-a".as_slice()));
                assert!(!interim.contains_key(b"w0-01-b".as_slice()));
                resume_tx.send(()).unwrap();
                for _ in 0..24 {
                    let rows: BTreeMap<_, _> =
                        store.scan(None, None, 256).unwrap().into_iter().collect();
                    assert_eq!(
                        rows.get(b"base".as_slice()).map(Vec::as_slice),
                        Some(&b"acknowledged"[..])
                    );
                    for worker in 0..3 {
                        for sequence in 0..8 {
                            let first = format!("w{worker}-{sequence:02}-a");
                            let second = format!("w{worker}-{sequence:02}-b");
                            assert_eq!(rows.get(first.as_bytes()), rows.get(second.as_bytes()));
                            if let Some(value) = rows.get(first.as_bytes()) {
                                assert_eq!(value, b"pair");
                            }
                        }
                    }
                    std::thread::yield_now();
                }
            });
        });
        let before_reopen = store.scan(None, None, 256).unwrap();
        let enabled = store.automatic_checkpoints_enabled().unwrap();
        let mut expected = BTreeMap::from([(b"base".to_vec(), b"acknowledged".to_vec())]);
        for worker in 0..3 {
            for sequence in 0..8 {
                for suffix in ['a', 'b'] {
                    expected.insert(
                        format!("w{worker}-{sequence:02}-{suffix}").into_bytes(),
                        b"pair".to_vec(),
                    );
                }
            }
        }
        assert_eq!(before_reopen, expected.into_iter().collect::<Vec<_>>());
        drop(store);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(reopened.automatic_checkpoints_enabled().unwrap(), enabled);
        assert_eq!(reopened.scan(None, None, 256).unwrap(), before_reopen);
    }
}

#[test]
fn configuration_races_with_required_rollovers_preserve_acknowledged_batches() {
    use crate::MutationFailure;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Barrier};

    for use_default_format in [false, true] {
        let (_dir, path, store) = if use_default_format {
            managed_default_fixture(4096, 2, 64)
        } else {
            managed_fixture(4096, 2, 64)
        };
        store.enable_automatic_checkpoints_detailed().unwrap();
        let selection_before = fs::read(path.join("checkpoint.selection")).unwrap();
        let store = Arc::new(store);
        let barrier = Arc::new(Barrier::new(5));
        std::thread::scope(|scope| {
            for worker in 0..3 {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for sequence in 0..4 {
                        let first = format!("r{worker}-{sequence}-a");
                        let second = format!("r{worker}-{sequence}-b");
                        let operations = [
                            BatchOperation::Put {
                                key: first.as_bytes(),
                                value: b"pair",
                            },
                            BatchOperation::Put {
                                key: second.as_bytes(),
                                value: b"pair",
                            },
                        ];
                        let mut rejections = 0;
                        loop {
                            match store.write_batch_detailed(&operations) {
                                Ok(()) => break,
                                Err(MutationFailure::Rejected(error))
                                    if error.to_string().contains(
                                        "checkpoint suffix key limit would be exceeded",
                                    ) =>
                                {
                                    rejections += 1;
                                    assert!(rejections < 256, "configuration did not settle");
                                    store.enable_automatic_checkpoints_detailed().unwrap();
                                }
                                other => panic!("unexpected batch outcome: {other:?}"),
                            }
                            std::thread::yield_now();
                        }
                    }
                });
            }
            let toggle_store = Arc::clone(&store);
            let toggle_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                toggle_barrier.wait();
                for sequence in 0..16 {
                    if sequence % 2 == 0 {
                        toggle_store
                            .disable_automatic_checkpoints_detailed()
                            .unwrap();
                    } else {
                        toggle_store
                            .enable_automatic_checkpoints_detailed()
                            .unwrap();
                    }
                    std::thread::yield_now();
                }
            });
            let scan_store = Arc::clone(&store);
            let scan_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                scan_barrier.wait();
                for _ in 0..24 {
                    let rows: BTreeMap<_, _> = scan_store
                        .scan(None, None, 128)
                        .unwrap()
                        .into_iter()
                        .collect();
                    for worker in 0..3 {
                        for sequence in 0..4 {
                            let first = format!("r{worker}-{sequence}-a");
                            let second = format!("r{worker}-{sequence}-b");
                            assert_eq!(rows.get(first.as_bytes()), rows.get(second.as_bytes()));
                            if let Some(value) = rows.get(first.as_bytes()) {
                                assert_eq!(value, b"pair");
                            }
                        }
                    }
                    std::thread::yield_now();
                }
            });
        });
        let mut expected = BTreeMap::from([(b"base".to_vec(), b"acknowledged".to_vec())]);
        for worker in 0..3 {
            for sequence in 0..4 {
                for suffix in ['a', 'b'] {
                    expected.insert(
                        format!("r{worker}-{sequence}-{suffix}").into_bytes(),
                        b"pair".to_vec(),
                    );
                }
            }
        }
        let rows = store.scan(None, None, 128).unwrap();
        assert_eq!(rows, expected.into_iter().collect::<Vec<_>>());
        assert_ne!(
            fs::read(path.join("checkpoint.selection")).unwrap(),
            selection_before
        );
        let enabled = store.automatic_checkpoints_enabled().unwrap();
        drop(store);
        let reopened = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(&path, 4096, 2, 64, None)
        })
        .unwrap();
        assert_eq!(reopened.automatic_checkpoints_enabled().unwrap(), enabled);
        assert_eq!(reopened.scan(None, None, 128).unwrap(), rows);
    }
}

#[test]
fn failed_configuration_blocks_a_concurrent_waiting_mutation() {
    use crate::{MutationFailure, PublicationFailure};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    for use_default_format in [false, true] {
        for operation in ["initial-enable", "re-enable", "disable"] {
            for after_publication in [false, true] {
                let (_dir, path, store) = if use_default_format {
                    managed_default_fixture(4096, 8, 256)
                } else {
                    managed_fixture(4096, 8, 256)
                };
                if operation != "initial-enable" {
                    store.enable_automatic_checkpoints_detailed().unwrap();
                }
                if operation == "re-enable" {
                    store.disable_automatic_checkpoints_detailed().unwrap();
                }
                store.put(b"ack", b"preserved").unwrap();
                let log_before = fs::read(path.join("records.log")).unwrap();
                let store = Arc::new(store);
                let (ready_tx, ready_rx) = mpsc::sync_channel(0);
                let (resume_tx, resume_rx) = mpsc::sync_channel(0);
                let (started_tx, started_rx) = mpsc::sync_channel(0);
                std::thread::scope(|scope| {
                    let configuring = Arc::clone(&store);
                    let configuration = scope.spawn(move || {
                        crate::publication_outcome::track(|tracker| {
                            let before = || {
                                if !after_publication {
                                    ready_tx.send(()).unwrap();
                                    resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                                    Err(io::Error::other("injected before publication"))
                                } else {
                                    Ok(())
                                }
                            };
                            let sync = |parent: &std::path::Path| {
                                if after_publication {
                                    ready_tx.send(()).unwrap();
                                    resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                                    Err(io::Error::other("injected parent sync"))
                                } else {
                                    crate::directory::sync_existing(parent)
                                }
                            };
                            if operation == "disable" {
                                configuring.disable_automatic_checkpoints_tracked(
                                    Some(tracker),
                                    before,
                                    sync,
                                )
                            } else {
                                configuring.enable_automatic_checkpoints_tracked(
                                    Some(tracker),
                                    before,
                                    sync,
                                )
                            }
                        })
                    });
                    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    let writing = Arc::clone(&store);
                    let writer = scope.spawn(move || {
                        started_tx.send(()).unwrap();
                        writing.put_detailed(b"waiting", b"must-not-commit")
                    });
                    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    resume_tx.send(()).unwrap();
                    let result = configuration.join().unwrap();
                    if after_publication {
                        assert!(matches!(result, Err(PublicationFailure::OutcomeUnknown(_))));
                    } else {
                        assert!(matches!(result, Err(PublicationFailure::NotPublished(_))));
                    }
                    assert!(matches!(
                        writer.join().unwrap(),
                        Err(MutationFailure::Rejected(_))
                    ));
                });
                assert!(store.get(b"ack").is_err());
                assert_eq!(fs::read(path.join("records.log")).unwrap(), log_before);
                drop(store);
                let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
                let enabled_before = operation == "disable";
                let enabled_after = operation != "disable";
                assert_eq!(
                    reopened.automatic_checkpoints_enabled().unwrap(),
                    if after_publication {
                        enabled_after
                    } else {
                        enabled_before
                    }
                );
                assert_eq!(
                    reopened.get(b"ack").unwrap().as_deref(),
                    Some(&b"preserved"[..])
                );
                assert_eq!(reopened.get(b"waiting").unwrap(), None);
            }
        }
    }
}
