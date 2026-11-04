use super::*;

#[test]
fn checkpoint_view_limit_rejects_before_adopting_a_complete_tail() {
    let (_dir, path, checkpoint, digest) = fixture();
    let tail = record::encode(Operation::Put, b"tail", b"unacknowledged").unwrap();
    OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(&tail)
        .unwrap();
    let log_before = fs::read(path.join("records.log")).unwrap();
    let witness_before = fs::read(path.join("commit.state")).unwrap();
    let limits = CheckpointStoreLimits {
        checkpoint_view: crate::CheckpointViewLimits {
            max_file_bytes: fs::metadata(&checkpoint).unwrap().len() - 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let error =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .err()
            .expect("checkpoint admission must reject");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(fs::read(path.join("records.log")).unwrap(), log_before);
    assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness_before);
    let reopened = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        reopened.get(b"tail").unwrap().as_deref(),
        Some(&b"unacknowledged"[..])
    );
}

#[test]
fn complete_unacknowledged_tail_is_validated_synchronized_and_adopted() {
    let (dir, path, checkpoint, digest) = fixture();
    let witness_before = fs::read(path.join("commit.state")).unwrap();
    let record = record::encode(Operation::Put, b"tail", b"unacknowledged").unwrap();
    OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(&record)
        .unwrap();
    assert_eq!(
        retry_lock(|| crate::RecoveredCheckpointView::open(&path, &checkpoint, &digest))
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        store.get(b"tail").unwrap().as_deref(),
        Some(&b"unacknowledged"[..])
    );
    assert_ne!(fs::read(path.join("commit.state")).unwrap(), witness_before);
    let next = dir.path().join("after-recovery");
    let next_digest = store.rollover_to(&next).unwrap();
    assert_eq!(store.tail_bytes().unwrap(), 0);
    store.put(b"tail", b"acknowledged").unwrap();
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open(&path, &next, &next_digest)).unwrap();
    assert_eq!(
        reopened.get(b"tail").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    drop(reopened);
    assert_eq!(
        retry_lock(|| Store::open(&path))
            .unwrap()
            .get(b"tail")
            .unwrap()
            .as_deref(),
        Some(&b"acknowledged"[..])
    );
}

#[test]
fn complete_unacknowledged_batch_replays_after_witnessed_suffix() {
    let (_dir, path, checkpoint, digest) = fixture();
    let ordinary = retry_lock(|| Store::open(&path)).unwrap();
    ordinary.put(b"witnessed", b"old").unwrap();
    drop(ordinary);
    let payload = encode_payload(&[
        BatchOperation::Put {
            key: b"witnessed",
            value: b"new",
        },
        BatchOperation::Delete { key: b"base" },
        BatchOperation::Put {
            key: b"inserted",
            value: b"value",
        },
    ])
    .unwrap();
    let record = record::encode(Operation::Batch, &[0], &payload).unwrap();
    OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(&record)
        .unwrap();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        store.get(b"witnessed").unwrap().as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(store.get(b"base").unwrap(), None);
    assert_eq!(
        store.get(b"inserted").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    drop(store);
    let ordinary = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        ordinary.get(b"witnessed").unwrap().as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(ordinary.get(b"base").unwrap(), None);
    assert_eq!(
        ordinary.get(b"inserted").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn incomplete_single_and_batch_tails_fail_without_modifying_log_or_witness() {
    let payload = encode_payload(&[
        BatchOperation::Put {
            key: b"left",
            value: b"one",
        },
        BatchOperation::Delete { key: b"base" },
    ])
    .unwrap();
    let records = [
        record::encode(Operation::Put, b"tail", b"value").unwrap(),
        record::encode(Operation::Batch, &[0], &payload).unwrap(),
    ];
    for bytes in records {
        for offset in 1..bytes.len() {
            let (_dir, path, checkpoint, digest) = fixture();
            let witness = fs::read(path.join("commit.state")).unwrap();
            let log = path.join("records.log");
            OpenOptions::new()
                .append(true)
                .open(&log)
                .unwrap()
                .write_all(&bytes[..offset])
                .unwrap();
            let before = fs::read(&log).unwrap();
            let error = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest))
                .err()
                .unwrap();
            assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
            assert_eq!(fs::read(&log).unwrap(), before);
            assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness);
        }
    }
}

#[test]
fn unacknowledged_tail_limits_and_non_boundary_witness_fail_without_adoption() {
    for cap in ["bytes", "keys", "key-bytes", "boundary"] {
        let (_dir, path, checkpoint, digest) = fixture();
        let bytes = record::encode(Operation::Put, b"tail", b"value").unwrap();
        let log = path.join("records.log");
        let before_len = fs::metadata(&log).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        if cap == "boundary" {
            let header = fs::read(&log).unwrap();
            let id = witness::parse_header(header[..witness::LOG_HEADER_SIZE].try_into().unwrap())
                .unwrap();
            fs::write(
                path.join("commit.state"),
                witness::encode_for_test(&id, before_len + 1),
            )
            .unwrap();
        }
        let before_log = fs::read(&log).unwrap();
        let before_witness = fs::read(path.join("commit.state")).unwrap();
        let (max_tail, max_keys, max_key_bytes) = match cap {
            "bytes" => (bytes.len() as u64 - 1, 5, 100),
            "keys" => (1000, 0, 100),
            "key-bytes" => (1000, 5, 3),
            _ => (1000, 5, 100),
        };
        assert!(
            retry_lock(|| CheckpointStore::open_with_limits(
                &path,
                &checkpoint,
                &digest,
                max_tail,
                max_keys,
                max_key_bytes
            ))
            .is_err()
        );
        assert_eq!(fs::read(&log).unwrap(), before_log);
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), before_witness);
    }
}

#[test]
fn whole_log_limit_rejects_unacknowledged_tail_before_adoption() {
    let (_dir, path, checkpoint, digest) = fixture();
    let record = record::encode(Operation::Put, b"tail", b"value").unwrap();
    let log = path.join("records.log");
    OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(&record)
        .unwrap();
    let before = fs::read(&log).unwrap();
    let witness_before = fs::read(path.join("commit.state")).unwrap();
    let too_small = before.len() as u64 - 1;
    let result = retry_lock(|| {
        CheckpointStore::open_with_resource_limits(
            &path,
            &checkpoint,
            &digest,
            MAX_RECOVERED_VIEW_TAIL_BYTES,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            Some(too_small),
        )
    });
    assert_eq!(result.err().unwrap().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(fs::read(&log).unwrap(), before);
    assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness_before);
    let file = OpenOptions::new().write(true).open(&log).unwrap();
    let offset = before.len() as u64 - 1;
    FileExt::write_all_at(&file, &[before[offset as usize] ^ 0x40], offset).unwrap();
    let error = retry_lock(|| {
        CheckpointStore::open_with_resource_limits(
            &path,
            &checkpoint,
            &digest,
            MAX_RECOVERED_VIEW_TAIL_BYTES,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            Some(too_small),
        )
    })
    .err()
    .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    FileExt::write_all_at(&file, &before[offset as usize..=offset as usize], offset).unwrap();
    let recovered = retry_lock(|| {
        CheckpointStore::open_with_resource_limits(
            &path,
            &checkpoint,
            &digest,
            MAX_RECOVERED_VIEW_TAIL_BYTES,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            Some(before.len() as u64),
        )
    })
    .unwrap();
    assert_eq!(
        recovered.get(b"tail").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn unacknowledged_tail_log_sync_failures_return_error_and_retry_safely() {
    for fault in [LogFault::SyncBefore, LogFault::SyncAfter] {
        let (_dir, path, checkpoint, digest) = fixture();
        let record = record::encode(Operation::Put, b"tail", b"value").unwrap();
        OpenOptions::new()
            .append(true)
            .open(path.join("records.log"))
            .unwrap()
            .write_all(&record)
            .unwrap();
        let log_before = fs::read(path.join("records.log")).unwrap();
        let witness_before = fs::read(path.join("commit.state")).unwrap();
        let result = retry_lock(|| {
            CheckpointStore::open_using(
                &path,
                &checkpoint,
                &digest,
                OpenLimits {
                    max_tail_bytes: MAX_RECOVERED_VIEW_TAIL_BYTES,
                    max_tail_keys: MAX_RECOVERED_VIEW_TAIL_KEYS,
                    max_tail_key_bytes: MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                    max_log_bytes: None,
                    ..Default::default()
                },
                |path, boundary, length| {
                    let mut log = Log::open_checkpoint_writer(path, boundary, length)?;
                    let file = OpenOptions::new()
                        .read(true)
                        .append(true)
                        .open(path.join("records.log"))?;
                    log.replace_io_for_test(Box::new(FaultLog { file, fault }));
                    Ok(log)
                },
            )
        });
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("outcome is uncertain")
        );
        assert_eq!(fs::read(path.join("records.log")).unwrap(), log_before);
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness_before);
        let recovered = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        assert_eq!(
            recovered.get(b"tail").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}

#[test]
fn unacknowledged_tail_witness_failures_never_return_a_store() {
    let faults = (0..=witness::WITNESS_SIZE)
        .map(WitnessFault::Partial)
        .chain([
            WitnessFault::Panic(3),
            WitnessFault::Panic(witness::WITNESS_SIZE),
            WitnessFault::SyncBefore,
            WitnessFault::SyncAfter,
        ]);
    for fault in faults {
        let (_dir, path, checkpoint, digest) = fixture();
        let log_path = path.join("records.log");
        let header = fs::read(&log_path).unwrap();
        let id =
            witness::parse_header(header[..witness::LOG_HEADER_SIZE].try_into().unwrap()).unwrap();
        let record = record::encode(Operation::Put, b"tail", b"value").unwrap();
        OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(&record)
            .unwrap();
        let log_before = fs::read(&log_path).unwrap();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            retry_lock(|| {
                CheckpointStore::open_using(
                    &path,
                    &checkpoint,
                    &digest,
                    OpenLimits {
                        max_tail_bytes: MAX_RECOVERED_VIEW_TAIL_BYTES,
                        max_tail_keys: MAX_RECOVERED_VIEW_TAIL_KEYS,
                        max_tail_key_bytes: MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                        max_log_bytes: None,
                        ..Default::default()
                    },
                    |path, boundary, length| {
                        let mut log = Log::open_checkpoint_writer(path, boundary, length)?;
                        let file = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(path.join("commit.state"))?;
                        log.attach_test_witness(Witness::for_test(
                            Box::new(FaultWitness { file, fault }),
                            id,
                            boundary,
                        ));
                        Ok(log)
                    },
                )
            })
        }));
        assert!(outcome.is_err() || outcome.unwrap().is_err());
        assert_eq!(fs::read(&log_path).unwrap(), log_before);
        match retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)) {
            Ok(recovered) => assert_eq!(
                recovered.get(b"tail").unwrap().as_deref(),
                Some(&b"value"[..])
            ),
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )),
        }
    }
}
