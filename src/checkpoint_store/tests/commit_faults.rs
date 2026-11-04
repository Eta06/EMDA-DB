use super::*;

#[test]
fn every_partial_append_and_sync_failure_poison_until_reopen() {
    let length = record::encode(Operation::Put, b"k", b"new").unwrap().len();
    let faults = (0..=length).map(LogFault::Partial).chain([
        LogFault::Panic(3),
        LogFault::Panic(length),
        LogFault::SyncBefore,
        LogFault::SyncAfter,
    ]);
    for fault in faults {
        let (_dir, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.put(b"k", b"new")));
        assert!(outcome.is_err() || outcome.unwrap().is_err());
        failed_instance_rejects_all_operations(&store, &path);
        drop(store);
        let complete = match fault {
            LogFault::Partial(n) | LogFault::Panic(n) => n == 0 || n >= length,
            LogFault::SyncBefore | LogFault::SyncAfter => true,
        };
        let reopened = retry_lock(|| Store::open(&path));
        if complete {
            let reopened = reopened.unwrap();
            assert_eq!(
                reopened.get(b"base").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            let expected = match fault {
                LogFault::Partial(0) => None,
                _ => Some(&b"new"[..]),
            };
            assert_eq!(reopened.get(b"k").unwrap().as_deref(), expected);
        } else {
            assert!(matches!(
                reopened.err().unwrap().kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
        }
    }
}

#[test]
fn every_partial_witness_write_and_sync_failure_poison_until_reopen() {
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
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let header = fs::read(path.join("records.log")).unwrap();
        let id =
            witness::parse_header(header[..witness::LOG_HEADER_SIZE].try_into().unwrap()).unwrap();
        let boundary = header.len() as u64;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("commit.state"))
            .unwrap();
        store.log.attach_test_witness(Witness::for_test(
            Box::new(FaultWitness { file, fault }),
            id,
            boundary,
        ));
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.put(b"k", b"new")));
        assert!(outcome.is_err() || outcome.unwrap().is_err());
        failed_instance_rejects_all_operations(&store, &path);
        drop(store);
        match retry_lock(|| Store::open(&path)) {
            Ok(reopened) => {
                assert_eq!(
                    reopened.get(b"base").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                assert_eq!(reopened.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
            }
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )),
        }
    }
}

#[test]
fn interrupted_batch_is_never_partially_recovered() {
    let operations = [
        BatchOperation::Put {
            key: b"left",
            value: b"one",
        },
        BatchOperation::Put {
            key: b"right",
            value: b"two",
        },
    ];
    let payload = encode_payload(&operations).unwrap();
    let length = record::encode(Operation::Batch, &[0], &payload)
        .unwrap()
        .len();
    for offset in 0..=length {
        let (_dir, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store.log.replace_io_for_test(Box::new(FaultLog {
            file,
            fault: LogFault::Partial(offset),
        }));
        assert!(store.write_batch(&operations).is_err());
        failed_instance_rejects_all_operations(&store, &path);
        drop(store);
        let reopened = retry_lock(|| Store::open(&path));
        if offset == 0 || offset == length {
            let reopened = reopened.unwrap();
            let values = reopened.get_many(&[b"left", b"right"]).unwrap();
            if offset == 0 {
                assert_eq!(values, vec![None, None]);
            } else {
                assert_eq!(values, vec![Some(b"one".to_vec()), Some(b"two".to_vec())]);
            }
            assert_eq!(
                reopened.get(b"base").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
        } else {
            assert!(matches!(
                reopened.err().unwrap().kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
        }
    }
}
