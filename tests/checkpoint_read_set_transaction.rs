use database_engine::{
    BatchOperation, CheckpointStore, CheckpointTransactionMode, MAX_TRANSACTION_READ_CONSTRAINTS,
    Store,
};
use std::io;
use std::time::{Duration, Instant};

fn fixture() -> (tempfile::TempDir, CheckpointStore) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"a", b"old").unwrap();
    ordinary.put(b"c", b"base").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    (dir, store)
}

#[test]
fn unrelated_writes_can_commit_but_observed_present_versions_conflict() {
    let (_dir, store) = fixture();
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
            store.put(b"unrelated", b"changed")?;
            tx.put(b"a", b"new")
        })
        .unwrap();
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
    let result = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"new"[..]));
        store.put(b"a", b"other")?;
        store.put(b"a", b"new")?;
        tx.put(b"must-not-commit", b"x")
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(store.get(b"must-not-commit").unwrap(), None);
}

#[test]
fn a_concurrent_atomic_update_cannot_expose_a_fractured_pair() {
    let (_dir, store) = fixture();
    let result = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
        store.write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"new-a",
            },
            BatchOperation::Put {
                key: b"c",
                value: b"new-c",
            },
        ])?;
        assert_eq!(tx.get(b"c").unwrap_err().kind(), io::ErrorKind::WouldBlock);
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn observed_conflicts_stay_conflicts_even_if_callback_catches_them() {
    let (_dir, store) = fixture();
    let result = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.get(b"missing")?, None);
        store.put(b"missing", b"temporary")?;
        assert_eq!(
            tx.get(b"missing").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        store.delete(b"missing")?;
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    // If the temporary value was never observed, absence can match again.
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"missing")?, None);
            store.put(b"missing", b"temporary")?;
            store.delete(b"missing")?;
            tx.put(b"after-absence", b"ok")
        })
        .unwrap();
    assert_eq!(
        store.get(b"after-absence").unwrap().as_deref(),
        Some(&b"ok"[..])
    );
}

#[test]
fn scans_become_conservative_and_staged_reads_do_not_add_dependencies() {
    let (_dir, store) = fixture();
    store
        .transaction_with_read_set(|tx| {
            tx.put(b"a", b"staged")?;
            store.put(b"a", b"independent")?;
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"staged"[..]));
            Ok(())
        })
        .unwrap();
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"staged"[..]));
    let result = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.scan(Some(b"a"), Some(b"d"), 10)?.len(), 1);
        store.put(b"outside-range", b"changed")?;
        assert_eq!(tx.get(b"a").unwrap_err().kind(), io::ErrorKind::WouldBlock);
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    // An unrelated write before the first scan is permitted when earlier points still match.
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"staged"[..]));
            store.put(b"other", b"changed")?;
            assert_eq!(tx.scan(Some(b"a"), Some(b"d"), 10)?.len(), 1);
            tx.put(b"done", b"ok")
        })
        .unwrap();
}

#[test]
fn point_dependency_limit_rejects_before_returning_an_untracked_read() {
    let (_dir, store) = fixture();
    let result = store.transaction_with_read_set(|tx| {
        for index in 0..=MAX_TRANSACTION_READ_CONSTRAINTS {
            assert_eq!(tx.get(format!("missing-{index:05}").as_bytes())?, None);
        }
        Ok(())
    });
    assert!(result.is_err());
    store.put(b"later", b"ok").unwrap();
}

#[test]
fn read_set_modes_work_with_fifo_and_deadlines() {
    let (_dir, store) = fixture();
    store
        .transaction_with_read_set_fifo(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
            store.put(b"unrelated", b"changed")?;
            tx.put(b"result", b"fifo")
        })
        .unwrap();
    for mode in [
        CheckpointTransactionMode::ReadSet,
        CheckpointTransactionMode::ReadSetFifo,
    ] {
        store
            .transaction_until(Instant::now() + Duration::from_secs(30), mode, |tx| {
                assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
                store.put(b"unrelated", b"again")?;
                tx.put(b"result", b"timed")
            })
            .unwrap();
    }
    assert_eq!(
        store.get(b"result").unwrap().as_deref(),
        Some(&b"timed"[..])
    );
}
