use super::*;

#[test]
fn staged_budget_is_shared_and_released_on_abort_and_panic() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let value = [7; 48];
    let cost = HEADER_SIZE + 1 + value.len() + CHECKSUM_SIZE;
    store.transaction_budget.lock().unwrap().limit = cost * 2 - 1;
    let result: io::Result<()> = store.transaction(|outer| {
        outer.put(b"a", &value)?;
        outer.put(b"a", b"x")?;
        assert_eq!(
            store.transaction_budget.lock().unwrap().used,
            HEADER_SIZE + 2 + CHECKSUM_SIZE
        );
        outer.put(b"a", &value)?;
        let rejected: io::Result<()> = store.transaction(|inner| inner.put(b"b", &value));
        assert!(rejected.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, cost);
        let aborted: io::Result<()> = store.transaction(|inner| {
            inner.put(b"b", b"x")?;
            Err(io::Error::other("abort nested transaction"))
        });
        assert!(aborted.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, cost);
        Err(io::Error::other("abort outer transaction"))
    });
    assert!(result.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    assert_eq!(store.get(b"a").unwrap(), None);
    assert_eq!(store.get(b"b").unwrap(), None);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: io::Result<()> = store.transaction(|tx| {
            tx.put(b"a", &value)?;
            panic!("abort transaction callback")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    store.transaction(|tx| tx.put(b"a", b"ok")).unwrap();
}

#[test]
fn generation_exhaustion_rejects_a_write_before_append() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.write().unwrap().generation = u64::MAX;
    let before = fs::read(path.join("records.log")).unwrap();
    assert!(store.put(b"new", b"value").is_err());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(store.get(b"new").unwrap(), None);
}
