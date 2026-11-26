//! The bounded cache of verified checkpoint blocks and memory reporting.
use super::*;

const KEYS: u32 = 2_000;

fn key(index: u32) -> Vec<u8> {
    format!("key-{index:05}").into_bytes()
}

fn value(index: u32) -> Vec<u8> {
    vec![(index % 251) as u8; 48]
}

// A managed store whose checkpoint spans many blocks, with one suffix key.
fn fixture() -> (tempfile::TempDir, std::path::PathBuf, CheckpointStore) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let ordinary = Store::open(&path).unwrap();
    let pairs = (0..KEYS)
        .map(|index| (key(index), value(index)))
        .collect::<Vec<_>>();
    for chunk in pairs.chunks(500) {
        let batch = chunk
            .iter()
            .map(|(key, value)| crate::BatchOperation::Put { key, value })
            .collect::<Vec<_>>();
        ordinary.write_batch(&batch).unwrap();
    }
    let digest = ordinary.checkpoint_to(dir.path().join("base")).unwrap();
    drop(ordinary);
    let store =
        retry_lock(|| CheckpointStore::open(&path, dir.path().join("base"), &digest)).unwrap();
    store.rollover_managed(dir.path().join("managed")).unwrap();
    store.put(b"suffix", b"overlay").unwrap();
    (dir, path, store)
}

fn read_all(store: &CheckpointStore) {
    for index in (0..KEYS).step_by(7) {
        assert_eq!(store.get(&key(index)).unwrap(), Some(value(index)));
    }
}

#[test]
fn the_cache_is_off_by_default_and_bounded_when_enabled() {
    let (_dir, _path, store) = fixture();
    read_all(&store);
    let status = store.checkpoint_memory_status().unwrap();
    assert_eq!(status.block_cache, Default::default());
    assert!(status.directory_bytes > 0);
    assert_eq!((status.overlay_keys, status.overlay_key_bytes), (1, 6));
    // A limit above the checkpoint file limit is refused.
    let too_large = (crate::MAX_CHECKPOINT_VIEW_FILE_BYTES + 1) as usize;
    assert_eq!(
        store
            .set_checkpoint_block_cache_limit(too_large)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let limit = 64 * 1024;
    store.set_checkpoint_block_cache_limit(limit).unwrap();
    read_all(&store);
    read_all(&store);
    let cache = store.checkpoint_memory_status().unwrap().block_cache;
    assert_eq!(cache.limit_bytes, limit);
    assert!(cache.hits > 0 && cache.misses > 0, "{cache:?}");
    assert!(
        cache.cached_bytes <= limit && cache.cached_blocks > 0,
        "{cache:?}"
    );
    // Lowering the limit evicts down to it; zero empties the cache.
    store.set_checkpoint_block_cache_limit(limit / 4).unwrap();
    let lowered = store.checkpoint_memory_status().unwrap().block_cache;
    assert!(lowered.cached_bytes <= limit / 4, "{lowered:?}");
    store.set_checkpoint_block_cache_limit(0).unwrap();
    let off = store.checkpoint_memory_status().unwrap().block_cache;
    assert_eq!((off.cached_bytes, off.cached_blocks), (0, 0));
    read_all(&store);
    assert_eq!(
        store.checkpoint_memory_status().unwrap().block_cache.hits,
        off.hits
    );
}

#[test]
fn cached_blocks_stay_correct_and_uncached_reads_still_verify() {
    let (_dir, path, store) = fixture();
    store.set_checkpoint_block_cache_limit(1 << 20).unwrap();
    assert_eq!(store.get(&key(0)).unwrap(), Some(value(0)));
    // Damage every checkpoint block after the header on disk.
    let checkpoint = crate::checkpoint_store::selection::load(&path)
        .unwrap()
        .unwrap()
        .checkpoint;
    let mut bytes = fs::read(&checkpoint).unwrap();
    let end = bytes.len() - 64;
    for byte in &mut bytes[128..end] {
        *byte ^= 0x5a;
    }
    fs::write(&checkpoint, &bytes).unwrap();
    // The cached, previously verified block still serves its keys.
    assert_eq!(store.get(&key(0)).unwrap(), Some(value(0)));
    // Without the cache the damaged block fails its verification.
    store.set_checkpoint_block_cache_limit(0).unwrap();
    let error = store.get(&key(0)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
}

#[test]
fn the_limit_follows_rollover_and_old_snapshots_read_uncached() {
    let (dir, _path, store) = fixture();
    store.set_checkpoint_block_cache_limit(1 << 20).unwrap();
    let snapshot = store.snapshot().unwrap();
    read_all(&store);
    // Snapshots of the current checkpoint share its cache.
    let before = store.checkpoint_memory_status().unwrap().block_cache;
    assert_eq!(snapshot.get(&key(0)).unwrap(), Some(value(0)));
    assert!(store.checkpoint_memory_status().unwrap().block_cache.hits > before.hits);
    store.put(b"after", b"rollover").unwrap();
    store.rollover_managed(dir.path().join("next")).unwrap();
    // The new checkpoint has the limit and starts empty.
    let rolled = store.checkpoint_memory_status().unwrap();
    assert_eq!(rolled.block_cache.limit_bytes, 1 << 20);
    assert_eq!(
        (rolled.block_cache.cached_blocks, rolled.block_cache.hits),
        (0, 0)
    );
    assert_eq!((rolled.overlay_keys, rolled.overlay_key_bytes), (0, 0));
    read_all(&store);
    assert!(
        store
            .checkpoint_memory_status()
            .unwrap()
            .block_cache
            .cached_blocks
            > 0
    );
    // The old snapshot still reads its captured state, without a cache.
    for index in (0..KEYS).step_by(97) {
        assert_eq!(snapshot.get(&key(index)).unwrap(), Some(value(index)));
    }
    assert_eq!(snapshot.get(b"after").unwrap(), None);
    drop(snapshot);
    drop(store);
    // The setting resets on reopen.
    let reopened = retry_lock(|| CheckpointStore::open_managed(dir.path().join("store"))).unwrap();
    assert_eq!(
        reopened
            .checkpoint_memory_status()
            .unwrap()
            .block_cache
            .limit_bytes,
        0
    );
    assert_eq!(
        reopened.get(b"after").unwrap().as_deref(),
        Some(&b"rollover"[..])
    );
}
