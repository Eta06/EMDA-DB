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
fn default_cache_is_enabled_lazy_bounded_and_can_be_disabled() {
    let (_dir, _path, store) = fixture();
    // Fresh open starts with the bounded default limit and zero cached blocks
    // until a checkpoint block is actually read and verified.
    let initial = store.checkpoint_memory_status().unwrap();
    assert_eq!(
        initial.block_cache.limit_bytes,
        DEFAULT_CHECKPOINT_BLOCK_CACHE_BYTES
    );
    assert_eq!(
        (
            initial.block_cache.cached_bytes,
            initial.block_cache.cached_blocks,
            initial.block_cache.hits,
            initial.block_cache.misses
        ),
        (0, 0, 0, 0)
    );
    assert!(initial.directory_bytes > 0);
    assert_eq!((initial.overlay_keys, initial.overlay_key_bytes), (1, 6));

    // Repeated reads populate the cache lazily and hit on the second pass.
    read_all(&store);
    read_all(&store);
    let warmed = store.checkpoint_memory_status().unwrap().block_cache;
    assert_eq!(warmed.limit_bytes, DEFAULT_CHECKPOINT_BLOCK_CACHE_BYTES);
    assert!(warmed.hits > 0 && warmed.misses > 0, "{warmed:?}");
    assert!(
        warmed.cached_bytes <= DEFAULT_CHECKPOINT_BLOCK_CACHE_BYTES && warmed.cached_blocks > 0,
        "{warmed:?}"
    );

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
fn default_cache_limit_clamps_to_configured_checkpoint_file_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("base");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"a", b"1").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let max_file_bytes = fs::metadata(&checkpoint).unwrap().len() + 1024;
    assert!((max_file_bytes as usize) < DEFAULT_CHECKPOINT_BLOCK_CACHE_BYTES);
    let limits = CheckpointStoreLimits {
        checkpoint_view: crate::CheckpointViewLimits {
            max_file_bytes,
            ..Default::default()
        },
        ..Default::default()
    };
    let store =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .unwrap();
    let status = store.checkpoint_memory_status().unwrap().block_cache;
    assert_eq!(status.limit_bytes, max_file_bytes as usize);
    assert_eq!(status.cached_bytes, 0);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert!(
        store
            .checkpoint_memory_status()
            .unwrap()
            .block_cache
            .cached_bytes
            <= max_file_bytes as usize
    );
}

#[test]
fn corrupted_block_before_first_read_fails_with_default_cache_and_is_not_cached() {
    let (_dir, path, store) = fixture();
    // Corrupt every checkpoint block on disk before any read populates the cache.
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
    let first = store.get(&key(0)).unwrap_err();
    assert_eq!(first.kind(), io::ErrorKind::InvalidData, "{first}");
    let status = store.checkpoint_memory_status().unwrap().block_cache;
    assert_eq!(
        (status.cached_blocks, status.cached_bytes, status.hits),
        (0, 0, 0)
    );
    let second = store.get(&key(0)).unwrap_err();
    assert_eq!(second.kind(), io::ErrorKind::InvalidData, "{second}");
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
    // The setting resets to the bounded default on reopen.
    let reopened = retry_lock(|| CheckpointStore::open_managed(dir.path().join("store"))).unwrap();
    let reopened_cache = reopened.checkpoint_memory_status().unwrap().block_cache;
    assert_eq!(
        reopened_cache.limit_bytes,
        DEFAULT_CHECKPOINT_BLOCK_CACHE_BYTES
    );
    assert_eq!(reopened_cache.cached_blocks, 0);
    assert_eq!(
        reopened.get(b"after").unwrap().as_deref(),
        Some(&b"rollover"[..])
    );
}

#[test]
fn rollover_isolates_cached_blocks_and_detects_corrupt_successor_checkpoint() {
    let (dir, path, store) = fixture();
    // Warm block 0 in checkpoint N.
    assert_eq!(store.get(&key(0)).unwrap(), Some(value(0)));
    assert!(
        store
            .checkpoint_memory_status()
            .unwrap()
            .block_cache
            .cached_blocks
            > 0
    );
    // Update key 0 and roll over to checkpoint N+1, where key 0 is again in block 0.
    store.put(&key(0), b"updated-value").unwrap();
    let next_checkpoint = dir.path().join("next-ckpt");
    store.rollover_managed(&next_checkpoint).unwrap();
    assert_eq!(
        store
            .checkpoint_memory_status()
            .unwrap()
            .block_cache
            .cached_blocks,
        0
    );
    // Corrupt checkpoint N+1 before its first read.
    let selected = crate::checkpoint_store::selection::load(&path)
        .unwrap()
        .unwrap()
        .checkpoint;
    let mut bytes = fs::read(&selected).unwrap();
    let end = bytes.len() - 64;
    for byte in &mut bytes[128..end] {
        *byte ^= 0xa5;
    }
    fs::write(&selected, &bytes).unwrap();
    // Checkpoint N's cached block 0 must never be served for checkpoint N+1.
    let err = store.get(&key(0)).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}

#[test]
fn compact_and_replace_and_online_handoff_preserve_cache_limit() {
    for configured in [0_usize, 128 * 1024] {
        let (dir, path, store) = fixture();
        store.set_checkpoint_block_cache_limit(configured).unwrap();
        read_all(&store);
        let (replaced, _) = store
            .compact_and_replace(dir.path().join("previous"))
            .unwrap();
        let after = replaced.checkpoint_memory_status().unwrap().block_cache;
        assert_eq!(after.limit_bytes, configured);
        assert_eq!((after.cached_blocks, after.cached_bytes), (0, 0));
        read_all(&replaced);
        let after_read = replaced.checkpoint_memory_status().unwrap().block_cache;
        if configured == 0 {
            assert_eq!(after_read.cached_blocks, 0);
        } else {
            assert!(after_read.cached_blocks > 0 && after_read.cached_bytes <= configured);
        }
        drop(replaced);

        let online = retry_lock(|| crate::OnlineCheckpointStore::open_managed(&path)).unwrap();
        online
            .with_store(|s| s.set_checkpoint_block_cache_limit(configured))
            .unwrap();
        online
            .with_store(|s| {
                read_all(s);
                Ok(())
            })
            .unwrap();
        let snapshot = online.snapshot().unwrap();
        online
            .compact_and_replace(dir.path().join("previous-online"), &Default::default())
            .unwrap();
        let online_after = online
            .with_store(|s| s.checkpoint_memory_status())
            .unwrap()
            .block_cache;
        assert_eq!(online_after.limit_bytes, configured);
        assert_eq!(
            (online_after.cached_blocks, online_after.cached_bytes),
            (0, 0)
        );
        // Retired snapshot still reads accurately.
        assert_eq!(
            snapshot.as_snapshot().unwrap().get(&key(0)).unwrap(),
            Some(value(0))
        );
        drop(snapshot);
    }
}

#[test]
fn concurrent_readers_and_limit_reductions_never_exceed_configured_limit() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (_dir, _path, store) = fixture();
    let store = Arc::new(store);
    let stop = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        for thread_id in 0..6_u32 {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            scope.spawn(move || {
                let mut i = thread_id * 17;
                while !stop.load(Ordering::Relaxed) {
                    let idx = i % KEYS;
                    assert_eq!(store.get(&key(idx)).unwrap(), Some(value(idx)));
                    i = i.wrapping_add(13);
                }
            });
        }
        for round in 0..40 {
            for &limit in &[256 * 1024, 64 * 1024, 32 * 1024, 0, 128 * 1024, 0] {
                store.set_checkpoint_block_cache_limit(limit).unwrap();
                let status = store.checkpoint_memory_status().unwrap().block_cache;
                assert!(
                    status.cached_bytes <= status.limit_bytes,
                    "round {round} limit {limit}: {status:?}"
                );
                if limit == 0 {
                    assert_eq!(
                        (status.cached_bytes, status.cached_blocks),
                        (0, 0),
                        "round {round}: concurrent reader inserted into disabled cache: {status:?}"
                    );
                }
                std::thread::yield_now();
            }
        }
        stop.store(true, Ordering::Relaxed);
    });
}
