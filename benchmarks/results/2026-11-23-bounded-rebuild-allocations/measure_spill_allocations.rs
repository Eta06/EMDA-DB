use database_engine::{BatchOperation, BoundedFullRebuildLimits, CheckpointStore, Store};
use sha2::{Digest, Sha256};
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

struct CountingAllocator;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REQUESTED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        REQUESTED.fetch_add(layout.size() as u64, Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        REQUESTED.fetch_add(size as u64, Relaxed);
        unsafe { System.realloc(ptr, old, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn main() {
    const KEYS: usize = 4096;
    let root = std::env::args_os().nth(1).expect("measurement directory");
    let root = tempfile::tempdir_in(Path::new(&root)).unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let source = Store::open(&path).unwrap();
    let keys: Vec<_> = (0..KEYS).map(|n| (n as u64).to_be_bytes()).collect();
    for pass in 0..3 {
        let value = [pass as u8; 64];
        for offset in (0..KEYS).step_by(256) {
            let operations: Vec<_> = (offset..offset + 256)
                .map(|i| (i * 2053 + pass * 101) % KEYS)
                .map(|i| {
                    if pass == 2 && i % 7 == 0 {
                        BatchOperation::Delete { key: &keys[i] }
                    } else {
                        BatchOperation::Put { key: &keys[i], value: &value }
                    }
                })
                .collect();
            source.write_batch(&operations).unwrap();
        }
    }
    let reference = source.checkpoint_to(root.path().join("reference")).unwrap();
    drop(source);
    let log_path = path.join("records.log");
    let source_before = Sha256::digest(std::fs::read(&log_path).unwrap());
    let limits = BoundedFullRebuildLimits {
        max_log_bytes: std::fs::metadata(&log_path).unwrap().len(),
        max_operations: (3 * KEYS) as u64,
        max_live_keys: KEYS,
        max_live_key_bytes: (8 * KEYS) as u64,
        max_chunk_keys: 64,
        max_chunk_key_bytes: 512,
        max_spill_bytes: 4 << 20,
        max_merge_fan_in: 4,
    };
    let start = Instant::now();
    let allocs = ALLOCS.load(Relaxed);
    let bytes = REQUESTED.load(Relaxed);
    let digest = CheckpointStore::rebuild_managed_selection_full_bounded(&path, &checkpoint, limits)
        .unwrap();
    let allocs = ALLOCS.load(Relaxed) - allocs;
    let bytes = REQUESTED.load(Relaxed) - bytes;
    let elapsed = start.elapsed();
    assert_eq!(digest, reference);
    assert_eq!(std::fs::read(checkpoint).unwrap(), std::fs::read(root.path().join("reference")).unwrap());
    assert_eq!(Sha256::digest(std::fs::read(&log_path).unwrap()), source_before);
    let reopened = CheckpointStore::open_managed(&path).unwrap();
    for (i, key) in keys.iter().enumerate() {
        let expected = (i % 7 != 0).then(|| vec![2; 64]);
        assert_eq!(reopened.get(key).unwrap(), expected);
    }
    println!("calls={allocs} requested_bytes={bytes} elapsed_us={} verified=true", elapsed.as_micros());
}
