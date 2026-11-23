use database_engine::{
    BatchOperation, BoundedSuffixRebuildLimits, CheckpointStore, CheckpointViewLimits, Store,
};
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

const KEYS: u64 = 131_072;

fn main() {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next().expect("mode");
    let root = args.next().expect("existing measurement directory");
    let root = Path::new(&root);
    let store = root.join("store");
    let base = root.join("base");
    let log = store.join("records.log");
    let witness = store.join("commit.state");
    if mode == "prepare" {
        let mut source = Store::open(&store).unwrap();
        for start in (0..KEYS).step_by(1024) {
            let keys: Vec<_> = (start..start + 1024).map(u64::to_be_bytes).collect();
            let ops: Vec<_> = keys
                .iter()
                .map(|key| BatchOperation::Put { key, value: key })
                .collect();
            source.write_batch(&ops).unwrap();
        }
        source.checkpoint_to(&base).unwrap();
        return;
    }

    let name = mode.to_str().unwrap();
    let output = root.join(name);
    let base_bytes = std::fs::read(&base).unwrap();
    let digest: [u8; 32] = base_bytes[base_bytes.len() - 32..].try_into().unwrap();
    let log_before = Sha256::digest(std::fs::read(&log).unwrap());
    let witness_before = std::fs::read(&witness).unwrap();
    let file_limits = CheckpointViewLimits {
        max_file_bytes: base_bytes.len() as u64,
        max_blocks: 105,
    };
    let limits = BoundedSuffixRebuildLimits {
        max_log_bytes: std::fs::metadata(&log).unwrap().len(),
        max_operations: 0,
        max_chunk_keys: 1,
        max_chunk_key_bytes: 8,
        max_spill_bytes: 0,
        max_merge_fan_in: 2,
        base_checkpoint: file_limits,
        output_checkpoint: file_limits,
    };
    let start = Instant::now();
    let allocs = ALLOCS.load(Relaxed);
    let bytes = REQUESTED.load(Relaxed);
    let actual = CheckpointStore::rebuild_managed_selection_bounded_with_limits(
        &store, &output, &base, &digest, limits,
    )
    .unwrap();
    let allocs = ALLOCS.load(Relaxed) - allocs;
    let bytes = REQUESTED.load(Relaxed) - bytes;
    let elapsed = start.elapsed();

    assert_eq!(actual, digest);
    assert_eq!(std::fs::read(output).unwrap(), base_bytes);
    assert_eq!(Sha256::digest(std::fs::read(log).unwrap()), log_before);
    assert_eq!(std::fs::read(witness).unwrap(), witness_before);
    let recovered = CheckpointStore::open_managed(&store).unwrap();
    let mut observed = 0_u64;
    let mut after = None;
    loop {
        let page = recovered.scan(after.as_deref(), None, 1024).unwrap();
        if page.is_empty() {
            break;
        }
        for (key, value) in page {
            assert_eq!(key, observed.to_be_bytes());
            assert_eq!(value, key);
            observed += 1;
            after = Some(key);
        }
    }
    assert_eq!(observed, KEYS);
    println!(
        "calls={allocs} requested_bytes={bytes} elapsed_us={} keys={KEYS} verified=true",
        elapsed.as_micros()
    );
}
