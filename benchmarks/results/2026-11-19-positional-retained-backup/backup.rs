use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits, SuffixLimits,
};
use std::{path::Path, time::Instant};

const MIB: usize = 1024 * 1024;
const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 64 * MIB as u64,
        entries: 100,
        key_bytes: 1024,
        work_bytes: 64 * MIB as u64 + 1,
    },
    index: IndexLimits {
        blocks: 64,
        anchor_bytes: 1024,
    },
    suffix: SuffixLimits {
        bytes: 8 * MIB as u64,
        operations: 32,
        work: 1024,
    },
    overlay: OverlayLimits {
        entries: 32,
        key_bytes: 1024,
    },
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    let [_, root, backup, restored] = args.as_slice() else {
        return Err("expected catalog, backup and restore paths".into());
    };
    let source = ManagedRetainedStore::open(Path::new(root), LIMITS)?;
    let start = Instant::now();
    let expected = source.backup_to(Path::new(backup))?;
    println!(
        "backup_ns={} suffix_bytes={}",
        start.elapsed().as_nanos(),
        expected.end
    );
    ManagedRetainedStore::restore_backup(
        Path::new(backup),
        Path::new(restored),
        LIMITS,
        &expected,
    )?;
    let restored = ManagedRetainedStore::open(Path::new(restored), LIMITS)?;
    let keys = [1, 2, 4, 5, 6, 7, 8];
    let page = restored.scan(None, None, 8)?;
    assert_eq!(page.len(), keys.len());
    for ((key, value), expected) in page.iter().zip(keys) {
        assert_eq!(key, &[expected]);
        if expected == 6 {
            assert_eq!(value, b"replacement");
        } else {
            assert_eq!(value.len(), 4 * MIB);
            assert!(value.iter().all(|&byte| byte == expected));
        }
    }
    println!("verified_keys={}", keys.len());
    Ok(())
}
