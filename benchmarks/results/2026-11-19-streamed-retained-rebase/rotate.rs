use database_engine::RetainedExportLimits;
use database_engine::Store;
use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits, SuffixLimits,
};
use std::{fs, path::Path, time::Instant};

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
    let [_, phase, root] = args.as_slice() else {
        return Err("expected phase and root".into());
    };
    let root = Path::new(root);
    match phase.to_str().ok_or("phase must be UTF-8")? {
        "setup" => {
            fs::create_dir(root)?;
            let source_path = root.join("source");
            let source = Store::open(&source_path)?;
            for key in 1..=7_u8 {
                source.put(&[key], &vec![key; 4 * MIB])?;
            }
            let destination = root.join("managed");
            ManagedRetainedStore::copy_from_store(
                source,
                source_path,
                &destination,
                RetainedExportLimits {
                    max_log_bytes: 64 * MIB as u64,
                    max_live_keys: 100,
                    max_live_key_bytes: 1024,
                    max_output_bytes: 64 * MIB as u64,
                },
                LIMITS,
            )?;
            let mut store = ManagedRetainedStore::open(destination, LIMITS)?;
            store.delete(&[3])?;
            store.put(&[6], b"replacement")?;
            store.put(&[8], &vec![8; 4 * MIB])?;
        }
        "rotate" => {
            let mut store = ManagedRetainedStore::open(root, LIMITS)?;
            let previous = store.generation_id()?;
            let start = Instant::now();
            let next = store.compact()?;
            println!("compact_ns={}", start.elapsed().as_nanos());
            assert_ne!(next, previous);
        }
        "verify" => {
            let mut store = ManagedRetainedStore::open(root, LIMITS)?;
            let keys = [1, 2, 4, 5, 6, 7, 8];
            let page = store.scan(None, None, 8)?;
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
            println!(
                "verified_keys={} verified_large_value_bytes={}",
                keys.len(),
                24 * MIB
            );
        }
        _ => return Err("unknown phase".into()),
    }
    Ok(())
}
