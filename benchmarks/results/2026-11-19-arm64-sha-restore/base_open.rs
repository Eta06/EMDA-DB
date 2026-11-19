use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{RetainedExportLimits, Store};
use std::{fs, path::Path, time::Instant};

const VALUE_BYTES: usize = 16 * 1024 * 1024;
const BYTE_LIMIT: u64 = 144 * 1024 * 1024;
const KEYS: u8 = 8;
const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: BYTE_LIMIT,
        entries: 8,
        key_bytes: 8,
        work_bytes: BYTE_LIMIT + 1,
    },
    index: IndexLimits {
        blocks: 8,
        anchor_bytes: 8,
    },
    suffix: SuffixLimits {
        bytes: 4096,
        operations: 16,
        work: 128,
    },
    overlay: OverlayLimits {
        entries: 8,
        key_bytes: 8,
    },
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args_os().collect::<Vec<_>>();
    let [_, phase, root] = args.as_slice() else {
        return Err("expected phase and root".into());
    };
    let root = Path::new(root);
    match phase.to_str() {
        Some("setup") => {
            fs::create_dir(root)?;
            let source_path = root.join("source");
            let source = Store::open(&source_path)?;
            for key in 1..=KEYS {
                source.put(&[key], &vec![key; VALUE_BYTES])?;
            }
            let catalog = root.join("catalog");
            ManagedRetainedStore::copy_from_store(
                source,
                &source_path,
                &catalog,
                RetainedExportLimits {
                    max_log_bytes: BYTE_LIMIT,
                    max_live_keys: usize::from(KEYS),
                    max_live_key_bytes: usize::from(KEYS),
                    max_output_bytes: BYTE_LIMIT,
                },
                LIMITS,
            )?;
            println!(
                "base_bytes={}",
                fs::metadata(catalog.join("generation-a/base"))?.len()
            );
        }
        Some("reopen") => {
            let start = Instant::now();
            let catalog = ManagedRetainedStore::open(root, LIMITS)?;
            println!("open_ns={}", start.elapsed().as_nanos());
            for key in 1..=KEYS {
                let value = catalog
                    .get(&[key], VALUE_BYTES)?
                    .ok_or("expected live value")?;
                assert_eq!(value.len(), VALUE_BYTES);
                assert!(value.iter().all(|&byte| byte == key));
            }
            println!("verified_bytes={}", usize::from(KEYS) * VALUE_BYTES);
        }
        _ => return Err("unknown phase".into()),
    }
    Ok(())
}
