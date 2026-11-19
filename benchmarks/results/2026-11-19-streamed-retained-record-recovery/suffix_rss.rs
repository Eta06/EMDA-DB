use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{BatchOperation, RetainedExportLimits, Store};
use std::{fs, path::Path, time::Instant};

const MIB: usize = 1024 * 1024;
const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 4 * MIB as u64,
        entries: 100,
        key_bytes: 1024,
        work_bytes: 4 * MIB as u64 + 1,
    },
    index: IndexLimits {
        blocks: 64,
        anchor_bytes: 1024,
    },
    suffix: SuffixLimits {
        bytes: 96 * MIB as u64,
        operations: 96,
        work: 1024,
    },
    overlay: OverlayLimits {
        entries: 16,
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
            source.put(b"base-a", b"removed")?;
            source.put(b"base-z", b"kept")?;
            let destination = root.join("managed");
            ManagedRetainedStore::copy_from_store(
                source,
                source_path,
                &destination,
                RetainedExportLimits {
                    max_log_bytes: MIB as u64,
                    max_live_keys: 100,
                    max_live_key_bytes: 1024,
                    max_output_bytes: MIB as u64,
                },
                LIMITS,
            )?;
            let mut store = ManagedRetainedStore::open(&destination, LIMITS)?;
            store.delete(b"base-a")?;
            for revision in 0..48_u8 {
                let size = match revision {
                    0 => 16 * MIB - 128,
                    1 => 16 * MIB,
                    _ => MIB,
                };
                let value = vec![revision; size];
                let key = if revision % 2 == 0 { b"v0" } else { b"v1" };
                if revision % 3 == 0 {
                    store.write_batch(&[
                        BatchOperation::Put {
                            key,
                            value: b"overwritten",
                        },
                        BatchOperation::Put { key, value: &value },
                    ])?;
                } else {
                    store.put(key, &value)?;
                }
            }
            println!(
                "log_bytes={}",
                fs::metadata(destination.join("generation-a/records.log"))?.len()
            );
        }
        "reopen" => {
            let start = Instant::now();
            let store = ManagedRetainedStore::open(root, LIMITS)?;
            println!("open_ns={}", start.elapsed().as_nanos());
            assert!(store.get(b"base-a", 0)?.is_none());
            let page = store.scan(None, None, 8)?;
            assert_eq!(page.len(), 3);
            assert_eq!(page[0], (b"base-z".to_vec(), b"kept".to_vec()));
            for (key, byte) in [(b"v0", 46_u8), (b"v1", 47_u8)] {
                let value = store.get(key, MIB)?.ok_or("missing suffix value")?;
                let pos = usize::from(byte - 45);
                assert_eq!(page[pos].0.as_slice(), key);
                assert_eq!(page[pos].1, value);
                assert_eq!(value.len(), MIB);
                assert!(value.iter().all(|&b| b == byte));
            }
            println!("verified_keys=3 verified_suffix_value_bytes={}", 2 * MIB);
        }
        _ => return Err("unknown phase".into()),
    }
    Ok(())
}
