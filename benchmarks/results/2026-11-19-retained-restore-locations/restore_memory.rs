use database_engine::retained_base_v2::{Limits, StagingLimits};
use database_engine::retained_candidate::{
    Expected, RestoreLimits, SuffixExpected, SuffixLimits, restore_exported_suffix,
};
use database_engine::{RetainedExportLimits, Store};
use std::{fs, path::Path, time::Instant};

const LENGTH: usize = 16 * 1024 * 1024;
const LIMIT_BYTES: u64 = 144 * 1024 * 1024;
const BASE: Limits = Limits {
    file_bytes: LIMIT_BYTES,
    entries: 8,
    key_bytes: 8,
    work_bytes: LIMIT_BYTES + 1,
};
const STAGING: StagingLimits = StagingLimits {
    value_bytes: LENGTH,
    staging_bytes: LIMIT_BYTES,
};
const SUFFIX: SuffixLimits = SuffixLimits {
    bytes: LIMIT_BYTES,
    operations: 4,
    work: 128,
};
const EXPORT: RetainedExportLimits = RetainedExportLimits {
    max_log_bytes: LIMIT_BYTES,
    max_live_keys: 8,
    max_live_key_bytes: 8,
    max_output_bytes: LIMIT_BYTES,
};
const RESTORE: RestoreLimits = RestoreLimits {
    log_bytes: LIMIT_BYTES,
    live_keys: 8,
    live_key_bytes: 8,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    let [_, mode, directory, destination] = args.as_slice() else {
        return Err("expected mode, fixture directory and destination".into());
    };
    let root = Path::new(directory);
    let candidate = root.join("candidate");
    let suffix = root.join("suffix");
    let binding_path = root.join("binding");
    match mode.to_str() {
        Some("setup") => {
            fs::create_dir(root)?;
            let store = Store::open(root.join("source"))?;
            for key in 1..=4 {
                store.put(&[key], &vec![key; LENGTH])?;
            }
            let expected = store.build_retained_candidate(&candidate, [3; 16], [4; 16], EXPORT)?;
            for key in 5..=8 {
                store.put(&[key], &vec![key; LENGTH])?;
            }
            let binding = store.export_retained_suffix(
                &candidate, &expected, &suffix, BASE, STAGING, SUFFIX, EXPORT,
            )?;
            let mut bytes = Vec::new();
            bytes.extend(expected.header);
            bytes.extend(expected.witness);
            bytes.extend(expected.base_digest);
            bytes.extend(binding.end.to_le_bytes());
            bytes.extend(binding.digest);
            fs::write(binding_path, bytes)?;
            println!(
                "base_bytes={} suffix_bytes={}",
                fs::metadata(candidate.join("base"))?.len(),
                binding.end
            );
        }
        Some("restore") => {
            let bytes = fs::read(binding_path)?;
            let binding = SuffixExpected {
                candidate: Expected {
                    header: bytes[..112].try_into()?,
                    witness: bytes[112..144].try_into()?,
                    base_digest: bytes[144..176].try_into()?,
                },
                end: u64::from_le_bytes(bytes[176..184].try_into()?),
                digest: bytes[184..216].try_into()?,
            };
            let start = Instant::now();
            restore_exported_suffix(
                candidate,
                suffix,
                destination,
                BASE,
                STAGING,
                SUFFIX,
                &binding,
                RESTORE,
            )?;
            println!("restore_ns={}", start.elapsed().as_nanos());
        }
        Some("verify") => {
            let store = Store::open(destination)?;
            for key in 1..=8 {
                let value = store.get(&[key])?.ok_or("missing key")?;
                assert_eq!(value.len(), LENGTH);
                assert!(value.iter().all(|&byte| byte == key));
            }
            assert!(store.scan(Some(&[8]), None, 1)?.is_empty());
            println!("verified_bytes={}", 8 * LENGTH);
        }
        _ => return Err("unrecognized mode".into()),
    }
    Ok(())
}
