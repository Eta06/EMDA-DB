use database_engine::retained_base_v2::{Limits, StagingLimits};
use database_engine::retained_candidate::{
    Expected, IndexLimits, IndexedSuffix, OverlayLimits, SuffixExpected, SuffixLimits,
};
use database_engine::{RetainedExportLimits, Store};
use std::{fs, path::Path, time::Instant};

const VALUE_BYTES: usize = 16 * 1024 * 1024;
const LARGE_KEYS: u8 = 8;
const FILE_BYTES: u64 = 140 * 1024 * 1024;
const BASE: Limits = Limits {
    file_bytes: 1024,
    entries: 16,
    key_bytes: 32,
    work_bytes: 1025,
};
const SUFFIX: SuffixLimits = SuffixLimits {
    bytes: FILE_BYTES,
    operations: 9,
    work: 1000,
};
const INDEX: IndexLimits = IndexLimits {
    blocks: 1,
    anchor_bytes: 4,
};
const OVERLAY: OverlayLimits = OverlayLimits {
    entries: 9,
    key_bytes: 12,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    let [_, mode, directory] = args.as_slice() else {
        return Err("expected mode and fixture path".into());
    };
    let root = Path::new(directory);
    let candidate = root.join("candidate");
    let suffix = root.join("suffix");
    let binding_path = root.join("binding");
    if mode == "setup" {
        fs::create_dir(root)?;
        let source = Store::open(root.join("source"))?;
        source.put(b"base", b"before")?;
        let export = RetainedExportLimits {
            max_log_bytes: FILE_BYTES,
            max_live_keys: 16,
            max_live_key_bytes: 32,
            max_output_bytes: FILE_BYTES,
        };
        let base = source.build_retained_candidate(&candidate, [3; 16], [4; 16], export)?;
        for id in 1..=LARGE_KEYS {
            source.put(&[id], &vec![id; VALUE_BYTES])?;
        }
        source.put(b"tail", b"last")?;
        let binding = source.export_retained_suffix(
            candidate,
            &base,
            suffix,
            BASE,
            StagingLimits {
                value_bytes: VALUE_BYTES,
                staging_bytes: FILE_BYTES,
            },
            SUFFIX,
            export,
        )?;
        let mut bytes = Vec::new();
        bytes.extend(base.header);
        bytes.extend(base.witness);
        bytes.extend(base.base_digest);
        bytes.extend(binding.end.to_le_bytes());
        bytes.extend(binding.digest);
        fs::write(binding_path, bytes)?;
        println!("suffix_bytes={}", binding.end);
        return Ok(());
    }
    let bytes = fs::read(binding_path)?;
    let binding = SuffixExpected {
        candidate: Expected {
            header: bytes[0..112].try_into()?,
            witness: bytes[112..144].try_into()?,
            base_digest: bytes[144..176].try_into()?,
        },
        end: u64::from_le_bytes(bytes[176..184].try_into()?),
        digest: bytes[184..216].try_into()?,
    };
    let start = Instant::now();
    let view = match mode.to_str() {
        Some("buffered" | "verify-buffered") => {
            IndexedSuffix::open(candidate, suffix, BASE, INDEX, SUFFIX, OVERLAY, &binding)?
        }
        Some("file" | "verify-file") => IndexedSuffix::open_file_backed(
            candidate, suffix, BASE, INDEX, SUFFIX, OVERLAY, &binding,
        )?,
        _ => return Err("unknown mode".into()),
    };
    println!("open_ns={}", start.elapsed().as_nanos());
    assert_eq!(view.get(b"base", 6)?.as_deref(), Some(b"before".as_slice()));
    assert_eq!(view.get(b"tail", 4)?.as_deref(), Some(b"last".as_slice()));
    if mode.to_string_lossy().starts_with("verify-") {
        for id in 1..=LARGE_KEYS {
            let bytes = view.get(&[id], VALUE_BYTES)?.ok_or("missing entry")?;
            assert_eq!(bytes.len(), VALUE_BYTES);
            assert!(bytes.iter().all(|&byte| byte == id));
        }
        println!(
            "verified_large_value_bytes={}",
            usize::from(LARGE_KEYS) * VALUE_BYTES
        );
    }
    Ok(())
}
