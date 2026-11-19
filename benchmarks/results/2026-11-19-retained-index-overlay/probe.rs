use database_engine::retained_base_v2::{Limits, StagingLimits};
use database_engine::retained_candidate::{
    Expected, IndexLimits, IndexedSuffix, OverlayLimits, SuffixExpected, SuffixLimits,
    open_exported_suffix,
};
use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path;

fn binding(root: &Path) -> Result<SuffixExpected, Box<dyn std::error::Error>> {
    let candidate = fs::read(root.join("candidate.binding"))?;
    let suffix = fs::read(root.join("suffix.binding"))?;
    assert_eq!(candidate.len(), 176);
    assert_eq!(suffix.len(), 40);
    Ok(SuffixExpected {
        candidate: Expected {
            header: candidate[..112].try_into()?,
            witness: candidate[112..144].try_into()?,
            base_digest: candidate[144..].try_into()?,
        },
        end: u64::from_le_bytes(suffix[..8].try_into()?),
        digest: suffix[8..].try_into()?,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let root = Path::new(&args[2]);
    let expected = binding(root)?;
    let candidate = root.join("candidate");
    let suffix = root.join("suffix");
    let base = Limits {
        file_bytes: 64 * 1024 * 1024,
        work_bytes: 64 * 1024 * 1024 + 1,
        entries: 20_000,
        key_bytes: 1024 * 1024,
    };
    let suffix_limits = SuffixLimits {
        bytes: 2 * 1024 * 1024,
        operations: 20_000,
        work: 100_000_000_000,
    };
    let mut hasher = DefaultHasher::new();
    let mut count = 0;
    let mut consume = |rows: Vec<database_engine::Entry>| {
        count += rows.len();
        for entry in rows {
            entry.hash(&mut hasher);
        }
    };
    match args[1].as_str() {
        "materialized" => {
            consume(open_exported_suffix(
                &candidate,
                &suffix,
                base,
                StagingLimits {
                    value_bytes: 16 * 1024 * 1024,
                    staging_bytes: 16 * 1024 * 1024,
                },
                suffix_limits,
                &expected,
            )?);
        }
        "indexed" => {
            let store = IndexedSuffix::open(
                &candidate,
                &suffix,
                base,
                IndexLimits {
                    blocks: 512,
                    anchor_bytes: 512 * 1024,
                },
                suffix_limits,
                OverlayLimits {
                    entries: 20_000,
                    key_bytes: 1024 * 1024,
                },
                &expected,
            )?;
            let mut after = None;
            loop {
                let page = store.scan(after.as_deref(), None, 1024)?;
                let Some((key, _)) = page.last() else { break };
                assert!(after.as_ref().is_none_or(|prior| prior < key));
                after = Some(key.clone());
                consume(page);
            }
        }
        mode => return Err(format!("unknown mode {mode}").into()),
    }
    println!("{count} {:016x}", hasher.finish());
    Ok(())
}
