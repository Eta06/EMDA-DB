use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    IndexLimits, OverlayLimits, RetainedStore, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{RetainedExportLimits, Store};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::args_os().nth(1).expect("fixture root"));
    let source = Store::open(root.join("source"))?;
    source.put(b"base", b"candidate")?;
    let candidate = root.join("candidate");
    let expected = source.build_retained_candidate(
        &candidate,
        [8; 16],
        [9; 16],
        RetainedExportLimits {
            max_log_bytes: 4096,
            max_live_keys: 16,
            max_live_key_bytes: 128,
            max_output_bytes: 4096,
        },
    )?;
    let limits = RetainedStoreLimits {
        base: Limits {
            file_bytes: 4096,
            work_bytes: 4097,
            entries: 16,
            key_bytes: 128,
        },
        index: IndexLimits {
            blocks: 4,
            anchor_bytes: 64,
        },
        suffix: SuffixLimits {
            bytes: 4096,
            operations: 16,
            work: 1024,
        },
        overlay: OverlayLimits {
            entries: 16,
            key_bytes: 128,
        },
    };
    let path = root.join("live");
    RetainedStore::create(&candidate, &path, limits, &expected)?;
    let mut store = RetainedStore::open(&path, limits, &expected)?;
    store.put(b"delta", b"committed suffix")?;
    drop(store);
    let store = RetainedStore::open(&path, limits, &expected)?;
    assert_eq!(store.scan(None, None, 4)?.len(), 2);
    Ok(())
}
