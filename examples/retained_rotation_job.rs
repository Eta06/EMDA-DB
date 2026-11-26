//! Disposable workload comparing writer exclusion during managed retained
//! rotation. Two identical catalogs hold 200,000 keys plus a suffix of
//! 2,000 updates. One rotates with `compact`, which excludes writers for the
//! whole rotation. The other prepares a `RetainedRotationJob` on another
//! thread while the store keeps acknowledging writes, then completes it.
//! Prints JSON with the times writers were excluded and the writes made
//! during preparation. Every value is checked after reopening.
use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{BatchOperation, RetainedExportLimits, Store};
use std::error::Error;
use std::path::Path;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const KEYS: u64 = 200_000;
const SUFFIX: u64 = 2_000;

const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 32 << 20,
        entries: KEYS + 4_096,
        key_bytes: (KEYS + 4_096) * 16,
        work_bytes: (32 << 20) + 1,
    },
    index: IndexLimits {
        blocks: 4_096,
        anchor_bytes: 64 << 10,
    },
    suffix: SuffixLimits {
        bytes: 768 << 10,
        operations: 8_192,
        work: 1_000_000_000,
    },
    overlay: OverlayLimits {
        entries: 8_192,
        key_bytes: 128 << 10,
    },
};

fn key(index: u64) -> Vec<u8> {
    format!("k{index:07}").into_bytes()
}

fn value(index: u64, round: u64) -> Vec<u8> {
    vec![((index + round) % 251) as u8; 64]
}

fn catalog(root: &Path) -> Result<ManagedRetainedStore> {
    let source = Store::open(root.join("source"))?;
    let pairs = (0..KEYS)
        .map(|index| (key(index), value(index, 0)))
        .collect::<Vec<_>>();
    for chunk in pairs.chunks(1_000) {
        let batch = chunk
            .iter()
            .map(|(key, value)| BatchOperation::Put { key, value })
            .collect::<Vec<_>>();
        source.write_batch(&batch)?;
    }
    let expected = source.build_retained_candidate(
        root.join("candidate"),
        [1; 16],
        [2; 16],
        RetainedExportLimits {
            max_log_bytes: 1 << 30,
            max_live_keys: KEYS as usize + 1,
            max_live_key_bytes: (KEYS as usize + 1) * 16,
            max_output_bytes: 1 << 30,
        },
    )?;
    drop(source);
    ManagedRetainedStore::create(
        root.join("candidate"),
        root.join("catalog"),
        LIMITS,
        &expected,
    )?;
    let mut store = ManagedRetainedStore::open(root.join("catalog"), LIMITS)?;
    for index in 0..SUFFIX {
        store.put(&key(index), &value(index, 1))?;
    }
    Ok(store)
}

fn check(root: &Path, written: u64) -> Result<()> {
    let store = ManagedRetainedStore::open(root.join("catalog"), LIMITS)?;
    for index in (0..KEYS).step_by(97) {
        let round = if index < written {
            2
        } else if index < SUFFIX {
            1
        } else {
            0
        };
        if store.get(&key(index), 64)? != Some(value(index, round)) {
            return Err(format!("value {index} mismatch").into());
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let parent = std::env::args()
        .nth(1)
        .ok_or("usage: retained_rotation_job <directory>")?;
    let root = tempfile::Builder::new()
        .prefix(".retained-rotation-job-")
        .tempdir_in(parent)?;
    let blocking = root.path().join("compact");
    let online = root.path().join("job");
    std::fs::create_dir(&blocking)?;
    std::fs::create_dir(&online)?;

    let mut store = catalog(&blocking)?;
    let start = Instant::now();
    store.compact()?;
    let compact_ms = start.elapsed().as_secs_f64() * 1e3;
    drop(store);
    check(&blocking, 0)?;

    let mut store = catalog(&online)?;
    let start = Instant::now();
    let mut job = store.begin_rotation()?;
    let begin_ms = start.elapsed().as_secs_f64() * 1e3;
    let preparing = std::thread::spawn(move || -> Result<(f64, _)> {
        let start = Instant::now();
        job.prepare()?;
        Ok((start.elapsed().as_secs_f64() * 1e3, job))
    });
    // Writers keep going until preparation is done.
    let mut written = 0;
    while !preparing.is_finished() && written < SUFFIX {
        store.put(&key(written), &value(written, 2))?;
        written += 1;
    }
    let (prepare_ms, job) = preparing.join().map_err(|_| "preparation panicked")??;
    let start = Instant::now();
    store.complete_rotation(job)?;
    let complete_ms = start.elapsed().as_secs_f64() * 1e3;
    drop(store);
    check(&online, written)?;
    println!(
        "{{\"keys\":{KEYS},\"suffix_updates\":{SUFFIX},\"compact_ms\":{compact_ms:.1},\
\"begin_ms\":{begin_ms:.1},\"prepare_ms\":{prepare_ms:.1},\"writes_during_prepare\":{written},\
\"complete_ms\":{complete_ms:.1},\"checked\":true}}"
    );
    Ok(())
}
