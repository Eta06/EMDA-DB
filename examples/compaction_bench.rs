//! Disposable separate-copy compaction measurement with content checks.
use database_engine::{BatchOperation, Store};
use std::error::Error;
use std::path::Path;
use std::time::Instant;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn key(index: usize) -> [u8; 8] {
    (index as u64).to_be_bytes()
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [parent, count] = args.as_slice() else {
        return Err("usage: compaction_bench <existing-directory> <keys>".into());
    };
    let count: usize = count.parse()?;
    if !Path::new(parent).is_dir() || !(16..=100_000).contains(&count) {
        return Err("invalid parent or key count".into());
    }
    let directory = tempfile::Builder::new()
        .prefix(".compact-bench-")
        .tempdir_in(parent)?;
    let path = directory.path().join("source");
    let destination = directory.path().join("compact");
    let store = Store::open(&path)?;
    for round in 0..2 {
        let limit = if round == 0 { count } else { count / 2 };
        for start in (0..limit).step_by(512) {
            let end = (start + 512).min(limit);
            let keys = (start..end).map(key).collect::<Vec<_>>();
            let values = (start..end)
                .map(|index| vec![((index + round) % 251) as u8; 64])
                .collect::<Vec<_>>();
            let operations = keys
                .iter()
                .zip(&values)
                .map(|(key, value)| BatchOperation::Put { key, value })
                .collect::<Vec<_>>();
            store.write_batch(&operations)?;
        }
    }
    let deleted_start = count - count / 8;
    for start in (deleted_start..count).step_by(512) {
        let end = (start + 512).min(count);
        let keys = (start..end).map(key).collect::<Vec<_>>();
        let operations = keys
            .iter()
            .map(|key| BatchOperation::Delete { key })
            .collect::<Vec<_>>();
        store.write_batch(&operations)?;
    }
    let before = std::fs::metadata(path.join("records.log"))?.len();
    let start = Instant::now();
    store.compact_to(&destination)?;
    let elapsed = start.elapsed().as_micros();
    let copy = Store::open(&destination)?;
    for index in [0, count / 2, deleted_start - 1] {
        let expected = ((index + usize::from(index < count / 2)) % 251) as u8;
        let value = copy.get(&key(index))?.ok_or("missing compacted value")?;
        if value != vec![expected; 64] {
            return Err("incorrect compacted value".into());
        }
    }
    if copy.get(&key(deleted_start))?.is_some() {
        return Err("deleted value survived compaction".into());
    }
    let after = std::fs::metadata(destination.join("records.log"))?.len();
    println!(
        "keys={count} live={} source_bytes={before} compact_bytes={after} compact_us={elapsed}",
        deleted_start
    );
    Ok(())
}
