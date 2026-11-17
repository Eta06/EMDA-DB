//! Create a disposable DB02 fixture compatible with `recovery_bench measure`.
use database_engine::{BatchOperation, MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS, Store};
use std::{error::Error, fs, io, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [path, records, value_bytes, group] = args.as_slice() else {
        return Err(
            "usage: recovery_batch_fixture <new-store> <records> <value-bytes> <group>".into(),
        );
    };
    let records: u64 = records.parse()?;
    let value_bytes: usize = value_bytes.parse()?;
    let group: usize = group.parse()?;
    if records == 0
        || records > 1_000_000
        || value_bytes == 0
        || group == 0
        || group > MAX_ATOMIC_BATCH_OPERATIONS
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid fixture size").into());
    }
    let batch_bytes = value_bytes
        .checked_add(17 + 8 + 4)
        .and_then(|size| size.checked_mul(group))
        .and_then(|size| size.checked_add(4));
    if batch_bytes.is_none_or(|size| size > MAX_ATOMIC_BATCH_BYTES) {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "batch exceeds encoded limit").into(),
        );
    }
    // Never reuse an existing path: this example writes a new store.
    fs::create_dir(Path::new(path))?;
    let store = Store::open(path)?;
    let mut start = 0_u64;
    while start < records {
        let end = records.min(start.saturating_add(group as u64));
        let keys: Vec<_> = (start..end).map(u64::to_be_bytes).collect();
        let values: Vec<_> = (start..end).map(|id| vec![id as u8; value_bytes]).collect();
        let operations: Vec<_> = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| BatchOperation::Put { key, value })
            .collect();
        store.write_batch(&operations)?;
        start = end;
    }
    println!(
        "prepared_records={records} value_bytes={value_bytes} group={group} log_bytes={}",
        fs::metadata(Path::new(path).join("records.log"))?.len()
    );
    Ok(())
}
