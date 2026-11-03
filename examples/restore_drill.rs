//! Local restore drill using separate temporary locations.
use database_engine::Store;
use std::error::Error;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [database_parent, backup_parent] = args.as_slice() else {
        return Err(
            "usage: restore_drill <existing-database-parent> <existing-backup-parent>".into(),
        );
    };
    if !Path::new(database_parent).is_dir() || !Path::new(backup_parent).is_dir() {
        return Err("both parent directories must exist".into());
    }
    let data = tempfile::Builder::new()
        .prefix(".db-drill-")
        .tempdir_in(database_parent)?;
    let backups = tempfile::Builder::new()
        .prefix(".db-drill-backup-")
        .tempdir_in(backup_parent)?;
    let source = data.path().join("source");
    let backup = backups.path().join("snapshot");
    let store = Store::open(&source)?;
    for index in 0_u32..100 {
        store.put(&index.to_be_bytes(), &[index as u8; 256])?;
    }
    store.delete(&0_u32.to_be_bytes())?;
    store.put(&1_u32.to_be_bytes(), &[0xa5; 256])?;
    store.backup(&backup)?;
    // A later acknowledged mutation must not appear in the restored snapshot.
    store.put(b"later", b"after backup")?;
    drop(store);
    fs::rename(&source, data.path().join("source-offline"))?;
    let restored = Store::open({
        let destination = data.path().join("restored");
        Store::restore(&backup, &destination)?;
        destination
    })?;
    if restored.get(&0_u32.to_be_bytes())?.is_some() || restored.get(b"later")?.is_some() {
        return Err("restored snapshot contains an unexpected key".into());
    }
    for index in 1_u32..100 {
        let expected = if index == 1 { 0xa5 } else { index as u8 };
        if restored.get(&index.to_be_bytes())?.as_deref() != Some(&[expected; 256]) {
            return Err(format!("restored value {index} does not match").into());
        }
    }
    println!(
        "verified_values=99 deleted_absent=true later_write_absent=true backup_bytes={} distinct_filesystems={}",
        fs::metadata(&backup)?.len(),
        fs::metadata(data.path())?.dev() != fs::metadata(backups.path())?.dev()
    );
    Ok(())
}
