use std::{fs, io, path::PathBuf};

/// Limits on logical bytes in regular files and the number of
/// directory entries inside the catalog, including stages and old slots.
/// Hard links are counted separately. Symbolic links and special files
/// are rejected. Filesystem block allocation and external storage are
/// governed separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogStorageLimits {
    pub bytes: u64,
    pub entries: u64,
}

pub(super) fn intersect(
    first: Option<CatalogStorageLimits>,
    second: Option<CatalogStorageLimits>,
) -> Option<CatalogStorageLimits> {
    match (first, second) {
        (Some(first), Some(second)) => Some(CatalogStorageLimits {
            bytes: first.bytes.min(second.bytes),
            entries: first.entries.min(second.entries),
        }),
        (first, second) => first.or(second),
    }
}

pub(super) struct CatalogStorage {
    pub(super) root: PathBuf,
    pub(super) limits: CatalogStorageLimits,
}

impl CatalogStorage {
    pub(super) fn room(&self, reserve_bytes: u64, reserve_entries: u64) -> io::Result<u64> {
        let mut bytes = self
            .limits
            .bytes
            .checked_sub(reserve_bytes)
            .ok_or_else(byte_limit)?;
        let mut entries = self
            .limits
            .entries
            .checked_sub(reserve_entries)
            .ok_or_else(entry_limit)?;
        let mut pending = vec![fs::read_dir(&self.root)?];
        while let Some(directory) = pending.last_mut() {
            let Some(entry) = directory.next() else {
                pending.pop();
                continue;
            };
            entries = entries.checked_sub(1).ok_or_else(entry_limit)?;
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_file() {
                bytes = bytes.checked_sub(meta.len()).ok_or_else(byte_limit)?;
            } else if meta.is_dir() {
                pending
                    .try_reserve(1)
                    .map_err(|_| io::Error::other("catalog storage traversal allocation failed"))?;
                pending.push(fs::read_dir(entry.path())?);
            } else {
                return Err(crate::record::invalid_data(
                    "unsupported file type in catalog storage",
                ));
            }
        }
        Ok(bytes)
    }
}

fn byte_limit() -> io::Error {
    super::invalid_input("catalog byte limit exceeded")
}

fn entry_limit() -> io::Error {
    super::invalid_input("catalog entry limit exceeded")
}
