//! Verified, non-overwriting compaction to a separate store.
use super::*;

impl Store {
    /// Copies current live keys into a verified new store with no old records.
    ///
    /// Mutations wait for the copy and verification; existing readers can
    /// continue. The source remains open and unchanged, and callers control any
    /// later switchover. The destination must not exist. An error after rename
    /// can leave a complete destination whose directory sync was unsuccessful.
    /// This operation currently requires memory for additional full indexes and
    /// is not a bounded-memory or online in-place compaction mechanism.
    pub fn compact_to(&self, destination: impl AsRef<Path>) -> io::Result<()> {
        self.compact_to_with_sync(destination.as_ref(), |parent| {
            File::open(parent)?.sync_all()
        })
    }

    fn compact_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        // Revalidate all log records, including obsolete ones and keys: value
        // reads alone would not detect a damaged historical record or key.
        self.log.verify_index(&inner.index)?;
        let parent = backup::parent_directory(destination)?;
        let staging = tempfile::Builder::new()
            .prefix(".db-compact-")
            .tempdir_in(parent)?;
        let target = Store::open(staging.path())?;
        for (key, &location) in &inner.index {
            target.put(key, &self.log.read(location)?)?;
        }
        drop(target);
        // Reopen to exercise durable recovery, validate every record, and
        // compare actual values and key count before publishing the directory.
        let verified = Store::open(staging.path())?;
        if verified.read()?.index.len() != inner.index.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted key count mismatch",
            ));
        }
        for (key, &location) in &inner.index {
            if verified.get(key)?.as_deref() != Some(self.log.read(location)?.as_slice()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compacted value mismatch",
                ));
            }
        }
        drop(verified);
        File::open(staging.path())?.sync_all()?;
        backup::publish_directory(staging.path(), destination)?;
        let _ = staging.keep();
        sync_parent(parent).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("compacted store may exist but directory synchronization failed: {error}"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_parent_sync_reports_uncertain_publication_and_preserves_source() {
        let dir = tempfile::tempdir().unwrap();
        let source = Store::open(dir.path().join("source")).unwrap();
        source.put(b"key", b"value").unwrap();
        let destination = dir.path().join("destination");
        let error = source
            .compact_to_with_sync(&destination, |_| {
                Err(io::Error::other("injected sync error"))
            })
            .unwrap_err();
        assert!(error.to_string().contains("may exist"));
        assert_eq!(source.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        assert_eq!(
            Store::open(&destination)
                .unwrap()
                .get(b"key")
                .unwrap()
                .as_deref(),
            Some(&b"value"[..])
        );
        assert!(
            matches!(source.compact_to(&destination), Err(e) if e.kind() == io::ErrorKind::AlreadyExists)
        );
    }
}
