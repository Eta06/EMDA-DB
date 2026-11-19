use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

/// A cooperating owner of an already acquired store lock.
///
/// A cloned owner shares the same descriptor and lock. The last owner in the
/// acquiring process explicitly releases it, even if an unrelated forked
/// child still holds a copy of the descriptor before exec.
#[derive(Clone)]
pub(crate) struct StoreLock(Arc<LockedFile>);

struct LockedFile {
    file: Option<File>,
    owner: u32,
}

impl StoreLock {
    /// Wraps a descriptor immediately after a successful flock acquisition.
    pub(crate) fn acquired(file: File) -> Self {
        Self(Arc::new(LockedFile {
            file: Some(file),
            owner: std::process::id(),
        }))
    }

    pub(crate) fn verify_identity(&self, path: &Path) -> io::Result<()> {
        crate::directory::verify_lock_identity(path, self.0.file.as_ref().unwrap())
    }

    /// Explicitly closes a private store and reports any unlock failure.
    pub(crate) fn release(self) -> io::Result<()> {
        Arc::try_unwrap(self.0)
            .map_err(|_| io::Error::other("private store lock still has an owner"))?
            .unlock()
    }

    #[cfg(test)]
    pub(crate) fn duplicate_descriptor(&self) -> io::Result<File> {
        self.0.file.as_ref().unwrap().try_clone()
    }
}

impl LockedFile {
    fn unlock(&mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            // An inherited object is unsupported in a forked child. Even if
            // the child drops it, it must leave the parent's flock alone.
            if self.owner == std::process::id() {
                fs2::FileExt::unlock(&file)?;
            }
        }
        Ok(())
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        let _ = self.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_an_inherited_owner_does_not_unlock_the_parent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.lock");
        let original = File::create(&path).unwrap();
        fs2::FileExt::try_lock_exclusive(&original).unwrap();

        // Model the owner PID check without running Rust code in a fork child.
        drop(LockedFile {
            file: Some(original.try_clone().unwrap()),
            owner: std::process::id().wrapping_add(1),
        });

        let contender = File::open(&path).unwrap();
        assert_eq!(
            fs2::FileExt::try_lock_exclusive(&contender)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(StoreLock::acquired(original));
        fs2::FileExt::try_lock_exclusive(&contender).unwrap();
    }
}
