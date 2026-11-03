//! Directory creation and synchronization for persistent store paths.

use std::fs::{self, File};
use std::io;
use std::path::Path;

pub(crate) fn create_and_sync(path: &Path) -> io::Result<()> {
    create_and_sync_with(path, |directory| File::open(directory)?.sync_all())
}

fn create_and_sync_with(
    path: &Path,
    mut sync: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    fs::create_dir_all(path)?;
    let canonical = fs::canonicalize(path)?;
    // Synchronize the created path and its ancestors bottom-up so newly
    // created directory entries are not omitted. Filesystems that do not
    // support directory synchronization fail rather than silently skip it.
    for directory in canonical.ancestors() {
        sync(directory)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_sync_failure_at_each_ancestor_is_propagated_in_order() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("one").join("two");
        fs::create_dir_all(&target).unwrap();
        let canonical = fs::canonicalize(&target).unwrap();
        let ancestors: Vec<_> = canonical.ancestors().map(Path::to_path_buf).collect();
        for fail_at in 0..ancestors.len() {
            let mut calls = Vec::new();
            let result = create_and_sync_with(&target, |directory| {
                calls.push(directory.to_path_buf());
                if calls.len() == fail_at + 1 {
                    Err(io::Error::other("injected directory sync failure"))
                } else {
                    Ok(())
                }
            });
            assert!(result.is_err(), "failure at ancestor {fail_at}");
            assert_eq!(calls, ancestors[..=fail_at]);
        }
    }
}
