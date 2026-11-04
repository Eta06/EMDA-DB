//! Directory creation and synchronization for persistent store paths.

use std::fs::{self, File};
use std::io;
use std::path::Path;

pub(crate) fn create_and_sync(path: &Path) -> io::Result<()> {
    create_and_sync_with(path, |directory| File::open(directory)?.sync_all())
}

/// Synchronize an existing directory and every resolved ancestor bottom-up.
/// Callers use this after publishing an entry when its parent may itself be a
/// newly created or moved directory. This does not synchronize directories
/// containing symbolic-link aliases that were resolved along the way.
pub(crate) fn sync_existing(path: &Path) -> io::Result<()> {
    sync_existing_with(path, |directory| File::open(directory)?.sync_all())
}

pub(crate) fn sync_existing_with(
    path: &Path,
    mut sync: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let canonical = fs::canonicalize(path)?;
    for directory in canonical.ancestors() {
        sync(directory)?;
    }
    Ok(())
}

fn create_and_sync_with(path: &Path, sync: impl FnMut(&Path) -> io::Result<()>) -> io::Result<()> {
    fs::create_dir_all(path)?;
    // Synchronize the created path and its ancestors bottom-up so newly
    // created directory entries are not omitted. Filesystems that do not
    // support directory synchronization fail rather than silently skip it.
    sync_existing_with(path, sync)
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

    #[test]
    fn existing_directory_sync_resolves_symlinks_before_visiting_ancestors() {
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real-parent").join("real");
        let alias_parent = root.path().join("alias-parent");
        let alias = alias_parent.join("alias");
        fs::create_dir_all(&real).unwrap();
        fs::create_dir(&alias_parent).unwrap();
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let canonical = fs::canonicalize(&real).unwrap();
        let mut visited = Vec::new();
        sync_existing_with(&alias, |directory| {
            visited.push(directory.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            visited,
            canonical
                .ancestors()
                .map(Path::to_path_buf)
                .collect::<Vec<_>>()
        );
        assert!(!visited.contains(&fs::canonicalize(alias_parent).unwrap()));
    }
}
