//! Directory creation and synchronization for persistent store paths.

use std::fs::{self, File};
use std::io;
use std::path::Path;

pub(crate) fn create_and_sync(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    let canonical = fs::canonicalize(path)?;
    // Synchronize the created path and its ancestors bottom-up so newly
    // created directory entries are not omitted. Filesystems that do not
    // support directory synchronization fail rather than silently skip it.
    for directory in canonical.ancestors() {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}
