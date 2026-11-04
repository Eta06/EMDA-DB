//! Reclaim a redundant previous store only after a synchronized handoff.
use super::{Store, compact_exchange};
use std::fs;
use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    BeforeMove,
    AfterMove,
    AfterRecords,
    AfterWitness,
    AfterLock,
    AfterRetiredSync,
    AfterRetiredRemoval,
    AfterStageSync,
    AfterStageRemoval,
    AfterParentSync,
}

#[cfg(target_os = "linux")]
fn rename_noreplace(source: &Path, destination: &Path) -> io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE).map_err(Into::into)
}

#[cfg(not(target_os = "linux"))]
fn rename_noreplace(_source: &Path, _destination: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "retired store reclamation requires Linux",
    ))
}

fn remove_optional(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) fn retire(
    source: &Path,
    previous: &Path,
    parent: &Path,
    current: &Store,
    retired: &Store,
    mut hook: impl FnMut(Point) -> io::Result<()>,
) -> io::Result<()> {
    crate::directory::verify_lock_identity(source, &current._lock_file)?;
    crate::directory::verify_lock_identity(previous, &retired._lock_file)?;
    compact_exchange::source_directory_is_owned(previous)?;
    let replacement_id = current.log.id().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "retired cleanup requires a witnessed replacement",
        )
    })?;
    let mut stage = crate::staging::Stage::retired_directory(parent, &replacement_id)?;
    let hidden = stage.path().join("retired");
    let mut moved = false;
    let result: io::Result<()> = (|| {
        hook(Point::BeforeMove)?;
        crate::directory::verify_lock_identity(source, &current._lock_file)?;
        crate::directory::verify_lock_identity(previous, &retired._lock_file)?;
        compact_exchange::source_directory_is_owned(previous)?;
        // Hide the old directory under an owned private name before unlinking
        // its lock. A newly created store at the old path cannot be touched.
        rename_noreplace(previous, &hidden)?;
        moved = true;
        stage.preserve_payload_on_drop();
        hook(Point::AfterMove)?;
        crate::directory::verify_lock_identity(&hidden, &retired._lock_file)?;
        compact_exchange::source_directory_is_owned(&hidden)?;
        fs::remove_file(hidden.join("records.log"))?;
        hook(Point::AfterRecords)?;
        remove_optional(&hidden.join("commit.state"))?;
        hook(Point::AfterWitness)?;
        fs::remove_file(hidden.join("store.lock"))?;
        hook(Point::AfterLock)?;
        fs::File::open(&hidden)?.sync_all()?;
        hook(Point::AfterRetiredSync)?;
        fs::remove_dir(&hidden)?;
        hook(Point::AfterRetiredRemoval)?;
        fs::File::open(stage.path())?.sync_all()?;
        hook(Point::AfterStageSync)?;
        fs::remove_dir(stage.path())?;
        hook(Point::AfterStageRemoval)?;
        crate::directory::sync_existing(parent)?;
        hook(Point::AfterParentSync)?;
        Ok(())
    })();
    if let Err(error) = result {
        if moved {
            let retained_path = stage.path().to_path_buf();
            // Preserve the ownership marker and any remaining data. The
            // explicit staging cleanup can finish after operators inspect it.
            let preserve = stage.retain_payload();
            let extra = preserve.err().map_or(String::new(), |failure| {
                format!("; retaining its marker also failed: {failure}")
            });
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "retired-store cleanup incomplete in {}: {error}{extra}",
                    retained_path.display()
                ),
            ));
        }
        return Err(io::Error::new(
            error.kind(),
            format!("retired store remains at {}: {error}", previous.display()),
        ));
    }
    stage.finish_retained_cleanup();
    Ok(())
}
