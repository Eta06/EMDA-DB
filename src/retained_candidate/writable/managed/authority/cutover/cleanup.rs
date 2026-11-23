use super::*;
use std::os::unix::fs::MetadataExt;

impl<A: RetainedAuthority> AuthorizedRetainedStore<A> {
    /// Discard one abandoned cutover preparation while the ordinary source
    /// still opens. Pass the 16-byte ID from its `retained-cutover-*` name
    /// and the same independent authority intended for the cutover. Its
    /// record must still be absent. The source lock spans the operation.
    /// The preparation's old files must be hard links to the source.
    /// The caller coordinates this operation with registration attempts
    /// that might still be using a previously issued cutover record.
    pub fn discard_unfenced_preparation(
        source: Store,
        root: impl AsRef<Path>,
        preparation: [u8; 16],
        ceiling: RetainedStoreLimits,
        authority: &mut A,
    ) -> Result<bool, PublicationFailure> {
        discard(
            root.as_ref(),
            preparation,
            ceiling,
            |root, path| {
                same_file(&root.join("records.log"), &path.join(ARCHIVE_LOG))?;
                same_file(&root.join("commit.state"), &path.join(ARCHIVE_WITNESS))
            },
            |root, _| {
                source.share_store_lock().verify_identity(root)?;
                let mut signature = [0; 8];
                open_regular(&root.join("records.log"))?.read_exact(&mut signature)?;
                if !matches!(&signature, b"EMDALOG2" | b"EMDALOG3") || authority.read()?.is_some() {
                    return Err(record::invalid_data("cutover is no longer unregistered"));
                }
                Ok(())
            },
        )
    }

    /// Discard one abandoned preparation after another cutover succeeded.
    /// Use the archive fingerprints from the successful cutover. The
    /// preparation selected by the fence or authority is protected.
    /// Directories and the independent authority must remain stable
    /// throughout cleanup.
    pub fn discard_other_cutover_preparation(
        &mut self,
        preparation: [u8; 16],
        ceiling: RetainedStoreLimits,
        archive: &CutoverArchiveBinding,
    ) -> Result<bool, PublicationFailure> {
        let fence = self.fence.as_ref().ok_or_else(|| {
            PublicationFailure::NotPublished(invalid_input("missing original-path cutover"))
        })?;
        if preparation == fence.marker.directory {
            return Err(PublicationFailure::NotPublished(invalid_input(
                "cannot discard the fenced cutover catalog",
            )));
        }
        let root = fence.root.clone();
        discard(
            &root,
            preparation,
            ceiling,
            |root, path| {
                same_file(&root.join("commit.state"), &path.join(ARCHIVE_WITNESS))?;
                if !check_archive_file(
                    &path.join(ARCHIVE_LOG),
                    archive.log_bytes,
                    &archive.log_digest,
                )? || !check_archive_file(
                    &path.join(ARCHIVE_WITNESS),
                    crate::witness::WITNESS_SIZE as u64,
                    &archive.witness_digest,
                )? {
                    return Err(record::invalid_data("cutover archive is missing"));
                }
                Ok(())
            },
            |_, catalog| {
                self.verify_current()?;
                if catalog == Some(self.current.committed.catalog)
                    || self
                        .current
                        .pending
                        .is_some_and(|point| catalog == Some(point.catalog))
                {
                    return Err(record::invalid_data("preparation is in use by authority"));
                }
                if catalog.is_some() {
                    self.inner.reclaim_room()?;
                }
                Ok(())
            },
        )
    }
}

// The caller keeps the original-path lock until the namespace sync. Paths
// and directory entries follow the catalog's trusted, stable directory model.
fn discard(
    root: &Path,
    id: [u8; 16],
    ceiling: RetainedStoreLimits,
    verify_archives: impl FnOnce(&Path, &Path) -> io::Result<()>,
    mut verify_root: impl FnMut(&Path, Option<[u8; 16]>) -> io::Result<()>,
) -> Result<bool, PublicationFailure> {
    crate::publication_outcome::track(|tracker| {
        let root = root.canonicalize()?;
        verify_root(&root, None)?;
        if id == [0; 16] {
            return Err(invalid_input("zero cutover preparation identifier"));
        }
        let path = catalog_path(&root, id);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                sync_existing(&root)?;
                return Ok(false);
            }
            Err(error) => return Err(error),
            Ok(meta) if !meta.file_type().is_dir() => {
                return Err(record::invalid_data(
                    "invalid cutover preparation directory",
                ));
            }
            Ok(_) => {}
        }
        let lock = lock_directory(&path)?;
        let catalog = read_catalog(&path, CUTOVER_MAGIC)?;
        let active = Active::load(&path, catalog)?;
        check_layout(&path, &active)?;
        // The preparation was never selected. Its proposed quota may now
        // be smaller than the enclosing root and must not block removal.
        let generation = RetainedStore::open_role(
            &active.slot.path(&path),
            RecoveryLimits::Ceiling(ceiling),
            &active.expected,
            (
                generation_role(CUTOVER_MAGIC, catalog),
                Some(active.generation),
                None,
            ),
            |_, end| {
                if end != 0 {
                    return Err(record::invalid_data("cutover preparation has been written"));
                }
                Ok(())
            },
            |_, _, _, _| Ok(()),
            || Ok(()),
        )?;
        verify_archives(&root, &path)?;
        lock.verify_identity(&path)?;
        verify_root(&root, Some(catalog))?;
        // Make the observed original-path state durable before removing
        // the preparation, including on retry after uncertain publication.
        sync_existing(&root)?;
        let mut stage = crate::staging::Stage::directory(&root)?;
        let moved = stage.child_path("preparation".as_ref())?;
        tracker.begin_publication();
        stage.preserve_payload_on_drop();
        crate::backup::publish_noreplace(&path, &moved)?;
        sync_existing(stage.path())?;
        // The child sync above also syncs its parent and the original root.
        fs::remove_dir_all(&moved)?;
        sync_existing(stage.path())?;
        stage.finish_retained_cleanup();
        drop(generation);
        drop(lock);
        Ok(true)
    })
}

fn check_layout(path: &Path, active: &Active) -> io::Result<()> {
    fn exact_children(path: &Path, files: &[&str], directory: Option<&str>) -> io::Result<()> {
        let mut count = 0;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            let kind = entry.file_type()?;
            if !(kind.is_file() && files.iter().any(|file| name == *file)
                || kind.is_dir() && directory.is_some_and(|directory| name == directory))
            {
                return Err(record::invalid_data("unknown entry in cutover preparation"));
            }
            count += 1;
        }
        if count != files.len() + usize::from(directory.is_some()) {
            return Err(record::invalid_data("incomplete cutover preparation"));
        }
        Ok(())
    }

    if active.slot != Slot::A {
        return Err(record::invalid_data(
            "cutover preparation has already rotated",
        ));
    }
    exact_children(
        path,
        &[
            "store.lock",
            CATALOG_FILE,
            ACTIVE_FILE,
            ARCHIVE_LOG,
            ARCHIVE_WITNESS,
        ],
        Some("generation-a"),
    )?;
    exact_children(
        &Slot::A.path(path),
        &["store.lock", "base", "records.log", "commit.state"],
        None,
    )
}

fn same_file(source: &Path, archived: &Path) -> io::Result<()> {
    let source = open_regular(source)?.metadata()?;
    let archived = open_regular(archived)?.metadata()?;
    if (source.dev(), source.ino()) != (archived.dev(), archived.ino()) {
        return Err(record::invalid_data(
            "cutover archive is not linked to the source",
        ));
    }
    Ok(())
}
