use super::*;
use crate::SuffixExportFailure;
use crate::retained_candidate::{check_binding, open_regular};
use std::io::Read;

const CANDIDATE: &str = "candidate";
const SUFFIX: &str = "suffix";

impl ManagedRetainedStore {
    /// Publish a backup of the selected generation at a new path outside
    /// this catalog. The source must remain trusted and stable. Recovery
    /// reads only the bounded base and suffix of that generation.
    ///
    /// Keep the returned binding independently of the backup, including on
    /// an uncertain publication. The binding authenticates this snapshot
    /// when compared with the retained value.
    pub fn backup_to(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<SuffixExpected, SuffixExportFailure> {
        self.backup_using(destination.as_ref(), sync_existing)
    }

    pub(super) fn backup_using(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<SuffixExpected, SuffixExportFailure> {
        let mut verified = None;
        let result = crate::publication_outcome::track(|tracker| {
            self.verify_selection()?;
            self.store.verify_complete()?;
            let parent = crate::backup::parent_directory(destination)?;
            check_separation(&self.root, parent)?;
            let mut stage = crate::staging::Stage::directory(parent)?;
            let candidate = stage.path().join(CANDIDATE);
            let suffix = stage.path().join(SUFFIX);
            fs::DirBuilder::new().mode(0o700).create(&candidate)?;
            let expected = SuffixExpected {
                candidate: self.active.expected.clone(),
                end: self.store.suffix.len() as u64,
                digest: self.store.hash.clone().finalize().into(),
            };
            let limits = self.store.limits;
            copy_base(
                &self.active.slot.path(&self.root),
                &candidate,
                limits,
                &expected.candidate,
            )?;
            write_new(&candidate.join("records.log"), &expected.candidate.header)?;
            write_new(&candidate.join("witness"), &expected.candidate.witness)?;
            write_new(&candidate.join("store.lock"), &[])?;
            write_new(&suffix, &self.store.suffix)?;
            check_files(&candidate, &expected.candidate)?;
            let base = crate::retained_base_v2::validate_buffered(
                open_regular(&candidate.join("base"))?,
                limits.base,
            )?;
            check_binding(&base, &expected.candidate)?;
            let mut hash = Sha256::new();
            let file = open_regular(&suffix)?;
            if io::copy(&mut file.take(expected.end + 1), &mut hash)? != expected.end
                || <[u8; 32]>::from(hash.finalize()) != expected.digest
            {
                return Err(record::invalid_data("staged backup suffix mismatch"));
            }
            sync_existing(&candidate)?;
            self.verify_selection()?;
            verified = Some(expected);
            tracker.begin_publication();
            stage.publish(destination)?;
            sync_parent(parent)?;
            stage.publication_synchronized();
            Ok(())
        });
        match result {
            Ok(()) => Ok(verified.expect("backup verified before publication")),
            Err(PublicationFailure::NotPublished(error)) => {
                Err(SuffixExportFailure::NotPublished(error))
            }
            Err(PublicationFailure::OutcomeUnknown(error)) => {
                Err(SuffixExportFailure::OutcomeUnknown {
                    error,
                    expected: Box::new(verified.expect("backup verified before publication")),
                })
            }
        }
    }

    /// Restore an independently bound backup into a new managed catalog.
    /// A new catalog and generation identity are allocated. The backup
    /// stays at its original path. The backup and its paths must remain
    /// trusted and stable during restore.
    pub fn restore_backup(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &SuffixExpected,
    ) -> Result<[u8; 16], PublicationFailure> {
        let backup = backup.as_ref();
        let destination = destination.as_ref();
        let parent = crate::backup::parent_directory(destination)
            .map_err(PublicationFailure::NotPublished)?;
        check_separation(backup, parent).map_err(PublicationFailure::NotPublished)?;
        Self::create_from_suffix(
            backup.join(CANDIDATE),
            backup.join(SUFFIX),
            destination,
            limits,
            expected,
        )
    }
}
