use super::*;

#[cfg(test)]
mod tests;

const NAME: &str = "authority.record";
const MAGIC: &[u8; 8] = b"EMDAAU01";
const CHECKSUM: usize = 232;
pub(super) const LENGTH: usize = CHECKSUM + 32;

/// A compare-and-swap provider for cooperating processes on one host.
/// Provision its directory on trusted, independently protected storage. The
/// directory and its ancestors must stay stable. Administrative restore or
/// rollback of this directory can defeat the catalog's freshness guarantees.
pub struct FileRetainedAuthority {
    root: PathBuf,
}

impl FileRetainedAuthority {
    /// Create an empty, unregistered authority at an absent path. Authority
    /// creation can have an uncertain publication outcome. Open and inspect
    /// the path before retrying.
    pub fn create(root: impl AsRef<Path>) -> Result<Self, PublicationFailure> {
        let root = root.as_ref();
        crate::publication_outcome::track(|tracker| {
            let parent = crate::backup::parent_directory(root)?;
            let mut stage = crate::staging::Stage::directory(parent)?;
            write_new(&stage.path().join("store.lock"), &[])?;
            write_new(&stage.path().join(NAME), &encode(None))?;
            sync_existing(stage.path())?;
            tracker.begin_publication();
            stage.publish(root)?;
            sync_existing(parent)?;
            stage.publication_synchronized();
            Self::open(root)
        })
    }

    /// Open an existing authority. Missing or damaged records fail closed,
    /// including in a directory containing a previously created lock.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref();
        if !root.symlink_metadata()?.file_type().is_dir() {
            return Err(record::invalid_data("invalid authority directory"));
        }
        let this = Self {
            root: root.canonicalize()?,
        };
        if !this
            .root
            .join(NAME)
            .symlink_metadata()?
            .file_type()
            .is_file()
        {
            return Err(record::invalid_data("invalid authority record path"));
        }
        let _guard = this.lock()?;
        sync_existing(&this.root)?;
        this.read_locked()?;
        Ok(this)
    }

    fn lock(&self) -> io::Result<StoreLock> {
        let lock = open_regular_with(&self.root.join("store.lock"), OFlags::RDWR)?;
        fs2::FileExt::lock_exclusive(&lock)?;
        let lock = StoreLock::acquired(lock);
        lock.verify_identity(&self.root)?;
        Ok(lock)
    }

    fn read_locked(&self) -> io::Result<Option<AuthorityRecord>> {
        let bytes = exact(&self.root.join(NAME), LENGTH)?;
        decode(&bytes)
    }

    fn compare_and_swap_using(
        &mut self,
        previous: Option<AuthorityRecord>,
        next: AuthorityRecord,
        before_replace: impl FnOnce() -> io::Result<()>,
        sync_directory: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<bool> {
        let _guard = self.lock()?;
        if self.read_locked()? != previous {
            return Ok(false);
        }
        validate_update(previous, next)?;
        let (mut stage, mut file) = crate::staging::Stage::file(&self.root)?;
        file.write_all(&encode(Some(next)))?;
        file.sync_all()?;
        drop(file);
        before_replace()?;
        stage.replace_file(&self.root.join(NAME))?;
        sync_directory(&self.root)?;
        stage.publication_synchronized();
        Ok(true)
    }
}

impl RetainedAuthority for FileRetainedAuthority {
    fn read(&mut self) -> io::Result<Option<AuthorityRecord>> {
        let _guard = self.lock()?;
        let record = self.read_locked()?;
        // A prior process may have replaced the record before it could
        // synchronize the directory. Synchronize the visible choice.
        sync_directory(&self.root)?;
        Ok(record)
    }

    fn compare_and_swap(
        &mut self,
        previous: Option<AuthorityRecord>,
        next: AuthorityRecord,
    ) -> io::Result<bool> {
        self.compare_and_swap_using(previous, next, || Ok(()), sync_directory)
    }
}

fn sync_directory(root: &Path) -> io::Result<()> {
    fs::File::open(root)?.sync_all()
}

pub(super) fn encode(record: Option<AuthorityRecord>) -> [u8; LENGTH] {
    let mut bytes = [0; LENGTH];
    bytes[..8].copy_from_slice(MAGIC);
    if let Some(record) = record {
        bytes[8] = if record.pending.is_some() { 2 } else { 1 };
        bytes[16..24].copy_from_slice(&record.revision.to_le_bytes());
        encode_point(&mut bytes[24..128], record.committed);
        if let Some(pending) = record.pending {
            encode_point(&mut bytes[128..CHECKSUM], pending);
        }
    }
    let digest = Sha256::digest(&bytes[..CHECKSUM]);
    bytes[CHECKSUM..].copy_from_slice(&digest);
    bytes
}

fn encode_point(bytes: &mut [u8], point: AuthorityPoint) {
    bytes[..16].copy_from_slice(&point.catalog);
    bytes[16..32].copy_from_slice(&point.generation);
    bytes[32..64].copy_from_slice(&point.header_digest);
    bytes[64..72].copy_from_slice(&point.suffix_end.to_le_bytes());
    bytes[72..].copy_from_slice(&point.suffix_digest);
}

pub(super) fn decode(bytes: &[u8]) -> io::Result<Option<AuthorityRecord>> {
    let invalid = || record::invalid_data("invalid retained authority record");
    let record = match bytes[8] {
        0 => None,
        tag @ (1 | 2) => Some(AuthorityRecord {
            revision: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            committed: decode_point(&bytes[24..128]),
            pending: (tag == 2).then(|| decode_point(&bytes[128..CHECKSUM])),
        }),
        _ => return Err(invalid()),
    };
    if bytes != encode(record) {
        return Err(invalid());
    }
    if let Some(record) = record {
        validate_record(record)?;
    }
    Ok(record)
}

fn decode_point(bytes: &[u8]) -> AuthorityPoint {
    AuthorityPoint {
        catalog: bytes[..16].try_into().unwrap(),
        generation: bytes[16..32].try_into().unwrap(),
        header_digest: bytes[32..64].try_into().unwrap(),
        suffix_end: u64::from_le_bytes(bytes[64..72].try_into().unwrap()),
        suffix_digest: bytes[72..].try_into().unwrap(),
    }
}

fn validate_record(record: AuthorityRecord) -> io::Result<()> {
    record.validate()?;
    if record.committed.catalog == [0; 16] || record.committed.generation == [0; 16] {
        return Err(record::invalid_data("invalid retained authority identity"));
    }
    Ok(())
}

fn validate_update(previous: Option<AuthorityRecord>, next: AuthorityRecord) -> io::Result<()> {
    validate_record(next)?;
    let valid = if let Some(old) = previous {
        let same_catalog = next.committed.catalog == old.committed.catalog;
        let next_revision = old.revision.checked_add(1) == Some(next.revision);
        let advances = match (old.pending, next.pending) {
            (Some(proposal), None) => next.committed == old.committed || next.committed == proposal,
            (None, Some(_)) => next.committed == old.committed,
            (None, None) => {
                next.committed == old.committed
                    || (next.committed.generation != old.committed.generation
                        && next.committed.suffix_end == 0
                        && next.committed.suffix_digest == <[u8; 32]>::from(Sha256::digest([])))
            }
            (Some(_), Some(_)) => false,
        };
        same_catalog && next_revision && advances
    } else {
        next.revision == 0 && next.pending.is_none()
    };
    if valid {
        Ok(())
    } else {
        Err(invalid_input("invalid retained authority transition"))
    }
}
