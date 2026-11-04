//! Versioned, checksummed selection for a store's managed checkpoint.
//!
//! The selection is trusted metadata: its checksum detects accidental damage,
//! not a malicious replacement of both selection and checkpoint.
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};

const NAME: &str = "checkpoint.selection";
const MAGIC: &[u8; 8] = b"EMDASEL1";
const MAX_PATH_BYTES: usize = 4096;
const FIXED_BYTES: usize = 8 + 4 + 32 + 32;
const MAX_BYTES: usize = FIXED_BYTES + MAX_PATH_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Selection {
    pub(super) checkpoint: PathBuf,
    pub(super) digest: [u8; 32],
}

impl Selection {
    pub(super) fn new(checkpoint: PathBuf, digest: [u8; 32]) -> io::Result<Self> {
        let bytes = checkpoint.as_os_str().as_bytes();
        if bytes.len() > MAX_PATH_BYTES
            || bytes.contains(&0)
            || !checkpoint.is_absolute()
            || checkpoint.file_name().is_none()
            || checkpoint
                .components()
                .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid managed checkpoint path",
            ));
        }
        Ok(Self { checkpoint, digest })
    }

    fn encode(&self) -> Vec<u8> {
        let path = self.checkpoint.as_os_str().as_bytes();
        let mut output = Vec::with_capacity(FIXED_BYTES + path.len());
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&(path.len() as u32).to_le_bytes());
        output.extend_from_slice(path);
        output.extend_from_slice(&self.digest);
        let checksum = Sha256::digest(&output);
        output.extend_from_slice(&checksum);
        output
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid checkpoint selection");
        if bytes.len() < FIXED_BYTES || bytes.len() > MAX_BYTES || &bytes[..8] != MAGIC {
            return Err(invalid());
        }
        let length =
            u32::from_le_bytes(bytes[8..12].try_into().expect("four-byte length")) as usize;
        if length > MAX_PATH_BYTES || FIXED_BYTES.checked_add(length) != Some(bytes.len()) {
            return Err(invalid());
        }
        let checksum_offset = bytes.len() - 32;
        if Sha256::digest(&bytes[..checksum_offset]).as_slice() != &bytes[checksum_offset..] {
            return Err(invalid());
        }
        let checkpoint = PathBuf::from(OsString::from_vec(bytes[12..12 + length].to_vec()));
        let digest = bytes[12 + length..checksum_offset]
            .try_into()
            .expect("verified digest length");
        Self::new(checkpoint, digest).map_err(|_| invalid())
    }
}

pub(super) fn path(store: &Path) -> PathBuf {
    store.join(NAME)
}

// Refuse symlinks and avoid blocking on a FIFO. The containing directory is
// still required to be trusted; external path replacement is unsupported.
fn open_regular(path: &Path) -> io::Result<File> {
    use rustix::fs::{Mode, OFlags, open};
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )?;
    let file = File::from(fd);
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "checkpoint selection must be a regular file",
        ));
    }
    Ok(file)
}

pub(super) fn load(store: &Path) -> io::Result<Option<Selection>> {
    let file = match open_regular(&path(store)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > MAX_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "checkpoint selection too large",
        ));
    }
    let mut bytes = Vec::new();
    file.take((MAX_BYTES + 1) as u64).read_to_end(&mut bytes)?;
    Selection::decode(&bytes).map(Some)
}

pub(super) fn synchronize(store: &Path) -> io::Result<()> {
    synchronize_with(store, File::sync_all, |parent| {
        File::open(parent)?.sync_all()
    })
}

pub(super) fn synchronize_with(
    store: &Path,
    sync_file: impl FnOnce(&File) -> io::Result<()>,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let file = open_regular(&path(store))?;
    sync_file(&file)?;
    sync_parent(store)
}

// The caller holds the exclusive store lock and verified the previous
// selection. Stage and replacement are in the same directory/filesystem.
pub(super) fn publish(
    store: &Path,
    selection: &Selection,
    before_rename: impl FnOnce() -> io::Result<()>,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let (stage, mut file) = crate::staging::Stage::file(store)?;
    file.write_all(&selection.encode())?;
    file.sync_all()?;
    drop(file);
    before_rename()?;
    std::fs::rename(stage.path(), path(store))?;
    sync_parent(store)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_roundtrips_non_utf8_and_rejects_every_single_byte_mutation_and_truncation() {
        let path = PathBuf::from(OsString::from_vec(b"/tmp/check-\xff".to_vec()));
        let original = Selection::new(path, [37; 32]).unwrap();
        let encoded = original.encode();
        assert_eq!(Selection::decode(&encoded).unwrap(), original);
        for length in 0..encoded.len() {
            assert!(
                Selection::decode(&encoded[..length]).is_err(),
                "truncated at {length}"
            );
        }
        for index in 0..encoded.len() {
            let mut damaged = encoded.clone();
            damaged[index] ^= 1;
            assert!(Selection::decode(&damaged).is_err(), "damaged at {index}");
        }
        let mut trailing = encoded;
        trailing.push(0);
        assert!(Selection::decode(&trailing).is_err());
    }

    #[test]
    fn codec_rejects_invalid_paths_even_with_valid_checksums() {
        for bytes in [b"".as_slice(), b"relative", b"/", b"/nul\0byte"] {
            assert!(
                Selection::new(PathBuf::from(OsString::from_vec(bytes.to_vec())), [0; 32]).is_err()
            );
            let mut encoded = Vec::new();
            encoded.extend_from_slice(MAGIC);
            encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            encoded.extend_from_slice(bytes);
            encoded.extend_from_slice(&[0; 32]);
            let checksum = Sha256::digest(&encoded);
            encoded.extend_from_slice(&checksum);
            assert!(Selection::decode(&encoded).is_err());
        }
        assert!(
            Selection::new(
                PathBuf::from(format!("/{}", "a".repeat(MAX_PATH_BYTES))),
                [0; 32]
            )
            .is_err()
        );
    }
}
