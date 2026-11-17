//! Experimental, non-authoritative retained candidate. This format is not a store.
//! Callers must keep the directory and its parents trusted and stable throughout
//! verification, and obtain the expected binding independently.
use crate::retained_base_v2::{self, Limits, Validated};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Read},
    path::Path,
};

/// An independently retained copy of the exact candidate header and witness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expected {
    pub header: [u8; 112],
    pub witness: [u8; 32],
    pub base_digest: [u8; 32],
}

/// Publication failure for a candidate. A binding is returned only after the
/// complete staged candidate has passed verification. `NotPublished` does not
/// imply that the destination was absent before the call or that all staging
/// artifacts were removed. For `OutcomeUnknown`, inspect and reconcile the
/// destination before retrying and retain `expected` independently of it.
#[derive(Debug)]
pub enum Failure {
    NotPublished(io::Error),
    OutcomeUnknown {
        error: io::Error,
        expected: Box<Expected>,
    },
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPublished(e) => write!(f, "candidate publication not started: {e}"),
            Self::OutcomeUnknown { error, .. } => {
                write!(f, "candidate publication outcome unknown: {error}")
            }
        }
    }
}
impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error) | Self::OutcomeUnknown { error, .. } => Some(error),
        }
    }
}

pub(crate) fn binding(base: &Validated, log_id: [u8; 16]) -> io::Result<Expected> {
    if log_id == [0; 16] || log_id == base.source_id || log_id == base.base_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid new log identity",
        ));
    }
    let mut header = [0; 112];
    header[..8].copy_from_slice(b"EMDALOG4");
    header[8..24].copy_from_slice(&base.source_id);
    header[24..40].copy_from_slice(&base.base_id);
    header[40..48].copy_from_slice(&base.boundary.to_le_bytes());
    header[48..56].copy_from_slice(&base.file_bytes.to_le_bytes());
    header[56..88].copy_from_slice(&base.digest);
    header[88..104].copy_from_slice(&log_id);
    // Bytes 104..112 encode the empty suffix boundary, zero.
    let mut hash = Sha256::new();
    hash.update(b"EMDA experimental candidate header v1\0");
    hash.update(header);
    Ok(Expected {
        header,
        witness: hash.finalize().into(),
        base_digest: base.digest,
    })
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid candidate")
}
fn open_regular(path: &Path) -> io::Result<fs::File> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid());
    }
    let file = fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid());
    }
    Ok(file)
}
fn exact(path: &Path, len: usize) -> io::Result<Vec<u8>> {
    let mut file = open_regular(path)?;
    if file.metadata()?.len() != len as u64 {
        return Err(invalid());
    }
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)?;
    let mut extra = [0];
    if file.read(&mut extra)? != 0 {
        return Err(invalid());
    }
    Ok(bytes)
}
/// Verify all candidate files without exposing values. Limits apply to the base,
/// not the whole candidate. Path checks do not prevent concurrent replacement;
/// the caller must keep the directory and its ancestors trusted and stable.
pub fn verify(
    directory: impl AsRef<Path>,
    limits: Limits,
    expected: &Expected,
) -> io::Result<Validated> {
    let dir = directory.as_ref();
    check_files(dir, expected)?;
    let base = retained_base_v2::validate(open_regular(&dir.join("base"))?, limits)?;
    check_binding(&base, expected)?;
    Ok(base)
}

/// Read one value only after validating the entire candidate against an independently
/// retained binding. The directory, its parents, and their contents must remain
/// trusted and stable throughout the operation. Limits apply to the base stream.
/// A limit or I/O error can occur before full validation; it does not establish
/// that the key exists or that the candidate matches the binding.
/// This is a read-only, non-authoritative operation, not a store or recovery.
pub fn lookup(
    directory: impl AsRef<Path>,
    limits: Limits,
    key: &[u8],
    max_value_bytes: usize,
    expected: &Expected,
) -> io::Result<Option<Vec<u8>>> {
    let dir = directory.as_ref();
    check_files(dir, expected)?;
    let base_expected = retained_base_v2::Expected {
        digest: expected.base_digest,
        source_id: expected.header[8..24].try_into().unwrap(),
        base_id: expected.header[24..40].try_into().unwrap(),
        boundary: u64::from_le_bytes(expected.header[40..48].try_into().unwrap()),
    };
    let (base, value) = retained_base_v2::lookup_with_metadata(
        open_regular(&dir.join("base"))?,
        limits,
        key,
        max_value_bytes,
        base_expected,
    )?;
    check_binding(&base, expected)?;
    Ok(value)
}

/// Load the complete live state privately, returning it only after stream and
/// independently supplied candidate binding validation. The directory, parents,
/// and contents must remain trusted and stable throughout. Limits bound entries,
/// total key bytes, each value, and staged key/value payload. Container and
/// allocator overhead is bounded by the entry limit, but is not payload bytes.
/// Budget or I/O errors may precede full validation and do not establish that
/// the candidate matches the independently supplied binding.
/// This is non-authoritative and does not open or modify a store.
pub fn load(
    directory: impl AsRef<Path>,
    limits: Limits,
    staging_limits: retained_base_v2::StagingLimits,
    expected: &Expected,
) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let dir = directory.as_ref();
    check_files(dir, expected)?;
    let (base, staged) = retained_base_v2::load_with_metadata(
        open_regular(&dir.join("base"))?,
        limits,
        staging_limits,
    )?;
    check_binding(&base, expected)?;
    Ok(staged)
}

fn check_files(dir: &Path, expected: &Expected) -> io::Result<()> {
    if !fs::symlink_metadata(dir)?.file_type().is_dir() {
        return Err(invalid());
    }
    let mut names = Vec::new();
    for entry in fs::read_dir(dir)? {
        if names.len() == 4 {
            return Err(invalid());
        }
        names.push(entry?.file_name());
    }
    names.sort();
    if names != ["base", "records.log", "store.lock", "witness"].map(std::ffi::OsString::from) {
        return Err(invalid());
    }
    exact(&dir.join("store.lock"), 0)?;
    let header = exact(&dir.join("records.log"), 112)?;
    let witness = exact(&dir.join("witness"), 32)?;
    if header != expected.header || witness != expected.witness {
        return Err(invalid());
    }
    Ok(())
}

fn check_binding(base: &Validated, expected: &Expected) -> io::Result<()> {
    if base.digest != expected.base_digest {
        return Err(invalid());
    }
    let log_id = expected.header[88..104].try_into().unwrap();
    if binding(base, log_id).map_err(|_| invalid())? != *expected {
        return Err(invalid());
    }
    Ok(())
}
