//! Authentication of bounded job progress with a caller-held key.
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;
use std::{
    fs::File,
    io::{self, Read},
};

pub(super) type Seal = [u8; 32];
type ProgressMac = Hmac<Sha256>;

/// The exact guarded job progress expected by an independent caller.
/// Keep the latest acknowledged token outside the database. After an
/// uncertain commit, reconcile before issuing another step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceProgress(Seal);

impl MaintenanceProgress {
    /// Restores a token saved in independently trusted caller storage.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Encodes the token for independent storage.
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// An independently kept per-job key for guarded document maintenance.
/// Persist the key before starting the job and use it for every advancement.
/// Generate a fresh key for each job and keep it outside the database.
pub struct MaintenanceKey([u8; 32]);

impl MaintenanceKey {
    /// Generate a key from the operating system's random source.
    pub fn generate() -> io::Result<Self> {
        let mut bytes = [0; 32];
        File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        Ok(Self(bytes))
    }

    /// Restore a previously saved, randomly generated key.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Export the secret bytes for independent storage.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    fn mac(&self, domain: &[u8], progress: impl Serialize) -> io::Result<ProgressMac> {
        let mut mac = ProgressMac::new_from_slice(&self.0).expect("fixed HMAC key length");
        mac.update(domain);
        mac.update(&serde_json::to_vec(&progress).map_err(io::Error::other)?);
        Ok(mac)
    }
}

pub(super) fn seal(
    key: Option<&MaintenanceKey>,
    domain: &[u8],
    progress: impl Serialize,
) -> io::Result<Option<Seal>> {
    key.map(|key| progress_token(key, domain, progress).map(MaintenanceProgress::to_bytes))
        .transpose()
}

pub(super) fn progress_token(
    key: &MaintenanceKey,
    domain: &[u8],
    progress: impl Serialize,
) -> io::Result<MaintenanceProgress> {
    Ok(MaintenanceProgress(
        key.mac(domain, progress)?.finalize().into_bytes().into(),
    ))
}

pub(super) fn verify(
    key: Option<&MaintenanceKey>,
    persisted: Option<&Seal>,
    domain: &[u8],
    progress: impl Serialize,
    expected: Option<MaintenanceProgress>,
) -> io::Result<()> {
    match (key, persisted) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "guarded document maintenance requires its key",
        )),
        (Some(_), None) => Err(invalid_progress()),
        (Some(key), Some(persisted)) => key
            .mac(domain, progress)?
            .verify_slice(persisted)
            .map_err(|_| invalid_progress()),
    }?;
    if expected.is_some_and(|expected| persisted != Some(&expected.0)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "document maintenance progress does not match the trusted token",
        ));
    }
    Ok(())
}

fn invalid_progress() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "document maintenance progress authentication failed",
    )
}
