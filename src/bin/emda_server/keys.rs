//! Local operator commands for the authoritative live credential file.

use super::{
    Config, CredentialConfig, CredentialFile, config_error, protocol, validate_credential_file,
};
use fs2::FileExt;
use rustix::fs::{Mode, OFlags};
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn run(args: &[OsString]) -> Result<(), Box<dyn std::error::Error>> {
    let usage = || {
        config_error(
            "usage: emda-server keys <config.json> [--policy-file <path>] list|issue|rotate|revoke <project> ...",
        )
    };
    let [config_path, command_args @ ..] = args else {
        return Err(usage().into());
    };
    let (override_path, command_args) = match command_args {
        [flag, path, tail @ ..] if flag == "--policy-file" => (Some(Path::new(path)), tail),
        tail => (None, tail),
    };
    let [command, project, rest @ ..] = command_args else {
        return Err(usage().into());
    };
    let config: Config = crate::read_json_file(Path::new(config_path))?;
    let configured_path = config
        .credentials_file
        .as_deref()
        .ok_or_else(|| config_error("configure credentials_file before using key commands"))?;
    let path = override_path.unwrap_or(configured_path);
    let mut policy = Policy::open(&config, path)?;
    let project = identifier(project)?;
    let credentials = policy.project(project)?;
    let output = match (command.to_str(), rest) {
        (Some("list"), []) => {
            let mut keys: Vec<_> = credentials.iter().collect();
            keys.sort_unstable_by(|left, right| left.id.cmp(&right.id));
            let mut stdout = io::stdout().lock();
            for key in keys {
                writeln!(stdout, "{}", serde_json::json!({
                    "id": key.id,
                    "role": key.role,
                    "expires_unix_seconds": key.expires_unix_seconds,
                    "scope": key.scope,
                }))?;
            }
            return Ok(());
        }
        (Some("issue"), [id, role, expiry, optional_scope @ ..]) if optional_scope.len() <= 1 => {
            let id = identifier(id)?;
            if credentials.iter().any(|key| key.id == id) {
                return Err(config_error("credential already exists").into());
            }
            let role = serde_json::from_value(serde_json::Value::String(
                role.to_str().ok_or_else(|| config_error("invalid role"))?.to_owned(),
            ))?;
            let expires_unix_seconds = future_expiry(expiry)?;
            let scope = optional_scope.first().map(|path| crate::read_json_file(Path::new(path))).transpose()?;
            let (token, token_sha256) = generate_token()?;
            credentials.push(CredentialConfig { id: id.to_owned(), role, expires_unix_seconds, scope, token_sha256 });
            Some(token)
        }
        (Some("rotate"), [id, expiry]) => {
            let id = identifier(id)?;
            let key = credentials.iter_mut().find(|key| key.id == id)
                .ok_or_else(|| config_error("credential does not exist"))?;
            let expires_unix_seconds = future_expiry(expiry)?;
            let (token, digest) = generate_token()?;
            key.expires_unix_seconds = expires_unix_seconds;
            key.token_sha256 = digest;
            Some(token)
        }
        (Some("revoke"), [id]) => {
            let id = identifier(id)?;
            let index = credentials.iter().position(|key| key.id == id)
                .ok_or_else(|| config_error("credential does not exist"))?;
            credentials.remove(index);
            None
        }
        _ => return Err(config_error("expected list, issue <id> <role> <expiry> [scope.json], rotate <id> <expiry>, or revoke <id>").into()),
    };
    policy.commit()?;
    if let Some(token) = output {
        writeln!(io::stdout().lock(), "{token}")?;
    }
    eprintln!("Credential policy saved. Reload the running service to apply it.");
    Ok(())
}

pub(super) struct Policy<'a> {
    config: &'a Config,
    path: &'a Path,
    parent: &'a Path,
    metadata: fs::Metadata,
    file: CredentialFile,
    _lock: File,
}

impl<'a> Policy<'a> {
    pub(super) fn open(
        config: &'a Config,
        path: &'a Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::open_with_lock(config, path, true)
    }

    pub(super) fn try_open(
        config: &'a Config,
        path: &'a Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::open_with_lock(config, path, false)
    }

    fn open_with_lock(
        config: &'a Config,
        path: &'a Path,
        wait: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if config
            .projects
            .iter()
            .any(|project| !project.credentials.is_empty())
        {
            return Err(
                config_error("live credentials and inline credentials cannot be combined").into(),
            );
        }
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let parent_metadata = fs::symlink_metadata(parent)?;
        if !parent_metadata.is_dir()
            || parent_metadata.uid() != rustix::process::geteuid().as_raw()
            || parent_metadata.mode() & 0o022 != 0
        {
            return Err(config_error(
                "credential directory must be operator-owned and not writable by others",
            )
            .into());
        }
        let lock = File::from(rustix::fs::open(
            parent.join(".emda-key-policy.lock"),
            OFlags::CREATE | OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?);
        check_operator_file(&lock.metadata()?)?;
        if wait {
            lock.lock_exclusive()?;
        } else {
            lock.try_lock_exclusive()?;
        }

        let metadata = fs::symlink_metadata(path)?;
        check_operator_file(&metadata)?;
        let policy: CredentialFile = crate::read_json_file(path)?;
        let configured = || config.projects.iter().map(|project| project.id.as_str());
        validate_credential_file(&policy, configured())?;

        Ok(Self {
            config,
            path,
            parent,
            metadata,
            file: policy,
            _lock: lock,
        })
    }

    pub(super) fn contains_digest(&self, digest: &str) -> bool {
        self.file
            .projects
            .iter()
            .flat_map(|project| &project.credentials)
            .any(|key| key.token_sha256 == digest)
    }

    pub(super) fn project(&mut self, id: &str) -> io::Result<&mut Vec<CredentialConfig>> {
        self.file
            .projects
            .iter_mut()
            .find(|project| project.id == id)
            .map(|project| &mut project.credentials)
            .ok_or_else(|| config_error("project is not configured"))
    }

    pub(super) fn commit(self) -> Result<(), Box<dyn std::error::Error>> {
        validate_credential_file(
            &self.file,
            self.config
                .projects
                .iter()
                .map(|project| project.id.as_str()),
        )?;
        publish(self.parent, self.path, &self.metadata, &self.file)
    }
}

fn identifier(value: &OsString) -> io::Result<&str> {
    let value = value
        .to_str()
        .ok_or_else(|| config_error("identifier must be UTF-8"))?;
    super::validate_id(value)?;
    Ok(value)
}

fn future_expiry(value: &OsString) -> Result<u64, Box<dyn std::error::Error>> {
    let expiry = value
        .to_str()
        .ok_or_else(|| config_error("expiry must be Unix seconds"))?
        .parse::<u64>()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    if expiry <= now {
        return Err(config_error("expiry must be in the future").into());
    }
    Ok(expiry)
}

pub(super) fn generate_token() -> io::Result<(String, String)> {
    let mut token = [0_u8; 32];
    File::open("/dev/urandom")?.read_exact(&mut token)?;
    Ok((protocol::hex(&token), protocol::hex(&Sha256::digest(token))))
}

pub(super) fn check_operator_file(metadata: &fs::Metadata) -> io::Result<()> {
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o022 != 0
    {
        return Err(config_error(
            "credential policy and lock must be regular, singly linked, operator-owned files without group or public write access",
        ));
    }
    Ok(())
}

fn publish(
    parent: &Path,
    path: &Path,
    metadata: &fs::Metadata,
    policy: &CredentialFile,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec(policy)?;
    if bytes.len() > crate::MAX_CONFIG_BYTES as usize {
        return Err(config_error("updated credential policy exceeds 1 MiB").into());
    }
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    if temporary.as_file().metadata()?.gid() != metadata.gid() {
        rustix::fs::fchown(
            temporary.as_file(),
            None,
            Some(rustix::process::Gid::from_raw(metadata.gid())),
        )?;
    }
    temporary
        .as_file()
        .set_permissions(metadata.permissions())?;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    let current = fs::symlink_metadata(path)?;
    check_operator_file(&current)?;
    if (
        current.dev(),
        current.ino(),
        current.mtime(),
        current.mtime_nsec(),
        current.len(),
    ) != (
        metadata.dev(),
        metadata.ino(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.len(),
    ) {
        return Err(config_error("credential policy changed outside the key command").into());
    }
    let directory = File::open(parent)?;
    temporary.persist(path)?;
    directory.sync_all().map_err(|error| {
        io::Error::other(format!(
            "policy replacement is visible but directory sync failed: {error}"
        ))
    })?;
    Ok(())
}
