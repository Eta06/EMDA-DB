//! Host-local control plane for delegated project API key management.

use super::{
    Config, CredentialConfig, CredentialScope, Role, config_error, decode_secret, keys, validate_id,
};
use fs2::FileExt;
use rustix::fs::{Mode, OFlags};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, File, Permissions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

const MAX_REQUEST: usize = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityFile {
    accounts: Vec<Account>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Account {
    id: String,
    token_sha256: String,
    expires_unix_seconds: u64,
    projects: HashMap<String, Grant>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Grant {
    List,
    ManageRead,
    ManageWrite,
}

impl Grant {
    fn allows(self, role: Role) -> bool {
        match self {
            Self::List => false,
            Self::ManageRead => role == Role::Read,
            Self::ManageWrite => role <= Role::Write,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    account: String,
    token: String,
    project: String,
    operation: Action,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    List,
    Issue {
        id: String,
        role: Role,
        expires_unix_seconds: u64,
        #[serde(default, deserialize_with = "super::deserialize_scope")]
        scope: Option<CredentialScope>,
    },
    Rotate {
        id: String,
        expires_unix_seconds: u64,
    },
    Revoke {
        id: String,
    },
}

pub fn run(args: &[OsString]) -> Result<(), Box<dyn std::error::Error>> {
    let [config, identities, policy, socket] = args else {
        return Err(config_error(
            "usage: emda-server key-api <config.json> <accounts.json> <host-policy.json> <socket>",
        )
        .into());
    };
    let config: Config = crate::read_json_file(Path::new(config))?;
    if config.credentials_file.is_none() {
        return Err(config_error("key API requires configured live credentials").into());
    }
    let policy = Path::new(policy);
    // Reject invalid policies before creating a listening socket.
    drop(keys::Policy::open(&config, policy)?);
    let identities = Path::new(identities);
    read_accounts(&config, identities)?;

    let socket = Path::new(socket);
    let parent = socket
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    super::ensure_private_directory(parent)?;
    let lock = File::from(rustix::fs::open(
        parent.join(".emda-key-api.lock"),
        OFlags::CREATE | OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    if !lock.metadata()?.is_file() {
        return Err(config_error("invalid key API listener lock").into());
    }
    lock.try_lock_exclusive()?;
    match fs::symlink_metadata(socket) {
        Ok(meta) => {
            use std::os::unix::fs::FileTypeExt;
            if !meta.file_type().is_socket() || meta.uid() != rustix::process::geteuid().as_raw() {
                return Err(config_error("socket path is occupied by another file").into());
            }
            // The directory lock excludes other instances of this API.
            match UnixStream::connect(socket) {
                Ok(_) => return Err(config_error("a listener already uses this socket").into()),
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
                Err(error) => return Err(error.into()),
            }
            fs::remove_file(socket)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(socket)?;
    fs::set_permissions(socket, Permissions::from_mode(0o600))?;
    println!("READY");
    io::stdout().flush()?;
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let _ = serve(&mut stream, &config, identities, policy);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn read_accounts(config: &Config, path: &Path) -> Result<IdentityFile, Box<dyn std::error::Error>> {
    keys::check_operator_file(&fs::symlink_metadata(path)?)?;
    let accounts: IdentityFile = crate::read_json_file(path)?;
    if accounts.accounts.len() > 128 {
        return Err(config_error("too many management accounts").into());
    }
    let mut ids = HashSet::new();
    let mut digests = HashSet::new();
    for account in &accounts.accounts {
        validate_id(&account.id)?;
        let digest = decode_secret(&account.token_sha256)
            .ok_or_else(|| config_error("invalid management token digest"))?;
        if !ids.insert(&account.id)
            || !digests.insert(digest)
            || account.expires_unix_seconds == 0
            || account.projects.len() > 128
            || !account
                .projects
                .keys()
                .all(|id| config.projects.iter().any(|p| p.id == *id))
        {
            return Err(config_error("invalid management account configuration").into());
        }
    }
    Ok(accounts)
}

fn serve(
    stream: &mut UnixStream,
    config: &Config,
    identities: &Path,
    policy: &Path,
) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let response = match read_request(stream, deadline)? {
        Some(request) => match execute(config, identities, policy, request) {
            Ok(value) => json!({"ok":true,"result":value}),
            Err(code) => json!({"ok":false,"error":code}),
        },
        None => json!({"ok":false,"error":"invalid_request"}),
    };
    let mut body = serde_json::to_vec(&response)?;
    if body.len() > crate::MAX_CONFIG_BYTES as usize {
        body = br#"{"ok":false,"error":"limit"}"#.to_vec();
    }
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    stream.write_all(&(body.len() as u32).to_be_bytes())?;
    stream.write_all(&body)
}

fn read_request(stream: &mut UnixStream, deadline: Instant) -> io::Result<Option<Request>> {
    let mut read = |mut buf: &mut [u8]| -> io::Result<()> {
        while !buf.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::ErrorKind::TimedOut.into());
            }
            stream.set_read_timeout(Some(remaining))?;
            let n = stream.read(buf)?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            buf = &mut buf[n..];
        }
        Ok(())
    };
    let mut header = [0; 4];
    read(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    if len == 0 || len > MAX_REQUEST {
        return Ok(None);
    }
    let mut body = vec![0; len];
    read(&mut body)?;
    Ok(serde_json::from_slice(&body).ok())
}

fn execute(
    config: &Config,
    identities: &Path,
    policy: &Path,
    request: Request,
) -> Result<Value, &'static str> {
    // Check the current identity snapshot on each request, including revocations.
    let accounts = read_accounts(config, identities).map_err(|_| "unavailable")?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "unavailable")?
        .as_secs();
    let token = decode_secret(&request.token).ok_or("unauthorized")?;
    let actual: [u8; 32] = Sha256::digest(token).into();
    let account = accounts
        .accounts
        .iter()
        .find(|a| a.id == request.account)
        .ok_or("unauthorized")?;
    let expected = decode_secret(&account.token_sha256).ok_or("unauthorized")?;
    if !bool::from(expected.ct_eq(&actual)) || now >= account.expires_unix_seconds {
        return Err("unauthorized");
    }
    let grant = *account.projects.get(&request.project).ok_or("forbidden")?;
    let mut state = keys::Policy::try_open(config, policy).map_err(|_| "unavailable")?;
    if state.contains_digest(&account.token_sha256) {
        return Err("unauthorized");
    }
    let credentials = state.project(&request.project).map_err(|_| "unavailable")?;
    let result = match request.operation {
        Action::List => {
            let listed: Vec<_> = credentials.iter().map(|key| {
                json!({"id":key.id,"role":key.role,"scope":key.scope,"expires_unix_seconds":key.expires_unix_seconds})
            }).collect();
            return Ok(json!(listed));
        }
        Action::Issue {
            id,
            role,
            expires_unix_seconds,
            scope,
        } => {
            if !grant.allows(role) {
                return Err("forbidden");
            }
            validate_id(&id).map_err(|_| "invalid_request")?;
            if expires_unix_seconds <= now || expires_unix_seconds > account.expires_unix_seconds {
                return Err("invalid_request");
            }
            if let Some(scope) = &scope {
                scope.validate().map_err(|_| "invalid_request")?;
            }
            if credentials.len() >= 64 {
                return Err("limit");
            }
            if credentials.iter().any(|key| key.id == id) {
                return Err("already_exists");
            }
            let (token, digest) = keys::generate_token().map_err(|_| "unavailable")?;
            credentials.push(CredentialConfig {
                id,
                role,
                expires_unix_seconds,
                scope,
                token_sha256: digest,
            });
            json!({"token":token})
        }
        Action::Rotate {
            id,
            expires_unix_seconds,
        } => {
            let key = credentials
                .iter_mut()
                .find(|key| key.id == id)
                .ok_or("not_found")?;
            if !grant.allows(key.role) {
                return Err("forbidden");
            }
            if expires_unix_seconds <= now || expires_unix_seconds > account.expires_unix_seconds {
                return Err("invalid_request");
            }
            let (token, digest) = keys::generate_token().map_err(|_| "unavailable")?;
            key.token_sha256 = digest;
            key.expires_unix_seconds = expires_unix_seconds;
            json!({"token":token})
        }
        Action::Revoke { id } => {
            let index = credentials
                .iter()
                .position(|key| key.id == id)
                .ok_or("not_found")?;
            if !grant.allows(credentials[index].role) {
                return Err("forbidden");
            }
            credentials.remove(index);
            Value::Null
        }
    };
    state.commit().map_err(|_| "outcome_unknown")?;
    Ok(result)
}
