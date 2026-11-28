use super::protocol::{self, Failure, Operation, Request};
use database_engine::sql::SqlStatementKind;
use database_engine::{
    DocumentDatabase, DocumentTransaction, DocumentTransactionMode, SqlDatabase, SqlError,
    SqlLimits, Store, TransactionFailure, is_transaction_conflict,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, DirBuilder};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub bind: std::net::SocketAddr,
    pub data_root: PathBuf,
    pub tls_cert: PathBuf,
    pub tls_key: PathBuf,
    #[serde(default = "require_hybrid_kex_by_default")]
    pub require_hybrid_kex: bool,
    pub max_connections: usize,
    pub timeout_ms: u64,
    pub projects: Vec<ProjectConfig>,
}

fn require_hybrid_kex_by_default() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    id: String,
    max_in_flight: usize,
    data_log_bytes: u64,
    sql_log_bytes: u64,
    max_live_keys_per_store: usize,
    credentials: Vec<CredentialConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialConfig {
    id: String,
    token_sha256: String,
    expires_unix_seconds: u64,
    role: Role,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum Role {
    Read,
    Write,
    Admin,
}

struct Credential {
    digest: [u8; 32],
    expires_unix_seconds: u64,
    role: Role,
}

pub struct Project {
    data: Store,
    sql: Store,
    credentials: HashMap<String, Credential>,
    admission: Arc<Admission>,
}

impl Config {
    pub fn open_projects(&self) -> io::Result<HashMap<String, Project>> {
        if !(1..=128).contains(&self.max_connections)
            || !(100..=60_000).contains(&self.timeout_ms)
            || !(1..=128).contains(&self.projects.len())
        {
            return Err(config_error("invalid server limits"));
        }
        ensure_private_directory(&self.data_root)?;

        let mut project_ids = HashSet::new();
        let mut digests = HashSet::new();
        let mut settings = Vec::with_capacity(self.projects.len());
        for spec in &self.projects {
            validate_id(&spec.id)?;
            if !project_ids.insert(&spec.id)
                || !(1..=self.max_connections).contains(&spec.max_in_flight)
                || !(1..=64).contains(&spec.credentials.len())
                || spec.data_log_bytes < 4096
                || spec.sql_log_bytes < 4096
                || spec
                    .data_log_bytes
                    .checked_add(spec.sql_log_bytes)
                    .is_none()
                || !(1..=1_000_000).contains(&spec.max_live_keys_per_store)
            {
                return Err(config_error("invalid or duplicate project configuration"));
            }
            let mut credentials = HashMap::new();
            for credential in &spec.credentials {
                validate_id(&credential.id)?;
                let digest = decode_secret(&credential.token_sha256).ok_or_else(|| {
                    config_error("credential digest must be 64 lowercase hex digits")
                })?;
                if !digests.insert(digest)
                    || credentials.contains_key(&credential.id)
                    || credential.expires_unix_seconds == 0
                {
                    return Err(config_error(
                        "invalid or duplicate credential configuration",
                    ));
                }
                credentials.insert(
                    credential.id.clone(),
                    Credential {
                        digest,
                        expires_unix_seconds: credential.expires_unix_seconds,
                        role: credential.role,
                    },
                );
            }
            settings.push((spec, credentials));
        }
        let mut opened = HashMap::new();
        for (spec, credentials) in settings {
            let directory = self.data_root.join(&spec.id);
            create_private_directory(&directory)?;
            let data = open_project_store(
                &directory.join("data"),
                spec.data_log_bytes,
                spec.max_live_keys_per_store,
            )?;
            let sql = open_project_store(
                &directory.join("sql"),
                spec.sql_log_bytes,
                spec.max_live_keys_per_store,
            )?;
            opened.insert(
                spec.id.clone(),
                Project {
                    data,
                    sql,
                    credentials,
                    admission: Arc::new(Admission::new(spec.max_in_flight)),
                },
            );
        }
        Ok(opened)
    }
}

fn open_project_store(path: &Path, log_bytes: u64, live_keys: usize) -> io::Result<Store> {
    create_private_directory(path)?;
    for entry in fs::read_dir(path)? {
        let info = fs::symlink_metadata(entry?.path())?;
        if !info.is_file() || info.nlink() != 1 || info.uid() != rustix::process::geteuid().as_raw()
        {
            return Err(config_error(
                "project storage must contain regular, unlinked files",
            ));
        }
    }
    Store::open_with_resource_limits(
        path,
        Some(log_bytes),
        Some(live_keys),
        Some(8 * 1024 * 1024),
    )
}

fn create_private_directory(path: &Path) -> io::Result<()> {
    match DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    ensure_private_directory(path)
}

fn validate_id(id: &str) -> io::Result<()> {
    if id.is_empty()
        || id.len() > 48
        || !id
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
    {
        return Err(config_error("invalid project or credential identifier"));
    }
    Ok(())
}

fn config_error(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn ensure_private_directory(path: &Path) -> io::Result<()> {
    let info = fs::symlink_metadata(path)?;
    if !info.is_dir()
        || info.permissions().mode() & 0o077 != 0
        || info.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(config_error(
            "data directories must be real, private directories",
        ));
    }
    Ok(())
}

fn decode_secret(input: &str) -> Option<[u8; 32]> {
    if input.len() != 64 {
        return None;
    }
    protocol::from_hex(input)?.try_into().ok()
}

pub struct Admission {
    in_flight: AtomicUsize,
    limit: usize,
}

impl Admission {
    pub fn new(limit: usize) -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            limit,
        }
    }

    pub fn enter(self: &Arc<Self>) -> Option<Permit> {
        let mut current = self.in_flight.load(Ordering::Relaxed);
        while current < self.limit {
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(Permit(Arc::clone(self))),
                Err(value) => current = value,
            }
        }
        None
    }
}

pub struct Permit(Arc<Admission>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Release);
    }
}

impl Project {
    pub fn execute(&self, request: Request, deadline: Instant) -> Result<Value, Failure> {
        let role = self.authenticate(&request.credential, &request.token)?;
        let _permit = self.admission.enter().ok_or(Failure::Busy)?;
        if Instant::now() >= deadline {
            return Err(Failure::Timeout);
        }
        if let Operation::Sql { statement, params } = request.operation {
            return self.sql(&statement, params, role, deadline);
        }

        let required = match &request.operation {
            Operation::KvGet { .. } | Operation::DocumentGet { .. } => Role::Read,
            Operation::DocumentCreate { .. } => Role::Admin,
            _ => Role::Write,
        };
        if role < required {
            return Err(Failure::Forbidden);
        }
        let db = DocumentDatabase::new(&self.data);
        db.transaction_detailed(
            DocumentTransactionMode::ReadSet,
            Some(deadline),
            None,
            |tx| execute_data(tx, request.operation),
        )
        .map_err(transaction_failure)
    }

    fn authenticate(&self, id: &str, token: &str) -> Result<Role, Failure> {
        let token = decode_secret(token).ok_or(Failure::Unauthorized)?;
        let actual: [u8; 32] = Sha256::digest(token).into();
        let credential = self.credentials.get(id).ok_or(Failure::Unauthorized)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Failure::Internal)?
            .as_secs();
        if !bool::from(credential.digest.ct_eq(&actual)) || now >= credential.expires_unix_seconds {
            return Err(Failure::Unauthorized);
        }
        Ok(credential.role)
    }

    fn sql(
        &self,
        statement: &str,
        params: Vec<Value>,
        role: Role,
        deadline: Instant,
    ) -> Result<Value, Failure> {
        let limits = SqlLimits {
            max_sql_bytes: 16_384,
            max_insert_rows: 64,
            max_mutation_rows: 64,
            max_select_rows: 128,
            max_sort_rows: 512,
            max_groups: 512,
            max_distinct_values: 512,
            max_subquery_values: 512,
            max_visited_rows: 4096,
            max_params: 256,
            max_text_bytes: 4096,
            ..SqlLimits::default()
        };
        let db = SqlDatabase::with_limits(DocumentDatabase::new(&self.sql), limits)
            .map_err(sql_failure)?;
        let statement = db.prepare(statement).map_err(sql_failure)?;
        let required = match statement.kind() {
            SqlStatementKind::Query => Role::Read,
            SqlStatementKind::DataMutation => Role::Write,
            SqlStatementKind::Schema => Role::Admin,
        };
        if role < required {
            return Err(Failure::Forbidden);
        }
        let params = params
            .into_iter()
            .map(protocol::sql_value)
            .collect::<Result<Vec<_>, _>>()?;
        let result = db
            .transaction_until(deadline, |tx| tx.execute_prepared(&statement, &params))
            .map_err(sql_failure)?;
        protocol::sql_result(result)
    }
}

fn execute_data(tx: &mut DocumentTransaction<'_, '_>, op: Operation) -> io::Result<Value> {
    Ok(match op {
        Operation::KvGet { key } => tx
            .get_raw(&kv_key(&key)?)?
            .map_or(Value::Null, |value| protocol::hex(&value).into()),
        Operation::KvPut { key, value_hex } => {
            let value =
                protocol::from_hex(&value_hex).ok_or_else(|| config_error("invalid hex value"))?;
            tx.put_raw(&kv_key(&key)?, &value)?;
            Value::Null
        }
        Operation::KvDelete { key } => {
            tx.delete_raw(&kv_key(&key)?)?;
            Value::Null
        }
        Operation::DocumentCreate { collection, schema } => {
            tx.create_collection(&collection, schema)?;
            Value::Null
        }
        Operation::DocumentGet { collection, id } => {
            tx.get(&collection, &id)?.unwrap_or(Value::Null)
        }
        Operation::DocumentInsert {
            collection,
            id,
            document,
        } => {
            tx.insert(&collection, &id, &document)?;
            Value::Null
        }
        Operation::DocumentReplace {
            collection,
            id,
            document,
        } => {
            tx.replace(&collection, &id, &document, false)?;
            Value::Null
        }
        Operation::DocumentDelete { collection, id } => json!(tx.delete(&collection, &id)?),
        Operation::Sql { .. } => unreachable!("SQL uses a separate store"),
    })
}

fn kv_key(key: &str) -> io::Result<Vec<u8>> {
    if !(1..=1023).contains(&key.len()) {
        return Err(config_error("invalid KV key length"));
    }
    let mut bytes = Vec::with_capacity(key.len() + 1);
    bytes.push(b'K');
    bytes.extend_from_slice(key.as_bytes());
    Ok(bytes)
}

fn transaction_failure(error: TransactionFailure) -> Failure {
    match error {
        TransactionFailure::DeadlineExceeded | TransactionFailure::Cancelled => Failure::Timeout,
        TransactionFailure::OutcomeUnknown(_) => Failure::OutcomeUnknown,
        TransactionFailure::Rejected(error) | TransactionFailure::Callback(error) => {
            io_failure(&error)
        }
        _ => Failure::Internal,
    }
}

fn sql_failure(error: SqlError) -> Failure {
    match error {
        SqlError::Conflict(_) => Failure::Conflict,
        SqlError::OutcomeUnknown(_) => Failure::OutcomeUnknown,
        SqlError::Bounds(_) => Failure::Limit,
        SqlError::NoSuchTable(_) | SqlError::NoSuchColumn { .. } | SqlError::NoSuchIndex { .. } => {
            Failure::NotFound
        }
        SqlError::Io(error) => io_failure(&error),
        _ => Failure::InvalidOperation,
    }
}

fn io_failure(error: &io::Error) -> Failure {
    if is_transaction_conflict(error) {
        return Failure::Conflict;
    }
    match error.kind() {
        io::ErrorKind::TimedOut => Failure::Timeout,
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded | io::ErrorKind::OutOfMemory => {
            Failure::Limit
        }
        io::ErrorKind::Other
            if matches!(
                error.to_string().as_str(),
                "configured log size limit would be exceeded"
                    | "configured live key limit would be exceeded"
                    | "configured live key byte limit would be exceeded"
            ) =>
        {
            Failure::Limit
        }
        io::ErrorKind::WouldBlock => Failure::Busy,
        io::ErrorKind::NotFound => Failure::NotFound,
        io::ErrorKind::InvalidInput | io::ErrorKind::AlreadyExists => Failure::InvalidOperation,
        _ => Failure::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_saturated_project_does_not_consume_another_projects_admission() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let credential = |secret: u8| CredentialConfig {
            id: "client".into(),
            token_sha256: protocol::hex(&Sha256::digest([secret; 32])),
            role: Role::Read,
            expires_unix_seconds: u64::MAX,
        };
        let project = |id: &str, secret| ProjectConfig {
            id: id.into(),
            max_in_flight: 1,
            data_log_bytes: 4096,
            sql_log_bytes: 4096,
            max_live_keys_per_store: 128,
            credentials: vec![credential(secret)],
        };
        let config = Config {
            bind: "127.0.0.1:0".parse().unwrap(),
            data_root: directory.path().to_owned(),
            tls_cert: PathBuf::new(),
            tls_key: PathBuf::new(),
            require_hybrid_kex: true,
            max_connections: 2,
            timeout_ms: 1000,
            projects: vec![project("alpha", 1), project("beta", 2)],
        };
        let projects = config.open_projects().unwrap();
        let request = |id: &str, secret| Request {
            version: protocol::VERSION,
            project: id.into(),
            credential: "client".into(),
            token: protocol::hex(&[secret; 32]),
            operation: Operation::KvGet { key: "x".into() },
        };
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let occupied = projects["alpha"].admission.enter().unwrap();
        assert_eq!(
            projects["alpha"].execute(request("alpha", 1), deadline),
            Err(Failure::Busy)
        );
        assert_eq!(
            projects["beta"].execute(request("beta", 2), deadline),
            Ok(Value::Null)
        );
        drop(occupied);
        assert_eq!(
            projects["alpha"].execute(request("alpha", 1), deadline),
            Ok(Value::Null)
        );
    }
}
