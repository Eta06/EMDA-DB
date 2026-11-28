use super::protocol::{self, Failure, Operation, Request};
use database_engine::sql::SqlStatementKind;
use database_engine::{
    DocumentDatabase, DocumentTransaction, DocumentTransactionMode, SqlDatabase, SqlError,
    SqlLimits, Store, TransactionFailure, is_transaction_conflict,
};
use parking_lot::{Mutex, RwLock};
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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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
    pub credentials_file: Option<PathBuf>,
    #[serde(default = "default_concurrent_wakes")]
    max_concurrent_wakes: usize,
    pub projects: Vec<ProjectConfig>,
}

fn default_concurrent_wakes() -> usize {
    2
}

fn require_hybrid_kex_by_default() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    id: String,
    max_in_flight: usize,
    idle_timeout_ms: Option<u64>,
    data_log_bytes: u64,
    sql_log_bytes: u64,
    max_live_keys_per_store: usize,
    #[serde(default)]
    credentials: Vec<CredentialConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialConfig {
    id: String,
    token_sha256: String,
    expires_unix_seconds: u64,
    role: Role,
    #[serde(default, deserialize_with = "deserialize_scope")]
    scope: Option<CredentialScope>,
}

fn deserialize_scope<'de, D>(deserializer: D) -> Result<Option<CredentialScope>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    CredentialScope::deserialize(deserializer).map(Some)
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialScope {
    #[serde(default)]
    kv_prefixes: HashMap<String, DatasetAccess>,
    #[serde(default)]
    document_collections: HashMap<String, DatasetAccess>,
    #[serde(default)]
    sql_tables: HashMap<String, DatasetAccess>,
}

impl CredentialScope {
    fn validate(&self) -> io::Result<()> {
        if self.kv_prefixes.len() > 64
            || self.document_collections.len() > 64
            || self.sql_tables.len() > 64
            || self
                .kv_prefixes
                .keys()
                .any(|prefix| !(1..=1023).contains(&prefix.len()))
            || self
                .document_collections
                .keys()
                .any(|name| !(1..=128).contains(&name.len()))
            || self.sql_tables.keys().any(|name| {
                !(1..=database_engine::sql::MAX_SQL_IDENTIFIER_BYTES).contains(&name.len())
            })
        {
            return Err(config_error("invalid credential scope"));
        }
        Ok(())
    }

    fn allows(&self, operation: &Operation, required: Role) -> bool {
        let access = match operation {
            Operation::KvGet { key }
            | Operation::KvPut { key, .. }
            | Operation::KvDelete { key } => self
                .kv_prefixes
                .iter()
                .filter(|(prefix, _)| key.starts_with(prefix.as_str()))
                .max_by_key(|(prefix, _)| prefix.len())
                .map(|(_, access)| access),
            Operation::DocumentGet { collection, .. }
            | Operation::DocumentInsert { collection, .. }
            | Operation::DocumentReplace { collection, .. }
            | Operation::DocumentDelete { collection, .. } => {
                self.document_collections.get(collection)
            }
            Operation::DocumentCreate { .. } | Operation::Sql { .. } => None,
        };
        Self::permits(access, required)
    }

    fn permits(access: Option<&DatasetAccess>, required: Role) -> bool {
        match access {
            Some(DatasetAccess::Read) => required == Role::Read,
            Some(DatasetAccess::Write) => required <= Role::Write,
            Some(DatasetAccess::WriteOnly) => required == Role::Write,
            None => false,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DatasetAccess {
    Read,
    Write,
    WriteOnly,
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
    scope: Option<CredentialScope>,
}

type ProjectCredentials = HashMap<String, Arc<Credential>>;
type CredentialBook = HashMap<String, ProjectCredentials>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFile {
    projects: Vec<CredentialProject>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialProject {
    id: String,
    credentials: Vec<CredentialConfig>,
}

pub struct HostedProjects {
    pub projects: HashMap<String, Project>,
    credentials: Arc<RwLock<CredentialBook>>,
}

pub struct Project {
    id: String,
    storage: ProjectStorage,
    credentials: Arc<RwLock<CredentialBook>>,
    admission: Arc<Admission>,
}

impl HostedProjects {
    pub fn reload_credentials(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let book = load_credential_file(path, self.projects.keys().map(String::as_str))?;
        // Publish all projects at once after validating the complete candidate.
        let previous = std::mem::replace(&mut *self.credentials.write(), book);
        drop(previous);
        Ok(())
    }
}

fn load_credential_file<'a>(
    path: &Path,
    project_ids: impl Iterator<Item = &'a str>,
) -> Result<CredentialBook, Box<dyn std::error::Error>> {
    let mut expected: HashSet<_> = project_ids.collect();
    let source: CredentialFile = super::read_json_file(path)?;
    if source.projects.len() != expected.len() {
        return Err(
            config_error("credentials file must list every configured project once").into(),
        );
    }
    let mut book = HashMap::with_capacity(expected.len());
    let mut digests = HashSet::new();
    for project in source.projects {
        if !expected.remove(project.id.as_str()) {
            return Err(
                config_error("credentials file has an unknown or duplicate project").into(),
            );
        }
        book.insert(
            project.id,
            build_credentials(&project.credentials, &mut digests)?,
        );
    }
    Ok(book)
}

fn build_credentials(
    specs: &[CredentialConfig],
    digests: &mut HashSet<[u8; 32]>,
) -> io::Result<ProjectCredentials> {
    if specs.len() > 64 {
        return Err(config_error("too many project credentials"));
    }
    let mut credentials = HashMap::with_capacity(specs.len());
    for spec in specs {
        validate_id(&spec.id)?;
        if let Some(scope) = &spec.scope {
            scope.validate()?;
        }
        let digest = decode_secret(&spec.token_sha256)
            .ok_or_else(|| config_error("credential digest must be 64 lowercase hex digits"))?;
        if !digests.insert(digest)
            || credentials.contains_key(&spec.id)
            || spec.expires_unix_seconds == 0
        {
            return Err(config_error(
                "invalid or duplicate credential configuration",
            ));
        }
        credentials.insert(
            spec.id.clone(),
            Arc::new(Credential {
                digest,
                expires_unix_seconds: spec.expires_unix_seconds,
                role: spec.role,
                scope: spec.scope.clone(),
            }),
        );
    }
    Ok(credentials)
}

struct ProjectStorage {
    directory: PathBuf,
    data_log_bytes: u64,
    sql_log_bytes: u64,
    max_live_keys: usize,
    idle_timeout: Option<Duration>,
    waking: Arc<Admission>,
    state: Mutex<StorageState>,
}

struct ProjectStores {
    data: Store,
    sql: Store,
}

struct StorageState {
    lifecycle: Lifecycle,
    last_use: Instant,
}

enum Lifecycle {
    Closed { retry_at: Option<Instant> },
    Transitioning,
    Open(Arc<ProjectStores>),
}

struct StoreLease<'a> {
    stores: Arc<ProjectStores>,
    idle_state: Option<&'a Mutex<StorageState>>,
}

impl Drop for StoreLease<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.idle_state {
            state.lock().last_use = Instant::now();
        }
    }
}

struct Transition<'a> {
    state: &'a Mutex<StorageState>,
    finished: bool,
}

impl Transition<'_> {
    fn new(state: &Mutex<StorageState>) -> Transition<'_> {
        Transition {
            state,
            finished: false,
        }
    }

    fn finish(mut self, lifecycle: Lifecycle) {
        let mut state = self.state.lock();
        state.lifecycle = lifecycle;
        state.last_use = Instant::now();
        self.finished = true;
    }
}

impl Drop for Transition<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self.state.lock();
        if matches!(state.lifecycle, Lifecycle::Transitioning) {
            state.lifecycle = Lifecycle::Closed { retry_at: None };
        }
    }
}

impl ProjectStorage {
    fn open_stores(&self, deadline: Option<Instant>) -> io::Result<Arc<ProjectStores>> {
        check_wake_deadline(deadline)?;
        // Revalidate on every wake. Project files are never supplied by a request.
        ensure_private_directory(self.directory.parent().expect("project has a data root"))?;
        ensure_private_directory(&self.directory)?;
        let data = open_project_store(
            &self.directory.join("data"),
            self.data_log_bytes,
            self.max_live_keys,
        )?;
        check_wake_deadline(deadline)?;
        let sql = open_project_store(
            &self.directory.join("sql"),
            self.sql_log_bytes,
            self.max_live_keys,
        )?;
        Ok(Arc::new(ProjectStores { data, sql }))
    }

    fn lease(&self, deadline: Instant) -> Result<StoreLease<'_>, Failure> {
        if Instant::now() >= deadline {
            return Err(Failure::Timeout);
        }
        let mut state = self.state.lock();
        let idle_state = self.idle_timeout.map(|_| &self.state);
        match &state.lifecycle {
            Lifecycle::Open(stores) => {
                return Ok(StoreLease {
                    stores: Arc::clone(stores),
                    idle_state,
                });
            }
            Lifecycle::Transitioning => return Err(Failure::Busy),
            Lifecycle::Closed {
                retry_at: Some(time),
            } if Instant::now() < *time => {
                return Err(Failure::Unavailable);
            }
            Lifecycle::Closed { .. } => {}
        }
        let _wake = self.waking.enter().ok_or(Failure::Busy)?;
        state.lifecycle = Lifecycle::Transitioning;
        drop(state);
        let transition = Transition::new(&self.state);
        match self.open_stores(Some(deadline)) {
            Ok(stores) => {
                transition.finish(Lifecycle::Open(Arc::clone(&stores)));
                if Instant::now() >= deadline {
                    return Err(Failure::Timeout);
                }
                Ok(StoreLease { stores, idle_state })
            }
            Err(error) if error.kind() == io::ErrorKind::TimedOut && Instant::now() >= deadline => {
                Err(Failure::Timeout)
            }
            Err(error) => {
                transition.finish(Lifecycle::Closed {
                    retry_at: Some(Instant::now() + Duration::from_secs(1)),
                });
                eprintln!(
                    "emda-server project wake failed for {}: {error}",
                    self.directory.display()
                );
                Err(Failure::Unavailable)
            }
        }
    }

    fn reap_idle(&self, now: Instant) {
        let Some(idle_timeout) = self.idle_timeout else {
            return;
        };
        let mut state = self.state.lock();
        if now.saturating_duration_since(state.last_use) < idle_timeout
            || !matches!(&state.lifecycle, Lifecycle::Open(stores) if Arc::strong_count(stores) == 1)
        {
            return;
        }
        let stores = std::mem::replace(&mut state.lifecycle, Lifecycle::Transitioning);
        drop(state);
        let transition = Transition::new(&self.state);
        drop(stores);
        transition.finish(Lifecycle::Closed { retry_at: None });
    }
}

fn check_wake_deadline(deadline: Option<Instant>) -> io::Result<()> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "project wake deadline exceeded",
        ));
    }
    Ok(())
}

impl Config {
    pub fn open_projects(&self) -> Result<HostedProjects, Box<dyn std::error::Error>> {
        if !(1..=128).contains(&self.max_connections)
            || !(100..=60_000).contains(&self.timeout_ms)
            || !(1..=128).contains(&self.max_concurrent_wakes)
            || !(1..=128).contains(&self.projects.len())
        {
            return Err(config_error("invalid server limits").into());
        }
        ensure_private_directory(&self.data_root)?;

        let mut project_ids = HashSet::new();
        for spec in &self.projects {
            validate_id(&spec.id)?;
            if !project_ids.insert(&spec.id)
                || !(1..=self.max_connections).contains(&spec.max_in_flight)
                || spec
                    .idle_timeout_ms
                    .is_some_and(|ms| !(100..=86_400_000).contains(&ms))
                || (self.credentials_file.is_some() && !spec.credentials.is_empty())
                || (self.credentials_file.is_none() && spec.credentials.is_empty())
                || spec.data_log_bytes < 4096
                || spec.sql_log_bytes < 4096
                || spec
                    .data_log_bytes
                    .checked_add(spec.sql_log_bytes)
                    .is_none()
                || !(1..=1_000_000).contains(&spec.max_live_keys_per_store)
            {
                return Err(config_error("invalid or duplicate project configuration").into());
            }
        }

        let book = if let Some(path) = &self.credentials_file {
            load_credential_file(path, project_ids.into_iter().map(String::as_str))?
        } else {
            let mut digests = HashSet::new();
            self.projects
                .iter()
                .map(|spec| {
                    Ok((
                        spec.id.clone(),
                        build_credentials(&spec.credentials, &mut digests)?,
                    ))
                })
                .collect::<io::Result<_>>()?
        };
        let credentials = Arc::new(RwLock::new(book));
        let mut opened = HashMap::new();
        let waking = Arc::new(Admission::new(self.max_concurrent_wakes));
        for spec in &self.projects {
            let directory = self.data_root.join(&spec.id);
            create_private_directory(&directory)?;
            for name in ["data", "sql"] {
                let path = directory.join(name);
                create_private_directory(&path)?;
                validate_project_store(&path)?;
            }
            let mut storage = ProjectStorage {
                directory,
                data_log_bytes: spec.data_log_bytes,
                sql_log_bytes: spec.sql_log_bytes,
                max_live_keys: spec.max_live_keys_per_store,
                idle_timeout: spec.idle_timeout_ms.map(Duration::from_millis),
                waking: Arc::clone(&waking),
                state: Mutex::new(StorageState {
                    lifecycle: Lifecycle::Closed { retry_at: None },
                    last_use: Instant::now(),
                }),
            };
            if storage.idle_timeout.is_none() {
                let stores = storage.open_stores(None)?;
                storage.state.get_mut().lifecycle = Lifecycle::Open(stores);
            }
            opened.insert(
                spec.id.clone(),
                Project {
                    id: spec.id.clone(),
                    storage,
                    credentials: Arc::clone(&credentials),
                    admission: Arc::new(Admission::new(spec.max_in_flight)),
                },
            );
        }
        Ok(HostedProjects {
            projects: opened,
            credentials,
        })
    }
}

fn open_project_store(path: &Path, log_bytes: u64, live_keys: usize) -> io::Result<Store> {
    validate_project_store(path)?;
    Store::open_with_resource_limits(
        path,
        Some(log_bytes),
        Some(live_keys),
        Some(8 * 1024 * 1024),
    )
}

fn validate_project_store(path: &Path) -> io::Result<()> {
    ensure_private_directory(path)?;
    let mut has_lock = false;
    let mut has_records = false;
    let mut has_witness = false;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        has_lock |= entry.file_name() == "store.lock";
        has_records |= entry.file_name() == "records.log";
        has_witness |= entry.file_name() == "commit.state";
        let info = fs::symlink_metadata(entry.path())?;
        if !info.is_file() || info.nlink() != 1 || info.uid() != rustix::process::geteuid().as_raw()
        {
            return Err(config_error(
                "project storage must contain regular, unlinked files",
            ));
        }
    }
    if (has_lock || has_witness) && !has_records {
        return Err(config_error(
            "initialized project store lost its records log",
        ));
    }
    Ok(())
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

    pub fn is_idle(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) == 0
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
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.storage.idle_timeout
    }

    pub fn reap_idle(&self, now: Instant) {
        self.storage.reap_idle(now);
    }

    pub fn execute(&self, request: Request, deadline: Instant) -> Result<Value, Failure> {
        let credential = self.authenticate(&request.credential, &request.token)?;
        let role = credential.role;
        let _permit = self.admission.enter().ok_or(Failure::Busy)?;
        if Instant::now() >= deadline {
            return Err(Failure::Timeout);
        }
        if let Operation::Sql { statement, params } = request.operation {
            return self.sql(&statement, params, &credential, deadline);
        }

        let required = match &request.operation {
            Operation::KvGet { .. } | Operation::DocumentGet { .. } => Role::Read,
            Operation::DocumentCreate { .. } => Role::Admin,
            Operation::KvPut { .. }
            | Operation::KvDelete { .. }
            | Operation::DocumentInsert { .. }
            | Operation::DocumentReplace { .. }
            | Operation::DocumentDelete { .. } => Role::Write,
            Operation::Sql { .. } => unreachable!("SQL authorizes its parsed statement"),
        };
        if role < required
            || credential
                .scope
                .as_ref()
                .is_some_and(|scope| !scope.allows(&request.operation, required))
        {
            return Err(Failure::Forbidden);
        }
        let lease = self.storage.lease(deadline)?;
        let db = DocumentDatabase::new(&lease.stores.data);
        db.transaction_detailed(
            DocumentTransactionMode::ReadSet,
            Some(deadline),
            None,
            |tx| execute_data(tx, request.operation),
        )
        .map_err(transaction_failure)
    }

    fn authenticate(&self, id: &str, token: &str) -> Result<Arc<Credential>, Failure> {
        let token = decode_secret(token).ok_or(Failure::Unauthorized)?;
        let actual: [u8; 32] = Sha256::digest(token).into();
        let credential = self
            .credentials
            .read()
            .get(&self.id)
            .and_then(|project| project.get(id))
            .cloned()
            .ok_or(Failure::Unauthorized)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Failure::Internal)?
            .as_secs();
        if !bool::from(credential.digest.ct_eq(&actual)) || now >= credential.expires_unix_seconds {
            return Err(Failure::Unauthorized);
        }
        Ok(credential)
    }

    fn sql(
        &self,
        statement: &str,
        params: Vec<Value>,
        credential: &Credential,
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
        let statement = database_engine::SqlStatement::parse_with_limits(statement, &limits)
            .map_err(sql_failure)?;
        let required = match statement.kind() {
            SqlStatementKind::Query => Role::Read,
            SqlStatementKind::DataMutation => Role::Write,
            SqlStatementKind::Schema => Role::Admin,
        };
        if credential.role < required {
            return Err(Failure::Forbidden);
        }
        let table = if let Some(scope) = &credential.scope {
            let table = statement.isolated_table().ok_or(Failure::Forbidden)?;
            if !CredentialScope::permits(scope.sql_tables.get(table), required) {
                return Err(Failure::Forbidden);
            }
            Some(table)
        } else {
            None
        };
        let params = params
            .into_iter()
            .map(protocol::sql_value)
            .collect::<Result<Vec<_>, _>>()?;
        let lease = self.storage.lease(deadline)?;
        let db = SqlDatabase::with_limits(DocumentDatabase::new(&lease.stores.sql), limits)
            .map_err(sql_failure)?;
        let result = match table {
            Some(table) => db.execute_isolated_until(table, &statement, &params, deadline),
            None => db.transaction_until(deadline, |tx| tx.execute_prepared(&statement, &params)),
        }
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
        io::ErrorKind::PermissionDenied => Failure::Forbidden,
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
            scope: None,
            expires_unix_seconds: u64::MAX,
        };
        let project = |id: &str, secret| ProjectConfig {
            id: id.into(),
            max_in_flight: 1,
            idle_timeout_ms: None,
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
            max_concurrent_wakes: 1,
            timeout_ms: 1000,
            credentials_file: None,
            projects: vec![project("alpha", 1), project("beta", 2)],
        };
        let projects = config.open_projects().unwrap().projects;
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

    #[test]
    fn wake_admission_and_leases_bound_lifecycle_transitions() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let project = directory.path().join("project");
        create_private_directory(&project).unwrap();
        for name in ["data", "sql"] {
            create_private_directory(&project.join(name)).unwrap();
        }
        let storage = ProjectStorage {
            directory: project.clone(),
            data_log_bytes: 4096,
            sql_log_bytes: 4096,
            max_live_keys: 32,
            idle_timeout: Some(Duration::from_millis(100)),
            waking: Arc::new(Admission::new(1)),
            state: Mutex::new(StorageState {
                lifecycle: Lifecycle::Closed { retry_at: None },
                last_use: Instant::now(),
            }),
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        let occupied = storage.waking.enter().unwrap();
        assert!(matches!(storage.lease(deadline), Err(Failure::Busy)));
        assert!(!project.join("data/store.lock").exists());
        drop(occupied);
        storage.state.lock().lifecycle = Lifecycle::Transitioning;
        let transitioning = Transition::new(&storage.state);
        assert!(matches!(storage.lease(deadline), Err(Failure::Busy)));
        drop(transitioning);
        let lease = storage.lease(deadline).unwrap();
        let concurrent = storage.lease(deadline).unwrap();
        assert!(Arc::ptr_eq(&lease.stores, &concurrent.stores));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let storage = &storage;
                scope.spawn(move || {
                    for _ in 0..128 {
                        assert!(storage.lease(deadline).is_ok(), "warm lease rejected");
                    }
                });
            }
        });
        storage.reap_idle(Instant::now() + Duration::from_secs(1));
        assert!(matches!(storage.state.lock().lifecycle, Lifecycle::Open(_)));
        drop(lease);
        drop(concurrent);
        storage.reap_idle(Instant::now());
        assert!(matches!(storage.state.lock().lifecycle, Lifecycle::Open(_)));
        storage.reap_idle(Instant::now() + Duration::from_secs(1));
        assert!(matches!(
            storage.state.lock().lifecycle,
            Lifecycle::Closed { .. }
        ));
        assert!(matches!(
            storage.lease(Instant::now()),
            Err(Failure::Timeout)
        ));
        assert!(matches!(
            storage.state.lock().lifecycle,
            Lifecycle::Closed { .. }
        ));
    }
}
