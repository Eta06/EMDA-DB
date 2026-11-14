//! Fixture preparation and fresh-process measurement for the multi-store driver.
use database_engine::{BatchOperation, CheckpointStore, Store};
use rusqlite::{Connection, params};
use serde_json::json;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const VALUE_BYTES: usize = 64;
const MAX_TOTAL_KEYS: usize = 200_000;

fn key(index: usize) -> [u8; 16] {
    let mut key = [0x5a; 16];
    key[..8].copy_from_slice(&(index as u64).to_be_bytes());
    key
}

fn value(project: usize, index: usize) -> Vec<u8> {
    vec![((project * 31 + index) % 251) as u8; VALUE_BYTES]
}

fn project(root: &Path, index: usize) -> PathBuf {
    root.join(format!("project-{index:04}"))
}

fn configure_sqlite(connection: &Connection) -> Result<()> {
    let journal: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    if !journal.eq_ignore_ascii_case("wal") {
        return Err("SQLite WAL mode was not enabled".into());
    }
    connection.pragma_update(None, "synchronous", "FULL")?;
    let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if synchronous != 2 {
        return Err("SQLite FULL synchronous mode was not enabled".into());
    }
    Ok(())
}

fn prepare(root: &Path, projects: usize, keys: usize) -> Result<()> {
    if !root.is_dir() || fs::read_dir(root)?.next().is_some() {
        return Err("fixture root must be an existing empty directory".into());
    }
    let mut v2 = 0;
    let mut v3 = 0;
    for project_index in 0..projects {
        let directory = project(root, project_index);
        fs::create_dir(&directory)?;
        let store = Store::open(directory.join("store"))?;
        for start in (0..keys).step_by(256) {
            let end = (start + 256).min(keys);
            let keys = (start..end).map(key).collect::<Vec<_>>();
            let values = (start..end)
                .map(|index| value(project_index, index))
                .collect::<Vec<_>>();
            let operations = keys
                .iter()
                .zip(&values)
                .map(|(key, value)| BatchOperation::Put { key, value })
                .collect::<Vec<_>>();
            store.write_batch(&operations)?;
        }
        let digest = store.checkpoint_to(directory.join("checkpoint"))?;
        fs::write(directory.join("digest"), digest)?;
        drop(store);
        let mut header = [0; 8];
        File::open(directory.join("store/records.log"))?.read_exact(&mut header)?;
        match &header {
            b"EMDALOG2" => v2 += 1,
            b"EMDALOG3" => v3 += 1,
            _ => return Err("unexpected engine log format".into()),
        }

        let mut sqlite = Connection::open(directory.join("sqlite.db"))?;
        configure_sqlite(&sqlite)?;
        sqlite
            .execute_batch("CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID")?;
        let tx = sqlite.transaction()?;
        {
            let mut insert = tx.prepare("INSERT INTO kv(k, v) VALUES (?1, ?2)")?;
            for index in 0..keys {
                insert.execute(params![&key(index)[..], value(project_index, index)])?;
            }
        }
        tx.commit()?;
    }
    println!(
        "{}",
        json!({"status":"prepared", "projects":projects, "keys_per_project":keys,
               "v2_stores":v2, "v3_stores":v3, "sqlite_journal":"WAL", "sqlite_synchronous":"FULL",
               "sqlite_version":rusqlite::version()})
    );
    Ok(())
}

fn counter(file: &str, field: &str) -> Result<u64> {
    let contents = fs::read_to_string(file)?;
    let line = contents
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or("missing proc counter")?;
    Ok(line
        .split_whitespace()
        .nth(1)
        .ok_or("invalid proc counter")?
        .parse()?)
}

fn rss_kib() -> Result<u64> {
    counter("/proc/self/status", "VmRSS:")
}

fn fds() -> Result<usize> {
    Ok(fs::read_dir("/proc/self/fd")?.count())
}

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
    Sqlite(Connection),
}

impl Backend {
    fn open(directory: &Path, mode: &str) -> Result<Self> {
        match mode {
            "ordinary" => Ok(Self::Ordinary(Store::open(directory.join("store"))?)),
            "checkpoint" => {
                let digest: [u8; 32] = fs::read(directory.join("digest"))?
                    .try_into()
                    .map_err(|_| "invalid fixture checkpoint digest")?;
                Ok(Self::Checkpoint(CheckpointStore::open(
                    directory.join("store"),
                    directory.join("checkpoint"),
                    &digest,
                )?))
            }
            "sqlite" => {
                let sqlite = Connection::open(directory.join("sqlite.db"))?;
                configure_sqlite(&sqlite)?;
                Ok(Self::Sqlite(sqlite))
            }
            _ => Err("unknown measurement mode".into()),
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(store) => Ok(store.get(key)?),
            Self::Checkpoint(store) => Ok(store.get(key)?),
            Self::Sqlite(store) => {
                use rusqlite::OptionalExtension;
                Ok(store
                    .prepare_cached("SELECT v FROM kv WHERE k=?1")?
                    .query_row([key], |row| row.get(0))
                    .optional()?)
            }
        }
    }
}

fn measure(root: &Path, projects: usize, keys: usize, mode: &str) -> Result<()> {
    let baseline_rss = rss_kib()?;
    let baseline_fds = fds()?;
    let read_bytes_before = counter("/proc/self/io", "read_bytes:")?;
    let rchar_before = counter("/proc/self/io", "rchar:")?;
    let mut stores = Vec::with_capacity(projects);
    let mut open_ns = Vec::with_capacity(projects);
    let start = Instant::now();
    for index in 0..projects {
        let started = Instant::now();
        stores.push(Backend::open(&project(root, index), mode)?);
        open_ns.push(started.elapsed().as_nanos());
    }
    let total_open_ns = start.elapsed().as_nanos();
    let open_rss = rss_kib()?;
    let open_fds = fds()?;
    let mut checks = 0;
    let started = Instant::now();
    for (project_index, store) in stores.iter().enumerate() {
        for index in [0, keys / 2, keys - 1] {
            let actual = store.get(&key(index))?;
            if actual.as_deref() != Some(value(project_index, index).as_slice()) {
                return Err(io::Error::other("fixture read did not match expected data").into());
            }
            checks += 1;
        }
    }
    let lookup_total_ns = started.elapsed().as_nanos();
    let after_reads_rss = rss_kib()?;
    let read_bytes_after = counter("/proc/self/io", "read_bytes:")?;
    let rchar_after = counter("/proc/self/io", "rchar:")?;
    drop(stores);
    println!(
        "{}",
        json!({"status":"verified", "mode":mode, "projects":projects, "keys_per_project":keys,
               "verified_reads":checks, "rss_before_kib":baseline_rss, "rss_open_kib":open_rss,
               "rss_after_reads_kib":after_reads_rss, "rss_after_drop_kib":rss_kib()?,
               "fds_before":baseline_fds, "fds_open":open_fds, "fds_after_drop":fds()?,
               "open_total_ns":total_open_ns, "open_samples_ns":open_ns,
               "lookup_total_ns":lookup_total_ns, "read_bytes":read_bytes_after.saturating_sub(read_bytes_before),
               "rchar":rchar_after.saturating_sub(rchar_before)})
    );
    Ok(())
}

pub(super) fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [operation, root, projects, keys, rest @ ..] = args.as_slice() else {
        return Err(
            "expected: prepare ROOT PROJECTS KEYS or measure ROOT PROJECTS KEYS MODE".into(),
        );
    };
    let projects: usize = projects.parse()?;
    let keys: usize = keys.parse()?;
    if !(1..=128).contains(&projects)
        || !(1..=10_000).contains(&keys)
        || projects
            .checked_mul(keys)
            .is_none_or(|total| total > MAX_TOTAL_KEYS)
    {
        return Err("fixture dimensions exceed allowed limits".into());
    }
    match (operation.as_str(), rest) {
        ("prepare", []) => prepare(Path::new(root), projects, keys),
        ("measure", [mode]) => measure(Path::new(root), projects, keys, mode),
        _ => Err("invalid measurement command".into()),
    }
}
