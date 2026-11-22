//! Phase runner for full-log, authority-backed, and SQLite recovery.
use database_engine::{
    BatchOperation, Entry, RetainedExportLimits, Store,
    retained_base_v2::Limits,
    retained_candidate::{
        AuthorizedRetainedStore, CutoverArchiveBinding, FileRetainedAuthority, IndexLimits,
        ManagedRetainedStore, OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixLimits,
    },
};
use rusqlite::{Connection, OpenFlags, config::DbConfig, params};
use rustix::time::{ClockId, clock_gettime};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io, num::NonZeroU64, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;

const BATCH: u64 = 128;
const CHURN: u64 = 4096;
const STANDARD_KEYS: u64 = 262_144;
const MAX_KEYS: u64 = 1_048_576;
const THRESHOLD: NonZeroU64 = NonZeroU64::new(640 * 1024).unwrap();
const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 32 << 20,
        entries: STANDARD_KEYS,
        key_bytes: STANDARD_KEYS * 8,
        work_bytes: (32 << 20) + 1,
    },
    index: IndexLimits {
        blocks: 1024,
        anchor_bytes: 16 << 10,
    },
    suffix: SuffixLimits {
        bytes: 768 << 10,
        operations: 8192,
        work: 1_000_000,
    },
    overlay: OverlayLimits {
        entries: 8192,
        key_bytes: 64 << 10,
    },
};

enum Backend {
    Ordinary(Box<Store>),
    Retained(Box<Catalog>),
    Sqlite(Connection),
}

fn sqlite(path: &Path, create: bool) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE;
    let connection = if create {
        fs::create_dir(path)?;
        Connection::open_with_flags(path.join("kv.db"), flags | OpenFlags::SQLITE_OPEN_CREATE)?
    } else {
        Connection::open_with_flags(path.join("kv.db"), flags)?
    };
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    let journal: String = connection.query_row(
        if create {
            "PRAGMA journal_mode=WAL"
        } else {
            "PRAGMA journal_mode"
        },
        [],
        |row| row.get(0),
    )?;
    let synchronous: i64 = connection.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if journal != "wal" || synchronous != 2 {
        return Err("SQLite requires WAL and synchronous=FULL".into());
    }
    if create {
        connection.execute_batch(
            "CREATE TABLE kv(key BLOB PRIMARY KEY, value BLOB NOT NULL, crc INTEGER NOT NULL) WITHOUT ROWID;",
        )?;
    }
    Ok(connection)
}

fn checked_sqlite_value(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<Vec<u8>> {
    let value: Vec<u8> = row.get(column)?;
    let crc: u32 = row.get(column + 1)?;
    if crc32fast::hash(&value) != crc {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Blob,
            std::io::Error::new(std::io::ErrorKind::InvalidData, "SQLite value CRC mismatch")
                .into(),
        ));
    }
    Ok(value)
}

impl Backend {
    fn open(root: &Path, name: &str, limits: RetainedStoreLimits) -> Result<Self> {
        match name {
            "ordinary" => Ok(Self::Ordinary(Box::new(Store::open(root.join(name))?))),
            "retained" => {
                let provider = FileRetainedAuthority::open(root.join("authority"))?;
                let store =
                    Catalog::open_with_auto_reclaim(root.join(name), limits, provider, THRESHOLD)?;
                Ok(Self::Retained(Box::new(store)))
            }
            "sqlite" => Ok(Self::Sqlite(sqlite(&root.join(name), false)?)),
            _ => Err("invalid backend".into()),
        }
    }

    fn write(&mut self, operations: &[BatchOperation<'_>]) -> Result<()> {
        match self {
            Self::Ordinary(store) => store.write_batch(operations)?,
            Self::Retained(store) => store.write_batch(operations)?,
            Self::Sqlite(connection) => {
                let transaction = connection.transaction()?;
                {
                    let mut put = transaction.prepare_cached(
                        "INSERT INTO kv VALUES (?1,?2,?3) ON CONFLICT(key) DO UPDATE SET value=excluded.value,crc=excluded.crc",
                    )?;
                    let mut delete = transaction.prepare_cached("DELETE FROM kv WHERE key=?1")?;
                    for operation in operations {
                        match operation {
                            BatchOperation::Put { key, value } => {
                                put.execute(params![key, value, crc32fast::hash(value)])?;
                            }
                            BatchOperation::Delete { key } => {
                                delete.execute([key])?;
                            }
                        }
                    }
                }
                transaction.commit()?;
            }
        }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Ordinary(store) => store.get(key)?.ok_or("missing probe key")?,
            Self::Retained(store) => store.get(key, 96)?.ok_or("missing probe key")?,
            Self::Sqlite(connection) => {
                connection.query_row("SELECT value,crc FROM kv WHERE key=?1", [key], |row| {
                    checked_sqlite_value(row, 0)
                })?
            }
        })
    }

    fn page(&mut self, after: Option<&[u8]>) -> Result<Vec<Entry>> {
        match self {
            Self::Ordinary(store) => Ok(store.scan(after, None, 1024)?),
            Self::Retained(store) => Ok(store.scan(after, None, 1024)?),
            Self::Sqlite(connection) => {
                let mut query = connection.prepare_cached(
                    "SELECT key,value,crc FROM kv WHERE key > ?1 ORDER BY key LIMIT 1024",
                )?;
                let rows = query.query_map([after.unwrap_or_default()], |row| {
                    Ok((row.get(0)?, checked_sqlite_value(row, 1)?))
                })?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            }
        }
    }
}

fn value(key: u64, epoch: u64) -> [u8; 96] {
    let mut bytes = [0; 96];
    let mut word = key ^ epoch.rotate_left(21);
    for part in bytes.chunks_exact_mut(8) {
        word = word.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mixed = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        part.copy_from_slice(&mixed.to_le_bytes());
    }
    bytes
}

fn deleted(key: u64, epoch: u64) -> bool {
    epoch != 0 && (key + epoch) % 11 == 0
}

fn cpu_ns() -> i128 {
    let time = clock_gettime(ClockId::ProcessCPUTime);
    i128::from(time.tv_sec) * 1_000_000_000 + i128::from(time.tv_nsec)
}

fn apply(store: &mut Backend, count: u64, epoch: u64) -> Result<()> {
    for start in (0..count).step_by(BATCH as usize) {
        let rows = (start..(start + BATCH).min(count))
            .map(|key| (key, key.to_be_bytes(), value(key, epoch)))
            .collect::<Vec<_>>();
        let operations = rows
            .iter()
            .map(|(id, key, value)| {
                if deleted(*id, epoch) {
                    BatchOperation::Delete { key }
                } else {
                    BatchOperation::Put { key, value }
                }
            })
            .collect::<Vec<_>>();
        store.write(&operations)?;
    }
    Ok(())
}

fn verify(
    mut next_page: impl FnMut(Option<&[u8]>) -> Result<Vec<Entry>>,
    keys: u64,
    epoch: u64,
) -> Result<()> {
    let mut expected = (0..keys).filter_map(|id| {
        let revision = if id < CHURN { epoch } else { 0 };
        (!deleted(id, revision)).then(|| (id.to_be_bytes(), value(id, revision)))
    });
    let mut after = None;
    let mut checked = 0;
    loop {
        let page = next_page(after.as_deref())?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            let (expected_key, expected_value) = expected.next().ok_or("extra entry")?;
            if key.as_slice() != expected_key || value.as_slice() != expected_value {
                return Err(format!("model mismatch after {checked} verified entries").into());
            }
            checked += 1;
        }
        after = page.last().map(|(key, _)| key.clone());
    }
    if expected.next().is_some() {
        return Err("missing entry".into());
    }
    println!("verified_entries={checked}");
    Ok(())
}

fn bind_legacy_archive(root: &Path) -> Result<()> {
    let source = root.join("retained");
    let _store = Store::open(&source)?;
    let mut log_digest = Sha256::new();
    let log_bytes = io::copy(
        &mut fs::File::open(source.join("records.log"))?,
        &mut log_digest,
    )?;
    let witness_digest = Sha256::digest(fs::read(source.join("commit.state"))?);
    let mut bytes = [0; 72];
    bytes[..8].copy_from_slice(&log_bytes.to_le_bytes());
    bytes[8..40].copy_from_slice(&log_digest.finalize());
    bytes[40..].copy_from_slice(&witness_digest);
    fs::write(root.join("archive-binding"), bytes)?;
    Ok(())
}

fn run_lifecycle(root: &Path, limits: RetainedStoreLimits, keys: u64, epoch: u64) -> Result<()> {
    let bytes: [u8; 72] = fs::read(root.join("archive-binding"))?
        .try_into()
        .map_err(|_| "invalid saved archive binding length")?;
    let archive = CutoverArchiveBinding {
        log_bytes: u64::from_le_bytes(bytes[..8].try_into()?),
        log_digest: bytes[8..40].try_into()?,
        witness_digest: bytes[40..].try_into()?,
    };
    let provider = FileRetainedAuthority::open(root.join("authority"))?;
    let mut catalog =
        Catalog::open_with_auto_reclaim(root.join("retained"), limits, provider, THRESHOLD)?;
    let backup = root.join("backup");
    let restored = root.join("restored");
    let start = Instant::now();
    let expected = catalog.backup_to(&backup)?;
    println!("backup_ns={}", start.elapsed().as_nanos());
    let start = Instant::now();
    if !catalog.retire_cutover_archive(&archive, &backup, &expected)? {
        return Err("expected to retire a legacy archive".into());
    }
    println!(
        "retire_ns={} retired_log_bytes={}",
        start.elapsed().as_nanos(),
        archive.log_bytes
    );
    let start = Instant::now();
    ManagedRetainedStore::restore_backup(&backup, &restored, limits, &expected)?;
    println!("restore_ns={}", start.elapsed().as_nanos());
    let restored = ManagedRetainedStore::open(restored, limits)?;
    verify(|after| Ok(restored.scan(after, None, 1024)?), keys, epoch)?;
    drop(catalog);
    let mut source = Backend::open(root, "retained", limits)?;
    verify(|after| source.page(after), keys, epoch)?;
    Ok(())
}

fn authority_state(root: &Path) -> Result<(u64, u64, [u8; 16])> {
    let record = FileRetainedAuthority::open(root.join("authority"))?
        .read()?
        .ok_or("missing authority record")?;
    if record.pending.is_some() {
        return Err("pending authority at successful benchmark phase".into());
    }
    Ok((
        record.revision,
        record.committed.suffix_end,
        record.committed.generation,
    ))
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let [_, phase, root, name, keys, epoch] = args.as_slice() else {
        return Err("usage: authorized_recovery_bench <setup|bind-archive|cutover|mutate|reopen|verify|lifecycle> <root> <ordinary|retained|sqlite|both|all> <keys> <epoch>".into());
    };
    let root = Path::new(root);
    let keys: u64 = keys.parse()?;
    let epoch: u64 = epoch.parse()?;
    if !(CHURN..=MAX_KEYS).contains(&keys) || epoch > 1000 {
        return Err("keys must be 4096–1048576 and epoch must be 0–1000".into());
    }
    let mut limits = LIMITS;
    if keys > STANDARD_KEYS {
        limits.base = Limits {
            file_bytes: 128 << 20,
            entries: MAX_KEYS,
            key_bytes: MAX_KEYS * 8,
            work_bytes: (128 << 20) + 1,
        };
    }
    match std::env::var("EMDA_BENCH_SUFFIX_WORK") {
        Ok(work) => limits.suffix.work = work.parse()?,
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    if limits.suffix.work == 0 {
        return Err("EMDA_BENCH_SUFFIX_WORK must be positive".into());
    }

    match phase.as_str() {
        "setup" => {
            fs::create_dir(root)?;
            for name in ["ordinary", "retained"] {
                let mut store = Backend::Ordinary(Box::new(Store::open(root.join(name))?));
                apply(&mut store, keys, 0)?;
            }
            if name == "all" {
                let mut store = Backend::Sqlite(sqlite(&root.join("sqlite"), true)?);
                apply(&mut store, keys, 0)?;
            }
        }
        "bind-archive" => bind_legacy_archive(root)?,
        "cutover" => {
            let path = root.join("retained");
            let source = Store::open(&path)?;
            let record = Catalog::cutover_from_store(
                source,
                &path,
                RetainedExportLimits {
                    max_log_bytes: limits.base.file_bytes * 2,
                    max_live_keys: limits.base.entries as usize,
                    max_live_key_bytes: limits.base.key_bytes as usize,
                    max_output_bytes: limits.base.file_bytes,
                },
                limits,
            )?;
            let mut provider = FileRetainedAuthority::create(root.join("authority"))?;
            if !provider.compare_and_swap(None, record)? {
                return Err("failed to register fresh authority".into());
            }
        }
        "mutate" | "reopen" | "verify" => {
            if phase == "mutate" && epoch == 0 {
                return Err("mutation epochs start at 1".into());
            }
            let start = Instant::now();
            let cpu_start = cpu_ns();
            let mut store = Backend::open(root, name, limits)?;
            let open_cpu = cpu_ns() - cpu_start;
            println!(
                "open_ns={} open_cpu_ns={open_cpu}",
                start.elapsed().as_nanos()
            );
            if phase == "mutate" {
                let before = (name == "retained")
                    .then(|| authority_state(root))
                    .transpose()?;
                let start = Instant::now();
                let cpu_start = cpu_ns();
                apply(&mut store, CHURN, epoch)?;
                let write_cpu = cpu_ns() - cpu_start;
                let write_time = start.elapsed().as_nanos();
                println!(
                    "mutate_ns={write_time} mutate_cpu_ns={write_cpu} batches={} operations={CHURN}",
                    CHURN / BATCH
                );
                if let Some((revision, _, _)) = before {
                    let (end, _, _) = authority_state(root)?;
                    let revisions = end
                        .checked_sub(revision + 2 * (CHURN / BATCH))
                        .ok_or("unexpected authority revision count")?;
                    if revisions % 2 != 0 {
                        return Err("unpaired rotation and reclamation revisions".into());
                    }
                    println!("rotations={}", revisions / 2);
                }
            } else if phase == "verify" {
                let start = Instant::now();
                let cpu_start = cpu_ns();
                verify(|after| store.page(after), keys, epoch)?;
                let verify_cpu = cpu_ns() - cpu_start;
                println!(
                    "verify_ns={} verify_cpu_ns={verify_cpu}",
                    start.elapsed().as_nanos()
                );
            } else {
                let (id, revision) = (0..keys)
                    .rev()
                    .map(|id| (id, if id < CHURN { epoch } else { 0 }))
                    .find(|&(id, revision)| !deleted(id, revision))
                    .ok_or("fixture has no probe key")?;
                if store.get(&id.to_be_bytes())? != value(id, revision) {
                    return Err("first read did not match the workload model".into());
                }
                let ready_cpu = cpu_ns() - cpu_start;
                println!(
                    "ready_ns={} ready_cpu_ns={ready_cpu}",
                    start.elapsed().as_nanos()
                );
            }
            if name == "retained" {
                let (revision, suffix, generation) = authority_state(root)?;
                assert!(suffix <= limits.suffix.bytes);
                println!("revision={revision} suffix_bytes={suffix} generation={generation:02x?}");
            } else if name == "ordinary" {
                println!(
                    "log_bytes={}",
                    fs::metadata(root.join("ordinary/records.log"))?.len()
                );
            } else {
                let wal = root.join("sqlite/kv.db-wal");
                println!(
                    "sqlite_version={} wal_bytes={}",
                    rusqlite::version(),
                    fs::metadata(wal)?.len()
                );
            }
        }
        "lifecycle" => run_lifecycle(root, limits, keys, epoch)?,
        _ => return Err("invalid phase".into()),
    }
    println!("suffix_work={}", limits.suffix.work);
    println!("phase={phase} backend={name} keys={keys} epoch={epoch}");
    Ok(())
}
