//! Phase runner for full-log, checkpoint, authority-backed, and SQLite recovery.
use database_engine::{
    BatchOperation, CheckpointStore, CheckpointStoreLimits, Entry, RetainedExportLimits, Store,
    retained_base_v2::Limits,
    retained_candidate::{
        AuthorizedRetainedStore, CutoverArchiveBinding, FileRetainedAuthority, IndexLimits,
        ManagedRetainedStore, OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixLimits,
    },
};
use rusqlite::{Connection, OpenFlags, config::DbConfig, params};
use rustix::time::{ClockId, clock_gettime};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io, num::NonZeroU64, path::Path, sync::Mutex, thread, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;

const BATCH: u64 = 128;
const DEFAULT_CHURN: u64 = 4096;
const WIDE_CHURN: u64 = 32_768;
const STANDARD_KEYS: u64 = 262_144;
const MAX_KEYS: u64 = 1_048_576;
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
    Checkpoint(Box<CheckpointStore>),
    Retained(Box<Catalog>),
    Sqlite(Connection),
}

fn threshold(limits: RetainedStoreLimits) -> NonZeroU64 {
    NonZeroU64::new(limits.suffix.bytes / 6 * 5).expect("benchmark suffix budget is positive")
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
            "checkpoint" => Ok(Self::Checkpoint(Box::new(open_checkpoint(
                &root.join(name),
            )?))),
            "retained" => {
                let provider = FileRetainedAuthority::open(root.join("authority"))?;
                let store = Catalog::open_with_auto_reclaim(
                    root.join(name),
                    limits,
                    provider,
                    threshold(limits),
                )?;
                Ok(Self::Retained(Box::new(store)))
            }
            "sqlite" => Ok(Self::Sqlite(sqlite(&root.join(name), false)?)),
            _ => Err("invalid backend".into()),
        }
    }

    fn write(&mut self, operations: &[BatchOperation<'_>]) -> Result<()> {
        match self {
            Self::Ordinary(store) => store.write_batch(operations)?,
            Self::Checkpoint(store) => store.write_batch(operations)?,
            Self::Retained(store) => store.write_batch(operations)?,
            Self::Sqlite(connection) => write_sqlite(connection, operations)?,
        }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Ordinary(store) => store.get(key)?.ok_or("missing probe key")?,
            Self::Checkpoint(store) => store.get(key)?.ok_or("missing probe key")?,
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
            Self::Checkpoint(store) => Ok(store.scan(after, None, 1024)?),
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

fn open_checkpoint(path: &Path) -> io::Result<CheckpointStore> {
    CheckpointStore::open_managed_with_options(
        path,
        CheckpointStoreLimits {
            max_tail_bytes: 2 << 20,
            ..CheckpointStoreLimits::default()
        },
    )
}

fn checkpoint_lengths(root: &Path, store: &CheckpointStore) -> Result<(u64, u64)> {
    let length = fs::metadata(root.join("checkpoint/records.log"))?.len();
    let boundary = length
        .checked_sub(store.tail_bytes()?)
        .ok_or("checkpoint suffix exceeds its log")?;
    Ok((length, boundary))
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

fn write_sqlite(connection: &mut Connection, operations: &[BatchOperation<'_>]) -> Result<()> {
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
    Ok(())
}

fn apply_batches(
    starts: impl Iterator<Item = u64>,
    count: u64,
    epoch: u64,
    mut write: impl FnMut(&[BatchOperation<'_>]) -> Result<()>,
) -> Result<()> {
    for start in starts {
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
        write(&operations)?;
    }
    Ok(())
}

fn apply(store: &mut Backend, count: u64, epoch: u64) -> Result<()> {
    apply_batches((0..count).step_by(BATCH as usize), count, epoch, |ops| {
        store.write(ops)
    })
}

fn run_writers(
    count: u64,
    epoch: u64,
    writers: usize,
    write_batches: impl Fn(std::iter::StepBy<std::ops::Range<u64>>, u64, u64) -> Result<()> + Sync,
) -> Result<()> {
    let start = Mutex::new(());
    thread::scope(|scope| -> Result<()> {
        // Release waiting writers only after launch. Dropping on a spawn error
        // also releases them so the scope can join the threads already started.
        let release = start.lock().map_err(|_| "writer start gate poisoned")?;
        let mut workers = Vec::with_capacity(writers);
        for writer in 0..writers {
            let write_batches = &write_batches;
            let start = &start;
            workers.push(thread::Builder::new().stack_size(256 * 1024).spawn_scoped(
                scope,
                move || {
                    drop(start.lock().map_err(|_| "writer start gate poisoned")?);
                    let starts = (writer as u64 * BATCH..count).step_by(writers * BATCH as usize);
                    write_batches(starts, count, epoch).map_err(|error| error.to_string())
                },
            )?);
        }
        drop(release);
        for worker in workers {
            worker.join().map_err(|_| "writer panicked")??;
        }
        Ok(())
    })
}

fn apply_concurrently(
    store: &mut Backend,
    root: &Path,
    count: u64,
    epoch: u64,
    writers: usize,
) -> Result<()> {
    match store {
        Backend::Ordinary(store) => run_writers(count, epoch, writers, |starts, count, epoch| {
            apply_batches(starts, count, epoch, |ops| Ok(store.write_batch(ops)?))
        }),
        Backend::Checkpoint(store) => run_writers(count, epoch, writers, |starts, count, epoch| {
            apply_batches(starts, count, epoch, |ops| Ok(store.write_batch(ops)?))
        }),
        Backend::Retained(store) => {
            // Authorized writes require exclusive access to one authority owner.
            let store = Mutex::new(store);
            run_writers(count, epoch, writers, |starts, count, epoch| {
                apply_batches(starts, count, epoch, |ops| {
                    let mut store = store.lock().map_err(|_| "authority owner poisoned")?;
                    Ok(store.write_batch(ops)?)
                })
            })
        }
        Backend::Sqlite(_) => run_writers(count, epoch, writers, |starts, count, epoch| {
            let mut connection = sqlite(&root.join("sqlite"), false)?;
            connection.busy_timeout(std::time::Duration::from_secs(5))?;
            apply_batches(starts, count, epoch, |ops| {
                write_sqlite(&mut connection, ops)
            })
        }),
    }
}

fn verify(
    mut next_page: impl FnMut(Option<&[u8]>) -> Result<Vec<Entry>>,
    keys: u64,
    epoch: u64,
    churn: u64,
) -> Result<()> {
    let mut expected = (0..keys).filter_map(|id| {
        let revision = if id < churn { epoch } else { 0 };
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

fn run_lifecycle(
    root: &Path,
    limits: RetainedStoreLimits,
    keys: u64,
    epoch: u64,
    authorized: bool,
    churn: u64,
) -> Result<()> {
    let bytes: [u8; 72] = fs::read(root.join("archive-binding"))?
        .try_into()
        .map_err(|_| "invalid saved archive binding length")?;
    let archive = CutoverArchiveBinding {
        log_bytes: u64::from_le_bytes(bytes[..8].try_into()?),
        log_digest: bytes[8..40].try_into()?,
        witness_digest: bytes[40..].try_into()?,
    };
    let provider = FileRetainedAuthority::open(root.join("authority"))?;
    let mut catalog = Catalog::open_with_auto_reclaim(
        root.join("retained"),
        limits,
        provider,
        threshold(limits),
    )?;
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
    if authorized {
        let source_authority = FileRetainedAuthority::open(root.join("authority"))?
            .read()?
            .ok_or("missing source authority")?;
        let mut receipt = FileRetainedAuthority::create(root.join("restore-receipt"))?;
        let prepared = Catalog::prepare_from_backup_recorded(
            &backup,
            &restored,
            limits,
            None,
            &expected,
            |prepared| {
                if !receipt.compare_and_swap(None, prepared)? {
                    return Err(io::Error::other("restore receipt already exists"));
                }
                Ok(())
            },
        )?;
        println!("restore_prepare_ns={}", start.elapsed().as_nanos());
        let saved = receipt.read()?.ok_or("missing recorded restore receipt")?;
        assert_eq!(saved, prepared);
        assert_ne!(saved.committed.catalog, source_authority.committed.catalog);
        assert_ne!(
            saved.committed.generation,
            source_authority.committed.generation
        );
        assert_eq!(saved.committed.suffix_digest, expected.digest);
        let authority = root.join("restored-authority");
        let register = Instant::now();
        drop(Catalog::register_prepared(
            &restored,
            limits,
            FileRetainedAuthority::create(&authority)?,
            saved,
        )?);
        println!("restore_register_ns={}", register.elapsed().as_nanos());
        println!("restore_ns={}", start.elapsed().as_nanos());
        let mut restored =
            Catalog::open(&restored, limits, FileRetainedAuthority::open(authority)?)?;
        verify(
            |after| Ok(restored.scan(after, None, 1024)?),
            keys,
            epoch,
            churn,
        )?;
        assert_eq!(
            FileRetainedAuthority::open(root.join("authority"))?.read()?,
            Some(source_authority)
        );
    } else {
        ManagedRetainedStore::restore_backup(&backup, &restored, limits, &expected)?;
        println!("restore_ns={}", start.elapsed().as_nanos());
        let restored = ManagedRetainedStore::open(restored, limits)?;
        verify(
            |after| Ok(restored.scan(after, None, 1024)?),
            keys,
            epoch,
            churn,
        )?;
    }
    drop(catalog);
    let mut source = Backend::open(root, "retained", limits)?;
    verify(|after| source.page(after), keys, epoch, churn)?;
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
    let [_, phase, root, name, keys, epoch, options @ ..] = args.as_slice() else {
        return Err("usage: authorized_recovery_bench <setup|checkpoint-setup|bind-archive|cutover|mutate|reopen|verify|lifecycle> <root> <ordinary|checkpoint|retained|sqlite|both|all> <keys> <epoch> [--wide-suffix] [--writers=N]".into());
    };
    let mut churn = DEFAULT_CHURN;
    let mut writers = 1;
    for option in options {
        if option == "--wide-suffix" {
            churn = WIDE_CHURN;
        } else if let Some(value) = option.strip_prefix("--writers=") {
            writers = value.parse()?;
        } else {
            return Err("unexpected benchmark option".into());
        }
    }
    if !(1..=16).contains(&writers) {
        return Err("writers must be 1–16".into());
    }
    let root = Path::new(root);
    let keys: u64 = keys.parse()?;
    let epoch: u64 = epoch.parse()?;
    if !(churn..=MAX_KEYS).contains(&keys) || epoch > 1000 {
        return Err(format!("keys must be {churn}–1048576 and epoch must be 0–1000").into());
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
    if churn == WIDE_CHURN {
        limits.suffix = SuffixLimits {
            bytes: 12 << 20,
            operations: WIDE_CHURN * 4,
            work: 12_000_000_000,
        };
        limits.overlay = OverlayLimits {
            entries: WIDE_CHURN as usize,
            key_bytes: WIDE_CHURN as usize * 8,
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
        "checkpoint-setup" if name == "checkpoint" => {
            let path = root.join(name);
            let mut store = Backend::Ordinary(Box::new(Store::open(&path)?));
            apply(&mut store, keys, 0)?;
            drop(store);
            CheckpointStore::rebuild_managed_selection(&path, path.join("initial.index"))?;
            open_checkpoint(&path)?.enable_automatic_checkpoints()?;
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
                let checkpoint_before = match &store {
                    Backend::Checkpoint(store) => Some(checkpoint_lengths(root, store)?.1),
                    _ => None,
                };
                let start = Instant::now();
                let cpu_start = cpu_ns();
                if writers == 1 {
                    apply(&mut store, churn, epoch)?;
                } else {
                    apply_concurrently(&mut store, root, churn, epoch, writers)?;
                }
                let write_cpu = cpu_ns() - cpu_start;
                let write_time = start.elapsed().as_nanos();
                println!(
                    "mutate_ns={write_time} mutate_cpu_ns={write_cpu} batches={} operations={churn} writers={writers}",
                    churn / BATCH
                );
                if let Some((revision, _, _)) = before {
                    let (end, _, _) = authority_state(root)?;
                    let revisions = end
                        .checked_sub(revision + 2 * (churn / BATCH))
                        .ok_or("unexpected authority revision count")?;
                    if revisions % 2 != 0 {
                        return Err("unpaired rotation and reclamation revisions".into());
                    }
                    println!("rotations={}", revisions / 2);
                }
                if let (Some(before), Backend::Checkpoint(store)) = (checkpoint_before, &store) {
                    let (_, after) = checkpoint_lengths(root, store)?;
                    println!("checkpoint_advanced={}", u8::from(after > before));
                }
            } else if phase == "verify" {
                let start = Instant::now();
                let cpu_start = cpu_ns();
                verify(|after| store.page(after), keys, epoch, churn)?;
                let verify_cpu = cpu_ns() - cpu_start;
                println!(
                    "verify_ns={} verify_cpu_ns={verify_cpu}",
                    start.elapsed().as_nanos()
                );
            } else {
                let (id, revision) = (0..keys)
                    .rev()
                    .map(|id| (id, if id < churn { epoch } else { 0 }))
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
            } else if let Backend::Checkpoint(store) = &store {
                let (length, boundary) = checkpoint_lengths(root, store)?;
                println!(
                    "log_bytes={length} checkpoint_boundary={boundary} suffix_bytes={}",
                    length - boundary
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
        "lifecycle" | "authorized-lifecycle" => {
            run_lifecycle(
                root,
                limits,
                keys,
                epoch,
                phase == "authorized-lifecycle",
                churn,
            )?;
        }
        _ => return Err("invalid phase".into()),
    }
    println!("suffix_work={}", limits.suffix.work);
    println!("churn={churn}");
    println!("phase={phase} backend={name} keys={keys} epoch={epoch}");
    Ok(())
}
