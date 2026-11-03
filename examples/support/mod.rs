//! Shared, checksum-verifying backends for the local benchmark examples.
use database_engine::Store;
use rusqlite::{Connection, params};
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub(crate) type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) enum Worker {
    Prototype(Arc<Store>),
    Sqlite(Connection),
}

impl Worker {
    pub(crate) fn read_value(&mut self, key: &[u8]) -> BenchResult<Vec<u8>> {
        match self {
            Self::Prototype(store) => Ok(store.get(key)?.ok_or("missing key")?),
            Self::Sqlite(connection) => {
                let mut statement =
                    connection.prepare_cached("SELECT value, checksum FROM kv WHERE key = ?1")?;
                let (value, expected): (Vec<u8>, u32) =
                    statement.query_row([key], |row| Ok((row.get(0)?, row.get(1)?)))?;
                if crc32fast::hash(&value) != expected {
                    return Err("SQLite value checksum mismatch".into());
                }
                Ok(value)
            }
        }
    }

    pub(crate) fn read(&mut self, key: &[u8], value_size: usize) -> BenchResult<()> {
        let value = self.read_value(key)?;
        if value.len() != value_size || value.iter().any(|byte| *byte != value[0]) {
            return Err("read returned an invalid value".into());
        }
        Ok(())
    }

    pub(crate) fn write(&mut self, key: &[u8], value: &[u8]) -> BenchResult<()> {
        match self {
            Self::Prototype(store) => store.put(key, value)?,
            Self::Sqlite(connection) => {
                let mut statement = connection.prepare_cached(
                    "INSERT INTO kv(key, value, checksum) VALUES (?1, ?2, ?3) \
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value, checksum = excluded.checksum",
                )?;
                statement.execute(params![key, value, crc32fast::hash(value)])?;
            }
        }
        Ok(())
    }
}

pub(crate) fn sqlite_connection(path: &Path) -> BenchResult<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(30))?;
    connection.execute_batch("PRAGMA synchronous = FULL;")?;
    Ok(connection)
}

pub(crate) fn show_latency(label: &str, samples: &mut [u64]) {
    if samples.is_empty() {
        println!("{label}: count=0");
        return;
    }
    samples.sort_unstable();
    let p = |per_mille: u64| -> f64 { nearest_rank(samples, per_mille) as f64 / 1_000.0 };
    println!(
        "{label}: count={} p50_us={:.1} p95_us={:.1} p99_us={:.1} p99_9_us={:.1} max_us={:.1}",
        samples.len(),
        p(500),
        p(950),
        p(990),
        p(999),
        p(1000)
    );
}

// `samples` must already be sorted, and per_mille must be between 1 and 1000.
pub(crate) fn nearest_rank(samples: &[u64], per_mille: u64) -> u64 {
    debug_assert!(!samples.is_empty() && (1..=1000).contains(&per_mille));
    let rank = (samples.len() as u64 * per_mille).div_ceil(1000);
    samples[(rank - 1) as usize]
}
