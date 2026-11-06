//! Linux SQLite companion for the clean-restart resource diagnostic.
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::json;
use std::error::Error;
use std::io;
use std::path::Path;
use std::sync::Barrier;
use std::time::{Duration, Instant};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const VALUE_BYTES: usize = 64;

fn key(number: usize, length: usize) -> Vec<u8> {
    let mut key = (number as u64).to_be_bytes().to_vec();
    key.resize(length, 0x5a);
    key
}

fn parse(value: &str, min: usize, max: usize) -> BenchResult<usize> {
    let value = value.parse()?;
    if !(min..=max).contains(&value) {
        return Err("benchmark argument out of range".into());
    }
    Ok(value)
}

fn configure(connection: &Connection) -> BenchResult<()> {
    connection.busy_timeout(Duration::from_secs(30))?;
    connection.execute_batch("PRAGMA synchronous = FULL;")?;
    let journal: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if journal != "wal" || synchronous != 2 {
        return Err("SQLite requires WAL and synchronous=FULL".into());
    }
    Ok(())
}

fn open(path: &Path) -> BenchResult<Connection> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    configure(&connection)?;
    Ok(connection)
}

fn prepare(parent: &Path, count: usize, key_len: usize, tail: usize) -> BenchResult<()> {
    if !parent.is_dir() {
        return Err("fixture parent must exist".into());
    }
    let path = parent.join("sqlite.db");
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => return Err("fixture already exists".into()),
    }
    let mut connection = Connection::open(&path)?;
    let journal: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    if journal != "wal" {
        return Err("SQLite WAL mode is unavailable".into());
    }
    configure(&connection)?;
    connection.execute_batch("CREATE TABLE kv(key BLOB PRIMARY KEY, value BLOB NOT NULL, checksum INTEGER NOT NULL) WITHOUT ROWID;")?;
    for start in (0..count + tail).step_by(512) {
        let end = (start + 512).min(count + tail);
        let transaction = connection.transaction()?;
        {
            let mut insert =
                transaction.prepare("INSERT INTO kv(key,value,checksum) VALUES (?1,?2,?3)")?;
            for number in start..end {
                let value = vec![(number % 251) as u8; VALUE_BYTES];
                insert.execute(params![
                    key(number, key_len),
                    value,
                    crc32fast::hash(&value)
                ])?;
            }
        }
        transaction.commit()?;
    }
    let (busy, _, _): (u32, i32, i32) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if busy != 0 {
        return Err("SQLite fixture checkpoint was busy".into());
    }
    drop(connection);
    println!(
        "{}",
        json!({"keys":count,"tail_keys":tail,"key_bytes":key_len,
        "database_bytes":std::fs::metadata(path)?.len(),"sqlite_version":rusqlite::version(),
        "journal":"wal","synchronous":"FULL"})
    );
    Ok(())
}

fn verify(connection: &Connection, number: usize, key_len: usize) -> BenchResult<()> {
    let row: Option<(Vec<u8>, u32)> = connection
        .prepare_cached("SELECT value,checksum FROM kv WHERE key=?1")?
        .query_row([key(number, key_len)], |row| Ok((row.get(0)?, row.get(1)?)))
        .optional()?;
    let (value, checksum) = row.ok_or("missing SQLite value")?;
    if value.len() != VALUE_BYTES
        || value.iter().any(|byte| *byte != (number % 251) as u8)
        || crc32fast::hash(&value) != checksum
    {
        return Err(format!("incorrect SQLite value for key {number}").into());
    }
    Ok(())
}

fn proc_field(path: &str, field: &str) -> BenchResult<u64> {
    let contents = std::fs::read_to_string(path)?;
    Ok(contents
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or("missing process metric")?
        .split_whitespace()
        .nth(1)
        .ok_or("invalid process metric")?
        .parse()?)
}

struct ProcessIo {
    rchar: u64,
    wchar: u64,
    read_bytes: u64,
    write_bytes: u64,
    read_calls: u64,
    write_calls: u64,
}
impl ProcessIo {
    fn now() -> BenchResult<Self> {
        let field = |name| proc_field("/proc/self/io", name);
        Ok(Self {
            rchar: field("rchar:")?,
            wchar: field("wchar:")?,
            read_bytes: field("read_bytes:")?,
            write_bytes: field("write_bytes:")?,
            read_calls: field("syscr:")?,
            write_calls: field("syscw:")?,
        })
    }
    fn delta(&self, before: &Self) -> serde_json::Value {
        json!({"rchar":self.rchar.saturating_sub(before.rchar),"wchar":self.wchar.saturating_sub(before.wchar),
            "read_bytes":self.read_bytes.saturating_sub(before.read_bytes),"write_bytes":self.write_bytes.saturating_sub(before.write_bytes),
            "read_calls":self.read_calls.saturating_sub(before.read_calls),"write_calls":self.write_calls.saturating_sub(before.write_calls)})
    }
}

fn run(args: &[String]) -> BenchResult<()> {
    let [
        parent,
        base,
        tail,
        key_len,
        workers,
        operations,
        epoch,
        phase,
    ] = args
    else {
        return Err("usage: sqlite_restart_write_bench run <parent> <base-keys> <tail-keys> <key-bytes> <workers> <writes-per-worker> <epoch> <write|verify>".into());
    };
    let parent = Path::new(parent);
    let base = parse(base, 1, 1_000_000)?;
    let tail = parse(tail, 0, 4096)?;
    let key_len = parse(key_len, 8, 1024)?;
    let workers = parse(workers, 1, 32)?;
    let operations = parse(operations, 1, 1024)?;
    let epoch = parse(epoch, 0, 32)?;
    if phase != "write" && phase != "verify" {
        return Err("unknown phase".into());
    }
    let path = parent.join("sqlite.db");
    let prior = epoch * workers * operations;
    let current = if phase == "write" {
        workers * operations
    } else {
        0
    };
    let first = base + tail;
    let io_before = ProcessIo::now()?;
    let start = Instant::now();
    let connection = open(&path)?;
    let open_us = start.elapsed().as_micros();
    let rss_after_open_kib = proc_field("/proc/self/status", "VmRSS:")?;
    let integrity_started = Instant::now();
    let check: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    let integrity_check_us = integrity_started.elapsed().as_micros();
    if check != "ok" {
        return Err(format!("SQLite integrity check failed: {check}").into());
    }
    for index in 0..16 {
        verify(&connection, index * (base - 1) / 15, key_len)?;
        if tail > 0 {
            verify(&connection, base + index * (tail - 1) / 15, key_len)?;
        }
    }
    for number in first..first + prior {
        verify(&connection, number, key_len)?;
    }
    let mut latencies_ns = Vec::with_capacity(current);
    let mut elapsed_ns = 0;
    if phase == "write" {
        let mut connections = Vec::with_capacity(workers);
        for _ in 0..workers {
            connections.push(open(&path)?);
        }
        let barrier = Barrier::new(workers + 1);
        std::thread::scope(|scope| -> BenchResult<()> {
            let mut threads = Vec::new();
            for (worker, connection) in connections.into_iter().enumerate() {
                let barrier = &barrier;
                threads.push(scope.spawn(move || -> BenchResult<Vec<u128>> {
                    let mut samples = Vec::with_capacity(operations);
                    barrier.wait();
                    // Setup errors must not strand other workers at the barrier.
                    let mut statement = connection.prepare_cached("INSERT INTO kv(key,value,checksum) VALUES (?1,?2,?3) ON CONFLICT(key) DO UPDATE SET value=excluded.value,checksum=excluded.checksum")?;
                    for op in 0..operations {
                        let number = first + prior + worker * operations + op;
                        let value = vec![(number % 251) as u8; VALUE_BYTES];
                        let key = key(number, key_len);
                        let start = Instant::now();
                        statement.execute(params![key, value, crc32fast::hash(&value)])?;
                        samples.push(start.elapsed().as_nanos());
                    }
                    Ok(samples)
                }));
            }
            let start = Instant::now();
            barrier.wait();
            for thread in threads {
                latencies_ns.extend(
                    thread
                        .join()
                        .map_err(|_| "SQLite write worker panicked")??,
                );
            }
            elapsed_ns = start.elapsed().as_nanos();
            Ok(())
        })?;
        for number in first + prior..first + prior + current {
            verify(&connection, number, key_len)?;
        }
    }
    drop(connection);
    let io_after = ProcessIo::now()?;
    let peak_rss_kib = proc_field("/proc/self/status", "VmHWM:")?;
    println!(
        "{}",
        json!({"mode":"sqlite","phase":phase,"epoch":epoch,"base_keys":base,"tail_keys":tail,"key_bytes":key_len,
        "workers":workers,"writes_per_worker":operations,"prior_verified":prior,"current_verified":current,"open_us":open_us,
        "write_elapsed_ns":elapsed_ns,"write_latencies_ns":latencies_ns,"rss_after_open_kib":rss_after_open_kib,
        "peak_rss_kib":peak_rss_kib,"process_io":io_after.delta(&io_before),"integrity_check_us":integrity_check_us,
        "sqlite_version":rusqlite::version(),"journal":"wal","synchronous":"FULL"})
    );
    Ok(())
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command,parent,count,key_len,tail] if command == "prepare" => prepare(Path::new(parent),parse(count,1,1_000_000)?,parse(key_len,8,1024)?,parse(tail,0,4096)?),
        [command,rest @ ..] if command == "run" => run(rest),
        _ => Err("usage: sqlite_restart_write_bench prepare <parent> <base-keys> <key-bytes> <tail-keys> | run <parent> <base-keys> <tail-keys> <key-bytes> <workers> <writes-per-worker> <epoch> <write|verify>".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verification_rejects_missing_and_corrupt_values() {
        let root = tempfile::tempdir().unwrap();
        prepare(root.path(), 32, 8, 4).unwrap();
        let connection = open(&root.path().join("sqlite.db")).unwrap();
        assert!(verify(&connection, 36, 8).is_err());
        connection
            .execute(
                "UPDATE kv SET checksum=checksum+1 WHERE key=?1",
                [key(0, 8)],
            )
            .unwrap();
        assert!(verify(&connection, 0, 8).is_err());
    }
}
