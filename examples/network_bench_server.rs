//! Loopback-only adapter for comparing benchmark clients over TCP.
//!
//! This is benchmark tooling, not a database network service: there is no
//! authentication, transport security, admission control, or public protocol.
#[allow(dead_code)]
mod support;

use database_engine::Store;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use support::{BenchResult, Worker, sqlite_connection};

enum Backend {
    Prototype(Arc<Store>),
    Sqlite(PathBuf),
}

fn handle(mut stream: TcpStream, mut worker: Worker) -> BenchResult<()> {
    stream.set_nodelay(true)?;
    loop {
        let mut header = [0_u8; 9];
        match stream.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        let key_len = u32::from_be_bytes(header[1..5].try_into()?) as usize;
        let value_len = u32::from_be_bytes(header[5..9].try_into()?) as usize;
        if !(1..=1024).contains(&key_len) || value_len > 16 * 1024 * 1024 {
            return Err("request exceeds benchmark limits".into());
        }
        let mut key = vec![0; key_len];
        let mut value = vec![0; value_len];
        stream.read_exact(&mut key)?;
        stream.read_exact(&mut value)?;
        let result: BenchResult<Vec<u8>> = match header[0] {
            b'R' if value_len == 0 => worker.read_value(&key),
            b'W' => worker.write(&key, &value).map(|_| Vec::new()),
            _ => Err("invalid benchmark request".into()),
        };
        let (status, response) = match result {
            Ok(value) => (0, value),
            Err(error) => (1, error.to_string().into_bytes()),
        };
        stream.write_all(&[status])?;
        stream.write_all(&(response.len() as u32).to_be_bytes())?;
        stream.write_all(&response)?;
    }
}

fn main() -> BenchResult<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [backend, directory] = args.as_slice() else {
        return Err("usage: network_bench_server <prototype|sqlite> <existing-directory>".into());
    };
    let directory = PathBuf::from(directory);
    if !directory.is_dir() {
        return Err("benchmark directory must exist".into());
    }
    let backend = match backend.as_str() {
        "prototype" => Backend::Prototype(Arc::new(Store::open(directory.join("store"))?)),
        "sqlite" => {
            let path = directory.join("sqlite.db");
            let connection = sqlite_connection(&path)?;
            let journal: String =
                connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
            if journal != "wal"
                || connection.query_row("PRAGMA synchronous", [], |row| row.get::<_, u32>(0))? != 2
            {
                return Err("SQLite WAL and synchronous=FULL are required".into());
            }
            connection.execute_batch("CREATE TABLE IF NOT EXISTS kv(key BLOB PRIMARY KEY, value BLOB NOT NULL, checksum INTEGER NOT NULL) WITHOUT ROWID;")?;
            Backend::Sqlite(path)
        }
        _ => return Err("unknown backend".into()),
    };
    let listener = TcpListener::bind("127.0.0.1:0")?;
    println!("READY {}", listener.local_addr()?.port());
    io::stdout().flush()?;
    for incoming in listener.incoming() {
        let stream = incoming?;
        let worker = match &backend {
            Backend::Prototype(store) => Worker::Prototype(Arc::clone(store)),
            Backend::Sqlite(path) => Worker::Sqlite(sqlite_connection(path)?),
        };
        thread::spawn(move || {
            if let Err(error) = handle(stream, worker) {
                eprintln!("benchmark connection: {error}");
            }
        });
    }
    Ok(())
}
