//! TLS front end for the standalone EMDA-DB service.

mod project;
mod protocol;

use project::{Admission, Config, Project};
use protocol::Failure;
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Server {
    tls: Arc<ServerConfig>,
    projects: HashMap<String, Project>,
    connections: Arc<Admission>,
    timeout: Duration,
}

impl Server {
    fn open(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let certificates =
            CertificateDer::pem_file_iter(&config.tls_cert)?.collect::<Result<Vec<_>, _>>()?;
        let key = PrivateKeyDer::from_pem_file(&config.tls_key)?;
        let mut provider = aws_lc_rs::default_provider();
        if config.require_hybrid_kex {
            provider.kx_groups = vec![aws_lc_rs::kx_group::X25519MLKEM768];
            provider.cipher_suites = vec![aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384];
        }
        let mut tls = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(certificates, key)?;
        tls.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        tls.send_tls13_tickets = 0;
        Ok(Self {
            tls: Arc::new(tls),
            projects: config.open_projects()?,
            connections: Arc::new(Admission::new(config.max_connections)),
            timeout: Duration::from_millis(config.timeout_ms),
        })
    }

    fn accept(self: &Arc<Self>, socket: TcpStream) {
        let Some(permit) = self.connections.enter() else {
            return;
        };
        let server = Arc::clone(self);
        let deadline = Instant::now() + self.timeout;
        // A permit lives inside the closure, so a failed spawn also releases it.
        let _ = std::thread::Builder::new()
            .name("emda-client".into())
            .spawn(move || {
                let _permit = permit;
                let _ = server.handle(socket, deadline);
            });
    }

    fn handle(&self, socket: TcpStream, deadline: Instant) -> io::Result<()> {
        socket.set_nodelay(true)?;
        let mut connection =
            ServerConnection::new(Arc::clone(&self.tls)).map_err(io::Error::other)?;
        connection.set_buffer_limit(Some(protocol::MAX_FRAME));
        let mut stream = StreamOwned::new(connection, DeadlineSocket { socket, deadline });
        let result = protocol::read_request(&mut stream)?.and_then(|request| {
            if request.version != protocol::VERSION {
                return Err(Failure::UnsupportedVersion);
            }
            let project = self
                .projects
                .get(&request.project)
                .ok_or(Failure::Unauthorized)?;
            project.execute(request, deadline)
        });
        // A committed mutation may outlive the request deadline. Give its
        // status one short write window without tying up the connection slot.
        stream.sock.deadline = Instant::now() + Duration::from_secs(1);
        protocol::write_response(&mut stream, result)?;
        stream.conn.send_close_notify();
        stream.flush()
    }
}

struct DeadlineSocket {
    socket: TcpStream,
    deadline: Instant,
}

impl DeadlineSocket {
    fn remaining(&self) -> io::Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection deadline exceeded",
            ));
        }
        Ok(remaining)
    }
}

impl Read for DeadlineSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.socket.set_read_timeout(Some(self.remaining()?))?;
        self.socket.read(bytes)
    }
}

impl Write for DeadlineSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        self.socket.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}

fn read_config(path: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err("configuration exceeds 1 MiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn run(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let config = read_config(path)?;
    let server = Arc::new(Server::open(&config)?);
    let listener = TcpListener::bind(config.bind)?;
    println!("READY {}", listener.local_addr()?);
    io::stdout().flush()?;
    for incoming in listener.incoming() {
        match incoming {
            Ok(socket) => server.accept(socket),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let [config] = args.as_slice() else {
        eprintln!("usage: emda-server <config.json>");
        return ExitCode::FAILURE;
    };
    match run(Path::new(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("emda-server startup or listener failure: {error}");
            ExitCode::FAILURE
        }
    }
}
