//! TLS front end for the standalone EMDA-DB service.

mod project;
mod protocol;

use project::{Admission, Config, HostedProjects, Project};
use protocol::Failure;
use rustix::event::{PollFd, PollFlags, poll};
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Server {
    tls: Arc<ServerConfig>,
    hosted: HostedProjects,
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
            hosted: config.open_projects()?,
            connections: Arc::new(Admission::new(config.max_connections)),
            timeout: Duration::from_millis(config.timeout_ms),
        })
    }

    fn start_idle_reaper(self: &Arc<Self>) -> io::Result<()> {
        let Some(shortest) = self
            .hosted
            .projects
            .values()
            .filter_map(Project::idle_timeout)
            .min()
        else {
            return Ok(());
        };
        let interval = (shortest / 2).min(Duration::from_secs(1));
        let server = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("emda-idle".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    let Some(server) = server.upgrade() else {
                        break;
                    };
                    let now = Instant::now();
                    for project in server.hosted.projects.values() {
                        project.reap_idle(now);
                    }
                }
            })?;
        Ok(())
    }

    fn accept(self: &Arc<Self>, socket: TcpStream) -> io::Result<()> {
        let Some(permit) = self.connections.enter() else {
            return Ok(());
        };
        let server = Arc::clone(self);
        let deadline = Instant::now() + self.timeout;
        // A permit lives inside the closure, so a failed spawn also releases it.
        std::thread::Builder::new()
            .name("emda-client".into())
            .spawn(move || {
                let _permit = permit;
                let _ = server.handle(socket, deadline);
            })?;
        Ok(())
    }

    fn handle(&self, socket: TcpStream, deadline: Instant) -> io::Result<()> {
        socket.set_nonblocking(false)?;
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
                .hosted
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

const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

fn read_json_file<T: serde::de::DeserializeOwned>(
    path: &Path,
) -> Result<T, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    use rustix::fs::{Mode, OFlags};
    let file = File::from(rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    if !file.metadata()?.is_file() {
        return Err("configuration must be a regular file".into());
    }
    file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err("configuration exceeds 1 MiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn run(path: &Path, single_project: bool) -> Result<(), Box<dyn std::error::Error>> {
    let (signal, notify) = UnixStream::pair()?;
    signal_hook::low_level::pipe::register(SIGTERM, notify.try_clone()?)?;
    signal_hook::low_level::pipe::register(SIGINT, notify)?;
    let (mut reload, notify_reload) = UnixStream::pair()?;
    signal_hook::low_level::pipe::register(SIGHUP, notify_reload)?;
    let mut config: Config = read_json_file(path)?;
    if single_project {
        if config.projects.len() != 1 {
            return Err("--single-project requires exactly one configured project".into());
        }
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        config.tls_cert = parent.join(&config.tls_cert);
        config.tls_key = parent.join(&config.tls_key);
    }
    let server = Arc::new(Server::open(&config)?);
    let listener = TcpListener::bind(config.bind)?;
    listener.set_nonblocking(true)?;
    server.start_idle_reaper()?;
    notify_ready()?;
    println!("READY {}", listener.local_addr()?);
    io::stdout().flush()?;

    let result = accept_until_signal(
        &server,
        &listener,
        &signal,
        &mut reload,
        config.credentials_file.as_deref(),
        config.credentials_poll_ms.map(Duration::from_millis),
    );
    drop(listener);
    // Keep owning the stores until every admitted request releases its slot,
    // including a committed write still sending its response.
    while !server.connections.is_idle() {
        std::thread::sleep(Duration::from_millis(10));
    }
    result.map_err(Into::into)
}

fn accept_until_signal(
    server: &Arc<Server>,
    listener: &TcpListener,
    signal: &UnixStream,
    reload: &mut UnixStream,
    credentials_file: Option<&Path>,
    poll_interval: Option<Duration>,
) -> io::Result<()> {
    let mut poll_due = poll_interval.map(|interval| Instant::now() + interval);
    let mut seen_policy = None;
    loop {
        let mut fds = [
            PollFd::new(signal, PollFlags::IN),
            PollFd::new(reload, PollFlags::IN),
            PollFd::new(listener, PollFlags::IN),
        ];
        let timeout = poll_due.map(|when| {
            let delay = when.saturating_duration_since(Instant::now());
            rustix::event::Timespec {
                tv_sec: delay.as_secs() as _,
                tv_nsec: delay.subsec_nanos().into(),
            }
        });
        match poll(&mut fds, timeout.as_ref()) {
            Ok(_) => {}
            Err(rustix::io::Errno::INTR) => continue,
            Err(error) => return Err(error.into()),
        }
        if !fds[0].revents().is_empty() {
            return Ok(());
        }
        let flush_reload = !fds[1].revents().is_empty();
        let accept = !fds[2].revents().is_empty();
        if flush_reload {
            let mut bytes = [0; 256];
            let _ = reload.read(&mut bytes)?;
            match credentials_file {
                None => eprintln!("emda-server ignored SIGHUP without a credentials file"),
                Some(path) => match server.hosted.reload_credentials(path) {
                    Ok(()) => eprintln!("emda-server credential policy reloaded"),
                    Err(_) => eprintln!(
                        "emda-server rejected credential reload; previous policy retained"
                    ),
                },
            }
        }
        if let (Some(due), Some(interval), Some(path)) = (poll_due, poll_interval, credentials_file)
        {
            if Instant::now() >= due {
                poll_due = Some(Instant::now() + interval);
                if let Ok(meta) = std::fs::metadata(path) {
                    let current = (
                        meta.dev(),
                        meta.ino(),
                        meta.len(),
                        meta.mtime(),
                        meta.mtime_nsec(),
                    );
                    if seen_policy != Some(current) {
                        match server.hosted.reload_credentials(path) {
                            Ok(()) => {
                                seen_policy = Some(current);
                                eprintln!("emda-server credential policy reloaded");
                            }
                            Err(_) => eprintln!(
                                "emda-server rejected credential reload; previous policy retained"
                            ),
                        }
                    }
                }
            }
        }
        if !accept {
            continue;
        }
        match listener.accept() {
            Ok((socket, _)) => {
                if let Err(error) = server.accept(socket) {
                    eprintln!("emda-server could not start a client handler: {error}");
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted
                        | io::ErrorKind::WouldBlock
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) => {}
            Err(error)
                if matches!(error.raw_os_error(),
                Some(raw) if raw == rustix::io::Errno::MFILE.raw_os_error()
                    || raw == rustix::io::Errno::NFILE.raw_os_error()) =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(target_os = "linux")]
fn notify_ready() -> io::Result<()> {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::{SocketAddr, UnixDatagram};
    let Some(path) = std::env::var_os("NOTIFY_SOCKET") else {
        return Ok(());
    };
    let address = match path.as_bytes().strip_prefix(b"@") {
        Some(name) => SocketAddr::from_abstract_name(name)?,
        None => SocketAddr::from_pathname(path)?,
    };
    UnixDatagram::unbound()?.send_to_addr(b"READY=1", &address)?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn notify_ready() -> io::Result<()> {
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if let [command, rest @ ..] = args.as_slice() {
        if command == "keys" || command == "key-api" {
            let result = if command == "keys" {
                project::keys::run(rest)
            } else {
                project::key_api::run(rest)
            };
            return match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("emda-server {}: {error}", command.to_string_lossy());
                    ExitCode::FAILURE
                }
            };
        }
    }
    let (config, single_project) = match args.as_slice() {
        [flag, config] if flag == "--single-project" => (config, true),
        [config] if config != "--single-project" => (config, false),
        _ => {
            eprintln!(
                "usage: emda-server [--single-project] <config.json> or emda-server keys <config.json> <command> ..."
            );
            return ExitCode::FAILURE;
        }
    };
    match run(Path::new(config), single_project) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("emda-server startup or listener failure: {error}");
            ExitCode::FAILURE
        }
    }
}
