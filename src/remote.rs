//! Rust client for the standalone EMDA-DB TLS service.

use crate::{CollectionSchema, SqlValue};
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use serde::Serialize;
use serde_json::Value;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Version of the standalone request protocol.
pub const PROTOCOL_VERSION: u16 = 1;
/// Largest allowed serialized request or response body in bytes.
pub const MAX_FRAME_BYTES: usize = 256 * 1024;

/// An authenticated operation sent to an EMDA-DB server.
#[derive(Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Operation<'a> {
    KvGet {
        key: &'a str,
    },
    KvPut {
        key: &'a str,
        value_hex: String,
    },
    KvDelete {
        key: &'a str,
    },
    DocumentCreate {
        collection: &'a str,
        schema: &'a CollectionSchema,
    },
    DocumentGet {
        collection: &'a str,
        id: &'a str,
    },
    DocumentInsert {
        collection: &'a str,
        id: &'a str,
        document: &'a Value,
    },
    DocumentReplace {
        collection: &'a str,
        id: &'a str,
        document: &'a Value,
    },
    DocumentDelete {
        collection: &'a str,
        id: &'a str,
    },
    Sql {
        statement: &'a str,
        params: Vec<Value>,
    },
}

impl<'a> Operation<'a> {
    /// Constructs a write operation for an arbitrary binary value.
    pub fn kv_put(key: &'a str, value: &[u8]) -> Result<Self, Error> {
        if value.len() > MAX_FRAME_BYTES / 2 {
            return Err(Error::Limit);
        }
        const DIGITS: &[u8] = b"0123456789abcdef";
        let mut hex = String::with_capacity(value.len() * 2);
        for &byte in value {
            hex.push(char::from(DIGITS[usize::from(byte >> 4)]));
            hex.push(char::from(DIGITS[usize::from(byte & 15)]));
        }
        Ok(Self::KvPut {
            key,
            value_hex: hex,
        })
    }

    /// Encodes the engine's typed SQL arguments as JSON scalars.
    pub fn sql(statement: &'a str, params: &[SqlValue]) -> Result<Self, Error> {
        // The service checks tighter SQL bounds after authentication.
        if params.len() > 256 {
            return Err(Error::Limit);
        }
        let mut bytes = statement.len();
        let mut cells = Vec::with_capacity(params.len());
        for value in params {
            bytes = bytes.saturating_add(match value {
                SqlValue::Text(text) => text.len(),
                _ => 1,
            });
            if bytes > MAX_FRAME_BYTES {
                return Err(Error::Limit);
            }
            let cell = match value {
                SqlValue::Null => Value::Null,
                SqlValue::Integer(value) => (*value).into(),
                SqlValue::Real(value) if value.is_finite() => (*value).into(),
                SqlValue::Real(_) => {
                    return Err(Error::InvalidInput("non-finite SQL parameter"));
                }
                SqlValue::Boolean(value) => (*value).into(),
                SqlValue::Text(text) => text.clone().into(),
            };
            cells.push(cell);
        }
        Ok(Self::Sql {
            statement,
            params: cells,
        })
    }
}

/// Connection and authentication settings. The token is an unlogged secret.
pub struct ClientOptions {
    pub address: SocketAddr,
    pub server_name: String,
    pub project: String,
    pub credential: String,
    pub token: String,
    pub timeout: Duration,
}

/// A reusable client configuration. Each request opens one TLS connection.
pub struct Client {
    options: ClientOptions,
    server_name: ServerName<'static>,
    tls: Arc<ClientConfig>,
}

/// Failure to issue a request or obtain its response.
#[derive(Debug)]
pub enum Error {
    InvalidInput(&'static str),
    Limit,
    /// The client failed before transmitting any application request.
    Connect(io::Error),
    /// Transmission started and a response could not be verified.
    /// Reconcile mutations before attempting them again.
    ResponseUnknown(io::Error),
    /// An authenticated server error code, including `outcome_unknown`.
    Server(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => f.write_str(message),
            Self::Limit => f.write_str("request exceeds the frame limit"),
            Self::Connect(error) => write!(f, "could not connect to EMDA-DB: {error}"),
            Self::ResponseUnknown(error) => write!(f, "EMDA-DB response unavailable: {error}"),
            Self::Server(code) => write!(f, "EMDA-DB returned {code}"),
        }
    }
}
impl std::error::Error for Error {}

impl Client {
    /// Requires the same hybrid key agreement and cipher as the default server.
    /// `roots` contains the trusted issuer certificates for this service.
    pub fn new(options: ClientOptions, roots: RootCertStore) -> Result<Self, Error> {
        let server_name = ServerName::try_from(options.server_name.clone())
            .map_err(|_| Error::InvalidInput("invalid server DNS name or IP"))?;
        if options.timeout.is_zero() || options.timeout > Duration::from_secs(60) {
            return Err(Error::InvalidInput(
                "timeout must be at most 60 seconds and positive",
            ));
        }
        let valid_name = |value: &str| {
            (1..=48).contains(&value.len())
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        };
        if !valid_name(&options.project) || !valid_name(&options.credential) {
            return Err(Error::InvalidInput(
                "invalid project or credential identifier",
            ));
        }
        if options.token.len() != 64
            || !options
                .token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::InvalidInput("token must be 64 lowercase hex digits"));
        }
        let mut provider = aws_lc_rs::default_provider();
        provider.kx_groups = vec![aws_lc_rs::kx_group::X25519MLKEM768];
        provider.cipher_suites = vec![aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384];
        let tls = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|error| Error::Connect(io::Error::other(error)))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            options,
            server_name,
            tls: Arc::new(tls),
        })
    }

    /// Builds a client from a PEM bundle of trusted issuers.
    pub fn with_ca_pem(options: ClientOptions, path: impl AsRef<Path>) -> Result<Self, Error> {
        let certificates = CertificateDer::pem_file_iter(path)
            .map_err(|err| Error::Connect(io::Error::other(err)))?;
        let mut roots = RootCertStore::empty();
        for certificate in certificates {
            let certificate = certificate.map_err(|err| Error::Connect(io::Error::other(err)))?;
            roots
                .add(certificate)
                .map_err(|err| Error::Connect(io::Error::other(err)))?;
        }
        if roots.is_empty() {
            return Err(Error::InvalidInput("trusted issuer bundle is empty"));
        }
        Self::new(options, roots)
    }

    /// Sends one operation, returning its JSON result. No failures are retried.
    pub fn request(&self, operation: &Operation<'_>) -> Result<Value, Error> {
        #[derive(Serialize)]
        struct Request<'a, 'b> {
            version: u16,
            project: &'a str,
            credential: &'a str,
            token: &'a str,
            operation: &'b Operation<'b>,
        }
        let mut body = Frame(Vec::new());
        serde_json::to_writer(
            &mut body,
            &Request {
                version: PROTOCOL_VERSION,
                project: &self.options.project,
                credential: &self.options.credential,
                token: &self.options.token,
                operation,
            },
        )
        .map_err(|_| Error::Limit)?;

        let deadline = Instant::now() + self.options.timeout;
        let socket = TcpStream::connect_timeout(&self.options.address, self.options.timeout)
            .map_err(Error::Connect)?;
        let mut socket = DeadlineSocket { socket, deadline };
        let mut connection = ClientConnection::new(Arc::clone(&self.tls), self.server_name.clone())
            .map_err(|err| Error::Connect(io::Error::other(err)))?;
        connection.set_buffer_limit(Some(MAX_FRAME_BYTES));
        connection
            .complete_io(&mut socket)
            .map_err(Error::Connect)?;

        let mut stream = StreamOwned::new(connection, socket);
        stream
            .write_all(&(body.0.len() as u32).to_be_bytes())
            .map_err(Error::ResponseUnknown)?;
        stream.write_all(&body.0).map_err(Error::ResponseUnknown)?;
        stream.flush().map_err(Error::ResponseUnknown)?;
        read_response(&mut stream)
    }
}

fn read_response(stream: &mut impl Read) -> Result<Value, Error> {
    let mut size = [0_u8; 4];
    stream
        .read_exact(&mut size)
        .map_err(Error::ResponseUnknown)?;
    let len = u32::from_be_bytes(size) as usize;
    let invalid = || {
        Error::ResponseUnknown(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid response frame",
        ))
    };
    if !(1..=MAX_FRAME_BYTES).contains(&len) {
        return Err(invalid());
    }
    let mut body = vec![0; len];
    stream
        .read_exact(&mut body)
        .map_err(Error::ResponseUnknown)?;
    let object: Value =
        serde_json::from_slice(&body).map_err(|error| Error::ResponseUnknown(error.into()))?;
    let Value::Object(mut object) = object else {
        return Err(invalid());
    };
    match object.remove("ok") {
        Some(Value::Bool(true)) => object.remove("result").ok_or_else(invalid),
        Some(Value::Bool(false)) => match object.remove("error") {
            Some(Value::String(code))
                if (1..=48).contains(&code.len())
                    && code
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte == b'_') =>
            {
                Err(Error::Server(code))
            }
            _ => Err(invalid()),
        },
        _ => Err(invalid()),
    }
}

struct Frame(Vec<u8>);

impl Write for Frame {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_FRAME_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("request exceeds frame limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
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
