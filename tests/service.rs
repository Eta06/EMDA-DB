#![cfg(feature = "server")]

use rustls::crypto::aws_lc_rs;
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use rustls::{ClientConfig, ClientConnection, NamedGroup, RootCertStore, StreamOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

const ADMIN: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const READER: &str = "0202020202020202020202020202020202020202020202020202020202020202";
const WRITER: &str = "0303030303030303030303030303030303030303030303030303030303030303";
const OTHER: &str = "0404040404040404040404040404040404040404040404040404040404040404";
const EXPIRED: &str = "0505050505050505050505050505050505050505050505050505050505050505";

struct Fixture {
    dir: tempfile::TempDir,
    tls: RootCertStore,
}

struct Running {
    child: Child,
    addr: SocketAddr,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(
            root.join("tls.cnf"),
            "[req]
distinguished_name=dn
prompt=no
x509_extensions=extensions
[dn]
CN=localhost
[extensions]
subjectAltName=DNS:localhost
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
",
        )
        .unwrap();
        let status = Command::new("openssl")
            .current_dir(root)
            .args([
                "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config",
                "tls.cnf", "-keyout", "key.pem", "-out", "cert.pem",
            ])
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(
            status.success(),
            "openssl must generate the local TLS test certificate"
        );

        let mut tls = RootCertStore::empty();
        tls.add(CertificateDer::from_pem_file(root.join("cert.pem")).unwrap())
            .unwrap();
        // Keep certificate and configuration files outside the storage root.
        let data = root.join("projects");
        fs::create_dir(&data).unwrap();
        fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
        let project = |id, credentials| {
            json!({
                "id": id, "data_log_bytes": 4096, "sql_log_bytes": 16_384,
                "max_live_keys_per_store": 128, "max_in_flight": 1, "credentials": credentials,
            })
        };
        fs::write(
            root.join("server.json"),
            json!({
                "bind": "127.0.0.1:0", "data_root": data, "tls_cert": root.join("cert.pem"),
                "tls_key": root.join("key.pem"), "max_connections": 8, "timeout_ms": 3000,
                "projects": [
                    project("alpha", vec![credential("admin", ADMIN, "admin", u64::MAX),
                        credential("reader", READER, "read", u64::MAX),
                        credential("writer", WRITER, "write", u64::MAX),
                        credential("expired", EXPIRED, "admin", 1)]),
                    project("beta", vec![credential("admin", OTHER, "admin", u64::MAX)]),
                ]
            })
            .to_string(),
        )
        .unwrap();
        Self { dir, tls }
    }

    fn start(&self) -> Running {
        let mut child = launch(&self.dir.path().join("server.json"));
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let addr = line
            .trim()
            .strip_prefix("READY ")
            .expect("server started")
            .parse()
            .unwrap();
        Running { child, addr }
    }

    fn connection(
        &self,
        server: &Running,
        classical_only: bool,
    ) -> StreamOwned<ClientConnection, TcpStream> {
        let mut provider = aws_lc_rs::default_provider();
        if classical_only {
            provider.kx_groups = vec![aws_lc_rs::kx_group::X25519];
        }
        let cfg = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(self.tls.clone())
            .with_no_client_auth();
        let connection =
            ClientConnection::new(Arc::new(cfg), ServerName::try_from("localhost").unwrap())
                .unwrap();
        let socket = TcpStream::connect_timeout(&server.addr, Duration::from_secs(3)).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        StreamOwned::new(connection, socket)
    }

    fn send(
        &self,
        server: &Running,
        project: &str,
        identity: &str,
        token: &str,
        operation: Value,
    ) -> Value {
        let stream = self.connection(server, false);
        self.send_on(stream, project, identity, token, operation)
    }

    fn send_on(
        &self,
        mut stream: StreamOwned<ClientConnection, TcpStream>,
        project: &str,
        identity: &str,
        token: &str,
        operation: Value,
    ) -> Value {
        let request = json!({"version": 1, "project": project, "credential": identity,
            "token": token, "operation": operation});
        let body = request.to_string();
        stream
            .write_all(&(body.len() as u32).to_be_bytes())
            .unwrap();
        stream.write_all(body.as_bytes()).unwrap();
        let response = response(&mut stream);
        assert_eq!(
            stream.conn.negotiated_key_exchange_group().unwrap().name(),
            NamedGroup::X25519MLKEM768
        );
        assert_eq!(
            stream.conn.negotiated_cipher_suite().unwrap().suite(),
            rustls::CipherSuite::TLS13_AES_256_GCM_SHA384
        );
        response
    }
}

fn launch(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_emda-server"))
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
}

fn credential(id: &str, token: &str, role: &str, expiry: u64) -> Value {
    let token: Vec<_> = token
        .as_bytes()
        .chunks_exact(2)
        .map(|chunk| u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap())
        .collect();
    json!({"id": id, "token_sha256": format!("{:x}", Sha256::digest(token)),
        "role": role, "expires_unix_seconds": expiry})
}

fn response(stream: &mut impl Read) -> Value {
    let mut size = [0; 4];
    stream.read_exact(&mut size).unwrap();
    let len = u32::from_be_bytes(size) as usize;
    assert!(len <= 256 * 1024);
    let mut message = vec![0; len];
    stream.read_exact(&mut message).unwrap();
    serde_json::from_slice(&message).unwrap()
}

fn error(code: &str) -> Value {
    json!({"ok": false, "error": code})
}

#[test]
fn remote_roles_project_boundaries_quota_and_reopen() {
    let fixture = Fixture::new();
    let server = fixture.start();
    let alpha = |op| fixture.send(&server, "alpha", "admin", ADMIN, op);
    let get = json!({"op": "kv_get", "key": "same"});
    let put = json!({"op": "kv_put", "key": "same", "value_hex": "00ff"});
    assert_eq!(alpha(put.clone()), json!({"ok": true, "result": null}));
    assert_eq!(
        fixture.send(&server, "beta", "admin", ADMIN, get.clone()),
        error("unauthorized")
    );
    assert_eq!(
        fixture.send(&server, "../alpha", "admin", ADMIN, get.clone()),
        error("unauthorized")
    );
    assert_eq!(
        fixture.send(&server, "alpha", "expired", EXPIRED, get.clone()),
        error("unauthorized")
    );
    assert_eq!(
        fixture.send(&server, "alpha", "reader", READER, put),
        error("forbidden")
    );
    assert_eq!(
        fixture.send(&server, "beta", "admin", OTHER, get.clone()),
        json!({"ok": true, "result": null})
    );

    let create = json!({"op":"document_create", "collection":"items", "schema":{"fields":[]}});
    assert_eq!(
        fixture.send(&server, "alpha", "writer", WRITER, create.clone()),
        error("forbidden")
    );
    assert_eq!(alpha(create)["ok"], true);
    let insert =
        json!({"op":"document_insert", "collection":"items", "id":"x", "document":{"n":5}});
    assert_eq!(
        fixture.send(&server, "alpha", "writer", WRITER, insert)["ok"],
        true
    );
    let document = json!({"op":"document_get", "collection":"items", "id":"x"});
    assert_eq!(alpha(document.clone())["result"], json!({"n":5}));

    let create =
        json!({"op":"sql", "statement":"CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER)"});
    assert_eq!(
        fixture.send(&server, "alpha", "writer", WRITER, create.clone()),
        error("forbidden")
    );
    assert_eq!(alpha(create)["ok"], true); // The document collection has the same name.
    let insert = json!({"op":"sql", "statement":"INSERT INTO items VALUES (?, ?)", "params":[1, 9007199254740993_i64]});
    assert_eq!(
        fixture.send(&server, "alpha", "reader", READER, insert.clone()),
        error("forbidden")
    );
    assert_eq!(
        fixture.send(&server, "alpha", "writer", WRITER, insert)["ok"],
        true
    );
    let sql = json!({"op":"sql", "statement":"SELECT n FROM items WHERE id = 1"});
    assert_eq!(
        fixture.send(&server, "alpha", "reader", READER, sql.clone())["result"]["rows"],
        json!([[9007199254740993_i64]])
    );
    assert_eq!(
        fixture.send(&server, "beta", "admin", OTHER, sql.clone()),
        error("not_found")
    );

    assert_eq!(
        alpha(json!({"op":"kv_put", "key":"too-big", "value_hex":"aa".repeat(4096)})),
        error("limit")
    );
    assert_eq!(
        alpha(json!({"op":"kv_get", "key":"too-big"}))["result"],
        Value::Null
    );
    assert_eq!(
        fixture.send(
            &server,
            "beta",
            "admin",
            OTHER,
            json!({"op":"kv_put", "key":"same", "value_hex":"bbbb"})
        )["ok"],
        true
    );
    assert_eq!(alpha(get.clone())["result"], "00ff");
    drop(server);

    let server = fixture.start();
    assert_eq!(
        fixture.send(&server, "alpha", "admin", ADMIN, get.clone())["result"],
        "00ff"
    );
    assert_eq!(
        fixture.send(&server, "beta", "admin", OTHER, get)["result"],
        "bbbb"
    );
    assert_eq!(
        fixture.send(&server, "alpha", "admin", ADMIN, document)["result"],
        json!({"n":5})
    );
    assert_eq!(
        fixture.send(&server, "alpha", "admin", ADMIN, sql)["result"]["rows"],
        json!([[9007199254740993_i64]])
    );
}

#[test]
fn strict_tls_rejects_classical_clients_and_rejects_bad_frames() {
    let fixture = Fixture::new();
    let server = fixture.start();
    let mut classical = fixture.connection(&server, true);
    assert!(classical.conn.complete_io(&mut classical.sock).is_err());
    let mut hybrid = fixture.connection(&server, false);
    hybrid.write_all(&u32::MAX.to_be_bytes()).unwrap();
    assert_eq!(response(&mut hybrid), error("limit"));
    let mut hybrid = fixture.connection(&server, false);
    hybrid.write_all(&[0, 0, 0, 2, b'{', b'x']).unwrap();
    assert_eq!(response(&mut hybrid), error("invalid_request"));
}

#[test]
fn classical_compatibility_requires_explicit_opt_in() {
    let fixture = Fixture::new();
    let path = fixture.dir.path().join("server.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["require_hybrid_kex"] = false.into();
    fs::write(&path, config.to_string()).unwrap();
    let server = fixture.start();
    let mut stream = fixture.connection(&server, true);
    stream.conn.complete_io(&mut stream.sock).unwrap();
    assert_eq!(
        stream.conn.negotiated_key_exchange_group().unwrap().name(),
        NamedGroup::X25519
    );
}

#[test]
fn project_symlinks_and_hard_links_fail_at_startup() {
    let fixture = Fixture::new();
    let root = fixture.dir.path();
    std::os::unix::fs::symlink(root, root.join("projects/alpha")).unwrap();
    let rejected = launch(&root.join("server.json"))
        .wait_with_output()
        .unwrap();
    assert!(!rejected.status.success());
    fs::remove_file(root.join("projects/alpha")).unwrap();
    let server = fixture.start();
    drop(server);
    fs::hard_link(
        root.join("projects/alpha/data/store.lock"),
        root.join("projects/beta/data/foreign.lock"),
    )
    .unwrap();
    let rejected = launch(&root.join("server.json"))
        .wait_with_output()
        .unwrap();
    assert!(!rejected.status.success());
}

#[test]
fn stalled_tls_request_releases_the_global_connection_slot() {
    let fixture = Fixture::new();
    let path = fixture.dir.path().join("server.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["max_connections"] = 1.into();
    config["timeout_ms"] = 1500.into();
    fs::write(&path, config.to_string()).unwrap();
    let server = fixture.start();

    let mut stalled = fixture.connection(&server, false);
    stalled.conn.complete_io(&mut stalled.sock).unwrap();
    stalled.write_all(&[0, 0, 0, 32, b'{']).unwrap();
    let mut rejected = fixture.connection(&server, false);
    assert!(rejected.conn.complete_io(&mut rejected.sock).is_err());

    let start = std::time::Instant::now();
    assert!(stalled.read_exact(&mut [0]).is_err());
    assert!(start.elapsed() < Duration::from_secs(3));
    let retry_until = std::time::Instant::now() + Duration::from_secs(2);
    let available = loop {
        let mut attempt = fixture.connection(&server, false);
        if attempt.conn.complete_io(&mut attempt.sock).is_ok() {
            break attempt;
        }
        assert!(
            std::time::Instant::now() < retry_until,
            "connection slot remained busy"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        fixture.send_on(
            available,
            "alpha",
            "admin",
            ADMIN,
            json!({"op":"kv_get","key":"x"})
        ),
        json!({"ok": true, "result": null})
    );
}
