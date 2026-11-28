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
        Self::wait_ready(launch(&self.dir.path().join("server.json")))
    }

    fn wait_ready(mut child: Child) -> Running {
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

fn wait_for_store_release(path: &Path) {
    use fs2::FileExt;
    let lock = fs::File::open(path).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if lock.try_lock_exclusive().is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "idle store lock was not released"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn only_authorized_operations_wake_projects_and_idle_stores_reopen() {
    use fs2::FileExt;

    let fixture = Fixture::new();
    let root = fixture.dir.path();
    let path = root.join("server.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["projects"][0]["idle_timeout_ms"] = 500.into();
    config["projects"][1]["idle_timeout_ms"] = Value::Null;
    fs::write(&path, config.to_string()).unwrap();

    let server = fixture.start();
    let alpha_lock = root.join("projects/alpha/data/store.lock");
    let beta_lock = root.join("projects/beta/data/store.lock");
    let get = json!({"op":"kv_get","key":"durable"});
    let put = json!({"op":"kv_put","key":"durable","value_hex":"abcd"});

    assert!(!alpha_lock.exists());
    assert!(beta_lock.exists());
    assert_eq!(
        fixture.send(&server, "alpha", "admin", OTHER, get.clone()),
        error("unauthorized")
    );
    assert_eq!(
        fixture.send(&server, "alpha", "reader", READER, put.clone()),
        error("forbidden")
    );
    assert_eq!(
        fixture.send(
            &server,
            "alpha",
            "reader",
            READER,
            json!({"op":"sql","statement":"CREATE TABLE protected (id INT PRIMARY KEY)"})
        ),
        error("forbidden")
    );
    assert!(!alpha_lock.exists());

    assert_eq!(
        fixture.send(&server, "alpha", "admin", ADMIN, put)["ok"],
        true
    );
    assert_eq!(
        fixture.send(
            &server,
            "alpha",
            "admin",
            ADMIN,
            json!({"op":"sql","statement":"CREATE TABLE numbers (id INT PRIMARY KEY)"})
        )["ok"],
        true
    );
    assert_eq!(
        fixture.send(
            &server,
            "alpha",
            "admin",
            ADMIN,
            json!({"op":"sql","statement":"INSERT INTO numbers VALUES (99)"})
        )["ok"],
        true
    );
    wait_for_store_release(&alpha_lock);
    assert!(
        fs::File::open(beta_lock)
            .unwrap()
            .try_lock_exclusive()
            .is_err()
    );

    assert_eq!(
        fixture.send(&server, "alpha", "admin", ADMIN, get.clone())["result"],
        "abcd"
    );
    assert_eq!(
        fixture.send(
            &server,
            "alpha",
            "admin",
            ADMIN,
            json!({"op":"sql","statement":"SELECT id FROM numbers"})
        )["result"]["rows"],
        json!([[99]])
    );
    wait_for_store_release(&alpha_lock);

    fs::remove_file(root.join("projects/alpha/data/records.log")).unwrap();
    assert_eq!(
        fixture.send(&server, "alpha", "admin", ADMIN, get.clone()),
        error("unavailable")
    );
    assert!(!root.join("projects/alpha/data/records.log").exists());
    assert!(
        fixture.send(&server, "beta", "admin", OTHER, get)["ok"]
            .as_bool()
            .unwrap()
    );
}

#[test]
fn single_project_mode_rejects_multi_project_configuration_before_opening_stores() {
    let fixture = Fixture::new();
    let root = fixture.dir.path();
    let result = Command::new(env!("CARGO_BIN_EXE_emda-server"))
        .arg("--single-project")
        .arg(root.join("server.json"))
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("requires exactly one configured project")
    );
    assert!(!root.join("projects/alpha").exists());
}

#[test]
fn rust_client_verifies_the_peer_and_round_trips_all_data_models() {
    use database_engine::remote::{Client, ClientOptions, Error, Operation};
    use database_engine::{CollectionSchema, SqlValue};

    let fixture = Fixture::new();
    let server = fixture.start();
    let opts = |name: &str, token: &str| ClientOptions {
        address: server.addr,
        server_name: name.into(),
        project: "alpha".into(),
        credential: "admin".into(),
        token: token.into(),
        timeout: Duration::from_secs(3),
    };
    let ca = fixture.dir.path().join("cert.pem");
    let client = Client::with_ca_pem(opts("localhost", ADMIN), &ca).unwrap();
    let get = Operation::KvGet { key: "binary" };
    client
        .request(&Operation::kv_put("binary", &[0, 255]).unwrap())
        .unwrap();
    assert_eq!(client.request(&get).unwrap(), "00ff");

    let schema = CollectionSchema::default();
    client
        .request(&Operation::DocumentCreate {
            collection: "people",
            schema: &schema,
        })
        .unwrap();
    let document = json!({"name": "Emir"});
    client
        .request(&Operation::DocumentInsert {
            collection: "people",
            id: "1",
            document: &document,
        })
        .unwrap();
    assert_eq!(
        client
            .request(&Operation::DocumentGet {
                collection: "people",
                id: "1"
            })
            .unwrap(),
        document
    );

    let sql = |text, params: &[SqlValue]| {
        client
            .request(&Operation::sql(text, params).unwrap())
            .unwrap()
    };
    sql("CREATE TABLE numbers (id INTEGER PRIMARY KEY)", &[]);
    sql(
        "INSERT INTO numbers VALUES (?)",
        &[SqlValue::Integer(i64::MAX)],
    );
    assert_eq!(
        sql("SELECT id FROM numbers", &[])["rows"],
        json!([[i64::MAX]])
    );

    let wrong_dns = Client::with_ca_pem(opts("wrong.example", ADMIN), &ca).unwrap();
    assert!(matches!(wrong_dns.request(&get), Err(Error::Connect(_))));
    let wrong_token = Client::with_ca_pem(opts("localhost", OTHER), &ca).unwrap();
    assert!(
        matches!(wrong_token.request(&get), Err(Error::Server(code)) if code == "unauthorized")
    );
    let too_large = json!({"name": "x".repeat(256 * 1024)});
    assert!(matches!(
        client.request(&Operation::DocumentReplace {
            collection: "people",
            id: "1",
            document: &too_large
        }),
        Err(Error::Limit)
    ));
}

#[test]
fn api_key_scope_applies_distinct_permissions_to_each_dataset() {
    let fixture = Fixture::new();
    let path = fixture.dir.path().join("server.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let alpha = &mut config["projects"][0]["credentials"];
    for (name, token, role) in [
        ("scoped-read", READER, "read"),
        ("scoped-write", WRITER, "write"),
    ] {
        let mut grant = credential(name, token, role, u64::MAX);
        grant["scope"] = json!({
            "kv_prefixes": {"public/":"read", "metrics/":"write", "metrics/audit/":"read", "ingest/":"write_only"},
            "document_collections": {"public":"read", "metrics":"write"}
        });
        let list = alpha.as_array_mut().unwrap();
        let original = list
            .iter()
            .position(|prior| prior["token_sha256"] == grant["token_sha256"])
            .unwrap();
        list[original] = grant;
    }
    fs::write(&path, config.to_string()).unwrap();
    let server = fixture.start();
    let call =
        |credential, token, operation| fixture.send(&server, "alpha", credential, token, operation);
    let public_put = json!({"op":"kv_put", "key":"public/a", "value_hex":"11"});
    assert_eq!(call("admin", ADMIN, public_put.clone())["ok"], true);
    assert_eq!(call("scoped-write", WRITER, public_put), error("forbidden"));
    assert_eq!(
        call(
            "scoped-write",
            WRITER,
            json!({"op":"kv_get", "key":"public/a"})
        )["result"],
        "11"
    );
    let metrics_put = json!({"op":"kv_put", "key":"metrics/a", "value_hex":"22"});
    assert_eq!(
        call("scoped-read", READER, metrics_put.clone()),
        error("forbidden")
    );
    assert_eq!(call("scoped-write", WRITER, metrics_put)["ok"], true);
    assert_eq!(
        call(
            "scoped-read",
            READER,
            json!({"op":"kv_get", "key":"metrics/a"})
        )["result"],
        "22"
    );
    assert_eq!(
        call(
            "scoped-write",
            WRITER,
            json!({"op":"kv_put", "key":"metrics/audit/a", "value_hex":"33"})
        ),
        error("forbidden")
    );
    assert_eq!(
        call(
            "scoped-write",
            WRITER,
            json!({"op":"kv_put", "key":"ingest/a", "value_hex":"44"})
        )["ok"],
        true
    );
    assert_eq!(
        call(
            "scoped-write",
            WRITER,
            json!({"op":"kv_get", "key":"ingest/a"})
        ),
        error("forbidden")
    );
    for key in ["public", "Public/a", "publicity/a", "private/a"] {
        assert_eq!(
            call("scoped-read", READER, json!({"op":"kv_get", "key":key})),
            error("forbidden")
        );
        assert_eq!(
            call("scoped-write", WRITER, json!({"op":"kv_delete", "key":key})),
            error("forbidden")
        );
    }
    for collection in ["public", "metrics"] {
        assert_eq!(
            call(
                "admin",
                ADMIN,
                json!({"op":"document_create", "collection":collection, "schema":{"fields":[]}})
            )["ok"],
            true
        );
        let insert =
            json!({"op":"document_insert", "collection":collection, "id":"a", "document":{"x":10}});
        assert_eq!(
            call("scoped-write", WRITER, insert.clone())["ok"],
            collection == "metrics"
        );
        if collection == "public" {
            assert_eq!(call("admin", ADMIN, insert)["ok"], true);
        }
        assert_eq!(
            call(
                "scoped-write",
                WRITER,
                json!({"op":"document_get", "collection":collection, "id":"a"})
            )["result"],
            json!({"x":10})
        );
    }
    assert_eq!(
        call(
            "scoped-read",
            READER,
            json!({"op":"document_get", "collection":"private", "id":"a"})
        ),
        error("forbidden")
    );
    assert_eq!(
        call(
            "scoped-write",
            WRITER,
            json!({"op":"sql", "statement":"SELECT * FROM private"})
        ),
        error("forbidden")
    );
}

#[test]
fn a_null_credential_scope_fails_closed_at_startup() {
    let fixture = Fixture::new();
    let path = fixture.dir.path().join("server.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["projects"][0]["credentials"][0]["scope"] = Value::Null;
    fs::write(&path, config.to_string()).unwrap();
    let result = launch(&path).wait_with_output().unwrap();
    assert!(!result.status.success());
    assert!(!fixture.dir.path().join("projects/alpha").exists());
}

#[test]
fn sql_table_scopes_deny_indirect_table_reads_and_foreign_key_writes() {
    let fixture = Fixture::new();
    let path = fixture.dir.path().join("server.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["projects"][0]["credentials"][2]["scope"] = json!({
        "sql_tables": {"catalog":"read", "measurements":"write", "ingest":"write_only"}
    });
    fs::write(&path, config.to_string()).unwrap();
    let server = fixture.start();
    let sql = |identity, token, statement| {
        fixture.send(
            &server,
            "alpha",
            identity,
            token,
            json!({"op":"sql", "statement":statement}),
        )
    };
    for statement in [
        "CREATE TABLE catalog (id INT PRIMARY KEY)",
        "CREATE TABLE measurements (id INT UNIQUE, recorded_at INT, PRIMARY KEY (recorded_at, id))",
        "CREATE TABLE ingest (id INT PRIMARY KEY)",
        "CREATE TABLE private (id INT PRIMARY KEY)",
        "INSERT INTO catalog VALUES (1)",
        "INSERT INTO private VALUES (1)",
    ] {
        assert_eq!(sql("admin", ADMIN, statement)["ok"], true, "{statement}");
    }
    assert_eq!(
        sql("writer", WRITER, "SELECT id FROM catalog")["result"]["rows"],
        json!([[1]])
    );
    assert_eq!(
        sql(
            "writer",
            WRITER,
            "INSERT INTO measurements VALUES (1, 100), (2, 200)"
        )["ok"],
        true
    );
    assert_eq!(
        sql(
            "writer",
            WRITER,
            "SELECT id FROM measurements WHERE recorded_at >= 150 ORDER BY recorded_at"
        )["result"]["rows"],
        json!([[2]])
    );
    assert_eq!(
        sql("writer", WRITER, "INSERT INTO ingest VALUES (1)")["ok"],
        true
    );
    for statement in [
        "SELECT * FROM ingest",
        "SELECT * FROM private",
        "INSERT INTO catalog VALUES (2)",
        "SELECT (SELECT id FROM private) FROM catalog",
        "SELECT id FROM catalog WHERE id IN (SELECT id FROM private)",
        "SELECT catalog.id FROM catalog JOIN private ON catalog.id = private.id",
        "WITH leaked AS (SELECT id FROM private) SELECT id FROM leaked",
        "INSERT INTO measurements VALUES ((SELECT id FROM private), 0)",
        "UPDATE measurements SET recorded_at = (SELECT id FROM private)",
    ] {
        assert_eq!(
            sql("writer", WRITER, statement),
            error("forbidden"),
            "{statement}"
        );
    }
    assert_eq!(
        sql(
            "admin",
            ADMIN,
            "CREATE TABLE linked (id INT PRIMARY KEY, parent INT REFERENCES measurements(id) ON DELETE CASCADE)"
        )["ok"],
        true
    );
    assert_eq!(
        sql("admin", ADMIN, "INSERT INTO linked VALUES (1, 1)")["ok"],
        true
    );
    assert_eq!(
        sql("writer", WRITER, "DELETE FROM measurements WHERE id = 1"),
        error("forbidden")
    );
    assert_eq!(
        sql("admin", ADMIN, "SELECT id FROM linked")["result"]["rows"],
        json!([[1]])
    );
    assert_eq!(
        sql("writer", WRITER, "SELECT id FROM measurements ORDER BY id")["result"]["rows"],
        json!([[1], [2]])
    );
}

#[test]
fn sigterm_closes_the_listener_and_drains_an_admitted_mutation() {
    use rustix::process::{Pid, Signal, kill_process};

    let fixture = Fixture::new();
    let mut server = fixture.start();
    let mut connection = fixture.connection(&server, false);
    connection.conn.complete_io(&mut connection.sock).unwrap();
    let request = json!({
        "version":1, "project":"alpha", "credential":"admin", "token":ADMIN,
        "operation":{"op":"kv_put","key":"drained","value_hex":"12"}
    })
    .to_string();
    // Keep an admitted request in flight when shutdown starts.
    connection
        .write_all(&(request.len() as u32).to_be_bytes())
        .unwrap();
    kill_process(
        Pid::from_raw(server.child.id() as i32).unwrap(),
        Signal::TERM,
    )
    .unwrap();
    let stop_deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect_timeout(&server.addr, Duration::from_millis(50)) {
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => break,
            _ => {
                assert!(
                    std::time::Instant::now() < stop_deadline,
                    "listener stayed open"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    assert!(server.child.try_wait().unwrap().is_none());
    connection.write_all(request.as_bytes()).unwrap();
    assert_eq!(response(&mut connection), json!({"ok":true,"result":null}));
    drop(connection);
    let exit_deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            std::time::Instant::now() < exit_deadline,
            "server did not drain"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let server = fixture.start();
    let get = json!({"op":"kv_get","key":"drained"});
    assert_eq!(
        fixture.send(&server, "alpha", "admin", ADMIN, get)["result"],
        "12"
    );
}

#[test]
fn credential_reloads_rotate_revoke_and_reject_invalid_snapshots_atomically() {
    use rustix::process::{Pid, Signal, kill_process};

    const ROTATED: &str = "0606060606060606060606060606060606060606060606060606060606060606";
    let fixture = Fixture::new();
    let root = fixture.dir.path();
    let config_path = root.join("server.json");
    let credentials_path = root.join("credentials.json");
    let stderr_path = root.join("server.stderr");
    let mut config: Value = serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
    config["credentials_file"] = json!(credentials_path);
    let mut auth = json!({"projects":[]});
    for project in config["projects"].as_array_mut().unwrap() {
        let credentials = project
            .as_object_mut()
            .unwrap()
            .remove("credentials")
            .unwrap();
        auth["projects"]
            .as_array_mut()
            .unwrap()
            .push(json!({"id":project["id"], "credentials":credentials}));
    }
    fs::write(&config_path, config.to_string()).unwrap();
    let publish = |value: &Value| {
        let temporary = root.join("credentials.tmp");
        fs::write(&temporary, value.to_string()).unwrap();
        fs::rename(temporary, &credentials_path).unwrap();
    };
    publish(&auth);
    let server = Fixture::wait_ready(
        Command::new(env!("CARGO_BIN_EXE_emda-server"))
            .arg(&config_path)
            .stdout(Stdio::piped())
            .stderr(fs::File::create(&stderr_path).unwrap())
            .spawn()
            .unwrap(),
    );

    let get = |project, identity, token| {
        fixture.send(
            &server,
            project,
            identity,
            token,
            json!({"op":"kv_get","key":"durable"}),
        )
    };
    assert_eq!(
        fixture.send(
            &server,
            "alpha",
            "admin",
            ADMIN,
            json!({"op":"kv_put","key":"durable","value_hex":"ab"})
        )["ok"],
        true
    );
    let reload = |message: &str| {
        kill_process(
            Pid::from_raw(server.child.id() as i32).unwrap(),
            Signal::HUP,
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let log = fs::read_to_string(&stderr_path).unwrap();
            if log.contains(message) {
                assert!(!log.contains(ROTATED));
                assert!(!log.contains(ADMIN));
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "missing reload result"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    };

    let mut next = auth.clone();
    next["projects"][0]["credentials"] = json!([credential("admin", ROTATED, "read", u64::MAX)]);
    let mut invalid = next.clone();
    invalid["projects"][1]["credentials"] =
        json!([credential("other", ROTATED, "admin", u64::MAX)]);
    publish(&invalid);
    reload("rejected credential reload");
    assert_eq!(get("alpha", "admin", ADMIN)["result"], "ab");
    assert_eq!(get("alpha", "admin", ROTATED), error("unauthorized"));
    assert_eq!(get("beta", "admin", OTHER)["ok"], true);

    next["projects"][1]["credentials"] = json!([]);
    publish(&next);
    reload("credential policy reloaded");
    assert_eq!(get("alpha", "admin", ADMIN), error("unauthorized"));
    assert_eq!(get("alpha", "reader", READER), error("unauthorized"));
    assert_eq!(get("beta", "admin", OTHER), error("unauthorized"));
    assert_eq!(get("alpha", "admin", ROTATED)["result"], "ab");
    assert_eq!(
        fixture.send(
            &server,
            "alpha",
            "admin",
            ROTATED,
            json!({"op":"kv_put","key":"durable","value_hex":"ff"})
        ),
        error("forbidden")
    );

    drop(server);
    let server = fixture.start();
    assert_eq!(
        fixture.send(
            &server,
            "alpha",
            "admin",
            ROTATED,
            json!({"op":"kv_get","key":"durable"})
        )["result"],
        "ab"
    );
}

#[test]
fn a_project_rate_limit_preserves_other_projects_admission() {
    let fixture = Fixture::new();
    let path = fixture.dir.path().join("server.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["projects"][0]["request_rate"] = json!({"requests_per_second":1,"burst":1});
    config["projects"][0]["idle_timeout_ms"] = 500.into();
    fs::write(&path, config.to_string()).unwrap();
    let server = fixture.start();
    let status = json!({"op":"project_status"});
    assert_eq!(
        fixture.send(&server, "alpha", "reader", READER, status.clone()),
        error("forbidden")
    );
    let initial = fixture.send(&server, "alpha", "admin", ADMIN, status.clone());
    assert_eq!(initial["result"]["storage"], "closed");
    assert!(
        !fixture
            .dir
            .path()
            .join("projects/alpha/data/store.lock")
            .exists()
    );
    let get = json!({"op":"kv_get","key":"rate"});
    assert_eq!(
        fixture.send(&server, "alpha", "admin", OTHER, get.clone()),
        error("unauthorized")
    );
    assert_eq!(
        fixture.send(&server, "alpha", "admin", ADMIN, get.clone())["ok"],
        true
    );
    let mut throttled = false;
    for _ in 0..5 {
        if fixture.send(&server, "alpha", "admin", ADMIN, get.clone()) == error("rate_limited") {
            throttled = true;
            break;
        }
    }
    assert!(
        throttled,
        "no immediate request encountered the exhausted rate budget"
    );
    assert_eq!(
        fixture.send(&server, "beta", "admin", OTHER, get)["ok"],
        true
    );
    let alpha = fixture.send(&server, "alpha", "admin", ADMIN, status.clone());
    assert_eq!(
        alpha["result"]["request_rate"],
        json!({"requests_per_second":1,"burst":1})
    );
    assert_eq!(alpha["result"]["requests"]["rate_limited"], 1);
    let beta = fixture.send(&server, "beta", "admin", OTHER, status);
    assert_eq!(beta["result"]["requests"]["rate_limited"], 0);
}
