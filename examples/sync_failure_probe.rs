//! Disposable fixture operations for the Linux syscall EIO injection driver.
use database_engine::{Store, cleanup_abandoned_staging};
use serde_json::json;
use std::error::Error;
use std::io;
use std::path::Path;

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn check_values(store: &Store) -> ProbeResult<()> {
    if store.get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
        || store.get(b"old")?.as_deref() != Some(&b"original"[..])
    {
        return Err("acknowledged fixture values changed".into());
    }
    Ok(())
}

fn setup(root: &Path, format: &str) -> ProbeResult<()> {
    if !root.is_dir() || std::fs::read_dir(root)?.next().is_some() {
        return Err("fixture root must be an existing empty directory".into());
    }
    let source = root.join("source");
    std::fs::create_dir(&source)?;
    match format {
        "v2" => {
            std::fs::write(
                source.join("records.log"),
                include_bytes!("../tests/fixtures/v2-empty-records.log"),
            )?;
            std::fs::write(
                source.join("commit.state"),
                include_bytes!("../tests/fixtures/v2-empty-commit.state"),
            )?;
        }
        "v3" => {}
        _ => return Err("unknown format".into()),
    }
    let store = Store::open(&source)?;
    store.put(b"safe", b"acknowledged")?;
    store.put(b"old", b"original")?;
    let expected: &[u8] = if format == "v2" {
        b"EMDALOG2"
    } else {
        b"EMDALOG3"
    };
    if &std::fs::read(source.join("records.log"))?[..8] != expected {
        return Err("unexpected fixture log format".into());
    }
    store.backup(root.join("reference-backup"))?;
    println!("{}", json!({"status":"ready","format":format}));
    Ok(())
}

fn action(root: &Path, operation: &str) -> io::Result<()> {
    let source = root.join("source");
    let target = root.join("result");
    match operation {
        "write" => Store::open(source)?.put(b"new", b"attempted"),
        "backup" => Store::open(source)?.backup(target),
        "restore" => Store::restore(root.join("reference-backup"), target),
        "upgrade" => Store::open(source)?.upgrade_format(target),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unknown operation",
        )),
    }
}

fn attempt(root: &Path, operation: &str) -> ProbeResult<()> {
    let result = action(root, operation);
    match result {
        Ok(()) => println!("{}", json!({"status":"success","operation":operation})),
        Err(error) => println!(
            "{}",
            json!({"status":"error","operation":operation,"kind":format!("{:?}",error.kind()),"message":error.to_string()})
        ),
    }
    Ok(())
}

fn verify(root: &Path, operation: &str, status: &str) -> ProbeResult<()> {
    if !matches!(status, "success" | "error") {
        return Err("unknown outcome".into());
    }
    let source = Store::open(root.join("source"))?;
    check_values(&source)?;
    let new_value = source.get(b"new")?;
    if operation == "write" {
        if status == "success" && new_value.as_deref() != Some(&b"attempted"[..]) {
            return Err("successful write missing after reopen".into());
        }
        if new_value
            .as_deref()
            .is_some_and(|value| value != b"attempted")
        {
            return Err("unexpected recovered value".into());
        }
    } else if new_value.is_some() {
        return Err("copy operation changed source values".into());
    }
    drop(source);
    let target = root.join("result");
    let visible = target.exists();
    if operation != "write" && status == "success" && !visible {
        return Err("successful copy was not published".into());
    }
    if visible {
        if operation == "backup" {
            let restored = root.join("verify-copy");
            Store::restore(&target, &restored)?;
            check_values(&Store::open(restored)?)?;
        } else if matches!(operation, "restore" | "upgrade") {
            check_values(&Store::open(&target)?)?;
        } else {
            return Err("unexpected result path".into());
        }
    }
    let cleanup = cleanup_abandoned_staging(root)?;
    println!(
        "{}",
        json!({"status":"verified","operation":operation,"target_visible":visible,
        "new_present":new_value.is_some(),"cleanup_removed_markers":cleanup.removed_markers,
        "cleanup_removed_payloads":cleanup.removed_payloads,"cleanup_skipped":cleanup.skipped})
    );
    Ok(())
}

fn main() -> ProbeResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, root, format] if command == "setup" => setup(Path::new(root), format),
        [command, root, operation] if command == "attempt" => attempt(Path::new(root), operation),
        [command, root, operation, status] if command == "verify" => verify(Path::new(root), operation, status),
        _ => Err("usage: sync_failure_probe setup <empty-root> <v2|v3> | attempt <root> <write|backup|restore|upgrade> | verify <root> <operation> <success|error>".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifier_rejects_missing_data_and_partial_publication() {
        let root = tempfile::tempdir().unwrap();
        setup(root.path(), "v2").unwrap();
        assert!(verify(root.path(), "restore", "success").is_err());
        std::fs::write(root.path().join("result"), b"partial backup").unwrap();
        assert!(verify(root.path(), "backup", "error").is_err());
        std::fs::remove_file(root.path().join("result")).unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        store.put(b"safe", b"changed").unwrap();
        drop(store);
        assert!(verify(root.path(), "write", "error").is_err());
    }
}
