//! Disposable fixture operations for the Linux syscall EIO injection driver.
use database_engine::{CheckpointStore, CheckpointView, Store, cleanup_abandoned_staging};
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
    let checkpoint_digest = store.checkpoint_to(root.join("checkpoint"))?;
    std::fs::write(root.join("checkpoint.digest"), checkpoint_digest)?;
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
        "checkpoint" => Store::open(source)?.checkpoint_to(target).map(|_| ()),
        "compaction" => Store::open(source)?.compact_to(target),
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

fn inspect_source(
    get: impl Fn(&[u8]) -> io::Result<Option<Vec<u8>>>,
    operation: &str,
    status: &str,
) -> ProbeResult<bool> {
    if get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
        || get(b"old")?.as_deref() != Some(&b"original"[..])
    {
        return Err("acknowledged fixture values changed".into());
    }
    let new_value = get(b"new")?;
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
    Ok(new_value.is_some())
}

fn verify(root: &Path, operation: &str, status: &str, order: &str) -> ProbeResult<()> {
    if !matches!(status, "success" | "error") {
        return Err("unknown outcome".into());
    }
    let order = match order {
        "ordinary-first" => ["ordinary", "checkpoint"],
        "checkpoint-first" => ["checkpoint", "ordinary"],
        _ => return Err("unknown recovery order".into()),
    };
    let digest: [u8; 32] = std::fs::read(root.join("checkpoint.digest"))?
        .try_into()
        .map_err(|_| "invalid checkpoint digest")?;
    let mut new_present = None;
    for backend in order {
        let present = if backend == "ordinary" {
            let source = Store::open(root.join("source"))?;
            inspect_source(|key| source.get(key), operation, status)?
        } else {
            let source =
                CheckpointStore::open(root.join("source"), root.join("checkpoint"), &digest)?;
            inspect_source(|key| source.get(key), operation, status)?
        };
        if new_present.is_some_and(|previous| previous != present) {
            return Err("recovery paths disagree about the write".into());
        }
        new_present = Some(present);
    }
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
        } else if matches!(operation, "restore" | "upgrade" | "compaction") {
            check_values(&Store::open(&target)?)?;
        } else if operation == "checkpoint" {
            // This digest was independently saved before the attempted copy.
            let view = CheckpointView::open(root.join("source"), &target, &digest)?;
            if view.get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
                || view.get(b"old")?.as_deref() != Some(&b"original"[..])
            {
                return Err("published checkpoint has incorrect values".into());
            }
        } else {
            return Err("unexpected result path".into());
        }
    }
    let cleanup = cleanup_abandoned_staging(root)?;
    println!(
        "{}",
        json!({"status":"verified","operation":operation,"target_visible":visible,
        "new_present":new_present.unwrap(),"recovery_order":order,"cleanup_removed_markers":cleanup.removed_markers,
        "cleanup_removed_payloads":cleanup.removed_payloads,"cleanup_skipped":cleanup.skipped})
    );
    Ok(())
}

fn main() -> ProbeResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, root, format] if command == "setup" => setup(Path::new(root), format),
        [command, root, operation] if command == "attempt" => attempt(Path::new(root), operation),
        [command, root, operation, status, order] if command == "verify" => verify(Path::new(root), operation, status, order),
        _ => Err("usage: sync_failure_probe setup <empty-root> <v2|v3> | attempt <root> <write|backup|restore|upgrade|checkpoint|compaction> | verify <root> <operation> <success|error> <ordinary-first|checkpoint-first>".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifier_rejects_missing_data_and_partial_publication() {
        let root = tempfile::tempdir().unwrap();
        setup(root.path(), "v2").unwrap();
        assert!(verify(root.path(), "restore", "success", "ordinary-first").is_err());
        std::fs::write(root.path().join("result"), b"partial backup").unwrap();
        assert!(verify(root.path(), "backup", "error", "checkpoint-first").is_err());
        assert!(verify(root.path(), "checkpoint", "error", "ordinary-first").is_err());
        std::fs::remove_file(root.path().join("result")).unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        store.put(b"safe", b"changed").unwrap();
        drop(store);
        assert!(verify(root.path(), "write", "error", "ordinary-first").is_err());
    }
}
