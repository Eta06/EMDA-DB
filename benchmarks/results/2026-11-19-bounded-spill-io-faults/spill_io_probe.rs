use database_engine::{BatchOperation, CheckpointStore, ManagedRebuildFailure, Store};
use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};
use std::{fs, io, path::Path};

type ProbeResult<T> = Result<T, Box<dyn std::error::Error>>;

fn marker(store: &Path, v3: bool) -> io::Result<Vec<u8>> {
    if v3 {
        let mut bytes = [0; 64];
        let file = fs::File::open(store.join("records.log"))?;
        let size = fgetxattr(&file, "user.db.commit.v3", &mut bytes)?;
        assert_eq!(size, 36);
        Ok(bytes[..size].to_vec())
    } else {
        fs::read(store.join("commit.state"))
    }
}

fn set_marker(store: &Path, v3: bool, bytes: &[u8]) -> io::Result<()> {
    if v3 {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(store.join("records.log"))?;
        fsetxattr(&file, "user.db.commit.v3", bytes, XattrFlags::REPLACE)?;
        file.sync_all()
    } else {
        let path = store.join("commit.state");
        fs::write(&path, bytes)?;
        fs::File::open(path)?.sync_all()
    }
}

fn prepare(root: &Path, v3: bool, unacknowledged: bool) -> ProbeResult<()> {
    let source = root.join("store");
    if !v3 {
        fs::create_dir(&source)?;
        fs::write(
            source.join("records.log"),
            include_bytes!("../tests/fixtures/v2-empty-records.log"),
        )?;
        fs::write(
            source.join("commit.state"),
            include_bytes!("../tests/fixtures/v2-empty-commit.state"),
        )?;
    }
    let store = Store::open(&source)?;
    store.put(b"a", b"base")?;
    store.put(b"b", b"gone")?;
    drop(store);
    let digest = CheckpointStore::rebuild_managed_selection(&source, root.join("base"))?;
    fs::write(root.join("digest"), digest)?;
    let base_marker = marker(&source, v3)?;
    let store = Store::open(&source)?;
    store.write_batch(&[
        BatchOperation::Put {
            key: b"d",
            value: b"first",
        },
        BatchOperation::Delete { key: b"b" },
        BatchOperation::Put {
            key: b"a",
            value: b"middle",
        },
        BatchOperation::Put {
            key: b"e",
            value: b"",
        },
        BatchOperation::Put {
            key: b"d",
            value: b"final",
        },
        BatchOperation::Delete { key: b"missing" },
        BatchOperation::Delete { key: b"e" },
        BatchOperation::Put {
            key: b"a",
            value: b"latest",
        },
        BatchOperation::Put {
            key: b"f",
            value: b"fresh",
        },
    ])?;
    drop(store);
    let final_marker = marker(&source, v3)?;
    assert_ne!(base_marker, final_marker);
    fs::write(root.join("expected-witness"), &final_marker)?;
    if unacknowledged {
        set_marker(&source, v3, &base_marker)?;
    }
    let log = fs::read(source.join("records.log"))?;
    assert_eq!(&log[..8], if v3 { b"EMDALOG3" } else { b"EMDALOG2" });
    Ok(())
}

fn attempt(root: &Path) -> ProbeResult<()> {
    let digest = fs::read(root.join("digest"))?;
    match CheckpointStore::rebuild_managed_selection_bounded_detailed(
        root.join("store"),
        root.join("result"),
        root.join("base"),
        digest.as_slice().try_into()?,
        1,
        4096,
        1 << 20,
        2,
        9,
    ) {
        Ok(_) => println!("published"),
        Err(ManagedRebuildFailure::NotPublished(error)) => {
            println!("not_published_errno={:?}", error.raw_os_error());
            println!("{error}");
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn verify(root: &Path, v3: bool) -> ProbeResult<()> {
    let source = root.join("store");
    assert_eq!(
        marker(&source, v3)?,
        fs::read(root.join("expected-witness"))?
    );
    let expected: Vec<_> = [
        (b"a", b"latest".as_slice()),
        (b"d", b"final"),
        (b"f", b"fresh"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_vec(), v.to_vec()))
    .collect();
    let store = CheckpointStore::open_managed(&source)?;
    assert_eq!(store.overlay_keys()?, 0);
    assert_eq!(store.scan(None, None, 4)?, expected);
    drop(store);
    let store = Store::open(&source)?;
    assert_eq!(store.scan(None, None, 4)?, expected);
    store.checkpoint_to(root.join("reference"))?;
    assert_eq!(
        fs::read(root.join("result"))?,
        fs::read(root.join("reference"))?
    );
    println!("reference matches every key, value and checkpoint byte");
    Ok(())
}

fn main() -> ProbeResult<()> {
    let args: Vec<_> = std::env::args_os().collect();
    let [_, action, root, format, tail] = args.as_slice() else {
        return Err(
            "expected action, absolute fixture directory, v2 or v3, and acked or tail".into(),
        );
    };
    let root = Path::new(root);
    let v3 = match format.to_str() {
        Some("v2") => false,
        Some("v3") => true,
        _ => return Err("expected v2 or v3".into()),
    };
    match action.to_str() {
        Some("prepare") => prepare(root, v3, tail == "tail"),
        Some("attempt") => attempt(root),
        Some("verify") => verify(root, v3),
        _ => Err("unknown action".into()),
    }
}
