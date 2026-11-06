#![no_main]
//! Model recognized, malformed, retired and unrelated staging entries.
use database_engine::cleanup_abandoned_staging;
use libfuzzer_sys::fuzz_target;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

// A valid retired marker for an all-zero identity and its CRC32. Generic
// cleanup must skip it regardless of whether its payload looks removable.
const RETIRED_MARKER: &[u8] =
    b"EMDA-STAGING-V1-RETIRED\n\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x5d\xf8\x2f\xf0";

#[derive(Debug, PartialEq, Eq)]
enum Snapshot {
    Missing,
    File(Vec<u8>),
    Symlink(PathBuf),
    Directory(Vec<(OsString, Snapshot)>),
}

fn snapshot(path: &Path) -> Snapshot {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Snapshot::Missing,
        Err(error) => panic!("snapshot metadata: {error}"),
    };
    if metadata.file_type().is_symlink() {
        Snapshot::Symlink(fs::read_link(path).expect("read link"))
    } else if metadata.is_file() {
        Snapshot::File(fs::read(path).expect("read file"))
    } else if metadata.is_dir() {
        let mut entries: Vec<_> = fs::read_dir(path)
            .expect("read directory")
            .map(|entry| {
                let entry = entry.expect("read entry");
                (entry.file_name(), snapshot(&entry.path()))
            })
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Snapshot::Directory(entries)
    } else {
        panic!("unexpected fixture type");
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 64 {
        return;
    }
    let root = tempfile::tempdir().expect("temporary directory");
    let sentinel = root.path().join("unrelated");
    fs::write(&sentinel, b"preserve unrelated content").expect("write sentinel");
    let mut entries = Vec::new();
    let mut removed_payloads = 0;
    let mut removed_markers = 0;
    let mut skipped = 0;

    for (index, bytes) in data.chunks(3).take(8).enumerate() {
        let kind = bytes[0] % 4;
        let marker_state = bytes.get(1).copied().unwrap_or(0) % 7;
        let payload_state = bytes.get(2).copied().unwrap_or(0) % 4;
        let (prefix, content) = match kind {
            0 => ("f", &b"EMDA-STAGING-V1-FILE\n"[..]),
            1 => ("d", &b"EMDA-STAGING-V1-DIRECTORY\n"[..]),
            2 => ("r", RETIRED_MARKER),
            _ => ("x", &b"unrecognized marker\n"[..]),
        };
        let marker = root
            .path()
            .join(format!(".emda-stage-v1-{prefix}-{index:016}.owner"));
        let mut payload_name = marker.as_os_str().to_os_string();
        payload_name.push(".data");
        let payload = PathBuf::from(payload_name);
        match marker_state {
            0 => {}
            1 => fs::write(&marker, content).expect("write valid marker"),
            2 => {
                let mut corrupt = content.to_vec();
                corrupt[0] ^= 1;
                fs::write(&marker, corrupt).expect("write corrupt marker");
            }
            3 => fs::write(&marker, &content[..content.len() - 1]).expect("write short marker"),
            4 => {
                let mut long = content.to_vec();
                long.push(b'!');
                fs::write(&marker, long).expect("write long marker");
            }
            5 => symlink(&sentinel, &marker).expect("write marker symlink"),
            _ => fs::create_dir(&marker).expect("write marker directory"),
        }
        match payload_state {
            0 => {}
            1 => fs::write(&payload, [index as u8]).expect("write file payload"),
            2 => {
                fs::create_dir(&payload).expect("write directory payload");
                fs::write(payload.join("nested"), [index as u8]).expect("write nested payload");
                symlink(&sentinel, payload.join("link")).expect("write nested link");
            }
            _ => symlink(&sentinel, &payload).expect("write payload symlink"),
        }
        let matching = payload_state == 0
            || (kind == 0 && payload_state == 1)
            || (kind == 1 && payload_state == 2);
        let remove = kind < 2 && marker_state == 1 && matching;
        if remove {
            removed_markers += 1;
            removed_payloads += usize::from(payload_state != 0);
        } else if marker_state != 0 && kind < 3 {
            skipped += 1;
        }
        entries.push((
            marker.clone(),
            payload.clone(),
            snapshot(&marker),
            snapshot(&payload),
            remove,
        ));
    }

    let report = cleanup_abandoned_staging(root.path()).expect("cleanup fixture");
    assert_eq!(report.removed_markers, removed_markers);
    assert_eq!(report.removed_payloads, removed_payloads);
    assert_eq!(report.skipped, skipped);
    for (marker, payload, old_marker, old_payload, remove) in &entries {
        if *remove {
            assert_eq!(snapshot(marker), Snapshot::Missing);
            assert_eq!(snapshot(payload), Snapshot::Missing);
        } else {
            assert_eq!(&snapshot(marker), old_marker);
            assert_eq!(&snapshot(payload), old_payload);
        }
    }
    assert_eq!(
        fs::read(&sentinel).expect("sentinel retained"),
        b"preserve unrelated content"
    );
    let after = snapshot(root.path());
    let again = cleanup_abandoned_staging(root.path()).expect("repeat cleanup");
    assert_eq!(again.removed_markers, 0);
    assert_eq!(again.removed_payloads, 0);
    assert_eq!(snapshot(root.path()), after);
});
