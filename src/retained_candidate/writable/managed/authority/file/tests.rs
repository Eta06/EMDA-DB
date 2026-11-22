use super::*;
use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};

fn initial() -> AuthorityRecord {
    AuthorityRecord {
        revision: 0,
        committed: AuthorityPoint {
            catalog: [3; 16],
            generation: [5; 16],
            header_digest: [7; 32],
            suffix_end: 0,
            suffix_digest: Sha256::digest([]).into(),
        },
        pending: None,
    }
}

fn intent(value: u8) -> AuthorityRecord {
    let mut next = initial();
    next.revision = 1;
    next.pending = Some(AuthorityPoint {
        suffix_end: 12,
        suffix_digest: [value; 32],
        ..next.committed
    });
    next
}

#[test]
fn file_authority_validates_transitions_and_detects_damage() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("authority");
    let mut authority = FileRetainedAuthority::create(&root).unwrap();
    assert_eq!(authority.read().unwrap(), None);
    assert!(authority.compare_and_swap(None, initial()).unwrap());
    assert!(!authority.compare_and_swap(None, initial()).unwrap());
    let mut skipped = intent(1);
    skipped.revision = 2;
    assert!(
        authority
            .compare_and_swap(Some(initial()), skipped)
            .is_err()
    );
    let prepared = intent(1);
    assert!(
        authority
            .compare_and_swap(Some(initial()), prepared)
            .unwrap()
    );
    let resolved = prepared.finalized(prepared.pending.unwrap()).unwrap();
    assert!(
        authority
            .compare_and_swap(Some(prepared), resolved)
            .unwrap()
    );
    assert_eq!(
        FileRetainedAuthority::open(&root).unwrap().read().unwrap(),
        Some(resolved)
    );

    let replacement = AuthorityRecord {
        committed: AuthorityPoint {
            generation: [8; 16],
            ..initial().committed
        },
        revision: 3,
        pending: None,
    };
    assert!(
        authority
            .compare_and_swap(Some(resolved), replacement)
            .unwrap()
    );
    let path = root.join(NAME);
    let mut bytes = fs::read(&path).unwrap();
    bytes[24] ^= 1;
    fs::write(&path, bytes).unwrap();
    assert!(authority.read().is_err());
    assert!(FileRetainedAuthority::open(&root).is_err());
    fs::remove_file(&path).unwrap();
    assert!(FileRetainedAuthority::open(&root).is_err());
}

#[test]
fn reuse_requires_the_same_validated_inode_and_successful_sync() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("authority");
    let mut first = registered(&root);
    let mut second = FileRetainedAuthority::open(&root).unwrap();
    for provider in [&mut first, &mut second] {
        assert_eq!(
            provider
                .read_using(|_| panic!("known record was synced"))
                .unwrap(),
            Some(initial())
        );
    }

    let pending = intent(1);
    let fail = |_: &Path| Err(io::Error::other("injected directory sync failure"));
    assert!(
        first
            .compare_and_swap_using(Some(initial()), pending, || Ok(()), fail,)
            .is_err()
    );
    for provider in [&mut first, &mut second] {
        // Both providers must synchronize the replacement, even after one
        // of them has already succeeded. A failed sync must never be cached.
        assert_eq!(
            provider.read_using(fail).unwrap_err().to_string(),
            "injected directory sync failure"
        );
        let mut called = false;
        assert_eq!(
            provider
                .read_using(|root| {
                    called = true;
                    sync_directory(root)
                })
                .unwrap(),
            Some(pending)
        );
        assert!(called);
        assert_eq!(
            provider
                .read_using(|_| panic!("replacement already synced"))
                .unwrap(),
            Some(pending)
        );
    }

    let copy = root.join("replacement");
    write_new(&copy, &encode(Some(pending))).unwrap();
    fs::rename(copy, root.join(NAME)).unwrap();
    assert!(second.read_using(fail).is_err());

    let path = root.join(NAME);
    assert_eq!(first.read().unwrap(), Some(pending));
    let mut bytes = fs::read(&path).unwrap();
    bytes[24] ^= 1;
    fs::write(path, bytes).unwrap();
    assert!(
        first
            .read_using(|_| panic!("corrupt record needs no sync"))
            .is_err()
    );
}

#[test]
fn file_authority_child() {
    let Some(root) = std::env::var_os("EMDA_FILE_AUTHORITY_ROOT") else {
        return;
    };
    let action = std::env::var("EMDA_FILE_AUTHORITY_ACTION").unwrap();
    let mut authority = FileRetainedAuthority::open(root).unwrap();
    let pause = |boundary| -> io::Result<()> {
        if action == boundary {
            println!("authority boundary {boundary}");
            io::stdout().flush()?;
            loop {
                std::thread::park();
            }
        }
        Ok(())
    };
    let value = if action == "second" { 2 } else { 1 };
    let result = authority
        .compare_and_swap_using(
            Some(initial()),
            intent(value),
            || pause("before"),
            |root| {
                pause("after")?;
                sync_directory(root)
            },
        )
        .unwrap();
    println!("authority winner {result}");
}

fn child(root: &Path, action: &str) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "retained_candidate::writable::managed::authority::file::tests::file_authority_child",
            "--nocapture",
        ])
        .env("EMDA_FILE_AUTHORITY_ROOT", root)
        .env("EMDA_FILE_AUTHORITY_ACTION", action)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
}

fn registered(root: &Path) -> FileRetainedAuthority {
    let mut authority = FileRetainedAuthority::create(root).unwrap();
    assert!(authority.compare_and_swap(None, initial()).unwrap());
    authority
}

#[test]
fn two_processes_can_confirm_only_one_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("authority");
    let mut authority = registered(&root);
    let first = child(&root, "first");
    let second = child(&root, "second");
    let outputs = [
        first.wait_with_output().unwrap(),
        second.wait_with_output().unwrap(),
    ];
    let mut winners = Vec::new();
    for (index, output) in outputs.into_iter().enumerate() {
        assert!(output.status.success(), "{output:?}");
        let output = String::from_utf8(output.stdout).unwrap();
        if output.contains("authority winner true") {
            winners.push(index as u8 + 1);
        } else {
            assert!(output.contains("authority winner false"), "{output}");
        }
    }
    assert_eq!(winners.len(), 1);
    assert_eq!(authority.read().unwrap(), Some(intent(winners[0])));
}

#[test]
fn process_kills_preserve_one_complete_authority_record() {
    for boundary in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("authority");
        let mut authority = registered(&root);
        let mut child = child(&root, boundary);
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let (seen, notify) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in stdout.lines().map_while(Result::ok) {
                if line == format!("authority boundary {boundary}") {
                    seen.send(()).unwrap();
                    break;
                }
            }
        });
        let reached = notify.recv_timeout(Duration::from_secs(15));
        let killed = child.kill();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        reached.expect("authority child failed to reach kill boundary");
        killed.unwrap();
        assert!(!status.success());
        let visible = if boundary == "before" {
            initial()
        } else {
            intent(1)
        };
        assert_eq!(authority.read().unwrap(), Some(visible));
        crate::cleanup_abandoned_staging(&root).unwrap();
        assert_eq!(
            FileRetainedAuthority::open(&root).unwrap().read().unwrap(),
            Some(visible)
        );
    }
}
