//! Kill actual commits at selected log and witness I/O boundaries.
//! This exercises a running OS, not loss of kernel or device caches.
use super::*;
use crate::CheckpointStore;
use crate::log_io::LogIo;
use crate::test_support::retry_lock;
use crate::witness::{Witness, WitnessIo};
use rustix::fs::{XattrFlags, fgetxattr, fsetxattr, getxattr};
use std::fs;
use std::io::{BufRead, Write};
use std::os::unix::fs::FileExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const XATTR: &str = "user.db.commit.v3";
const KEYS: &[&[u8]] = &[b"a", b"b", b"c", b"safe"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    V2,
    V3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Point {
    BeforeAppend,
    PartialAppend,
    AfterAppend,
    BeforeLogSync,
    AfterLogSync,
    BeforeWitnessWrite,
    AfterWitnessWrite,
    BeforeWitnessSync,
    AfterWitnessSync,
}

impl Point {
    fn name(self) -> &'static str {
        match self {
            Self::BeforeAppend => "before-append",
            Self::PartialAppend => "partial-append",
            Self::AfterAppend => "after-append",
            Self::BeforeLogSync => "before-log-sync",
            Self::AfterLogSync => "after-log-sync",
            Self::BeforeWitnessWrite => "before-witness-write",
            Self::AfterWitnessWrite => "after-witness-write",
            Self::BeforeWitnessSync => "before-witness-sync",
            Self::AfterWitnessSync => "after-witness-sync",
        }
    }

    fn parse(name: &str) -> Self {
        ALL_POINTS
            .iter()
            .copied()
            .find(|point| point.name() == name)
            .expect("unknown commit interruption point")
    }
}

const ALL_POINTS: &[Point] = &[
    Point::BeforeAppend,
    Point::PartialAppend,
    Point::AfterAppend,
    Point::BeforeLogSync,
    Point::AfterLogSync,
    Point::BeforeWitnessWrite,
    Point::AfterWitnessWrite,
    Point::BeforeWitnessSync,
    Point::AfterWitnessSync,
];

fn pause_if(selected: Point, current: Point) {
    if selected == current {
        println!("EMDA_COMMIT_PROCESS_BOUNDARY_REACHED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}

struct PausingLog {
    file: File,
    point: Point,
}

impl LogIo for PausingLog {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, bytes, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        pause_if(self.point, Point::BeforeAppend);
        if self.point == Point::PartialAppend {
            assert!(bytes.len() > 1);
            (&self.file).write_all(&bytes[..bytes.len() / 2])?;
            pause_if(self.point, Point::PartialAppend);
        }
        (&self.file).write_all(bytes)?;
        pause_if(self.point, Point::AfterAppend);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        pause_if(self.point, Point::BeforeLogSync);
        self.file.sync_data()?;
        pause_if(self.point, Point::AfterLogSync);
        Ok(())
    }
}

struct PausingWitness {
    file: File,
    point: Point,
    format: Format,
}

impl WitnessIo for PausingWitness {
    fn len(&self) -> io::Result<u64> {
        match self.format {
            Format::V2 => Ok(self.file.metadata()?.len()),
            Format::V3 => {
                let mut bytes = [0; 36];
                Ok(fgetxattr(&self.file, XATTR, &mut bytes[..])? as u64)
            }
        }
    }

    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        match self.format {
            Format::V2 => FileExt::read_exact_at(&self.file, bytes, offset),
            Format::V3 => {
                assert_eq!(offset, 0);
                let length = fgetxattr(&self.file, XATTR, &mut *bytes)?;
                if length != bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "short witness",
                    ));
                }
                Ok(())
            }
        }
    }

    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        pause_if(self.point, Point::BeforeWitnessWrite);
        match self.format {
            Format::V2 => FileExt::write_all_at(&self.file, bytes, offset)?,
            Format::V3 => {
                assert_eq!(offset, 0);
                fsetxattr(&self.file, XATTR, bytes, XattrFlags::REPLACE)?;
            }
        }
        pause_if(self.point, Point::AfterWitnessWrite);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        pause_if(self.point, Point::BeforeWitnessSync);
        match self.format {
            Format::V2 => self.file.sync_data()?,
            Format::V3 => self.file.sync_all()?,
        }
        pause_if(self.point, Point::AfterWitnessSync);
        Ok(())
    }
}

fn instrument(log: &mut Log, path: &Path, format: Format, point: Point) {
    let log_path = path.join("records.log");
    let header = fs::read(&log_path).unwrap();
    let magic: &[u8] = match format {
        Format::V2 => b"EMDALOG2",
        Format::V3 => b"EMDALOG3",
    };
    assert_eq!(&header[..8], magic);
    let id = header[8..24].try_into().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let witness_path = match format {
        Format::V2 => path.join("commit.state"),
        Format::V3 => log_path,
    };
    let witness_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(witness_path)
        .unwrap();
    let (witness, _) = Witness::from_io(
        Box::new(PausingWitness {
            file: witness_file,
            point,
            format,
        }),
        id,
    )
    .unwrap();
    log.replace_io_for_test(Box::new(PausingLog { file, point }));
    log.attach_test_witness(witness);
}

#[test]
fn interrupted_commit_child() {
    let Some(path) = std::env::var_os("EMDA_COMMIT_PROCESS_PATH") else {
        return;
    };
    let path = Path::new(&path);
    let point = Point::parse(&std::env::var("EMDA_COMMIT_PROCESS_POINT").unwrap());
    let format = match std::env::var("EMDA_COMMIT_PROCESS_FORMAT")
        .unwrap()
        .as_str()
    {
        "v2" => Format::V2,
        "v3" => Format::V3,
        _ => panic!("unknown format"),
    };
    let operations = [
        crate::BatchOperation::Put {
            key: b"a",
            value: b"new-a",
        },
        crate::BatchOperation::Delete { key: b"b" },
        crate::BatchOperation::Put {
            key: b"c",
            value: b"new-c",
        },
    ];
    if std::env::var("EMDA_COMMIT_PROCESS_BACKEND").unwrap() == "checkpoint" {
        let checkpoint = std::env::var_os("EMDA_COMMIT_PROCESS_CHECKPOINT").unwrap();
        let digest: [u8; 32] = fs::read(std::env::var_os("EMDA_COMMIT_PROCESS_DIGEST").unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let mut store = retry_lock(|| CheckpointStore::open(path, &checkpoint, &digest)).unwrap();
        instrument(store.commit_process_test_log(), path, format, point);
        store.write_batch(&operations).unwrap();
    } else {
        let mut store = retry_lock(|| Store::open(path)).unwrap();
        instrument(&mut store.log, path, format, point);
        store.write_batch(&operations).unwrap();
    }
    panic!("commit passed its requested interruption point");
}

fn marker(path: &Path, format: Format) -> Vec<u8> {
    match format {
        Format::V2 => fs::read(path.join("commit.state")).unwrap(),
        Format::V3 => {
            let mut bytes = [0; 36];
            assert_eq!(
                getxattr(path.join("records.log"), XATTR, &mut bytes[..]).unwrap(),
                36
            );
            bytes.to_vec()
        }
    }
}

fn values(
    path: &Path,
    checkpoint: &Path,
    digest: &[u8; 32],
    checkpoint_backend: bool,
) -> io::Result<Vec<Option<Vec<u8>>>> {
    if checkpoint_backend {
        retry_lock(|| CheckpointStore::open(path, checkpoint, digest))?.get_many(KEYS)
    } else {
        retry_lock(|| Store::open(path))?.get_many(KEYS)
    }
}

fn expected(complete: bool) -> Vec<Option<Vec<u8>>> {
    if complete {
        vec![
            Some(b"new-a".to_vec()),
            None,
            Some(b"new-c".to_vec()),
            Some(b"guard".to_vec()),
        ]
    } else {
        vec![
            Some(b"old-a".to_vec()),
            Some(b"old-b".to_vec()),
            None,
            Some(b"guard".to_vec()),
        ]
    }
}

fn run_case(format: Format, checkpoint_backend: bool, point: Point) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    fs::create_dir(&path).unwrap();
    if format == Format::V2 {
        fs::write(
            path.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            path.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old-a").unwrap();
    store.put(b"b", b"old-b").unwrap();
    store.put(b"safe", b"guard").unwrap();
    let checkpoint = root.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let digest_path = root.path().join("digest");
    fs::write(&digest_path, digest).unwrap();
    let old_log = fs::read(path.join("records.log")).unwrap();
    let anchor = fs::read(path.join("commit.state")).unwrap();
    drop(store);

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "store::commit_process_tests::interrupted_commit_child",
            "--nocapture",
        ])
        .env("EMDA_COMMIT_PROCESS_PATH", &path)
        .env("EMDA_COMMIT_PROCESS_POINT", point.name())
        .env(
            "EMDA_COMMIT_PROCESS_FORMAT",
            if format == Format::V2 { "v2" } else { "v3" },
        )
        .env(
            "EMDA_COMMIT_PROCESS_BACKEND",
            if checkpoint_backend {
                "checkpoint"
            } else {
                "ordinary"
            },
        )
        .env("EMDA_COMMIT_PROCESS_CHECKPOINT", &checkpoint)
        .env("EMDA_COMMIT_PROCESS_DIGEST", &digest_path)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(output)
            .lines()
            .map_while(Result::ok)
        {
            if line.contains("EMDA_COMMIT_PROCESS_BOUNDARY_REACHED") {
                let _ = sender.send(());
                break;
            }
        }
    });
    let reached = receiver.recv_timeout(Duration::from_secs(20));
    let busy = reached.is_ok()
        && matches!(Store::open(&path), Err(ref error) if error.kind() == io::ErrorKind::WouldBlock);
    let interrupted_log = fs::read(path.join("records.log")).unwrap();
    let interrupted_marker = marker(&path, format);
    let killed = child.kill();
    child.wait().unwrap();
    reached.unwrap_or_else(|error| panic!("{format:?} {checkpoint_backend} {point:?}: {error}"));
    killed.unwrap();
    assert!(busy, "active commit must retain the store lock");
    assert!(interrupted_log.starts_with(&old_log));
    if point == Point::BeforeAppend {
        assert_eq!(interrupted_log, old_log);
    } else {
        assert!(interrupted_log.len() > old_log.len());
    }
    for backend in [checkpoint_backend, !checkpoint_backend] {
        let result = values(&path, &checkpoint, &digest, backend);
        if point == Point::PartialAppend {
            assert!(
                matches!(result, Err(ref error) if matches!(error.kind(), io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof))
            );
            assert_eq!(marker(&path, format), interrupted_marker);
        } else {
            assert_eq!(result.unwrap(), expected(point != Point::BeforeAppend));
            let current = marker(&path, format);
            assert_eq!(
                u64::from_le_bytes(current[24..32].try_into().unwrap()),
                interrupted_log.len() as u64
            );
        }
        assert_eq!(fs::read(path.join("records.log")).unwrap(), interrupted_log);
        if format == Format::V3 {
            assert_eq!(fs::read(path.join("commit.state")).unwrap(), anchor);
        }
    }
}

#[test]
fn killed_commits_preserve_acknowledged_data_or_fail_closed() {
    for format in [Format::V2, Format::V3] {
        for checkpoint_backend in [false, true] {
            for &point in ALL_POINTS {
                if format == Format::V3
                    && matches!(point, Point::BeforeLogSync | Point::AfterLogSync)
                {
                    continue;
                }
                run_case(format, checkpoint_backend, point);
            }
        }
    }
}
