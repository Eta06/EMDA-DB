//! Process termination during an actual group of independent writes.
//! The running OS retains its caches; this is not a power-loss simulation.
use super::*;
use crate::CheckpointStore;
use crate::log_io::LogIo;
use crate::test_support::retry_lock;
use crate::witness::{Witness, WitnessIo};
use rustix::fs::{XattrFlags, fgetxattr, fsetxattr, getxattr};
use std::fs;
use std::io::{BufRead, IoSlice, Write};
use std::os::unix::fs::FileExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const XATTR: &str = "user.db.commit.v3";
const KEYS: &[&[u8]] = &[b"safe", b"first", b"group-0", b"group-1", b"group-2"];

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

const POINTS: &[Point] = &[
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
        POINTS
            .iter()
            .copied()
            .find(|point| point.name() == name)
            .unwrap()
    }
}

struct Gate {
    point: Point,
    first_started: mpsc::Sender<()>,
    first_resume: Mutex<mpsc::Receiver<()>>,
    group_started: mpsc::Sender<()>,
    group_resume: Mutex<mpsc::Receiver<()>>,
    witness_writes: AtomicUsize,
    witness_syncs: AtomicUsize,
    log_syncs: AtomicUsize,
}

impl Gate {
    fn hit(&self, point: Point) {
        if self.point == point {
            self.group_started.send(()).unwrap();
            self.group_resume.lock().unwrap().recv().unwrap();
            println!("EMDA_GROUP_PROCESS_BOUNDARY_REACHED");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park();
            }
        }
    }
}

struct GroupLog {
    file: File,
    gate: Arc<Gate>,
}

impl LogIo for GroupLog {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, bytes, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        (&self.file).write_all(bytes)
    }

    fn append_vectored(&self, buffers: &mut [IoSlice<'_>]) -> io::Result<()> {
        assert_eq!(
            buffers.len(),
            3,
            "expected three independently queued writers"
        );
        self.gate.hit(Point::BeforeAppend);
        if self.gate.point == Point::PartialAppend {
            (&self.file).write_all(&buffers[0])?;
            let next = &buffers[1];
            assert!(next.len() > 1);
            (&self.file).write_all(&next[..next.len() / 2])?;
            self.gate.hit(Point::PartialAppend);
        }
        LogIo::append_vectored(&self.file, buffers)?;
        self.gate.hit(Point::AfterAppend);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let index = self.gate.log_syncs.fetch_add(1, Ordering::SeqCst);
        if index == 1 {
            self.gate.hit(Point::BeforeLogSync);
        }
        self.file.sync_data()?;
        if index == 1 {
            self.gate.hit(Point::AfterLogSync);
        }
        Ok(())
    }
}

struct GroupWitness {
    file: File,
    format: Format,
    gate: Arc<Gate>,
}

impl WitnessIo for GroupWitness {
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
                if fgetxattr(&self.file, XATTR, &mut *bytes)? != bytes.len() {
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
        let index = self.gate.witness_writes.fetch_add(1, Ordering::SeqCst);
        if index == 1 {
            self.gate.hit(Point::BeforeWitnessWrite);
        }
        match self.format {
            Format::V2 => FileExt::write_all_at(&self.file, bytes, offset)?,
            Format::V3 => {
                assert_eq!(offset, 0);
                fsetxattr(&self.file, XATTR, bytes, XattrFlags::REPLACE)?;
            }
        }
        if index == 1 {
            self.gate.hit(Point::AfterWitnessWrite);
        }
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let index = self.gate.witness_syncs.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            self.gate.first_started.send(()).unwrap();
            self.gate.first_resume.lock().unwrap().recv().unwrap();
        } else if index == 1 {
            self.gate.hit(Point::BeforeWitnessSync);
        }
        match self.format {
            Format::V2 => self.file.sync_data()?,
            Format::V3 => self.file.sync_all()?,
        }
        if index == 1 {
            self.gate.hit(Point::AfterWitnessSync);
        }
        Ok(())
    }
}

fn instrument(store: &mut Store, path: &Path, format: Format, gate: Arc<Gate>) {
    let log_path = path.join("records.log");
    let header = fs::read(&log_path).unwrap();
    assert_eq!(
        &header[..8],
        if format == Format::V2 {
            b"EMDALOG2"
        } else {
            b"EMDALOG3"
        }
    );
    let id = header[8..24].try_into().unwrap();
    let log_file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let witness_path = if format == Format::V2 {
        path.join("commit.state")
    } else {
        log_path
    };
    let witness_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(witness_path)
        .unwrap();
    let (witness, _) = Witness::from_io(
        Box::new(GroupWitness {
            file: witness_file,
            format,
            gate: Arc::clone(&gate),
        }),
        id,
    )
    .unwrap();
    store.log.replace_io_for_test(Box::new(GroupLog {
        file: log_file,
        gate,
    }));
    store.log.attach_test_witness(witness);
}

#[test]
fn interrupted_group_child() {
    let Some(path) = std::env::var_os("EMDA_GROUP_PROCESS_PATH") else {
        return;
    };
    let path = Path::new(&path);
    let format = match std::env::var("EMDA_GROUP_PROCESS_FORMAT").unwrap().as_str() {
        "v2" => Format::V2,
        "v3" => Format::V3,
        _ => panic!("unknown format"),
    };
    let point = Point::parse(&std::env::var("EMDA_GROUP_PROCESS_POINT").unwrap());
    let (first_started, first_started_rx) = mpsc::channel();
    let (first_resume, first_resume_rx) = mpsc::channel();
    let (group_started, group_started_rx) = mpsc::channel();
    let (group_resume, group_resume_rx) = mpsc::channel();
    let gate = Arc::new(Gate {
        point,
        first_started,
        first_resume: Mutex::new(first_resume_rx),
        group_started,
        group_resume: Mutex::new(group_resume_rx),
        witness_writes: AtomicUsize::new(0),
        witness_syncs: AtomicUsize::new(0),
        log_syncs: AtomicUsize::new(0),
    });
    let mut store = retry_lock(|| Store::open(path)).unwrap();
    instrument(&mut store, path, format, gate);
    let store = Arc::new(store);
    let first_store = Arc::clone(&store);
    let first = std::thread::spawn(move || first_store.put(b"first", b"acknowledged"));
    first_started_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let (done, done_rx) = mpsc::channel();
    for id in 0..3 {
        let store = Arc::clone(&store);
        let done = done.clone();
        std::thread::spawn(move || {
            let result = store.put(format!("group-{id}").as_bytes(), b"value");
            let _ = done.send(result);
        });
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while store.commits.queued_len() < 3 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let queued = store.commits.queued_len();
    first_resume.send(()).unwrap();
    assert_eq!(queued, 3);
    first.join().unwrap().unwrap();
    group_started_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    assert!(
        done_rx.try_recv().is_err(),
        "a grouped write returned before publication"
    );
    assert_eq!(
        store.get(b"first").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    for id in 0..3 {
        assert_eq!(store.get(format!("group-{id}").as_bytes()).unwrap(), None);
    }
    group_resume.send(()).unwrap();
    loop {
        std::thread::park();
    }
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

fn read_values(
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

fn run_case(format: Format, point: Point) {
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
    store.put(b"safe", b"guard").unwrap();
    let checkpoint = root.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let mut acknowledged_prefix = fs::read(path.join("records.log")).unwrap();
    acknowledged_prefix
        .extend_from_slice(&record::encode(Operation::Put, b"first", b"acknowledged").unwrap());
    let anchor = fs::read(path.join("commit.state")).unwrap();
    drop(store);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "store::group_commit_process_tests::interrupted_group_child",
            "--nocapture",
        ])
        .env("EMDA_GROUP_PROCESS_PATH", &path)
        .env("EMDA_GROUP_PROCESS_POINT", point.name())
        .env(
            "EMDA_GROUP_PROCESS_FORMAT",
            if format == Format::V2 { "v2" } else { "v3" },
        )
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
            if line.contains("EMDA_GROUP_PROCESS_BOUNDARY_REACHED") {
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
    reached.unwrap_or_else(|error| panic!("{format:?} {point:?}: {error}"));
    killed.unwrap();
    assert!(busy);
    assert!(interrupted_log.starts_with(&acknowledged_prefix));
    if point == Point::BeforeAppend {
        assert_eq!(interrupted_log.len(), acknowledged_prefix.len());
    } else {
        assert!(interrupted_log.len() > acknowledged_prefix.len());
    }
    for checkpoint_backend in [false, true] {
        let result = read_values(&path, &checkpoint, &digest, checkpoint_backend);
        if point == Point::PartialAppend {
            assert!(
                matches!(result, Err(ref error) if matches!(error.kind(), io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof))
            );
            assert_eq!(marker(&path, format), interrupted_marker);
        } else {
            let group = (point != Point::BeforeAppend).then(|| b"value".to_vec());
            assert_eq!(
                result.unwrap(),
                vec![
                    Some(b"guard".to_vec()),
                    Some(b"acknowledged".to_vec()),
                    group.clone(),
                    group.clone(),
                    group
                ]
            );
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
fn killed_groups_preserve_prior_acknowledgements_or_fail_closed() {
    for format in [Format::V2, Format::V3] {
        for &point in POINTS {
            if format == Format::V3 && matches!(point, Point::BeforeLogSync | Point::AfterLogSync) {
                continue;
            }
            run_case(format, point);
        }
    }
}
