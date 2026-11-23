//! Disk-backed suffix membership for an offline managed checkpoint rebuild.
//!
//! Each suffix mutation is applied in log order, including every child of a
//! DB02 record, before the next one is considered. Spill runs store the latest
//! state of keys that no longer fit in the bounded chunk; they are not a
//! substitute for that ordered peak calculation. EMDAIDX1 records a historical
//! live-key count, not a historical key-byte peak, and this format does not
//! invent one.
//!
//! Each entry carries a CRC-32 over its staging identity, index, absolute offset, header
//! locators, and body. Merge, emission, and random lookup verify that checksum
//! on the bytes of a single read before using the key or value. All opens
//! compare the trailer with the digest retained by the writer. Merge and
//! emission also hash the whole run before opening a cursor, then recheck
//! it before accepting the end of a buffered pass.
//! Random lookup skips runs outside the key bounds retained by that writer.
//! Runs cover consecutive chunks in replay order. A merge replaces a suffix
//! of that list, so its new run ID still orders it after every older run.

use crate::log::ValueLocation;
use crate::record;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::rc::Rc;

const MAGIC: &[u8; 8] = b"EMDASP01";
/// Version 3 binds the per-entry CRC-32 to the private run path.
const VERSION: u32 = 3;
const HEADER_LEN: u64 = 28;
const TAIL_LEN: usize = 17;
const ENTRY_CRC_LEN: usize = 4;
const MAX_SPILL_KEY_LEN: usize = 1024;
const MAX_ENTRY_ON_DISK: usize = 2 + MAX_SPILL_KEY_LEN + TAIL_LEN + ENTRY_CRC_LEN;
const ENTRY_OVERHEAD: u64 = 2 + TAIL_LEN as u64 + ENTRY_CRC_LEN as u64;
const CHECKSUM_LEN: u64 = 32;
const OFFSET_LEN: u64 = 8;

#[cfg(test)]
pub(super) mod corruption {
    use super::{Cell, FileExt, OFFSET_LEN, OpenOptions, Path, read_header};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(in crate::checkpoint_store) enum Point {
        SidecarFlushed,
        MergeVerified,
    }

    type Fault = (Point, fn(&Path));

    thread_local! {
        static FAULT: Cell<Option<Fault>> = const { Cell::new(None) };
    }

    pub(in crate::checkpoint_store) fn with_fault<T>(
        point: Point,
        damage: fn(&Path),
        run: impl FnOnce() -> T,
    ) -> T {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                FAULT.set(None);
            }
        }

        assert!(FAULT.replace(Some((point, damage))).is_none());
        let _reset = Reset;
        let result = run();
        assert!(FAULT.get().is_none(), "did not reach {point:?}");
        result
    }

    pub(super) fn apply(point: Point, path: &Path) {
        if let Some((_, damage)) = FAULT.get().filter(|(target, _)| *target == point) {
            FAULT.set(None);
            damage(path);
        }
    }

    pub(in crate::checkpoint_store) fn damage_last_merged_key(path: &Path) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let (count, table_at, _) = read_header(&file).unwrap();
        // The first binary-search probe of this fixture's two-entry run
        // selects the last entry. Leave the header and trailer intact.
        assert_eq!(count, 2);
        let mut offset = [0; OFFSET_LEN as usize];
        file.read_exact_at(&mut offset, table_at + OFFSET_LEN)
            .unwrap();
        let key_at = u64::from_le_bytes(offset) + 2;
        let mut byte = [0];
        file.read_exact_at(&mut byte, key_at).unwrap();
        file.write_all_at(&[byte[0] ^ 1], key_at).unwrap();
    }
}
const IO_BUFFER: usize = 8 * 1024;

/// Largest distinct-key chunk a caller may request.
pub(super) const MAX_REBUILD_CHUNK_KEYS: usize = 65_536;
/// Largest logical key-byte chunk a caller may request.
pub(super) const MAX_REBUILD_CHUNK_KEY_BYTES: usize = 64 * 1024 * 1024;
/// Largest merge fan-in a caller may request.
pub(super) const MAX_REBUILD_MERGE_FAN_IN: usize = 32;

#[derive(Clone, Copy, Debug)]
pub(super) struct SpillLimits {
    pub(super) max_chunk_keys: usize,
    pub(super) max_chunk_key_bytes: usize,
    pub(super) max_spill_bytes: u64,
    pub(super) max_merge_fan_in: usize,
    pub(super) max_operations: u64,
    pub(super) max_log_bytes: Option<u64>,
    pub(super) max_live_keys: Option<usize>,
    pub(super) max_live_key_bytes: Option<u64>,
    pub(super) base_checkpoint_limits: crate::CheckpointViewLimits,
    pub(super) checkpoint_limits: Option<crate::CheckpointViewLimits>,
}

impl SpillLimits {
    pub(super) fn new(
        max_chunk_keys: usize,
        max_chunk_key_bytes: usize,
        max_spill_bytes: u64,
        max_merge_fan_in: usize,
        max_operations: u64,
    ) -> io::Result<Self> {
        let limits = Self {
            max_chunk_keys,
            max_chunk_key_bytes,
            max_spill_bytes,
            max_merge_fan_in,
            max_operations,
            max_log_bytes: None,
            max_live_keys: None,
            max_live_key_bytes: None,
            base_checkpoint_limits: crate::CheckpointViewLimits::default(),
            checkpoint_limits: None,
        };
        limits.validate()?;
        Ok(limits)
    }

    fn validate(self) -> io::Result<()> {
        if self.max_chunk_keys == 0 || self.max_chunk_keys > MAX_REBUILD_CHUNK_KEYS {
            return Err(invalid_input(
                "rebuild chunk key limit must be between 1 and 65536",
            ));
        }
        if self.max_chunk_key_bytes == 0 || self.max_chunk_key_bytes > MAX_REBUILD_CHUNK_KEY_BYTES {
            return Err(invalid_input(
                "rebuild chunk key-byte limit must be between 1 and 64 MiB",
            ));
        }
        if self.max_merge_fan_in < 2 || self.max_merge_fan_in > MAX_REBUILD_MERGE_FAN_IN {
            return Err(invalid_input(
                "rebuild merge fan-in must be between 2 and 32",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RunMeta {
    id: u64,
    level: u32,
    identity: [u8; 32],
    digest: [u8; 32],
    path: PathBuf,
    bytes: u64,
    entries: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
}

struct FdInner {
    open: Cell<usize>,
    peak: Cell<usize>,
    limit: usize,
}

struct FdPermit {
    inner: Rc<FdInner>,
}

impl FdPermit {
    fn acquire(inner: &Rc<FdInner>) -> io::Result<Self> {
        let open = inner.open.get();
        if open >= inner.limit {
            return Err(io::Error::other(
                "rebuild spill file-descriptor limit exceeded",
            ));
        }
        let open = open + 1;
        inner.open.set(open);
        if open > inner.peak.get() {
            inner.peak.set(open);
        }
        Ok(Self {
            inner: Rc::clone(inner),
        })
    }
}

impl Drop for FdPermit {
    fn drop(&mut self) {
        self.inner.open.set(self.inner.open.get().saturating_sub(1));
    }
}

enum Find {
    Absent,
    Found(Option<ValueLocation>),
}

struct RunCursor {
    file: File,
    _permit: FdPermit,
    identity: [u8; 32],
    count: u64,
    index: u64,
    table_at: u64,
    expected_pos: u64,
    previous: Option<Vec<u8>>,
    sequential: Option<SequentialRun>,
}

struct SequentialRun {
    payload: ReadAhead,
    offsets: ReadAhead,
    digest: [u8; 32],
    end: u64,
}

#[derive(Default)]
struct ReadAhead {
    start: u64,
    bytes: Vec<u8>,
}

impl ReadAhead {
    fn read(&mut self, file: &File, start: u64, size: usize, end: u64) -> io::Result<&[u8]> {
        debug_assert!(size <= IO_BUFFER);
        let available = end
            .checked_sub(start)
            .filter(|&bytes| bytes >= size as u64)
            .ok_or_else(|| record::invalid_data("spill read exceeds its admitted region"))?;
        let relative = start
            .checked_sub(self.start)
            .and_then(|offset| usize::try_from(offset).ok())
            .filter(|&offset| offset <= self.bytes.len().saturating_sub(size));
        let offset = if let Some(offset) = relative.filter(|_| size <= self.bytes.len()) {
            offset
        } else {
            let capacity = available.min(IO_BUFFER as u64) as usize;
            self.bytes.resize(capacity, 0);
            if let Err(error) = file.read_exact_at(&mut self.bytes, start) {
                self.bytes.clear();
                return Err(error);
            }
            self.start = start;
            0
        };
        Ok(&self.bytes[offset..offset + size])
    }
}

fn run_table_end(count: u64, table_at: u64) -> Option<u64> {
    count
        .checked_mul(OFFSET_LEN)
        .and_then(|table_bytes| table_at.checked_add(table_bytes))
        .and_then(|end| end.checked_add(CHECKSUM_LEN))
}

impl RunCursor {
    fn open(fd: &Rc<FdInner>, run: &RunMeta, verify_hash: bool) -> io::Result<Self> {
        let permit = FdPermit::acquire(fd)?;
        let file = File::open(&run.path)?;
        let (count, table_at, len) = read_header(&file)?;
        let expected_len = run_table_end(count, table_at);
        if count != run.entries || len != run.bytes || expected_len != Some(len) {
            return Err(record::invalid_data(
                "spill run header does not match its record",
            ));
        }
        let mut trailer = [0_u8; 32];
        file.read_exact_at(&mut trailer, len - CHECKSUM_LEN)?;
        if trailer != run.digest
            || verify_hash && hash_prefix(&file, len - CHECKSUM_LEN)? != run.digest
        {
            return Err(record::invalid_data("spill run checksum mismatch"));
        }
        Ok(Self {
            file,
            _permit: permit,
            identity: run.identity,
            count,
            index: 0,
            table_at,
            expected_pos: HEADER_LEN,
            previous: None,
            sequential: verify_hash.then(|| SequentialRun {
                payload: ReadAhead::default(),
                offsets: ReadAhead::default(),
                digest: run.digest,
                end: len - CHECKSUM_LEN,
            }),
        })
    }

    fn find(&mut self, needle: &[u8]) -> io::Result<Find> {
        let mut lo = 0_u64;
        let mut hi = self.count;
        let mut buffer = [0_u8; MAX_ENTRY_ON_DISK];
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (key, state) = read_entry(
                &self.file,
                self.entry_range(mid)?,
                self.table_at,
                &self.identity,
                mid,
                self.count,
                &mut buffer,
            )?;
            match key.cmp(needle) {
                Ordering::Equal => return Ok(Find::Found(state)),
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
            }
        }
        Ok(Find::Absent)
    }

    fn next_entry(&mut self) -> io::Result<Option<(Vec<u8>, Option<ValueLocation>)>> {
        self.next_entry_with(|key, state| (key.to_vec(), state))
    }

    fn next_entry_with<T>(
        &mut self,
        take: impl FnOnce(&[u8], Option<ValueLocation>) -> T,
    ) -> io::Result<Option<T>> {
        if self.index == self.count {
            if self.expected_pos != self.table_at {
                return Err(record::invalid_data(
                    "spill run entries do not fill the payload",
                ));
            }
            if let Some(scan) = &self.sequential {
                let mut trailer = [0_u8; 32];
                self.file.read_exact_at(&mut trailer, scan.end)?;
                if self.file.metadata()?.len() != scan.end + CHECKSUM_LEN
                    || trailer != scan.digest
                    || hash_prefix(&self.file, scan.end)? != scan.digest
                {
                    return Err(record::invalid_data("spill run checksum mismatch"));
                }
                self.sequential = None;
            }
            return Ok(None);
        }
        let range = if let Some(scan) = &mut self.sequential {
            let last = self.index + 1 == self.count;
            let bytes = scan.offsets.read(
                &self.file,
                self.table_at + self.index * OFFSET_LEN,
                if last { 8 } else { 16 },
                scan.end,
            )?;
            decode_entry_range(bytes, self.table_at)
        } else {
            self.entry_range(self.index)?
        };
        if range.start != self.expected_pos {
            return Err(record::invalid_data("spill run entry offset is not packed"));
        }
        let end = range.end;
        let mut buffer;
        let (key, state) = if let Some(scan) = &mut self.sequential {
            let span = entry_span(&range, self.table_at)?;
            let bytes = scan
                .payload
                .read(&self.file, range.start, span, self.table_at)?;
            decode_entry(
                bytes,
                range.start,
                self.table_at,
                &self.identity,
                self.index,
                self.count,
            )?
        } else {
            buffer = [0_u8; MAX_ENTRY_ON_DISK];
            read_entry(
                &self.file,
                range,
                self.table_at,
                &self.identity,
                self.index,
                self.count,
                &mut buffer,
            )?
        };
        if self
            .previous
            .as_ref()
            .is_some_and(|previous| previous.as_slice() >= key)
        {
            return Err(record::invalid_data("spill run is not strictly ordered"));
        }
        let previous = self.previous.get_or_insert_with(Vec::new);
        previous.clear();
        previous.extend_from_slice(key);
        self.expected_pos = end;
        self.index += 1;
        Ok(Some(take(key, state)))
    }

    fn entry_range(&self, index: u64) -> io::Result<Range<u64>> {
        let mut bytes = [0_u8; 16];
        let last = index + 1 == self.count;
        let size = if last { 8 } else { 16 };
        self.file
            .read_exact_at(&mut bytes[..size], self.table_at + index * OFFSET_LEN)?;
        Ok(decode_entry_range(&bytes[..size], self.table_at))
    }
}

fn decode_entry_range(bytes: &[u8], table_at: u64) -> Range<u64> {
    let start = u64::from_le_bytes(bytes[..8].try_into().expect("offset slot"));
    let end = if bytes.len() == 8 {
        table_at
    } else {
        u64::from_le_bytes(bytes[8..].try_into().expect("next offset slot"))
    };
    start..end
}

struct RunWriter {
    file: io::BufWriter<File>,
    _permit: FdPermit,
    offsets: io::BufWriter<File>,
    _offset_permit: FdPermit,
    hasher: Sha256,
    cursor: u64,
    count: u64,
    key_bytes: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    expected_count: u64,
    expected_key_bytes: u64,
    table_at: u64,
    path: PathBuf,
    offset_path: PathBuf,
    id: u64,
    identity: [u8; 32],
    committed: bool,
    offset_removed: bool,
}

impl RunWriter {
    fn create(
        directory: &Path,
        id: u64,
        fd: &Rc<FdInner>,
        expected_count: u64,
        expected_key_bytes: u64,
    ) -> io::Result<Self> {
        if expected_count == 0 {
            return Err(record::invalid_data("refusing to write an empty spill run"));
        }
        let path = directory.join(format!("run-{id:06}"));
        // The stage name is independently generated for each rebuild. The
        // path includes that name and this run's sequence number.
        let identity = Sha256::digest(path.as_os_str().as_encoded_bytes()).into();
        let offset_path = directory.join(format!("run-{id:06}.off"));
        let permit = FdPermit::acquire(fd)?;
        let offset_permit = FdPermit::acquire(fd)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        let offsets = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&offset_path)?;
        let table_at = HEADER_LEN
            .checked_add(
                expected_count
                    .checked_mul(ENTRY_OVERHEAD)
                    .and_then(|bytes| bytes.checked_add(expected_key_bytes))
                    .ok_or_else(|| io::Error::other("spill size overflow"))?,
            )
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
        let mut writer = Self {
            file: io::BufWriter::with_capacity(IO_BUFFER, file),
            _permit: permit,
            offsets: io::BufWriter::with_capacity(IO_BUFFER, offsets),
            _offset_permit: offset_permit,
            hasher: Sha256::new(),
            cursor: 0,
            count: 0,
            key_bytes: 0,
            first_key: Vec::new(),
            last_key: Vec::new(),
            expected_count,
            expected_key_bytes,
            table_at,
            path,
            offset_path,
            id,
            identity,
            committed: false,
            offset_removed: false,
        };
        let mut header = [0_u8; HEADER_LEN as usize];
        header[..8].copy_from_slice(MAGIC);
        header[8..12].copy_from_slice(&VERSION.to_le_bytes());
        header[12..20].copy_from_slice(&expected_count.to_le_bytes());
        header[20..28].copy_from_slice(&table_at.to_le_bytes());
        writer.write_hashed(&header)?;
        Ok(writer)
    }

    fn write_entry(&mut self, key: &[u8], state: Option<ValueLocation>) -> io::Result<()> {
        record::validate_key(key)?;
        if self.last_key.as_slice() >= key {
            return Err(record::invalid_data("spill keys are not strictly ordered"));
        }
        let offset = self.cursor;
        let index = self.count;
        self.offsets.write_all(&offset.to_le_bytes())?;
        let key_len =
            u16::try_from(key.len()).map_err(|_| io::Error::other("index key too long"))?;
        let mut tail = [0_u8; TAIL_LEN];
        match state {
            None => {}
            Some(location) => {
                let length = u32::try_from(location.length)
                    .map_err(|_| io::Error::other("index value too large"))?;
                tail[0] = 1;
                tail[1..9].copy_from_slice(&location.offset.to_le_bytes());
                tail[9..13].copy_from_slice(&length.to_le_bytes());
                tail[13..17].copy_from_slice(&location.checksum.to_le_bytes());
            }
        }
        let mut buffer = [0_u8; MAX_ENTRY_ON_DISK];
        let tail_at = 2 + key.len();
        let body = &mut buffer[..tail_at + TAIL_LEN];
        body[..2].copy_from_slice(&key_len.to_le_bytes());
        body[2..tail_at].copy_from_slice(key);
        body[tail_at..].copy_from_slice(&tail);
        let crc = entry_checksum(
            &self.identity,
            index,
            offset,
            self.expected_count,
            self.table_at,
            body,
        );
        self.write_hashed(body)?;
        self.write_hashed(&crc.to_le_bytes())?;
        if self.count == 0 {
            self.first_key.extend_from_slice(key);
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.count += 1;
        self.key_bytes += key.len() as u64;
        Ok(())
    }

    fn finish(mut self) -> io::Result<RunMeta> {
        if self.count != self.expected_count || self.key_bytes != self.expected_key_bytes {
            return Err(record::invalid_data(
                "spill run size changed while it was written",
            ));
        }
        if self.cursor != self.table_at {
            return Err(record::invalid_data("spill run payload length mismatch"));
        }
        let table_bytes = self
            .count
            .checked_mul(OFFSET_LEN)
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
        let table_end = self
            .table_at
            .checked_add(table_bytes)
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
        self.offsets.flush()?;
        #[cfg(test)]
        {
            corruption::apply(corruption::Point::SidecarFlushed, &self.offset_path);
            crate::test_support::pause_bounded_rebuild("spill-sidecar-flushed")?;
        }
        self.offsets.get_mut().seek(SeekFrom::Start(0))?;
        let mut buffer = [0_u8; IO_BUFFER];
        let mut remaining = table_bytes;
        while remaining != 0 {
            let count = remaining.min(buffer.len() as u64) as usize;
            self.offsets.get_mut().read_exact(&mut buffer[..count])?;
            self.write_hashed(&buffer[..count])?;
            remaining -= count as u64;
        }
        if self.cursor != table_end || self.offsets.get_ref().metadata()?.len() != table_bytes {
            return Err(record::invalid_data("spill offset table length mismatch"));
        }
        let digest: [u8; 32] = self.hasher.clone().finalize().into();
        // The unlinked sidecar remains open until this writer is dropped, so
        // its bytes coexist with the finished run and must remain budgeted.
        self.file.flush()?;
        fs::remove_file(&self.offset_path)?;
        self.offset_removed = true;
        self.file.write_all(&digest)?;
        self.file.flush()?;
        let file = self.file.get_mut();
        let actual = hash_prefix(file, self.cursor)?;
        if actual != digest {
            return Err(record::invalid_data("spill run checksum mismatch"));
        }
        let bytes = file.metadata()?.len();
        if bytes != self.cursor + CHECKSUM_LEN {
            return Err(record::invalid_data("spill run length mismatch"));
        }
        self.committed = true;
        Ok(RunMeta {
            id: self.id,
            level: 0,
            identity: self.identity,
            digest,
            path: self.path.clone(),
            bytes,
            entries: self.count,
            first_key: std::mem::take(&mut self.first_key),
            last_key: std::mem::take(&mut self.last_key),
        })
    }

    fn write_hashed(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)?;
        self.hasher.update(bytes);
        self.cursor = self
            .cursor
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
        Ok(())
    }
}

impl Drop for RunWriter {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
        if !self.offset_removed {
            let _ = fs::remove_file(&self.offset_path);
        }
    }
}

enum Emit {
    Pending,
    Memory(std::collections::btree_map::IntoIter<Vec<u8>, Option<ValueLocation>>),
    File(Box<RunCursor>),
    Exhausted,
}

pub(super) struct SpillSession {
    limits: SpillLimits,
    fd: Rc<FdInner>,
    runs: Vec<RunMeta>,
    dirty: BTreeMap<Vec<u8>, Option<ValueLocation>>,
    chunk_key_bytes: usize,
    spill_bytes: u64,
    next_id: u64,
    current: usize,
    live_key_bytes: u64,
    peak: usize,
    applied: u64,
    emit: Emit,
    last_key: Option<Vec<u8>>,
    parent: PathBuf,
    stage: Option<crate::staging::Stage>,
}

impl SpillSession {
    pub(super) fn create(
        parent: &Path,
        limits: SpillLimits,
        base_live: usize,
        base_peak: usize,
    ) -> io::Result<Self> {
        limits.validate()?;
        if base_peak < base_live {
            return Err(record::invalid_data(
                "checkpoint peak is below its live key count",
            ));
        }
        if limits.max_live_keys.is_some_and(|max| base_peak > max) {
            return Err(invalid_input("rebuild historical live key limit exceeded"));
        }
        Ok(Self {
            fd: Rc::new(FdInner {
                open: Cell::new(0),
                peak: Cell::new(0),
                limit: limits
                    .max_merge_fan_in
                    .checked_add(2)
                    .ok_or_else(|| io::Error::other("rebuild fan-in overflow"))?,
            }),
            limits,
            runs: Vec::new(),
            dirty: BTreeMap::new(),
            chunk_key_bytes: 0,
            spill_bytes: 0,
            next_id: 1,
            current: base_live,
            live_key_bytes: 0,
            peak: base_peak,
            applied: 0,
            emit: Emit::Pending,
            last_key: None,
            parent: parent.to_path_buf(),
            stage: None,
        })
    }

    pub(super) fn live_count(&self) -> usize {
        self.current
    }

    pub(super) fn peak(&self) -> usize {
        self.peak
    }

    #[cfg(test)]
    pub(super) fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }

    #[cfg(test)]
    pub(super) fn peak_open_fds(&self) -> usize {
        self.fd.peak.get()
    }

    /// Applies one suffix mutation. `base` is consulted only when this key has
    /// not already been seen in the suffix. Callers must pass DB02 children in
    /// their recorded order; several children can belong to one outer record.
    pub(super) fn push(
        &mut self,
        key: &[u8],
        location: Option<ValueLocation>,
        mut base: impl FnMut(&[u8]) -> io::Result<Option<ValueLocation>>,
    ) -> io::Result<()> {
        if !matches!(self.emit, Emit::Pending) {
            return Err(io::Error::other("suffix spill is closed"));
        }
        record::validate_key(key)?;
        if self.applied == self.limits.max_operations {
            return Err(invalid_input("rebuild operation limit exceeded"));
        }
        if !self.dirty.contains_key(key) {
            if key.len() > self.limits.max_chunk_key_bytes {
                return Err(invalid_input("rebuild chunk key-byte limit exceeded"));
            }
            let next_bytes = self
                .chunk_key_bytes
                .checked_add(key.len())
                .ok_or_else(|| io::Error::other("chunk key bytes overflow"))?;
            if self.dirty.len() == self.limits.max_chunk_keys
                || next_bytes > self.limits.max_chunk_key_bytes
            {
                self.write_dirty()?;
            }
            let prior = match self.lookup_persisted(key)? {
                Find::Found(state) => state,
                Find::Absent => base(key)?,
            };
            self.chunk_key_bytes = self
                .chunk_key_bytes
                .checked_add(key.len())
                .ok_or_else(|| io::Error::other("chunk key bytes overflow"))?;
            if self.chunk_key_bytes > self.limits.max_chunk_key_bytes
                || self.dirty.len() >= self.limits.max_chunk_keys
            {
                self.chunk_key_bytes -= key.len();
                return Err(invalid_input("rebuild chunk limit exceeded"));
            }
            self.dirty.insert(key.to_vec(), prior);
        }
        let state = self
            .dirty
            .get_mut(key)
            .ok_or_else(|| io::Error::other("suffix chunk lost a key"))?;
        if state.is_none()
            && location.is_some()
            && self
                .limits
                .max_live_keys
                .is_some_and(|max| self.current >= max)
        {
            return Err(invalid_input("rebuild historical live key limit exceeded"));
        }
        if let Some(max_key_bytes) = self.limits.max_live_key_bytes {
            let key_bytes = key.len() as u64;
            let next_bytes = match (state.is_some(), location.is_some()) {
                (false, true) => self.live_key_bytes.checked_add(key_bytes),
                (true, false) => self.live_key_bytes.checked_sub(key_bytes),
                _ => Some(self.live_key_bytes),
            };
            self.live_key_bytes = next_bytes
                .filter(|&bytes| bytes <= max_key_bytes)
                .ok_or_else(|| invalid_input("rebuild historical live key-byte limit exceeded"))?;
        }
        apply_mutation(state, location, &mut self.current, &mut self.peak)?;
        self.applied += 1;
        Ok(())
    }

    pub(super) fn prepare_for_emit(&mut self) -> io::Result<()> {
        if !matches!(self.emit, Emit::Pending) {
            return Err(io::Error::other("suffix spill was already prepared"));
        }
        if !self.dirty.is_empty() {
            if self.runs.is_empty() {
                let items = std::mem::take(&mut self.dirty).into_iter();
                self.chunk_key_bytes = 0;
                self.emit = Emit::Memory(items);
                return Ok(());
            }
            self.write_dirty()?;
        }
        while self.runs.len() > 1 {
            self.merge_newest(self.runs.len().min(self.limits.max_merge_fan_in))?;
        }
        if self.runs.is_empty() {
            self.emit = Emit::Memory(BTreeMap::new().into_iter());
            return Ok(());
        }
        let cursor = RunCursor::open(&self.fd, &self.runs[0], true)?;
        self.emit = Emit::File(Box::new(cursor));
        Ok(())
    }

    pub(super) fn next_latest(&mut self) -> io::Result<Option<(Vec<u8>, Option<ValueLocation>)>> {
        // File cursors already check ordering against their previous key.
        // Avoid retaining a second copy of that key in the session.
        let file_backed = matches!(self.emit, Emit::File(_));
        let next = match &mut self.emit {
            Emit::Pending => return Err(io::Error::other("suffix spill was not prepared")),
            Emit::Exhausted => return Ok(None),
            Emit::Memory(items) => items.next(),
            Emit::File(cursor) => cursor.next_entry()?,
        };
        let Some((key, state)) = next else {
            self.emit = Emit::Exhausted;
            self.last_key = None;
            return Ok(None);
        };
        if file_backed {
            return Ok(Some((key, state)));
        }
        if self
            .last_key
            .as_ref()
            .is_some_and(|previous| previous.as_slice() >= key.as_slice())
        {
            return Err(record::invalid_data(
                "suffix spill keys are not strictly ordered",
            ));
        }
        self.last_key = Some(key.clone());
        Ok(Some((key, state)))
    }

    fn lookup_persisted(&self, key: &[u8]) -> io::Result<Find> {
        for run in self.runs.iter().rev() {
            // These bounds come from the verified writer, so even damage to
            // the file cannot make an out-of-range probe return a value.
            // Merge and emission still check every surviving run.
            if key < run.first_key.as_slice() || key > run.last_key.as_slice() {
                continue;
            }
            // The whole run is not rehashed on this path. The header and
            // trailer match the run metadata, and `find` checks every
            // probed entry before using its key or value.
            let mut cursor = RunCursor::open(&self.fd, run, false)?;
            if let Find::Found(state) = cursor.find(key)? {
                return Ok(Find::Found(state));
            }
        }
        Ok(Find::Absent)
    }

    fn write_dirty(&mut self) -> io::Result<()> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        self.merge_full_levels()?;
        let (count, key_bytes) = map_stats(&self.dirty)?;
        self.ensure_additional(temporary_bytes(count, key_bytes)?)?;
        let directory = self.stage_dir()?;
        let id = self.allocate_id();
        let mut writer = RunWriter::create(&directory, id, &self.fd, count, key_bytes)?;
        for (key, state) in &self.dirty {
            writer.write_entry(key, *state)?;
        }
        let run = writer.finish()?;
        self.spill_bytes = self
            .spill_bytes
            .checked_add(run.bytes)
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
        self.runs.push(run);
        self.dirty.clear();
        self.chunk_key_bytes = 0;
        Ok(())
    }

    // A new chunk has level zero. Carry a full group at the end of the
    // list before appending, just as carrying a digit in base fan-in.
    // Levels count source chunks, regardless of deduplication in a run.
    fn merge_full_levels(&mut self) -> io::Result<()> {
        let fan_in = self.limits.max_merge_fan_in;
        while self.runs.len() >= fan_in {
            let first = self.runs.len() - fan_in;
            if self.runs[first].level != self.runs.last().unwrap().level {
                break;
            }
            self.merge_newest(fan_in)?;
        }
        Ok(())
    }

    fn merge_newest(&mut self, inputs: usize) -> io::Result<()> {
        let first = self.runs.len() - inputs;
        let level = self.runs[first].level + 1;
        let (count, key_bytes) = self.plan_merge(&self.runs[first..])?;
        self.ensure_additional(temporary_bytes(count, key_bytes)?)?;
        let mut merged = self.write_merge(first, count, key_bytes)?;
        merged.level = level;
        #[cfg(test)]
        {
            corruption::apply(corruption::Point::MergeVerified, &merged.path);
            crate::test_support::pause_bounded_rebuild("spill-merge-written")?;
        }
        if merged.bytes != run_len(count, key_bytes)? {
            let _ = fs::remove_file(&merged.path);
            return Err(record::invalid_data("merged spill run length mismatch"));
        }
        let old_bytes = self.runs[first..]
            .iter()
            .try_fold(0_u64, |sum, run| sum.checked_add(run.bytes))
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
        for run in &self.runs[first..] {
            fs::remove_file(&run.path)?;
        }
        self.spill_bytes = self
            .spill_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(merged.bytes))
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
        self.runs.truncate(first);
        self.runs.push(merged);
        Ok(())
    }

    fn plan_merge(&self, runs: &[RunMeta]) -> io::Result<(u64, u64)> {
        let mut count = 0_u64;
        let mut key_bytes = 0_u64;
        let generations = runs.iter().map(|run| run.id).collect::<Vec<_>>();
        self.with_inputs(runs, |cursors| {
            merge_walk(cursors, &generations, |key, _| {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("spill count overflow"))?;
                key_bytes = key_bytes
                    .checked_add(key.len() as u64)
                    .ok_or_else(|| io::Error::other("spill size overflow"))?;
                Ok(())
            })
        })?;
        if count == 0 {
            return Err(record::invalid_data("spill merge produced no entries"));
        }
        Ok((count, key_bytes))
    }

    fn write_merge(&mut self, first: usize, count: u64, key_bytes: u64) -> io::Result<RunMeta> {
        let directory = self.stage_dir()?;
        let id = self.allocate_id();
        let runs = &self.runs[first..];
        let generations = runs.iter().map(|run| run.id).collect::<Vec<_>>();
        let fd = Rc::clone(&self.fd);
        self.with_inputs(runs, |cursors| {
            let mut writer = RunWriter::create(&directory, id, &fd, count, key_bytes)?;
            merge_walk(cursors, &generations, |key, state| {
                writer.write_entry(key, state)
            })?;
            writer.finish()
        })
    }

    fn with_inputs<T>(
        &self,
        runs: &[RunMeta],
        body: impl FnOnce(&mut [RunCursor]) -> io::Result<T>,
    ) -> io::Result<T> {
        if runs.len() > self.limits.max_merge_fan_in {
            return Err(io::Error::other("rebuild spill fan-in exceeded"));
        }
        let mut cursors = Vec::with_capacity(runs.len());
        for run in runs {
            cursors.push(RunCursor::open(&self.fd, run, true)?);
        }
        body(&mut cursors)
    }

    fn ensure_additional(&self, extra: u64) -> io::Result<()> {
        let total = self
            .spill_bytes
            .checked_add(extra)
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
        if total > self.limits.max_spill_bytes {
            Err(invalid_input("rebuild spill byte limit exceeded"))
        } else {
            Ok(())
        }
    }

    fn stage_dir(&mut self) -> io::Result<PathBuf> {
        if self.stage.is_none() {
            self.stage = Some(crate::staging::Stage::directory(&self.parent)?);
        }
        Ok(self
            .stage
            .as_ref()
            .expect("stage was just created")
            .path()
            .to_path_buf())
    }

    fn allocate_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

struct Head {
    key: Vec<u8>,
    state: Option<ValueLocation>,
    input: usize,
    generation: u64,
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Head {}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.generation.cmp(&other.generation))
            .then_with(|| self.input.cmp(&other.input))
    }
}

fn merge_walk(
    cursors: &mut [RunCursor],
    generations: &[u64],
    mut emit: impl FnMut(&[u8], Option<ValueLocation>) -> io::Result<()>,
) -> io::Result<()> {
    let mut heap = BinaryHeap::new();
    for (input, cursor) in cursors.iter_mut().enumerate() {
        let Some((key, state)) = cursor.next_entry()? else {
            return Err(record::invalid_data("empty spill run"));
        };
        heap.push(Head {
            key,
            state,
            input,
            generation: generations[input],
        });
    }
    // A head owns one key buffer. After emission, give its buffer back to
    // that input, exchanging it with the previous emitted key when needed.
    let mut last = Vec::new();
    while let Some(head) = heap.pop() {
        let Head {
            mut key,
            state,
            input,
            generation,
        } = head;
        if last != key {
            emit(&key, state)?;
            std::mem::swap(&mut last, &mut key);
        }
        if let Some(state) = cursors[input].next_entry_with(|next, state| {
            key.clear();
            key.extend_from_slice(next);
            state
        })? {
            heap.push(Head {
                key,
                state,
                input,
                generation,
            });
        }
    }
    for cursor in cursors {
        if cursor.next_entry()?.is_some() {
            return Err(record::invalid_data("spill merge left unread entries"));
        }
    }
    Ok(())
}

fn apply_mutation(
    state: &mut Option<ValueLocation>,
    location: Option<ValueLocation>,
    current: &mut usize,
    peak: &mut usize,
) -> io::Result<()> {
    match (*state, location) {
        (None, Some(location)) => {
            *current = current
                .checked_add(1)
                .ok_or_else(|| io::Error::other("live key count overflow"))?;
            *state = Some(location);
        }
        (Some(_), None) => {
            *current = current.checked_sub(1).ok_or_else(|| {
                record::invalid_data("live key count underflow during suffix replay")
            })?;
            *state = None;
        }
        (_, Some(location)) => *state = Some(location),
        (None, None) => {}
    }
    *peak = (*peak).max(*current);
    Ok(())
}

fn map_stats(map: &BTreeMap<Vec<u8>, Option<ValueLocation>>) -> io::Result<(u64, u64)> {
    let mut key_bytes = 0_u64;
    for key in map.keys() {
        key_bytes = key_bytes
            .checked_add(key.len() as u64)
            .ok_or_else(|| io::Error::other("spill size overflow"))?;
    }
    Ok((map.len() as u64, key_bytes))
}

fn run_len(count: u64, key_bytes: u64) -> io::Result<u64> {
    let entries = count
        .checked_mul(ENTRY_OVERHEAD)
        .and_then(|bytes| bytes.checked_add(key_bytes))
        .ok_or_else(|| io::Error::other("spill size overflow"))?;
    let table = count
        .checked_mul(OFFSET_LEN)
        .ok_or_else(|| io::Error::other("spill size overflow"))?;
    HEADER_LEN
        .checked_add(entries)
        .and_then(|bytes| bytes.checked_add(table))
        .and_then(|bytes| bytes.checked_add(CHECKSUM_LEN))
        .ok_or_else(|| io::Error::other("spill size overflow"))
}

fn temporary_bytes(count: u64, key_bytes: u64) -> io::Result<u64> {
    let final_len = run_len(count, key_bytes)?;
    let table = count
        .checked_mul(OFFSET_LEN)
        .ok_or_else(|| io::Error::other("spill size overflow"))?;
    final_len
        .checked_add(table)
        .ok_or_else(|| io::Error::other("spill size overflow"))
}

fn read_header(file: &File) -> io::Result<(u64, u64, u64)> {
    let len = file.metadata()?.len();
    if len < HEADER_LEN + CHECKSUM_LEN {
        return Err(record::invalid_data("incomplete spill run"));
    }
    let mut header = [0_u8; HEADER_LEN as usize];
    file.read_exact_at(&mut header, 0)?;
    if &header[..8] != MAGIC {
        return Err(record::invalid_data("unknown spill run version"));
    }
    let version = u32::from_le_bytes(header[8..12].try_into().expect("four bytes"));
    if version != VERSION {
        return Err(record::invalid_data("unsupported spill run version"));
    }
    let count = u64::from_le_bytes(header[12..20].try_into().expect("eight bytes"));
    let table_at = u64::from_le_bytes(header[20..28].try_into().expect("eight bytes"));
    if count == 0 || table_at < HEADER_LEN || table_at >= len {
        return Err(record::invalid_data("invalid spill run header"));
    }
    Ok((count, table_at, len))
}

/// Checksum of the entry identity and body.
///
/// `body` is `key_len || key || tag || location` and excludes the stored CRC.
/// The run identity, index, and absolute offset bind the offset-table slot that
/// located this read. `count` and `table_at` are the header values the cursor
/// is using. A caller must hash the same buffer it parses.
fn entry_checksum(
    identity: &[u8; 32],
    index: u64,
    offset: u64,
    count: u64,
    table_at: u64,
    body: &[u8],
) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(identity);
    hasher.update(&index.to_le_bytes());
    hasher.update(&offset.to_le_bytes());
    hasher.update(&count.to_le_bytes());
    hasher.update(&table_at.to_le_bytes());
    hasher.update(body);
    hasher.finalize()
}

fn read_entry<'a>(
    file: &File,
    range: Range<u64>,
    table_at: u64,
    identity: &[u8; 32],
    index: u64,
    count: u64,
    buffer: &'a mut [u8; MAX_ENTRY_ON_DISK],
) -> io::Result<(&'a [u8], Option<ValueLocation>)> {
    let span = entry_span(&range, table_at)?;
    // Read the complete entry before interpreting its fields.
    file.read_exact_at(&mut buffer[..span], range.start)?;
    decode_entry(
        &buffer[..span],
        range.start,
        table_at,
        identity,
        index,
        count,
    )
}

fn entry_span(range: &Range<u64>, table_at: u64) -> io::Result<usize> {
    if range.start < HEADER_LEN {
        return Err(record::invalid_data("spill entry overlaps its header"));
    }
    range
        .end
        .checked_sub(range.start)
        .filter(|&span| {
            range.end <= table_at && (ENTRY_OVERHEAD + 1..=MAX_ENTRY_ON_DISK as u64).contains(&span)
        })
        .map(|span| span as usize)
        .ok_or_else(|| record::invalid_data("invalid spill entry span"))
}

fn decode_entry<'a>(
    buffer: &'a [u8],
    pos: u64,
    table_at: u64,
    identity: &[u8; 32],
    index: u64,
    count: u64,
) -> io::Result<(&'a [u8], Option<ValueLocation>)> {
    let key_len = u16::from_le_bytes([buffer[0], buffer[1]]) as usize;
    if key_len == 0 || key_len > MAX_SPILL_KEY_LEN {
        return Err(record::invalid_data("invalid spill key length"));
    }
    let total = 2 + key_len + TAIL_LEN + ENTRY_CRC_LEN;
    if total != buffer.len() {
        return Err(record::invalid_data(
            "spill entry length does not match its span",
        ));
    }
    let tag_at = 2 + key_len;
    let body_end = tag_at + TAIL_LEN;
    // Structural tag failures stay distinct from checksum failures. A tag of
    // 0 or 1 still has to match the checksum before its payload is used.
    if buffer[tag_at] > 1 {
        return Err(record::invalid_data("unknown spill entry tag"));
    }
    let stored = u32::from_le_bytes(
        buffer[body_end..body_end + ENTRY_CRC_LEN]
            .try_into()
            .expect("four checksum bytes"),
    );
    let actual = entry_checksum(identity, index, pos, count, table_at, &buffer[..body_end]);
    if actual != stored {
        return Err(record::invalid_data("spill entry checksum mismatch"));
    }
    let key = &buffer[2..tag_at];
    record::validate_key(key)?;
    let tail = &buffer[tag_at..body_end];
    let offset = u64::from_le_bytes(tail[1..9].try_into().expect("eight bytes"));
    let length = u32::from_le_bytes(tail[9..13].try_into().expect("four bytes"));
    let checksum = u32::from_le_bytes(tail[13..17].try_into().expect("four bytes"));
    let state = match tail[0] {
        0 => {
            if offset != 0 || length != 0 || checksum != 0 {
                return Err(record::invalid_data("tombstone spill entry is not empty"));
            }
            None
        }
        1 => {
            if length as usize > record::MAX_VALUE
                || offset < crate::witness::LOG_HEADER_SIZE as u64
            {
                return Err(record::invalid_data("spill value location is invalid"));
            }
            Some(ValueLocation {
                offset,
                length: length as usize,
                checksum,
            })
        }
        _ => return Err(record::invalid_data("unknown spill entry tag")),
    };
    Ok((key, state))
}

fn hash_prefix(file: &File, len: u64) -> io::Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; IO_BUFFER];
    let mut offset = 0_u64;
    while offset < len {
        let count = (len - offset).min(buffer.len() as u64) as usize;
        file.read_exact_at(&mut buffer[..count], offset)?;
        hasher.update(&buffer[..count]);
        offset += count as u64;
    }
    Ok(hasher.finalize().into())
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(offset: u64) -> ValueLocation {
        ValueLocation {
            offset,
            length: 1,
            checksum: offset as u32,
        }
    }

    fn limits(chunk_keys: usize, spill_bytes: u64, operations: u64) -> SpillLimits {
        SpillLimits::new(chunk_keys, 1024, spill_bytes, 2, operations).unwrap()
    }

    fn replay(
        chunk_keys: usize,
        base: &BTreeMap<Vec<u8>, Option<ValueLocation>>,
        base_live: usize,
        base_peak: usize,
        ops: &[(Vec<u8>, Option<ValueLocation>)],
    ) -> (SpillSession, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let mut session = SpillSession::create(
            directory.path(),
            limits(chunk_keys, 64 << 20, 10_000),
            base_live,
            base_peak,
        )
        .unwrap();
        for (key, location) in ops {
            session
                .push(key, *location, |key| {
                    Ok(base.get(key).and_then(|state| *state))
                })
                .unwrap();
        }
        (session, directory)
    }

    fn reference(
        base: &BTreeMap<Vec<u8>, Option<ValueLocation>>,
        base_live: usize,
        base_peak: usize,
        ops: &[(Vec<u8>, Option<ValueLocation>)],
    ) -> (BTreeMap<Vec<u8>, ValueLocation>, usize, usize) {
        let mut live = BTreeMap::new();
        for (key, state) in base {
            if let Some(location) = state {
                live.insert(key.clone(), *location);
            }
        }
        let mut current = base_live;
        let mut peak = base_peak;
        for (key, location) in ops {
            match location {
                Some(location) => {
                    if !live.contains_key(key) {
                        current += 1;
                    }
                    live.insert(key.clone(), *location);
                }
                None => {
                    if live.remove(key).is_some() {
                        current -= 1;
                    }
                }
            }
            peak = peak.max(current);
        }
        (live, current, peak)
    }

    fn assert_same_locations(
        actual: &BTreeMap<Vec<u8>, ValueLocation>,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
    ) {
        assert_eq!(actual.len(), expected.len());
        for (key, location) in expected {
            let Some(found) = actual.get(key) else {
                panic!("missing key {}", String::from_utf8_lossy(key));
            };
            assert_eq!(found.offset, location.offset);
            assert_eq!(found.length, location.length);
            assert_eq!(found.checksum, location.checksum);
        }
    }

    fn overlay_states(session: &mut SpillSession) -> BTreeMap<Vec<u8>, Option<ValueLocation>> {
        session.prepare_for_emit().unwrap();
        let mut overlay = BTreeMap::new();
        while let Some((key, state)) = session.next_latest().unwrap() {
            assert!(overlay.insert(key, state).is_none());
        }
        assert!(session.next_latest().unwrap().is_none());
        overlay
    }

    fn reconstruct(
        base: &BTreeMap<Vec<u8>, Option<ValueLocation>>,
        overlay: &BTreeMap<Vec<u8>, Option<ValueLocation>>,
    ) -> BTreeMap<Vec<u8>, ValueLocation> {
        let mut live = BTreeMap::new();
        for (key, state) in base {
            if overlay.contains_key(key) {
                continue;
            }
            if let Some(location) = state {
                live.insert(key.clone(), *location);
            }
        }
        for (key, state) in overlay {
            if let Some(location) = state {
                live.insert(key.clone(), *location);
            }
        }
        live
    }

    fn emitted(session: &mut SpillSession) -> BTreeMap<Vec<u8>, ValueLocation> {
        session.prepare_for_emit().unwrap();
        let mut live = BTreeMap::new();
        while let Some((key, state)) = session.next_latest().unwrap() {
            if let Some(location) = state {
                assert!(live.insert(key, location).is_none());
            }
        }
        assert!(session.next_latest().unwrap().is_none());
        live
    }

    #[test]
    fn peak_follows_every_intermediate_state_for_tiny_and_wide_chunks() {
        let mut base = BTreeMap::new();
        base.insert(b"a".to_vec(), Some(loc(100)));
        base.insert(b"b".to_vec(), Some(loc(110)));
        let ops = [
            (b"a".to_vec(), None),
            (b"c".to_vec(), Some(loc(120))),
            (b"d".to_vec(), Some(loc(130))),
            (b"e".to_vec(), Some(loc(140))),
            (b"c".to_vec(), Some(loc(150))),
            (b"e".to_vec(), None),
            (b"d".to_vec(), None),
            (b"a".to_vec(), Some(loc(160))),
            (b"z".to_vec(), None),
        ];
        let (expected, count, peak) = reference(&base, 2, 2, &ops);
        assert!(peak > count);
        assert!(peak > 2);
        for chunk_keys in [1_usize, 8] {
            let (mut session, _directory) = replay(chunk_keys, &base, 2, 2, &ops);
            assert_eq!(session.live_count(), count);
            assert_eq!(session.peak(), peak);
            let overlay = overlay_states(&mut session);
            assert!(
                overlay.contains_key(b"d".as_slice())
                    && overlay.get(b"d".as_slice()).copied().flatten().is_none(),
                "deleted suffix key remains a tombstone"
            );
            assert!(
                overlay.contains_key(b"e".as_slice())
                    && overlay.get(b"e".as_slice()).copied().flatten().is_none()
            );
            assert!(
                overlay.contains_key(b"z".as_slice())
                    && overlay.get(b"z".as_slice()).copied().flatten().is_none()
            );
            assert!(!overlay.contains_key(b"b".as_slice()));
            assert_same_locations(&reconstruct(&base, &overlay), &expected);
            assert!(session.peak_open_fds() <= 4, "chunk {chunk_keys}");
            if chunk_keys == 1 {
                assert!(
                    session.peak_open_fds() >= 4,
                    "merge should open both inputs, the output, and the offset file"
                );
                assert!(session.spill_bytes() > 0);
            }
            drop(session);
        }
    }

    #[test]
    fn small_merges_leave_older_large_runs_and_preserve_latest_locations() {
        let directory = tempfile::tempdir().unwrap();
        let mut session = SpillSession::create(
            directory.path(),
            SpillLimits::new(1, 1024, 1 << 20, 2, 127).unwrap(),
            0,
            0,
        )
        .unwrap();
        let ops = (0..127_u64)
            .map(|step| {
                let key = (step % 64).to_be_bytes().to_vec();
                let location = (step < 64 || step % 2 == 0).then(|| loc(100 + step));
                (key, location)
            })
            .collect::<Vec<_>>();
        let mut oldest = None;
        for (step, (key, location)) in ops.iter().enumerate() {
            session.push(key, *location, |_| Ok(None)).unwrap();
            session.write_dirty().unwrap();
            // The 65th flush carries the first 64 chunks into one run.
            if step == 64 {
                assert_eq!(session.runs[0].entries, 64);
                oldest = Some(session.runs[0].id);
            }
            if let Some(id) = oldest {
                assert_eq!(session.runs[0].id, id, "flush {step}");
                assert!(session.runs.len() <= 8, "flush {step}");
            }
        }
        let (expected, count, peak) = reference(&BTreeMap::new(), 0, 0, &ops);
        assert_eq!(session.live_count(), count);
        assert_eq!(session.peak(), peak);
        assert_same_locations(&emitted(&mut session), &expected);
        assert_eq!(session.peak_open_fds(), 4);
    }

    #[test]
    fn an_in_memory_chunk_records_a_peak_above_its_final_state_without_spill() {
        let base = BTreeMap::new();
        let ops = [
            (b"a".to_vec(), Some(loc(10))),
            (b"b".to_vec(), Some(loc(20))),
            (b"a".to_vec(), None),
            (b"b".to_vec(), None),
            (b"c".to_vec(), Some(loc(30))),
        ];
        let (expected, count, peak) = reference(&base, 0, 0, &ops);
        assert_eq!(count, 1);
        assert_eq!(peak, 2);
        let directory = tempfile::tempdir().unwrap();
        let mut session = SpillSession::create(directory.path(), limits(8, 0, 10), 0, 0).unwrap();
        for (key, location) in &ops {
            session.push(key, *location, |_| Ok(None)).unwrap();
        }
        assert_eq!(session.peak(), peak);
        assert_same_locations(&emitted(&mut session), &expected);
        assert_eq!(session.spill_bytes(), 0);
        assert_eq!(session.peak_open_fds(), 0);
        drop(session);
        assert!(fs::read_dir(directory.path()).unwrap().next().is_none());
    }

    #[test]
    fn memory_emission_moves_entries_and_consumes_them_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let mut session = SpillSession::create(directory.path(), limits(8, 0, 10), 0, 0).unwrap();
        session.push(b"b", Some(loc(20)), |_| Ok(None)).unwrap();
        session.push(b"a", None, |_| Ok(None)).unwrap();
        let original_key = session.dirty.first_key_value().unwrap().0.as_ptr();
        session.prepare_for_emit().unwrap();
        let (key, state) = session.next_latest().unwrap().unwrap();
        assert_eq!(key, b"a");
        assert_eq!(key.as_ptr(), original_key);
        assert!(state.is_none());
        assert!(matches!(&session.emit, Emit::Memory(items) if items.len() == 1));
        let (key, state) = session.next_latest().unwrap().unwrap();
        assert_eq!(key, b"b");
        assert_eq!(state.unwrap().offset, 20);
        assert!(session.next_latest().unwrap().is_none());
        assert!(session.next_latest().unwrap().is_none());
    }

    #[test]
    fn operation_limit_does_not_apply_the_rejected_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let mut session = SpillSession::create(directory.path(), limits(8, 0, 1), 0, 0).unwrap();
        session.push(b"a", Some(loc(10)), |_| Ok(None)).unwrap();
        let error = session.push(b"a", Some(loc(99)), |_| Ok(None)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("operation limit"));
        assert_eq!(session.live_count(), 1);
        assert_eq!(session.peak(), 1);
        let found = emitted(&mut session).get(b"a".as_slice()).copied();
        assert_eq!(
            found.map(|location| (location.offset, location.length, location.checksum)),
            Some((10, 1, 10))
        );
    }

    #[test]
    fn merge_refuses_when_temporary_bytes_exceed_the_real_spill_limit() {
        for count in [1, 4, 5, 100] {
            assert_eq!(
                temporary_bytes(count, count).unwrap(),
                run_len(count, count).unwrap() + count * OFFSET_LEN
            );
        }
        let one = run_len(3, 3).unwrap();
        let merged_final = run_len(6, 6).unwrap();
        let merged_peak = temporary_bytes(6, 6).unwrap();
        assert!(merged_peak > merged_final);
        let limit = one
            .checked_mul(2)
            .unwrap()
            .checked_add(merged_final)
            .unwrap();
        assert!(
            limit
                < one
                    .checked_mul(2)
                    .unwrap()
                    .checked_add(merged_peak)
                    .unwrap()
        );
        let directory = tempfile::tempdir().unwrap();
        let mut session = SpillSession::create(
            directory.path(),
            SpillLimits::new(3, 1024, limit, 2, 100).unwrap(),
            0,
            0,
        )
        .unwrap();
        let mut failure = None;
        for key in 1_u8..=12 {
            if let Err(error) = session.push(&[key], Some(loc(u64::from(key) + 100)), |_| Ok(None))
            {
                failure = Some(error);
                break;
            }
        }
        let error = failure.expect("merge should hit the spill ceiling");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("spill byte limit"));
        drop(session);
        for entry in fs::read_dir(directory.path()).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(
                !name.to_string_lossy().starts_with(".emda-stage"),
                "spill stage leaked: {}",
                name.to_string_lossy()
            );
        }
    }

    #[test]
    fn fan_in_and_chunk_limits_fail_closed() {
        let error = SpillLimits::new(0, 10, 10, 2, 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let error = SpillLimits::new(1, 10, 10, 1, 1).unwrap_err();
        assert!(error.to_string().contains("fan-in"));
        let directory = tempfile::tempdir().unwrap();
        let mut session = SpillSession::create(
            directory.path(),
            SpillLimits::new(1, 1, 0, 2, 10).unwrap(),
            0,
            0,
        )
        .unwrap();
        let error = session
            .push(b"ab", Some(loc(100)), |_| Ok(None))
            .unwrap_err();
        assert!(error.to_string().contains("key-byte"));
    }

    fn assert_stage_removed(parent: &Path) {
        for entry in fs::read_dir(parent).unwrap() {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            assert!(
                !name.starts_with(".emda-stage"),
                "spill stage leaked: {name}"
            );
        }
        let cleaned = crate::cleanup_abandoned_staging(parent).unwrap();
        assert_eq!(cleaned.removed_markers, 0);
        assert_eq!(cleaned.removed_payloads, 0);
    }

    fn must_err<T>(result: io::Result<T>) -> io::Error {
        match result {
            Ok(_) => panic!("expected spill read to fail"),
            Err(error) => error,
        }
    }

    #[test]
    fn oversized_offset_sidecar_cannot_extend_the_staged_run() {
        let directory = tempfile::tempdir().unwrap();
        let fd = Rc::new(FdInner {
            open: Cell::new(0),
            peak: Cell::new(0),
            limit: 2,
        });
        let mut writer = RunWriter::create(directory.path(), 1, &fd, 1, 1).unwrap();
        writer.write_entry(b"a", Some(loc(100))).unwrap();
        let table_end = writer.table_at + OFFSET_LEN;
        let run = File::open(&writer.path).unwrap();
        writer.offsets.write_all(&[0; IO_BUFFER]).unwrap();

        let error = must_err(writer.finish());
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "spill offset table length mismatch");
        assert!(run.metadata().unwrap().len() <= table_end);
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        assert_eq!(fd.open.get(), 0);
    }

    fn emit_all(
        session: &mut SpillSession,
        emitted: &mut Vec<(Vec<u8>, Option<ValueLocation>)>,
    ) -> io::Result<()> {
        session.prepare_for_emit()?;
        while let Some(item) = session.next_latest()? {
            emitted.push(item);
        }
        Ok(())
    }

    /// One finished run and an empty chunk. Chunk limits are 2 keys and 1024
    /// key bytes, with a 1 MiB spill budget, fan-in 2, and 100 operations.
    fn finished_single_run() -> (SpillSession, tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let mut session =
            SpillSession::create(directory.path(), limits(2, 1 << 20, 100), 0, 0).unwrap();
        session.push(b"a", Some(loc(100)), |_| Ok(None)).unwrap();
        session.push(b"b", Some(loc(200)), |_| Ok(None)).unwrap();
        session.write_dirty().unwrap();
        assert!(session.dirty.is_empty());
        assert_eq!(session.runs.len(), 1);
        let path = session.runs[0].path.clone();
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.starts_with(MAGIC));
        assert!(bytes.len() as u64 > HEADER_LEN + CHECKSUM_LEN);
        (session, directory, path)
    }

    #[test]
    fn buffered_scan_rejects_changes_to_prefetched_entries_or_offsets_at_end() {
        for change_offset in [false, true] {
            let (session, _directory, path) = finished_single_run();
            let mut cursor = RunCursor::open(&session.fd, &session.runs[0], true).unwrap();
            let (first, _) = cursor.next_entry().unwrap().unwrap();
            assert_eq!(first, b"a");
            if change_offset {
                write_offset_slot(&path, 1, HEADER_LEN);
            } else {
                xor_byte(&path, one_byte_key_at(1), 1);
            }
            // The buffered entry is the original verified image. A caller
            // may use it only in private staging until the pass completes.
            let (second, _) = cursor.next_entry().unwrap().unwrap();
            assert_eq!(second, b"b");
            for _ in 0..2 {
                let error = must_err(cursor.next_entry());
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                assert_eq!(error.to_string(), "spill run checksum mismatch");
            }
        }
    }

    #[test]
    fn buffered_scan_rejects_corruption_at_payload_and_offset_refills() {
        let key_len = 4;
        let stride = ENTRY_OVERHEAD + key_len;
        let buffer_len = IO_BUFFER as u64;
        let first_offset_after_buffer = buffer_len / OFFSET_LEN;
        let count = first_offset_after_buffer + 2;
        let straddling_entry = buffer_len / stride;
        let changed_location =
            HEADER_LEN + (straddling_entry + 1) * stride - ENTRY_CRC_LEN as u64 - 1;
        assert!(changed_location >= HEADER_LEN + buffer_len);
        assert!(HEADER_LEN + straddling_entry * stride < HEADER_LEN + buffer_len);

        for corrupt_offsets in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let fd = Rc::new(FdInner {
                open: Cell::new(0),
                peak: Cell::new(0),
                limit: 2,
            });
            let mut writer =
                RunWriter::create(directory.path(), 1, &fd, count, count * key_len).unwrap();
            for index in 0..count {
                writer
                    .write_entry(&(index as u32).to_be_bytes(), Some(loc(100 + index)))
                    .unwrap();
            }
            let run = writer.finish().unwrap();
            let mut cursor = RunCursor::open(&fd, &run, true).unwrap();
            let assert_entry = |cursor: &mut RunCursor, index: u64| {
                let (key, state) = cursor.next_entry().unwrap().unwrap();
                assert_eq!(key, (index as u32).to_be_bytes());
                assert_eq!(located(state), located(Some(loc(100 + index))));
            };
            assert_entry(&mut cursor, 0);

            let rejected_index = if corrupt_offsets {
                let previous_start = HEADER_LEN + (first_offset_after_buffer - 1) * stride;
                write_offset_slot(&run.path, first_offset_after_buffer, previous_start);
                first_offset_after_buffer - 1
            } else {
                xor_byte(&run.path, changed_location, 1);
                straddling_entry
            };
            for index in 1..rejected_index {
                assert_entry(&mut cursor, index);
            }
            for _ in 0..2 {
                let error = must_read_err(cursor.next_entry());
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                if corrupt_offsets {
                    assert_eq!(error.to_string(), "invalid spill entry span");
                } else {
                    assert_entry_checksum(&error);
                }
                assert_eq!(cursor.index, rejected_index);
            }
            drop(cursor);
            if corrupt_offsets {
                let start = HEADER_LEN + first_offset_after_buffer * stride;
                write_offset_slot(&run.path, first_offset_after_buffer, start);
            } else {
                xor_byte(&run.path, changed_location, 1);
            }
            let mut retry = RunCursor::open(&fd, &run, true).unwrap();
            for index in 0..count {
                assert_entry(&mut retry, index);
            }
            assert!(retry.next_entry().unwrap().is_none());
        }
    }

    /// Two finished runs, not yet merged. Chunk limit is 1 key and 1024 key
    /// bytes, with a 1 MiB spill budget, fan-in 2, and 100 operations.
    fn two_finished_runs() -> (SpillSession, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let mut session =
            SpillSession::create(directory.path(), limits(1, 1 << 20, 100), 0, 0).unwrap();
        session.push(b"a", Some(loc(100)), |_| Ok(None)).unwrap();
        session.push(b"b", Some(loc(200)), |_| Ok(None)).unwrap();
        session.write_dirty().unwrap();
        assert!(session.dirty.is_empty());
        assert_eq!(session.runs.len(), 2);
        (session, directory)
    }

    #[test]
    fn run_key_bounds_skip_only_impossible_lookups_and_emission_still_verifies() {
        let directory = tempfile::tempdir().unwrap();
        let mut session =
            SpillSession::create(directory.path(), limits(2, 1 << 20, 100), 1, 1).unwrap();
        session.push(b"b", Some(loc(100)), |_| Ok(None)).unwrap();
        session.push(b"d", None, |_| Ok(None)).unwrap();
        session.write_dirty().unwrap();
        assert!(matches!(
            session.lookup_persisted(b"b").unwrap(),
            Find::Found(Some(value)) if value == loc(100)
        ));
        assert!(matches!(
            session.lookup_persisted(b"d").unwrap(),
            Find::Found(None)
        ));
        assert!(matches!(
            session.lookup_persisted(b"c").unwrap(),
            Find::Absent
        ));

        let path = &session.runs[0].path;
        // A forged key outside the retained range cannot influence replay.
        // A later merge must still reject the damaged file before emission.
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.write_all_at(b"a", one_byte_key_at(0)).unwrap();
        drop(file);
        for key in [b"a", b"e"] {
            assert!(matches!(
                session.lookup_persisted(key).unwrap(),
                Find::Absent
            ));
        }
        session
            .push(b"a", None, |key| {
                assert_eq!(key, b"a");
                Ok(Some(loc(200)))
            })
            .unwrap();
        assert_eq!(session.live_count(), 1);
        assert_eq!(session.peak(), 2);
        let mut emitted = Vec::new();
        assert_eq!(
            emit_all(&mut session, &mut emitted)
                .unwrap_err()
                .to_string(),
            "spill run checksum mismatch"
        );
        assert!(emitted.is_empty());
        drop(session);
        assert_stage_removed(directory.path());
    }

    #[test]
    fn old_valid_run_cannot_replace_another_rebuilds_run() {
        let (mut current, directory, path) = finished_single_run();
        let other_directory = tempfile::tempdir().unwrap();
        let mut old =
            SpillSession::create(other_directory.path(), limits(2, 1 << 20, 100), 0, 0).unwrap();
        old.push(b"a", Some(loc(300)), |_| Ok(None)).unwrap();
        old.push(b"b", Some(loc(400)), |_| Ok(None)).unwrap();
        old.write_dirty().unwrap();
        let stale = fs::read(&old.runs[0].path).unwrap();
        let original = fs::read(&path).unwrap();
        assert_eq!(current.runs[0].id, old.runs[0].id);
        assert_eq!(current.runs[0].entries, old.runs[0].entries);
        assert_eq!(original.len(), stale.len());
        assert_eq!(
            original[..HEADER_LEN as usize],
            stale[..HEADER_LEN as usize]
        );
        old.prepare_for_emit().unwrap();
        assert_eq!(
            old.next_latest().unwrap(),
            Some((b"a".to_vec(), Some(loc(300))))
        );

        fs::write(&path, &stale).unwrap();
        let lookup = must_err(current.lookup_persisted(b"a"));
        assert_eq!(lookup.to_string(), "spill run checksum mismatch");
        let prepare = current.prepare_for_emit().unwrap_err();
        assert_eq!(prepare.to_string(), "spill run checksum mismatch");
        assert_eq!(current.live_count(), 2);
        assert!(current.dirty.is_empty());

        fs::write(&path, original).unwrap();
        current.prepare_for_emit().unwrap();
        // Overwrite the same inode after the complete check, while the
        // cursor still has the original run open.
        fs::write(&path, stale).unwrap();
        assert_entry_checksum(&must_err(current.next_latest()));
        drop(current);
        assert_stage_removed(directory.path());
    }

    fn flip_byte(path: &Path, offset: u64) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact_at(&mut byte, offset).unwrap();
        byte[0] ^= 0xff;
        file.write_all_at(&byte, offset).unwrap();
    }

    fn corrupt_first_entry_tag(path: &Path) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut key_len_bytes = [0_u8; 2];
        file.read_exact_at(&mut key_len_bytes, HEADER_LEN).unwrap();
        let key_len = u16::from_le_bytes(key_len_bytes) as u64;
        assert!((1..=1024).contains(&key_len), "key length {key_len}");
        let tag_at = HEADER_LEN + 2 + key_len;
        let mut tag = [0_u8; 1];
        file.read_exact_at(&mut tag, tag_at).unwrap();
        assert_eq!(tag[0], 1, "expected a live spill entry");
        file.write_all_at(&[0x7f], tag_at).unwrap();
    }

    fn stage_run_names(session: &SpillSession) -> Vec<String> {
        let stage = session.stage.as_ref().unwrap().path();
        let mut names = fs::read_dir(stage)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn corrupted_finished_run_emits_nothing_and_cleans_up() {
        let (mut intact, intact_dir, _) = finished_single_run();
        let mut emitted = Vec::new();
        emit_all(&mut intact, &mut emitted).unwrap();
        assert_eq!(
            emitted
                .iter()
                .map(|(key, state)| {
                    let location = state.unwrap();
                    (
                        key.clone(),
                        location.offset,
                        location.length,
                        location.checksum,
                    )
                })
                .collect::<Vec<_>>(),
            vec![(b"a".to_vec(), 100, 1, 100), (b"b".to_vec(), 200, 1, 200),]
        );
        assert!(intact.next_latest().unwrap().is_none());
        drop(intact);
        assert_stage_removed(intact_dir.path());

        let (mut session, directory, path) = finished_single_run();
        flip_byte(&path, HEADER_LEN);
        let mut emitted = Vec::new();
        let error = emit_all(&mut session, &mut emitted).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("spill run checksum mismatch"),
            "{error}"
        );
        assert!(emitted.is_empty());
        assert!(matches!(session.emit, Emit::Pending));
        assert!(session.last_key.is_none());
        let blocked = must_err(session.next_latest());
        assert!(
            blocked
                .to_string()
                .contains("suffix spill was not prepared"),
            "{blocked}"
        );
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory) = two_finished_runs();
        flip_byte(&session.runs[0].path, HEADER_LEN);
        let mut emitted = Vec::new();
        let error = emit_all(&mut session, &mut emitted).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("spill run checksum mismatch"),
            "{error}"
        );
        assert!(emitted.is_empty());
        assert_eq!(session.runs.len(), 2);
        assert_eq!(
            stage_run_names(&session),
            vec!["run-000001".to_string(), "run-000002".to_string()]
        );
        assert!(matches!(session.emit, Emit::Pending));
        assert!(session.last_key.is_none());
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        session.prepare_for_emit().unwrap();
        assert!(matches!(session.emit, Emit::File(_)));
        corrupt_first_entry_tag(&path);
        let mut emitted = Vec::new();
        let error = (|| {
            while let Some(item) = session.next_latest()? {
                emitted.push(item);
            }
            Ok::<(), io::Error>(())
        })()
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("unknown spill entry tag"),
            "{error}"
        );
        assert!(emitted.is_empty());
        assert!(session.last_key.is_none());
        let again = must_err(session.next_latest());
        assert_eq!(again.kind(), io::ErrorKind::InvalidData);
        assert!(session.last_key.is_none());
        drop(session);
        assert_stage_removed(directory.path());
    }

    #[test]
    fn truncation_before_emission_emits_nothing_and_cleans_up() {
        let (mut session, directory, path) = finished_single_run();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(0)
            .unwrap();
        let mut emitted = Vec::new();
        let error = emit_all(&mut session, &mut emitted).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("incomplete spill run"),
            "{error}"
        );
        assert!(emitted.is_empty());
        assert!(matches!(session.emit, Emit::Pending));
        assert!(session.last_key.is_none());
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory) = two_finished_runs();
        OpenOptions::new()
            .write(true)
            .open(&session.runs[0].path)
            .unwrap()
            .set_len(0)
            .unwrap();
        let mut emitted = Vec::new();
        let error = emit_all(&mut session, &mut emitted).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("incomplete spill run"),
            "{error}"
        );
        assert!(emitted.is_empty());
        assert_eq!(session.runs.len(), 2);
        assert_eq!(
            stage_run_names(&session),
            vec!["run-000001".to_string(), "run-000002".to_string()]
        );
        assert!(session.last_key.is_none());
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        session.prepare_for_emit().unwrap();
        assert!(matches!(session.emit, Emit::File(_)));
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(0)
            .unwrap();
        let mut emitted = Vec::new();
        let error = (|| {
            while let Some(item) = session.next_latest()? {
                emitted.push(item);
            }
            Ok::<(), io::Error>(())
        })()
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(emitted.is_empty());
        assert!(session.last_key.is_none());
        let again = must_err(session.next_latest());
        assert_eq!(again.kind(), io::ErrorKind::UnexpectedEof);
        assert!(session.last_key.is_none());
        drop(session);
        assert_stage_removed(directory.path());
    }

    fn refuse_base(key: &[u8]) -> io::Result<Option<ValueLocation>> {
        panic!("base consulted for {}", String::from_utf8_lossy(key));
    }

    fn one_byte_stride() -> u64 {
        ENTRY_OVERHEAD + 1
    }

    fn one_byte_key_at(index: u64) -> u64 {
        HEADER_LEN + index * one_byte_stride() + 2
    }

    /// First byte of the value offset. `loc(100)` stays a legal location after
    /// this byte is flipped, so only the entry checksum can reject it.
    fn one_byte_value_offset_at(index: u64) -> u64 {
        HEADER_LEN + index * one_byte_stride() + 4
    }

    fn xor_byte(path: &Path, offset: u64, mask: u8) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact_at(&mut byte, offset).unwrap();
        byte[0] ^= mask;
        file.write_all_at(&byte, offset).unwrap();
    }

    fn write_offset_slot(path: &Path, index: u64, offset: u64) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut table = [0_u8; 8];
        file.read_exact_at(&mut table, 20).unwrap();
        let table_at = u64::from_le_bytes(table);
        file.write_all_at(&offset.to_le_bytes(), table_at + index * OFFSET_LEN)
            .unwrap();
    }

    fn assert_entry_checksum(error: &io::Error) {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("spill entry checksum mismatch"),
            "{error}"
        );
        assert!(
            !error.to_string().contains("spill run checksum mismatch"),
            "{error}"
        );
    }

    fn must_find_err(result: io::Result<Find>) -> io::Error {
        match result {
            Ok(Find::Found(_)) => panic!("lookup returned a spill value"),
            Ok(Find::Absent) => panic!("lookup reported the key absent"),
            Err(error) => error,
        }
    }

    fn must_read_err(result: io::Result<Option<(Vec<u8>, Option<ValueLocation>)>>) -> io::Error {
        match result {
            Ok(Some((key, _))) => {
                panic!("read returned {}", String::from_utf8_lossy(&key))
            }
            Ok(None) => panic!("read ended the run"),
            Err(error) => error,
        }
    }

    #[test]
    fn used_entry_bytes_fail_closed_for_lookup_and_emission() {
        let (session, directory, path) = finished_single_run();
        assert!(fs::read(&path).unwrap().starts_with(MAGIC));
        let mut version = [0_u8; 4];
        File::open(&path)
            .unwrap()
            .read_exact_at(&mut version, 8)
            .unwrap();
        assert_eq!(u32::from_le_bytes(version), VERSION);
        assert_eq!(session.spill_bytes(), run_len(2, 2).unwrap());
        assert_eq!(fs::metadata(&path).unwrap().len(), run_len(2, 2).unwrap());
        assert!(session.peak_open_fds() <= 4);
        {
            let mut cursor = RunCursor::open(&session.fd, &session.runs[0], true).unwrap();
            match cursor.find(b"a").unwrap() {
                Find::Found(Some(location)) => {
                    assert_eq!(
                        (location.offset, location.length, location.checksum),
                        (100, 1, 100)
                    );
                }
                Find::Found(None) => panic!("intact key was a tombstone"),
                Find::Absent => panic!("intact key was absent"),
            }
            match cursor.find(b"missing").unwrap() {
                Find::Absent => {}
                Find::Found(_) => panic!("missing key was present"),
            }
        }
        drop(session);
        assert_stage_removed(directory.path());

        let (session, directory, path) = finished_single_run();
        let mut cursor = RunCursor::open(&session.fd, &session.runs[0], true).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(b"B", one_byte_key_at(0))
            .unwrap();
        assert_entry_checksum(&must_find_err(cursor.find(b"a")));
        assert_entry_checksum(&must_find_err(cursor.find(b"B")));
        match cursor.find(b"b").unwrap() {
            Find::Found(Some(location)) => {
                assert_eq!(
                    (location.offset, location.length, location.checksum),
                    (200, 1, 200)
                );
            }
            Find::Found(None) => panic!("intact sibling became a tombstone"),
            Find::Absent => panic!("intact sibling was skipped"),
        }
        assert_entry_checksum(&must_read_err(cursor.next_entry()));
        drop(cursor);
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(b"B", one_byte_key_at(0))
            .unwrap();
        let error = session
            .push(b"a", None, refuse_base)
            .expect_err("corrupted key must not resolve");
        assert_entry_checksum(&error);
        assert_eq!(session.live_count(), 2);
        assert!(session.dirty.is_empty());
        // With two entries the first probe is the intact sibling. Its
        // authenticated value is applied; the unread damaged entry cannot
        // change that result.
        session.push(b"b", None, refuse_base).unwrap();
        assert_eq!(session.live_count(), 1);
        assert!(session.dirty.contains_key(b"b".as_slice()));
        assert!(!session.dirty.contains_key(b"a".as_slice()));
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(b"B", one_byte_key_at(1))
            .unwrap();
        // The first probe is this damaged entry. Trusting its key would
        // walk past both original keys and consult the base.
        for key in [b"a".as_slice(), b"b".as_slice()] {
            let error = session
                .push(key, None, refuse_base)
                .expect_err("damaged pivot must not become an absence");
            assert_entry_checksum(&error);
            assert_eq!(session.live_count(), 2);
            assert!(session.dirty.is_empty());
        }
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        flip_byte(&path, one_byte_value_offset_at(0));
        let error = session
            .push(b"a", None, refuse_base)
            .expect_err("retargeted value must not be applied");
        assert_entry_checksum(&error);
        assert_eq!(session.live_count(), 2);
        assert!(session.dirty.is_empty());
        drop(session);
        assert_stage_removed(directory.path());

        let directory = tempfile::tempdir().unwrap();
        let mut session =
            SpillSession::create(directory.path(), limits(3, 1 << 20, 100), 0, 0).unwrap();
        session.push(b"a", Some(loc(100)), |_| Ok(None)).unwrap();
        session.push(b"m", Some(loc(200)), |_| Ok(None)).unwrap();
        session.push(b"z", Some(loc(300)), |_| Ok(None)).unwrap();
        session.write_dirty().unwrap();
        assert_eq!(session.live_count(), 3);
        assert_eq!(session.runs.len(), 1);
        OpenOptions::new()
            .write(true)
            .open(&session.runs[0].path)
            .unwrap()
            .write_all_at(b"M", one_byte_key_at(1))
            .unwrap();
        let error = session
            .push(b"z", None, refuse_base)
            .expect_err("damaged binary-search pivot");
        assert_entry_checksum(&error);
        assert_eq!(session.live_count(), 3);
        assert!(session.dirty.is_empty());
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        session.prepare_for_emit().unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(b"B", 30)
            .unwrap();
        let mut emitted = Vec::new();
        let error = (|| {
            while let Some(item) = session.next_latest()? {
                emitted.push(item);
            }
            Ok::<(), io::Error>(())
        })()
        .unwrap_err();
        assert_entry_checksum(&error);
        assert!(emitted.is_empty());
        assert!(session.last_key.is_none());
        assert_entry_checksum(&must_err(session.next_latest()));
        assert!(session.last_key.is_none());
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        session.prepare_for_emit().unwrap();
        flip_byte(&path, one_byte_value_offset_at(0));
        let error = must_err(session.next_latest());
        assert_entry_checksum(&error);
        assert!(session.last_key.is_none());
        drop(session);
        assert_stage_removed(directory.path());

        let (session, directory) = two_finished_runs();
        let sources = session.runs.clone();
        let generations = sources.iter().map(|run| run.id).collect::<Vec<_>>();
        let mut cursors = Vec::new();
        for run in &sources {
            cursors.push(RunCursor::open(&session.fd, run, true).unwrap());
        }
        OpenOptions::new()
            .write(true)
            .open(&sources[0].path)
            .unwrap()
            .write_all_at(b"B", one_byte_key_at(0))
            .unwrap();
        let error = merge_walk(&mut cursors, &generations, |_, _| {
            panic!("merge emitted a damaged entry");
        })
        .unwrap_err();
        assert_entry_checksum(&error);
        drop(cursors);
        drop(session);
        assert_stage_removed(directory.path());
    }

    #[test]
    fn header_and_offset_corruption_cannot_retarget_a_lookup() {
        assert_eq!(run_table_end(u64::MAX, HEADER_LEN), None);
        assert_eq!(run_table_end(1, u64::MAX - OFFSET_LEN), None);
        let (session, directory, path) = finished_single_run();
        let file = File::open(&path).unwrap();
        let error = match read_entry(
            &file,
            HEADER_LEN - 1..HEADER_LEN + 32,
            HEADER_LEN + 32,
            &[0; 32],
            0,
            1,
            &mut [0_u8; MAX_ENTRY_ON_DISK],
        ) {
            Ok(_) => panic!("header bytes accepted as a spill entry"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("overlaps its header"));
        drop(file);
        drop(session);
        assert_stage_removed(directory.path());
        let (mut session, directory, path) = finished_single_run();
        write_offset_slot(&path, 0, HEADER_LEN + one_byte_stride());
        let error = session
            .push(b"a", None, refuse_base)
            .expect_err("retargeted offset");
        assert_eq!(error.to_string(), "invalid spill entry span");
        assert_eq!(session.live_count(), 2);
        assert!(session.dirty.is_empty());
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        session.prepare_for_emit().unwrap();
        // The preceding entry still has a valid CRC. A neighboring slot
        // that extends its span by a byte must nevertheless be rejected.
        write_offset_slot(&path, 1, HEADER_LEN + one_byte_stride() + 1);
        assert_eq!(
            must_err(session.next_latest()).to_string(),
            "spill entry length does not match its span"
        );
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        session.prepare_for_emit().unwrap();
        flip_byte(&path, {
            let file = File::open(&path).unwrap();
            let mut table = [0_u8; 8];
            file.read_exact_at(&mut table, 20).unwrap();
            u64::from_le_bytes(table)
        });
        let error = must_err(session.next_latest());
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("spill run entry offset is not packed"),
            "{error}"
        );
        assert!(session.last_key.is_none());
        drop(session);
        assert_stage_removed(directory.path());

        let (session, directory) = two_finished_runs();
        let entry_len = one_byte_stride() as usize;
        let mut entry = vec![0_u8; entry_len];
        File::open(&session.runs[0].path)
            .unwrap()
            .read_exact_at(&mut entry, HEADER_LEN)
            .unwrap();
        OpenOptions::new()
            .write(true)
            .open(&session.runs[1].path)
            .unwrap()
            .write_all_at(&entry, HEADER_LEN)
            .unwrap();
        let mut newest = RunCursor::open(&session.fd, &session.runs[1], false).unwrap();
        assert_entry_checksum(&must_find_err(newest.find(b"b")));
        assert_entry_checksum(&must_find_err(newest.find(b"a")));
        drop(newest);
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        xor_byte(&path, 12, 0x01);
        let error = session
            .push(b"a", None, refuse_base)
            .expect_err("corrupted count");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("spill run header does not match its record"),
            "{error}"
        );
        assert_eq!(session.live_count(), 2);
        assert!(session.dirty.is_empty());
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        xor_byte(&path, 20, 0x01);
        let error = session
            .push(b"a", None, refuse_base)
            .expect_err("corrupted table position");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("spill run header does not match its record"),
            "{error}"
        );
        assert_eq!(session.live_count(), 2);
        drop(session);
        assert_stage_removed(directory.path());

        let (mut session, directory, path) = finished_single_run();
        flip_byte(&path, HEADER_LEN + one_byte_stride() - 1);
        let error = session
            .push(b"a", None, refuse_base)
            .expect_err("corrupted entry checksum");
        assert_entry_checksum(&error);
        assert_eq!(session.live_count(), 2);
        drop(session);
        assert_stage_removed(directory.path());
    }

    type SpillEntry = (Vec<u8>, Option<ValueLocation>);

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum UsedByte {
        Key,
        Tag,
        ValueOffset,
        TableOffset,
        Other,
    }

    struct UsedSite {
        offset: u64,
        entry_index: usize,
        entry_at: u64,
        entry_len: usize,
        kind: UsedByte,
        mask: u8,
        forged_key: Option<Vec<u8>>,
    }

    fn located(state: Option<ValueLocation>) -> Option<(u64, usize, u32)> {
        state.map(|location| (location.offset, location.length, location.checksum))
    }

    fn spill_entry_variants() -> Vec<SpillEntry> {
        vec![
            (b"a".to_vec(), Some(loc(100))),
            (b"bb".to_vec(), None),
            (
                b"mid".to_vec(),
                Some(loc(crate::witness::LOG_HEADER_SIZE as u64)),
            ),
            (vec![0x7f, 0x80], Some(loc(1_000_000))),
        ]
    }

    fn variant_run(entries: &[SpillEntry]) -> (SpillSession, tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let mut session =
            SpillSession::create(directory.path(), limits(entries.len(), 1 << 20, 100), 0, 0)
                .unwrap();
        for (key, state) in entries {
            session.push(key, *state, |_| Ok(None)).unwrap();
        }
        session.write_dirty().unwrap();
        assert!(session.dirty.is_empty());
        assert_eq!(session.runs.len(), 1);
        let path = session.runs[0].path.clone();
        let key_bytes = entries.iter().map(|(key, _)| key.len() as u64).sum();
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            run_len(entries.len() as u64, key_bytes).unwrap()
        );
        (session, directory, path)
    }

    #[test]
    fn maximum_key_round_trips_through_spill_lookup_and_scan() {
        let entries = [(vec![b'a'; record::MAX_KEY], Some(loc(100)))];
        let (session, _directory, _) = variant_run(&entries);
        let (key, state) = &entries[0];
        for buffered in [false, true] {
            let mut cursor = RunCursor::open(&session.fd, &session.runs[0], buffered).unwrap();
            let Find::Found(found) = cursor.find(key).unwrap() else {
                panic!("maximum-length key was not found");
            };
            assert_eq!(located(found), located(*state));
            let (generated_key, generated_state) = cursor.next_entry().unwrap().unwrap();
            assert_eq!(&generated_key, key);
            assert_eq!(located(generated_state), located(*state));
            assert!(cursor.next_entry().unwrap().is_none());
        }
    }

    /// Sites are derived from the finished image so each used entry byte and
    /// each table-offset byte is corrupted once. The header and trailing
    /// digest are not sites: a cursor that has already opened does not reread
    /// them to authenticate an entry.
    fn used_sites(bytes: &[u8], entries: &[SpillEntry]) -> Vec<UsedSite> {
        let table_at =
            usize::try_from(u64::from_le_bytes(bytes[20..28].try_into().unwrap())).unwrap();
        let mut pos = usize::try_from(HEADER_LEN).unwrap();
        let mut starts = Vec::with_capacity(entries.len());
        let mut sites = Vec::new();
        for (index, (key, state)) in entries.iter().enumerate() {
            let key_len = usize::from(u16::from_le_bytes([bytes[pos], bytes[pos + 1]]));
            assert_eq!(key_len, key.len(), "entry {index} key length");
            assert_eq!(&bytes[pos + 2..pos + 2 + key_len], key.as_slice());
            let entry_len = 2 + key_len + TAIL_LEN + ENTRY_CRC_LEN;
            assert!(pos + entry_len <= table_at);
            let tag_at = 2 + key_len;
            assert_eq!(bytes[pos + tag_at], u8::from(state.is_some()));
            starts.push(pos);
            for rel in 0..entry_len {
                let kind = if (2..tag_at).contains(&rel) {
                    UsedByte::Key
                } else if rel == tag_at {
                    UsedByte::Tag
                } else if state.is_some() && (tag_at + 1..tag_at + 9).contains(&rel) {
                    UsedByte::ValueOffset
                } else {
                    UsedByte::Other
                };
                let forged_key = if kind == UsedByte::Key {
                    let mut forged = key.clone();
                    forged[rel - 2] ^= 0x01;
                    assert_ne!(&forged, key);
                    assert!(entries.iter().all(|(other, _)| other != &forged));
                    Some(forged)
                } else {
                    None
                };
                sites.push(UsedSite {
                    offset: pos as u64 + rel as u64,
                    entry_index: index,
                    entry_at: pos as u64,
                    entry_len,
                    kind,
                    mask: 0x01,
                    forged_key,
                });
            }
            pos += entry_len;
        }
        assert_eq!(pos, table_at);
        for (index, start) in starts.iter().enumerate() {
            let slot = table_at + index * usize::try_from(OFFSET_LEN).unwrap();
            let stored = u64::from_le_bytes(bytes[slot..slot + 8].try_into().unwrap());
            assert_eq!(stored, *start as u64);
            for byte in 0..usize::try_from(OFFSET_LEN).unwrap() {
                sites.push(UsedSite {
                    offset: (slot + byte) as u64,
                    entry_index: index,
                    entry_at: *start as u64,
                    entry_len: 2 + entries[index].0.len() + TAIL_LEN + ENTRY_CRC_LEN,
                    kind: UsedByte::TableOffset,
                    mask: 0x01,
                    forged_key: None,
                });
            }
        }
        let digest_at = table_at + entries.len() * usize::try_from(OFFSET_LEN).unwrap();
        assert_eq!(
            digest_at + usize::try_from(CHECKSUM_LEN).unwrap(),
            bytes.len()
        );
        let mut covered = vec![false; bytes.len()];
        for site in &sites {
            let index = usize::try_from(site.offset).unwrap();
            assert!(!covered[index], "byte {index} corrupted twice");
            covered[index] = true;
        }
        assert!(
            covered[..usize::try_from(HEADER_LEN).unwrap()]
                .iter()
                .all(|hit| !hit)
        );
        assert!(covered[digest_at..].iter().all(|hit| !hit));
        assert!(
            covered[usize::try_from(HEADER_LEN).unwrap()..digest_at]
                .iter()
                .all(|hit| *hit)
        );
        let key_bytes = entries.iter().map(|(key, _)| key.len()).sum::<usize>();
        let live = entries.iter().filter(|(_, state)| state.is_some()).count();
        assert_eq!(
            sites
                .iter()
                .filter(|site| site.kind == UsedByte::Key)
                .count(),
            key_bytes
        );
        assert_eq!(
            sites
                .iter()
                .filter(|site| site.kind == UsedByte::Tag)
                .count(),
            entries.len()
        );
        assert_eq!(
            sites
                .iter()
                .filter(|site| site.kind == UsedByte::ValueOffset)
                .count(),
            live * 8
        );
        assert_eq!(
            sites
                .iter()
                .filter(|site| site.kind == UsedByte::TableOffset)
                .count(),
            entries.len() * 8
        );
        for index in 0..entries.len() {
            assert!(
                sites
                    .iter()
                    .any(|site| { site.entry_index == index && site.kind == UsedByte::Key })
            );
            assert!(
                sites
                    .iter()
                    .any(|site| { site.entry_index == index && site.kind == UsedByte::Tag })
            );
        }
        sites
    }

    fn flip_one_byte(path: &Path, offset: u64, mask: u8) -> Vec<u8> {
        let before = fs::read(path).unwrap();
        xor_byte(path, offset, mask);
        let after = fs::read(path).unwrap();
        assert_eq!(before.len(), after.len());
        let diffs = before
            .iter()
            .zip(after.iter())
            .enumerate()
            .filter_map(|(index, (left, right))| (left != right).then_some(index))
            .collect::<Vec<_>>();
        let at = usize::try_from(offset).unwrap();
        assert_eq!(diffs, vec![at]);
        assert_eq!(before[at] ^ after[at], mask);
        after
    }

    fn assert_valid_variant(bytes: &[u8], entries: &[SpillEntry], site: &UsedSite) {
        let (key, state) = &entries[site.entry_index];
        let start = usize::try_from(site.entry_at).unwrap();
        let entry = &bytes[start..start + site.entry_len];
        let tag_at = 2 + key.len();
        match site.kind {
            UsedByte::Key => {
                let forged = site.forged_key.as_deref().unwrap();
                assert_eq!(&entry[2..tag_at], forged);
                record::validate_key(forged).unwrap();
                assert_eq!(entry[tag_at], u8::from(state.is_some()));
                if let Some(location) = *state {
                    let offset =
                        u64::from_le_bytes(entry[tag_at + 1..tag_at + 9].try_into().unwrap());
                    assert_eq!(offset, location.offset);
                }
            }
            UsedByte::Tag => {
                let tag = entry[tag_at];
                assert!(tag <= 1, "tag {tag} is not a valid spill tag");
                assert_ne!(tag, u8::from(state.is_some()));
            }
            UsedByte::ValueOffset => {
                let tail = &entry[tag_at..tag_at + TAIL_LEN];
                assert_eq!(tail[0], 1);
                let offset = u64::from_le_bytes(tail[1..9].try_into().unwrap());
                let length = u32::from_le_bytes(tail[9..13].try_into().unwrap());
                let checksum = u32::from_le_bytes(tail[13..17].try_into().unwrap());
                let original = state.unwrap();
                assert_ne!(offset, original.offset);
                assert!(offset >= crate::witness::LOG_HEADER_SIZE as u64);
                assert!(usize::try_from(length).unwrap() <= record::MAX_VALUE);
                assert_eq!(usize::try_from(length).unwrap(), original.length);
                assert_eq!(checksum, original.checksum);
                assert_eq!(&entry[2..tag_at], key.as_slice());
            }
            UsedByte::TableOffset | UsedByte::Other => {}
        }
    }

    fn assert_read_closed(error: &io::Error, label: &str) {
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ),
            "{label}: {error}"
        );
    }

    fn assert_scan_stops_at_damage(
        mut next: impl FnMut() -> io::Result<Option<(Vec<u8>, Option<ValueLocation>)>>,
        entries: &[SpillEntry],
        damaged: usize,
        label: &str,
    ) {
        for (index, (key, state)) in entries.iter().enumerate().take(damaged) {
            match next() {
                Ok(Some((found_key, found_state))) => {
                    assert_eq!(&found_key, key, "{label} changed an earlier key");
                    assert_eq!(
                        located(found_state),
                        located(*state),
                        "{label} changed an earlier location"
                    );
                }
                Ok(None) => panic!("{label} ended before intact entry {index}"),
                Err(error) => panic!("{label} rejected intact entry {index}: {error}"),
            }
        }
        match next() {
            Ok(Some((key, state))) => panic!(
                "{label} returned damaged entry {} {:?}",
                String::from_utf8_lossy(&key),
                located(state)
            ),
            Ok(None) => panic!("{label} ended on damaged entry {damaged}"),
            Err(error) => assert_read_closed(&error, label),
        }
        match next() {
            Ok(Some((key, state))) => panic!(
                "{label} continued after damage with {} {:?}",
                String::from_utf8_lossy(&key),
                located(state)
            ),
            Ok(None) => panic!("{label} ended after swallowing damage at {damaged}"),
            Err(error) => assert_read_closed(&error, label),
        }
    }

    fn first_affected_entry(site: &UsedSite) -> usize {
        // Each table offset ends the previous entry as well as starting its own.
        match site.kind {
            UsedByte::TableOffset => site.entry_index.saturating_sub(1),
            _ => site.entry_index,
        }
    }

    fn assert_find_closed(cursor: &mut RunCursor, entries: &[SpillEntry], site: &UsedSite) {
        let first = first_affected_entry(site);
        for (index, (key, state)) in entries.iter().enumerate() {
            match cursor.find(key) {
                Ok(Find::Found(found)) => {
                    assert!(
                        !(first..=site.entry_index).contains(&index),
                        "lookup returned damaged entry {}",
                        String::from_utf8_lossy(key)
                    );
                    assert_eq!(
                        located(found),
                        located(*state),
                        "lookup changed {}",
                        String::from_utf8_lossy(key)
                    );
                }
                Ok(Find::Absent) => {
                    panic!("lookup dropped {}", String::from_utf8_lossy(key))
                }
                Err(error) => assert_read_closed(&error, "lookup"),
            }
        }
        if let Some(forged) = &site.forged_key {
            match cursor.find(forged) {
                Ok(Find::Found(found)) => panic!(
                    "lookup returned changed key {} as {:?}",
                    String::from_utf8_lossy(forged),
                    located(found)
                ),
                Ok(Find::Absent) => {}
                Err(error) => assert_read_closed(&error, "forged lookup"),
            }
        }
    }

    fn assert_pristine(session: &SpillSession, entries: &[SpillEntry]) {
        let mut cursor = RunCursor::open(&session.fd, &session.runs[0], true).unwrap();
        for (key, state) in entries {
            match cursor.find(key).unwrap() {
                Find::Found(found) => assert_eq!(located(found), located(*state)),
                Find::Absent => panic!("missing {}", String::from_utf8_lossy(key)),
            }
        }
        for (key, state) in entries {
            let (found_key, found_state) = cursor.next_entry().unwrap().expect("intact entry");
            assert_eq!(found_key, *key);
            assert_eq!(located(found_state), located(*state));
        }
        assert!(cursor.next_entry().unwrap().is_none());
    }

    #[test]
    fn single_byte_corruption_after_cursor_open_returns_no_changed_entry() {
        let entries = spill_entry_variants();
        let (mut session, directory, path) = variant_run(&entries);
        let original = fs::read(&path).unwrap();
        assert!(original.starts_with(MAGIC));
        let sites = used_sites(&original, &entries);
        assert_pristine(&session, &entries);
        let live_count = session.live_count();

        for site in &sites {
            session.emit = Emit::Pending;
            session.last_key = None;
            fs::write(&path, &original).unwrap();
            {
                let mut cursor = RunCursor::open(&session.fd, &session.runs[0], true).unwrap();
                let mutated = flip_one_byte(&path, site.offset, site.mask);
                assert_valid_variant(&mutated, &entries, site);
                // The byte is inside the image checked at open. The cursor
                // already opened must still refuse it on the next read.
                let reopened = must_err(RunCursor::open(&session.fd, &session.runs[0], true));
                assert_eq!(reopened.kind(), io::ErrorKind::InvalidData);
                assert!(
                    reopened.to_string().contains("spill run checksum mismatch"),
                    "{reopened}"
                );
                assert_find_closed(&mut cursor, &entries, site);
                assert_scan_stops_at_damage(
                    || cursor.next_entry(),
                    &entries,
                    first_affected_entry(site),
                    "scan",
                );
            }
            fs::write(&path, &original).unwrap();
            session.prepare_for_emit().unwrap();
            assert!(matches!(session.emit, Emit::File(_)));
            let mutated = flip_one_byte(&path, site.offset, site.mask);
            assert_valid_variant(&mutated, &entries, site);
            assert_scan_stops_at_damage(
                || session.next_latest(),
                &entries,
                first_affected_entry(site),
                "emission",
            );
            // The file cursor owns the ordering state; the session need not
            // retain a second copy even when an entry read fails.
            assert!(session.last_key.is_none());
            assert_eq!(session.live_count(), live_count);
        }

        drop(session);
        assert_stage_removed(directory.path());
    }
}
