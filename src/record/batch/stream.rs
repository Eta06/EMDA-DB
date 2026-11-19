//! Streaming DB02 validation shared by ordinary and retained recovery.
use crate::record::{self, HEADER_SIZE, Header, Operation};
use crate::recovery::RecoveryReader;
use sha2::{Digest, Sha256};
use std::io;

pub(crate) trait ValueHash: Default {
    type Output;
    fn update_value(&mut self, bytes: &[u8]);
    fn finish(self) -> Self::Output;
}

impl ValueHash for crc32fast::Hasher {
    type Output = u32;
    fn update_value(&mut self, bytes: &[u8]) {
        crc32fast::Hasher::update(self, bytes);
    }
    fn finish(self) -> u32 {
        self.finalize()
    }
}

impl ValueHash for Sha256 {
    type Output = [u8; 32];
    fn update_value(&mut self, bytes: &[u8]) {
        Digest::update(self, bytes);
    }
    fn finish(self) -> [u8; 32] {
        self.finalize().into()
    }
}

impl ValueHash for () {
    type Output = ();
    fn update_value(&mut self, _: &[u8]) {}
    fn finish(self) {}
}

pub(crate) struct StreamValue<D> {
    pub(crate) offset: u64,
    pub(crate) length: usize,
    pub(crate) digest: D,
}

pub(crate) struct Update<D> {
    pub(crate) key: Vec<u8>,
    pub(crate) location: Option<StreamValue<D>>,
}

/// One forward scan of a DB02 payload.
///
/// Child keys and value locations are retained until `finish`. Value bytes
/// are hashed and dropped. At most [`record::batch::MAX_ATOMIC_BATCH_OPERATIONS`]
/// keys are stored. Nested failures release those keys while the caller
/// continues hashing the rest of the record for the outer checksum.
pub(crate) struct BatchScan<D: ValueHash, Admit> {
    phase: BatchPhase,
    pending: Option<PendingChild<D>>,
    error: Option<io::Error>,
    updates: Vec<Update<D::Output>>,
    admit: Option<Admit>,
    payload_base: u64,
    pos: usize,
    ops_left: usize,
}

struct PendingChild<D> {
    operation: Operation,
    key: Vec<u8>,
    key_need: usize,
    value_need: usize,
    value_len: usize,
    value_offset: usize,
    nested: crc32fast::Hasher,
    value_hash: D,
}

enum BatchPhase {
    Count {
        bytes: [u8; 4],
        filled: usize,
    },
    Header {
        bytes: [u8; HEADER_SIZE],
        filled: usize,
    },
    Key,
    Value,
    Checksum {
        bytes: [u8; 4],
        filled: usize,
    },
    Trailing,
    Draining,
}

impl<D: ValueHash, Admit: FnOnce(usize) -> io::Result<()>> BatchScan<D, Admit> {
    pub(crate) fn new(payload_base: u64, admit: Admit) -> Self {
        Self {
            phase: BatchPhase::Count {
                bytes: [0; 4],
                filled: 0,
            },
            pending: None,
            error: None,
            updates: Vec::new(),
            admit: Some(admit),
            payload_base,
            pos: 0,
            ops_left: 0,
        }
    }

    pub(crate) fn push(&mut self, mut chunk: &[u8]) {
        while !chunk.is_empty() {
            if matches!(self.phase, BatchPhase::Draining) {
                return;
            }
            chunk = self.consume(chunk);
        }
    }

    pub(crate) fn finish(self) -> io::Result<Vec<Update<D::Output>>> {
        if let Some(error) = self.error {
            return Err(error);
        }
        match self.phase {
            BatchPhase::Trailing => Ok(self.updates),
            BatchPhase::Count { .. } => Err(record::invalid_data("incomplete batch count")),
            BatchPhase::Header { .. } => Err(record::invalid_data("incomplete nested header")),
            BatchPhase::Key | BatchPhase::Value => {
                Err(record::invalid_data("incomplete nested payload"))
            }
            BatchPhase::Checksum { .. } => Err(record::invalid_data("incomplete nested checksum")),
            BatchPhase::Draining => Err(record::invalid_data("incomplete batch payload")),
        }
    }

    fn consume<'a>(&mut self, input: &'a [u8]) -> &'a [u8] {
        let phase = std::mem::replace(&mut self.phase, BatchPhase::Draining);
        match phase {
            BatchPhase::Draining => &[],
            BatchPhase::Trailing => {
                self.fail(record::invalid_data("trailing bytes in atomic batch"));
                &[]
            }
            BatchPhase::Count { mut bytes, filled } => {
                let (got, rest) = split_at_most(input, 4 - filled);
                bytes[filled..filled + got.len()].copy_from_slice(got);
                let filled = filled + got.len();
                self.pos += got.len();
                if filled < 4 {
                    self.phase = BatchPhase::Count { bytes, filled };
                    return rest;
                }
                let count = u32::from_le_bytes(bytes) as usize;
                if count == 0 || count > record::batch::MAX_ATOMIC_BATCH_OPERATIONS {
                    self.fail(record::invalid_data("invalid batch operation count"));
                    return &[];
                }
                if let Err(error) = (self.admit.take().expect("batch count admitted once"))(count) {
                    self.fail(error);
                    return &[];
                }
                if self.updates.try_reserve_exact(count).is_err() {
                    self.fail(io::Error::other("batch update allocation failed"));
                    return &[];
                }
                self.ops_left = count;
                self.phase = BatchPhase::Header {
                    bytes: [0; HEADER_SIZE],
                    filled: 0,
                };
                rest
            }
            BatchPhase::Header { mut bytes, filled } => {
                let (got, rest) = split_at_most(input, HEADER_SIZE - filled);
                bytes[filled..filled + got.len()].copy_from_slice(got);
                let filled = filled + got.len();
                self.pos += got.len();
                if filled < HEADER_SIZE {
                    self.phase = BatchPhase::Header { bytes, filled };
                    return rest;
                }
                self.begin_child(bytes);
                rest
            }
            BatchPhase::Key => {
                let (done, rest) = {
                    let child = self.pending.as_mut().expect("batch child key");
                    let (got, rest) = split_at_most(input, child.key_need);
                    child.key.extend_from_slice(got);
                    child.nested.update(got);
                    child.key_need -= got.len();
                    let done = child.key_need == 0;
                    self.pos += got.len();
                    (done, rest)
                };
                if done {
                    self.after_key();
                } else {
                    self.phase = BatchPhase::Key;
                }
                rest
            }
            BatchPhase::Value => {
                let (done, rest) = {
                    let child = self.pending.as_mut().expect("batch child value");
                    let (got, rest) = split_at_most(input, child.value_need);
                    child.nested.update(got);
                    child.value_hash.update_value(got);
                    child.value_need -= got.len();
                    let done = child.value_need == 0;
                    self.pos += got.len();
                    (done, rest)
                };
                if done {
                    self.phase = BatchPhase::Checksum {
                        bytes: [0; 4],
                        filled: 0,
                    };
                } else {
                    self.phase = BatchPhase::Value;
                }
                rest
            }
            BatchPhase::Checksum { mut bytes, filled } => {
                let (got, rest) = split_at_most(input, 4 - filled);
                bytes[filled..filled + got.len()].copy_from_slice(got);
                let filled = filled + got.len();
                self.pos += got.len();
                if filled < 4 {
                    self.phase = BatchPhase::Checksum { bytes, filled };
                    return rest;
                }
                self.accept(u32::from_le_bytes(bytes));
                rest
            }
        }
    }

    fn begin_child(&mut self, header_bytes: [u8; HEADER_SIZE]) {
        let parsed = match Header::parse(&header_bytes) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.fail(error);
                return;
            }
        };
        if parsed.operation == Operation::Batch {
            self.fail(record::invalid_data("nested batches are not supported"));
            return;
        }
        let value_offset = match self.pos.checked_add(parsed.key_len) {
            Some(offset) => offset,
            None => {
                self.fail(record::invalid_data("batch offset overflow"));
                return;
            }
        };
        if value_offset
            .checked_add(parsed.value_len)
            .and_then(|end| end.checked_add(record::CHECKSUM_SIZE))
            .is_none()
        {
            self.fail(record::invalid_data("batch offset overflow"));
            return;
        }
        let mut nested = crc32fast::Hasher::new();
        nested.update(&header_bytes);
        let key_need = parsed.key_len;
        let mut key = Vec::new();
        if key.try_reserve_exact(key_need).is_err() {
            self.fail(io::Error::other("batch key allocation failed"));
            return;
        }
        self.pending = Some(PendingChild {
            operation: parsed.operation,
            key,
            key_need,
            value_need: parsed.value_len,
            value_len: parsed.value_len,
            value_offset,
            nested,
            value_hash: D::default(),
        });
        if key_need == 0 {
            self.after_key();
        } else {
            self.phase = BatchPhase::Key;
        }
    }

    fn after_key(&mut self) {
        let value_need = self.pending.as_ref().expect("batch child").value_need;
        self.phase = if value_need == 0 {
            BatchPhase::Checksum {
                bytes: [0; 4],
                filled: 0,
            }
        } else {
            BatchPhase::Value
        };
    }

    fn accept(&mut self, checksum: u32) {
        let child = self.pending.take().expect("batch child checksum");
        if child.nested.finalize() != checksum {
            self.fail(record::invalid_data("record checksum mismatch"));
            return;
        }
        let location = match child.operation {
            Operation::Put => {
                let Some(offset) = self.payload_base.checked_add(child.value_offset as u64) else {
                    self.fail(record::invalid_data("batch value offset overflow"));
                    return;
                };
                Some(StreamValue {
                    offset,
                    length: child.value_len,
                    digest: child.value_hash.finish(),
                })
            }
            Operation::Delete => None,
            Operation::Batch => {
                self.fail(record::invalid_data("nested batch"));
                return;
            }
        };
        debug_assert!(self.updates.len() < record::batch::MAX_ATOMIC_BATCH_OPERATIONS);
        self.updates.push(Update {
            key: child.key,
            location,
        });
        self.ops_left -= 1;
        self.phase = if self.ops_left == 0 {
            BatchPhase::Trailing
        } else {
            BatchPhase::Header {
                bytes: [0; HEADER_SIZE],
                filled: 0,
            }
        };
    }

    fn fail(&mut self, error: io::Error) {
        if self.error.is_none() {
            self.error = Some(error);
            self.updates.clear();
            self.pending = None;
        }
        self.phase = BatchPhase::Draining;
    }
}

fn split_at_most(input: &[u8], n: usize) -> (&[u8], &[u8]) {
    let n = n.min(input.len());
    input.split_at(n)
}

/// Checksums one batch record without retaining its payload.
///
/// The outer checksum is compared first, then the outer key, then nested
/// decoding. The key is known before the value, but a key mismatch is
/// reported only after the outer checksum matches. Nested bytes are hashed
/// either way. Children are parsed only for the canonical key, so a key
/// failure cannot replace itself with a nested error. The caller publishes
/// the returned updates only after this function succeeds.
pub(crate) fn read<D: ValueHash>(
    reader: &mut RecoveryReader<'_>,
    header_bytes: &[u8; HEADER_SIZE],
    header: &Header,
    record_offset: u64,
    mut observe: impl FnMut(&[u8]),
    admit: impl FnOnce(usize) -> io::Result<()>,
) -> io::Result<Vec<Update<D::Output>>> {
    debug_assert!(header.operation == Operation::Batch);
    let mut record_hasher = crc32fast::Hasher::new();
    record_hasher.update(header_bytes);
    let mut key = [0_u8; 1];
    reader.read_exact(&mut key)?;
    observe(&key);
    record_hasher.update(&key);
    let key_ok = key == [0_u8];
    let payload_base = record_offset + HEADER_SIZE as u64 + header.key_len as u64;
    let mut scan = BatchScan::<D, _>::new(payload_base, admit);
    reader.read_with(header.value_len, |chunk| {
        observe(chunk);
        record_hasher.update(chunk);
        if key_ok {
            scan.push(chunk);
        }
    })?;
    let mut checksum = [0_u8; 4];
    reader.read_exact(&mut checksum)?;
    observe(&checksum);
    if record_hasher.finalize() != u32::from_le_bytes(checksum) {
        return Err(record::invalid_data("record checksum mismatch"));
    }
    if !key_ok {
        return Err(record::invalid_data("invalid batch record key"));
    }
    scan.finish()
}
