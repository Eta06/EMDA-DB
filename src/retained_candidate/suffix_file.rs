//! Positional reads within the admitted physical suffix.
use super::{
    SuffixExpected, SuffixLimits, invalid, open_suffix, require_suffix_eof, suffix_capacity,
    suffix_operation_work,
};
use crate::record::batch::stream::{self, StreamValue, Update, ValueHash};
use crate::record::{self, CHECKSUM_SIZE, HEADER_SIZE, Header, Operation};
use crate::recovery::{PositionalReader, RecoveryReader};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

const IMPORT_BUFFER_BYTES: usize = 1024 * 1024;

/// A bounded detached input, bound to one opened descriptor before staging.
pub(super) struct SuffixImport {
    input: File,
    len: usize,
    digest: [u8; 32],
}

impl SuffixImport {
    pub(super) fn open(
        path: &Path,
        limits: SuffixLimits,
        expected: &SuffixExpected,
    ) -> io::Result<Self> {
        let (input, len) = open_suffix(path, limits, expected.end)?;
        Ok(Self {
            input,
            len,
            digest: expected.digest,
        })
    }

    /// The caller must discard the private output if copying or binding fails.
    pub(super) fn write(mut self, output: &mut impl Write) -> io::Result<(u64, [u8; 32])> {
        let chunk_size = self.len.min(IMPORT_BUFFER_BYTES);
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(chunk_size)
            .map_err(|_| io::Error::other("retained suffix copy allocation failed"))?;
        scratch.resize(chunk_size, 0);
        let mut hash = Sha256::new();
        let mut remaining = self.len;
        while remaining != 0 {
            let len = remaining.min(scratch.len());
            let chunk = &mut scratch[..len];
            self.input.read_exact(chunk)?;
            hash.update(&*chunk);
            output.write_all(chunk)?;
            remaining -= len;
        }
        require_suffix_eof(&mut self.input)?;
        if <[u8; 32]>::from(hash.finalize()) != self.digest {
            return Err(invalid());
        }
        Ok((self.len as u64, self.digest))
    }
}

#[derive(Clone, Copy)]
pub(super) struct SuffixFile<'a> {
    pub file: &'a File,
    pub start: u64,
    pub len: usize,
}

impl SuffixFile<'_> {
    /// Verify one physical record before visiting any of its operations.
    /// The value hash is optional for callers that only need key history.
    /// Offsets passed to the visitor are relative to the suffix start.
    pub(super) fn replay<D: ValueHash>(
        self,
        committed: Option<(usize, [u8; 32])>,
        remaining_operations: u64,
        mut visit: impl FnMut(&[u8], Option<StreamValue<D::Output>>) -> io::Result<()>,
    ) -> io::Result<Sha256> {
        self.replay_records::<D>(committed, remaining_operations, None, |record| {
            match record {
                Changes::Batch(updates) => {
                    for update in updates {
                        visit(&update.key, update.location)?;
                    }
                }
                Changes::Single(key, location) => visit(key, location)?,
            }
            Ok(None)
        })
    }

    /// Pass one verified physical record to the visitor, retaining DB02
    /// child order and ownership of the decoded keys. The visitor supplies
    /// the updated work budget for admission of the next record.
    pub(super) fn replay_records<D: ValueHash>(
        self,
        committed: Option<(usize, [u8; 32])>,
        mut remaining_operations: u64,
        mut work: Option<ReplayWork>,
        mut visit: impl FnMut(Changes<'_, D::Output>) -> io::Result<Option<ReplayWork>>,
    ) -> io::Result<Sha256> {
        let end = self
            .start
            .checked_add(self.len as u64)
            .ok_or_else(invalid)?;
        let mut reader = RecoveryReader::at(self.file, end, self.start)?;
        let mut hash = Sha256::new();
        verify_boundary(&hash, 0, committed)?;
        let mut offset = 0;
        let mut header_bytes = [0; HEADER_SIZE];
        let mut key = [0; record::MAX_KEY];
        while offset < self.len {
            let remaining = self.len - offset;
            if remaining < HEADER_SIZE {
                return Err(record::invalid_data("incomplete retained suffix header"));
            }
            reader.read_exact(&mut header_bytes)?;
            let header = Header::parse(&header_bytes)?;
            let len = header.record_len() as usize;
            if len > remaining {
                return Err(record::invalid_data("incomplete retained suffix record"));
            }
            let next = offset + len;
            if committed.is_some_and(|(boundary, _)| offset < boundary && boundary < next) {
                return Err(record::invalid_data(
                    "commit witness splits a suffix record",
                ));
            }
            hash.update(header_bytes);
            let key = &mut key[..header.key_len];
            let updates = if header.operation == Operation::Batch {
                stream::read::<D>(
                    &mut reader,
                    &header_bytes,
                    &header,
                    offset as u64,
                    |bytes| hash.update(bytes),
                    |count| admit_operations(remaining_operations, count as u64, work),
                )
                .map(Changes::Batch)
            } else {
                read_single::<D>(
                    &mut reader,
                    &mut hash,
                    &header_bytes,
                    &header,
                    key,
                    offset,
                    admit_operations(remaining_operations, 1, work),
                )
                .map(|location| Changes::Single(key, location))
            };
            // At a complete acknowledged boundary, retain the established
            // digest error precedence over parsing or admission errors.
            if reader.position() == self.start + next as u64 {
                verify_boundary(&hash, next, committed)?;
            }
            let changes = updates?;
            remaining_operations -= match &changes {
                Changes::Batch(updates) => updates.len() as u64,
                Changes::Single(..) => 1,
            };
            work = visit(changes)?;
            offset = next;
        }
        Ok(hash)
    }

    pub(super) fn copy_to(self, output: &mut impl Write) -> io::Result<()> {
        let mut reader = PositionalReader::new(self.file, self.start).take(self.len as u64);
        if io::copy(&mut reader, output)? != self.len as u64 {
            return Err(record::invalid_data(
                "retained suffix shortened during copy",
            ));
        }
        Ok(())
    }
}

pub(super) enum Changes<'a, D> {
    Single(&'a [u8], Option<StreamValue<D>>),
    Batch(Vec<Update<D>>),
}

#[derive(Clone, Copy)]
pub(super) struct ReplayWork {
    pub remaining: u64,
    pub entries: usize,
}

fn read_single<D: ValueHash>(
    reader: &mut RecoveryReader<'_>,
    hash: &mut Sha256,
    header_bytes: &[u8; HEADER_SIZE],
    header: &Header,
    key: &mut [u8],
    offset: usize,
    admission: io::Result<()>,
) -> io::Result<Option<StreamValue<D::Output>>> {
    reader.read_exact(key)?;
    hash.update(&*key);
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(header_bytes);
    checksum.update(key);
    // The record and suffix checks still cover rejected records, but only
    // an admitted put produces an indexed value location and digest.
    let mut value_hash = (admission.is_ok() && header.operation == Operation::Put).then(D::default);
    reader.read_with(header.value_len, |bytes| {
        hash.update(bytes);
        checksum.update(bytes);
        if let Some(value_hash) = &mut value_hash {
            value_hash.update_value(bytes);
        }
    })?;
    let mut stored = [0; CHECKSUM_SIZE];
    reader.read_exact(&mut stored)?;
    hash.update(stored);
    if checksum.finalize() != u32::from_le_bytes(stored) {
        return Err(record::invalid_data("record checksum mismatch"));
    }
    admission?;
    Ok(value_hash.map(|value_hash| StreamValue {
        offset: (offset + HEADER_SIZE + header.key_len) as u64,
        length: header.value_len,
        digest: value_hash.finish(),
    }))
}

fn admit_operations(remaining: u64, needed: u64, work: Option<ReplayWork>) -> io::Result<()> {
    if needed > remaining {
        return Err(suffix_capacity("suffix operation limit exceeded"));
    }
    // An overlay never shrinks during replay, even for a delete. Every
    // child costs at least one operation against the current entry count.
    if let Some(work) = work {
        let minimum = suffix_operation_work(work.entries)?;
        if minimum
            .checked_mul(needed)
            .is_none_or(|minimum| minimum > work.remaining)
        {
            return Err(suffix_capacity("suffix work limit exceeded"));
        }
    }
    Ok(())
}

fn verify_boundary(
    hash: &Sha256,
    offset: usize,
    committed: Option<(usize, [u8; 32])>,
) -> io::Result<()> {
    if let Some((boundary, expected)) = committed {
        if offset == boundary && <[u8; 32]>::from(hash.clone().finalize()) != expected {
            return Err(record::invalid_data("committed suffix digest mismatch"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retained_candidate::Expected;
    use std::os::unix::fs::FileExt;

    struct NoValueDigest;

    impl Default for NoValueDigest {
        fn default() -> Self {
            panic!("an over-budget put must not initialize a value digest");
        }
    }

    impl ValueHash for NoValueDigest {
        type Output = ();

        fn update_value(&mut self, _: &[u8]) {
            unreachable!()
        }

        fn finish(self) -> Self::Output {
            unreachable!()
        }
    }

    #[test]
    fn over_budget_single_put_checks_record_and_witness_without_hashing_for_the_index() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("suffix");
        let prefix = b"outside suffix";
        let value = vec![0x4a; 256 * 1024 + 7];
        let put = record::encode(Operation::Put, b"key", &value).unwrap();
        let end = put.len();
        let digest = Sha256::digest(&put).into();
        let mut damaged = put.clone();
        damaged[end - CHECKSUM_SIZE - 1] ^= 1;
        for (bytes, committed, kind, message) in [
            (
                &put,
                None,
                io::ErrorKind::InvalidInput,
                "suffix operation limit exceeded",
            ),
            (
                &damaged,
                None,
                io::ErrorKind::InvalidData,
                "record checksum mismatch",
            ),
            (
                &put,
                Some((end, digest)),
                io::ErrorKind::InvalidInput,
                "suffix operation limit exceeded",
            ),
            (
                &damaged,
                Some((end, digest)),
                io::ErrorKind::InvalidData,
                "committed suffix digest mismatch",
            ),
        ] {
            std::fs::write(&path, [prefix.as_slice(), bytes].concat()).unwrap();
            let file = File::open(&path).unwrap();
            let suffix = SuffixFile {
                file: &file,
                start: prefix.len() as u64,
                len: end,
            };
            let error = suffix
                .replay::<NoValueDigest>(committed, 0, |_, _| {
                    panic!("an over-budget record must not reach the index");
                })
                .unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), message);
        }

        std::fs::write(&path, [prefix.as_slice(), &put].concat()).unwrap();
        let file = File::open(&path).unwrap();
        let suffix = SuffixFile {
            file: &file,
            start: prefix.len() as u64,
            len: end,
        };
        let mut visits = 0;
        let hash = suffix
            .replay::<Sha256>(Some((end, digest)), 1, |key, location| {
                assert_eq!(key, b"key");
                let location = location.unwrap();
                assert_eq!(location.offset, (HEADER_SIZE + key.len()) as u64);
                assert_eq!(location.length, value.len());
                assert_eq!(location.digest, <[u8; 32]>::from(Sha256::digest(&value)));
                visits += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(visits, 1);
        assert_eq!(<[u8; 32]>::from(hash.finalize()), digest);
    }

    #[test]
    fn streamed_batch_checks_crc_and_budget_before_visiting_any_child() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("suffix");
        let prefix = b"outside suffix";
        let child = record::encode(Operation::Put, b"first", &[0x4a; 70 * 1024]).unwrap();
        let mut payload = 2_u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&child);
        payload.extend_from_slice(&[0; HEADER_SIZE]);
        let malformed = record::encode(Operation::Batch, &[0], &payload).unwrap();
        let mut bad_crc = malformed.clone();
        *bad_crc.last_mut().unwrap() ^= 1;
        for (bytes, operations, commitment, kind, message) in [
            (
                &malformed,
                1,
                None,
                io::ErrorKind::InvalidInput,
                "suffix operation limit exceeded",
            ),
            (
                &malformed,
                2,
                None,
                io::ErrorKind::InvalidData,
                "invalid record header",
            ),
            (
                &bad_crc,
                1,
                None,
                io::ErrorKind::InvalidData,
                "record checksum mismatch",
            ),
            (
                &bad_crc,
                1,
                Some((bad_crc.len(), Sha256::digest(&malformed).into())),
                io::ErrorKind::InvalidData,
                "committed suffix digest mismatch",
            ),
        ] {
            let mut file = File::create(&path).unwrap();
            file.write_all(prefix).unwrap();
            file.write_all(bytes).unwrap();
            drop(file);
            let file = File::open(&path).unwrap();
            let suffix = SuffixFile {
                file: &file,
                start: prefix.len() as u64,
                len: bytes.len(),
            };
            let error = suffix
                .replay::<Sha256>(commitment, operations, |_, _| {
                    panic!("the malformed or over-budget batch exposed a child");
                })
                .unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), message);
        }

        let mut payload = 2_u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&child);
        payload.extend_from_slice(&record::encode(Operation::Delete, b"first", &[]).unwrap());
        let bytes = record::encode(Operation::Batch, &[0], &payload).unwrap();
        let mut file = File::create(&path).unwrap();
        file.write_all(prefix).unwrap();
        file.write_all(&bytes).unwrap();
        drop(file);
        let file = File::open(&path).unwrap();
        let suffix = SuffixFile {
            file: &file,
            start: prefix.len() as u64,
            len: bytes.len(),
        };
        let expected = Sha256::digest(&bytes);
        let mut updates = Vec::new();
        let observed = suffix
            .replay::<Sha256>(Some((bytes.len(), expected.into())), 2, |key, loc| {
                assert_eq!(key, b"first");
                updates.push(loc.map(|loc| (loc.offset, loc.length, loc.digest)));
                Ok(())
            })
            .unwrap();
        assert_eq!(observed.finalize(), expected);
        assert_eq!(
            updates,
            [
                Some((
                    (2 * HEADER_SIZE + 1 + 4 + 5) as u64,
                    70 * 1024,
                    Sha256::digest([0x4a; 70 * 1024]).into()
                )),
                None
            ]
        );
    }

    #[test]
    fn impossible_work_skips_batch_child_hashing_and_preserves_outer_checks() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("suffix");
        let child = record::encode(Operation::Put, b"key", &[0x4a; 70 * 1024]).unwrap();
        let mut payload = 2_u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&child);
        payload.extend_from_slice(&child);
        let batch = record::encode(Operation::Batch, &[0], &payload).unwrap();
        let digest = Sha256::digest(&batch).into();
        let mut corrupt = batch.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        for (bytes, commitment, message) in [
            (
                &batch,
                Some((batch.len(), digest)),
                "suffix work limit exceeded",
            ),
            (&corrupt, None, "record checksum mismatch"),
            (
                &corrupt,
                Some((batch.len(), digest)),
                "committed suffix digest mismatch",
            ),
        ] {
            std::fs::write(&path, bytes).unwrap();
            let file = File::open(&path).unwrap();
            let error = SuffixFile {
                file: &file,
                start: 0,
                len: bytes.len(),
            }
            .replay_records::<NoValueDigest>(
                commitment,
                2,
                Some(ReplayWork {
                    remaining: 7,
                    entries: 1,
                }),
                |_| panic!("an over-budget batch must not reach the overlay"),
            )
            .unwrap_err();
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn checked_import_caps_writes_and_rejects_late_input_changes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("suffix");
        let bytes = vec![0x34; 2 * IMPORT_BUFFER_BYTES + 7];
        let expected = SuffixExpected {
            candidate: Expected {
                header: [0; 112],
                witness: [0; 32],
                base_digest: [0; 32],
            },
            end: bytes.len() as u64,
            digest: Sha256::digest(&bytes).into(),
        };
        let limits = SuffixLimits {
            bytes: expected.end,
            operations: 0,
            work: 0,
        };

        for mode in ["unchanged", "grown", "shortened", "substituted"] {
            std::fs::write(&path, &bytes).unwrap();
            let import = SuffixImport::open(&path, limits, &expected).unwrap();
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            match mode {
                "grown" => file.write_all_at(b"extra", expected.end).unwrap(),
                "shortened" => file.set_len(expected.end - 1).unwrap(),
                "substituted" => file.write_all_at(b"changed", expected.end - 7).unwrap(),
                _ => {}
            }
            let mut output = Vec::new();
            let result = import.write(&mut output);
            match mode {
                "unchanged" => {
                    assert_eq!(result.unwrap(), (expected.end, expected.digest));
                    assert_eq!(output, bytes);
                }
                "shortened" => {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
                    assert_eq!(output, bytes[..2 * IMPORT_BUFFER_BYTES]);
                }
                _ => {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
                    assert_eq!(output.len(), bytes.len());
                }
            }
        }
    }
}
