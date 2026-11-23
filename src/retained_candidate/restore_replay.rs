use super::suffix_file::SuffixFile;
use super::{
    DetachedReplayLimits, Expected, Limits, ReplayBudget, SuffixExpected, SuffixLimits,
    charge_suffix_work, check_binding, copy_replay_bytes, invalid, open_suffix,
    preflight_replay_base, suffix_capacity,
};
use crate::retained_base_v2::index::OverlayValue;
use crate::retained_base_v2::{
    CapturedValue, FRAME_BYTES, StagingLimits, scan_value_lengths, scan_value_locations,
};
use crate::store::{GroupedCopy, RestoreSize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Seek},
    path::Path,
};

pub(crate) struct RestoreReplay {
    base: File,
    base_len: u64,
    suffix: File,
    suffix_len: u64,
    expected: Expected,
    limits: Limits,
    staging: StagingLimits,
    changes: Vec<(Vec<u8>, Option<CapturedValue>)>,
    pub(super) size: RestoreSize,
}

enum ReplayValue {
    BaseLength(usize),
    SuffixLocation(CapturedValue),
}

impl ReplayValue {
    fn len(&self) -> usize {
        match self {
            Self::BaseLength(len) => *len,
            Self::SuffixLocation(value) => value.length,
        }
    }
}

impl RestoreReplay {
    pub(crate) fn open(
        candidate: &Path,
        suffix: &Path,
        limits: Limits,
        staging: StagingLimits,
        suffix_limits: SuffixLimits,
        expected: &SuffixExpected,
    ) -> io::Result<Self> {
        Self::open_with_limits(
            candidate,
            suffix,
            DetachedReplayLimits::from_existing(limits, staging, suffix_limits),
            expected,
        )
    }

    pub(super) fn open_with_limits(
        candidate: &Path,
        suffix: &Path,
        limits: DetachedReplayLimits,
        expected: &SuffixExpected,
    ) -> io::Result<Self> {
        let DetachedReplayLimits {
            base: limits,
            staging,
            suffix: suffix_limits,
            touched,
        } = limits;
        let (suffix, len) = open_suffix(suffix, suffix_limits, expected.end)?;
        let (base, entries) =
            preflight_replay_base(candidate, limits, staging, suffix_limits, expected)?;
        let source = SuffixFile {
            file: &suffix,
            start: 0,
            len,
        };
        let boundary = Some((len, expected.digest));
        let mut minimum_work = entries;
        let mut minimum_live = usize::try_from(entries).map_err(|_| invalid())?;
        let mut touched_keys = HashMap::new();
        let mut touched_key_bytes = 0;
        // Keep discovered keys private until replay authenticates the suffix.
        let discovery = source.replay::<()>(boundary, suffix_limits.operations, |key, value| {
            // Puts cannot shrink the live set. Each delete can remove at
            // most one entry. Admission still uses exact membership below.
            charge_suffix_work(&mut minimum_work, suffix_limits.work, minimum_live)?;
            if value.is_none() {
                minimum_live = minimum_live.saturating_sub(1);
            }
            if !touched_keys.contains_key(key) {
                touched_key_bytes = super::indexed_suffix::admit_key(
                    touched,
                    touched_keys.len(),
                    touched_key_bytes,
                    key,
                )?;
                touched_keys.try_reserve(1).map_err(io::Error::other)?;
                touched_keys.insert(copy_replay_bytes(key)?, None);
            }
            Ok(())
        });
        if let Err(error) = discovery {
            drop(touched_keys);
            // A parser or budget error can end discovery before the final
            // boundary. Check the raw binding before reporting that error.
            let mut hash = Sha256::new();
            source.copy_to(&mut hash)?;
            if <[u8; 32]>::from(hash.finalize()) != expected.digest
                || suffix.metadata()?.len() != expected.end
            {
                return Err(invalid());
            }
            return Err(error);
        }
        // Successful discovery already checked the digest at the exact end.
        if suffix.metadata()?.len() != expected.end {
            return Err(invalid());
        }
        let metadata = scan_value_lengths(&base, limits, staging, |key, length| {
            if let Some(value) = touched_keys.get_mut(key) {
                *value = Some(ReplayValue::BaseLength(length));
            }
            Ok(())
        })?;
        check_binding(&metadata, &expected.candidate)?;
        if metadata.entries > suffix_limits.work {
            return Err(suffix_capacity("suffix work limit exceeded"));
        }
        let mut budget = ReplayBudget {
            limits,
            staging,
            work: metadata.entries,
            work_limit: suffix_limits.work,
            entries: metadata.entries,
            keys: metadata.key_bytes,
            // Every validated entry has two four-byte lengths.
            payload: metadata.file_bytes - FRAME_BYTES - 8 * metadata.entries,
        };
        source.replay::<Sha256>(boundary, suffix_limits.operations, |key, value| {
            // Preserve admission under the published work formula, even
            // though only suffix-touched keys are stored during replay.
            let live = usize::try_from(budget.entries).map_err(|_| invalid())?;
            charge_suffix_work(&mut budget.work, budget.work_limit, live)?;
            let current = touched_keys.get_mut(key).ok_or_else(invalid)?;
            let next = budget.changed_live_set(
                key,
                current.as_ref().map(ReplayValue::len),
                value.as_ref().map(|value| value.length),
            )?;
            *current = value.map(|value| {
                ReplayValue::SuffixLocation(CapturedValue {
                    offset: value.offset,
                    length: value.length,
                    digest: value.digest,
                })
            });
            (budget.entries, budget.keys, budget.payload) = next;
            Ok(())
        })?;
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(touched_keys.len())
            .map_err(io::Error::other)?;
        for (key, value) in touched_keys {
            let location = match value {
                None => None,
                Some(ReplayValue::SuffixLocation(location)) => Some(location),
                Some(ReplayValue::BaseLength(_)) => return Err(invalid()),
            };
            changes.push((key, location));
        }
        changes.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let replay = Self {
            base,
            base_len: metadata.file_bytes,
            suffix,
            suffix_len: expected.end,
            expected: expected.candidate.clone(),
            limits,
            staging,
            changes,
            size: RestoreSize {
                entries: budget.entries,
                keys: budget.keys,
                payload: budget.payload,
            },
        };
        replay.check_lengths()?;
        Ok(replay)
    }

    pub(crate) fn check_lengths(&self) -> io::Result<()> {
        if self.base.metadata()?.len() != self.base_len
            || self.suffix.metadata()?.len() != self.suffix_len
        {
            return Err(crate::record::invalid_data(
                "retained restore input length changed",
            ));
        }
        Ok(())
    }

    pub(super) fn into_entries(self) -> io::Result<Vec<crate::Entry>> {
        let count = usize::try_from(self.size.entries).map_err(|_| invalid())?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(count).map_err(io::Error::other)?;
        self.visit_rows(|file, key, value| {
            if rows.len() == count {
                return Err(invalid());
            }
            let bytes = OverlayValue::Disk {
                file,
                offset: value.offset,
                len: value.length,
                digest: &value.digest,
            }
            .to_owned_bytes()?;
            rows.push((copy_replay_bytes(key)?, bytes));
            Ok(())
        })?;
        if rows.len() != count {
            return Err(invalid());
        }
        self.check_lengths()?;
        Ok(rows)
    }

    pub(super) fn write(
        &self,
        copy: &mut GroupedCopy<'_>,
        before_value: impl Fn() -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        let mut hash = Sha256::new();
        self.visit_rows(|file, key, value| {
            copy.put_retained(
                key,
                OverlayValue::Disk {
                    file,
                    offset: value.offset,
                    len: value.length,
                    digest: &value.digest,
                },
                &before_value,
            )?;
            hash_row(&mut hash, key, value.length, value.digest);
            Ok(())
        })?;
        Ok(hash.finalize().into())
    }

    /// Visit sorted live keys and their value locations. Callers must keep
    /// results private until the complete base binding has been checked.
    pub(crate) fn visit_rows(
        &self,
        mut emit: impl FnMut(&File, &[u8], &CapturedValue) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut changes = self.changes.iter().peekable();
        let mut base_reader = &self.base;
        base_reader.rewind()?;
        let metadata =
            scan_value_locations(base_reader, self.limits, self.staging, |key, value| {
                while let Some((other, replacement)) =
                    changes.next_if(|(other, _)| other.as_slice() <= key)
                {
                    if let Some(replacement) = replacement {
                        emit(&self.suffix, other, replacement)?;
                    }
                    if other.as_slice() == key {
                        return Ok(());
                    }
                }
                emit(&self.base, key, &value)
            })?;
        check_binding(&metadata, &self.expected)?;
        for (key, value) in changes {
            if let Some(value) = value {
                emit(&self.suffix, key, value)?;
            }
        }
        Ok(())
    }

    pub(super) fn verify_output(
        log: &crate::log::Log,
        end: u64,
        expected: [u8; 32],
    ) -> io::Result<()> {
        let mut hash = Sha256::new();
        log.verify_restored_records(end, |key, length, digest| {
            hash_row(&mut hash, key, length, digest);
            Ok(())
        })?;
        if <[u8; 32]>::from(hash.finalize()) != expected {
            return Err(crate::record::invalid_data("restored state mismatch"));
        }
        Ok(())
    }
}

fn hash_row(hash: &mut Sha256, key: &[u8], value_len: usize, value: [u8; 32]) {
    hash.update((key.len() as u64).to_le_bytes());
    hash.update(key);
    hash.update((value_len as u64).to_le_bytes());
    hash.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PublicationFailure, RetainedExportLimits, Store};
    use std::fs;

    #[test]
    fn restore_rejects_changed_input_and_staged_output() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        let candidate = root.path().join("candidate");
        let suffix = root.path().join("suffix");
        let output = root.path().join("restored");
        let export = RetainedExportLimits {
            max_log_bytes: 4096,
            max_live_keys: 16,
            max_live_key_bytes: 64,
            max_output_bytes: 4096,
        };
        let limits = Limits {
            file_bytes: 4096,
            entries: 16,
            key_bytes: 64,
            work_bytes: 4097,
        };
        let staging = StagingLimits {
            value_bytes: 64,
            staging_bytes: 4096,
        };
        let suffix_limits = SuffixLimits {
            bytes: 4096,
            operations: 2,
            work: 64,
        };
        store.put(b"a", b"base-data").unwrap();
        store.put(b"m", b"obsolete").unwrap();
        let expected = store
            .build_retained_candidate(&candidate, [3; 16], [4; 16], export)
            .unwrap();
        store.put(b"z", b"suffix-data").unwrap();
        store.delete(b"m").unwrap();
        let binding = store
            .export_retained_suffix(
                &candidate,
                &expected,
                &suffix,
                limits,
                staging,
                suffix_limits,
                export,
            )
            .unwrap();
        let admit = |work| {
            RestoreReplay::open(
                &candidate,
                &suffix,
                limits,
                staging,
                SuffixLimits {
                    work,
                    ..suffix_limits
                },
                &binding,
            )
        };
        let base_path = candidate.join("base");
        let base_bytes = fs::read(&base_path).unwrap();
        let mut damaged = base_bytes.clone();
        let body = damaged
            .windows(b"base-data".len())
            .position(|value| value == b"base-data")
            .unwrap();
        damaged[body] ^= 1;
        fs::write(&base_path, damaged).unwrap();
        // The two declared live entries and two operations require at
        // least 16 units. An adequate lower bound still authenticates the body.
        assert_eq!(
            admit(15).err().unwrap().to_string(),
            "suffix work limit exceeded"
        );
        assert_eq!(admit(16).err().unwrap().to_string(), "digest mismatch");
        fs::write(&base_path, base_bytes).unwrap();
        assert_eq!(
            admit(18).err().unwrap().to_string(),
            "suffix work limit exceeded"
        );
        let verified = admit(19).unwrap();
        let suffix_bytes = fs::read(&suffix).unwrap();
        let value_offset = suffix_bytes
            .windows(b"suffix-data".len())
            .position(|bytes| bytes == b"suffix-data")
            .unwrap();
        let mut changed_value = suffix_bytes.clone();
        changed_value[value_offset] ^= 1;
        fs::write(&suffix, changed_value).unwrap();
        let error = verified.into_entries().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("value digest mismatch"),
            "{error}"
        );
        fs::write(&suffix, &suffix_bytes).unwrap();
        assert_eq!(
            admit(19).unwrap().into_entries().unwrap(),
            [
                (b"a".to_vec(), b"base-data".to_vec()),
                (b"z".to_vec(), b"suffix-data".to_vec()),
            ]
        );
        let no_operations = || {
            RestoreReplay::open(
                &candidate,
                &suffix,
                limits,
                staging,
                SuffixLimits {
                    operations: 0,
                    ..suffix_limits
                },
                &binding,
            )
        };
        assert_eq!(
            no_operations().err().unwrap().to_string(),
            "suffix operation limit exceeded"
        );
        // The operation cap rejects the first record. Damage to the later
        // record must still be reported as a binding failure.
        let mut damaged_later = suffix_bytes.clone();
        *damaged_later.last_mut().unwrap() ^= 1;
        fs::write(&suffix, damaged_later).unwrap();
        let error = no_operations().err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "invalid candidate");
        let mut damaged = suffix_bytes.clone();
        damaged[0] ^= 1;
        fs::write(&suffix, damaged).unwrap();
        assert_eq!(admit(15).err().unwrap().to_string(), "invalid candidate");
        fs::write(&suffix, suffix_bytes).unwrap();
        let delete_first = root.path().join("delete-first");
        let ordered = [
            crate::record::encode(crate::record::Operation::Delete, b"m", b"").unwrap(),
            crate::record::encode(crate::record::Operation::Put, b"z", b"suffix-data").unwrap(),
        ]
        .concat();
        fs::write(&delete_first, &ordered).unwrap();
        let delete_binding = SuffixExpected {
            candidate: binding.candidate.clone(),
            end: ordered.len() as u64,
            digest: Sha256::digest(&ordered).into(),
        };
        RestoreReplay::open(
            &candidate,
            &delete_first,
            limits,
            staging,
            SuffixLimits {
                work: 13,
                ..suffix_limits
            },
            &delete_binding,
        )
        .unwrap();
        let names = || {
            fs::read_dir(root.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect::<std::collections::BTreeSet<_>>()
        };
        let before = names();
        let restore = |hook: &dyn Fn(&str) -> io::Result<()>| {
            super::super::restore_with_hooks(
                &candidate,
                &suffix,
                &output,
                limits,
                staging,
                suffix_limits,
                &binding,
                super::super::RestoreLimits {
                    log_bytes: 4096,
                    live_keys: 16,
                    live_key_bytes: 64,
                },
                crate::directory::sync_existing,
                hook,
            )
        };
        for (file, value, message) in [
            (
                candidate.join("base"),
                b"base-data".as_slice(),
                "digest mismatch",
            ),
            (
                candidate.join("base"),
                b"obsolete".as_slice(),
                "digest mismatch",
            ),
            (
                suffix.clone(),
                b"suffix-data".as_slice(),
                "retained suffix value digest mismatch",
            ),
        ] {
            let original = fs::read(&file).unwrap();
            let mut changed = original.clone();
            let offset = changed
                .windows(value.len())
                .position(|bytes| bytes == value)
                .unwrap();
            changed[offset] ^= 1;
            let result = restore(&|point| {
                if point == "init" {
                    fs::write(&file, &changed)?;
                }
                Ok(())
            });
            let Err(PublicationFailure::NotPublished(error)) = result else {
                panic!("restore published from changed input: {result:?}");
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(error.to_string() == message, "{error}");
            assert_eq!(names(), before);
            assert_eq!(fs::read(&file).unwrap(), changed);
            fs::write(&file, original).unwrap();
        }
        for (fault, message) in [
            ("checksum", "record checksum mismatch"),
            ("value", "restored state mismatch"),
            ("key", "restored state mismatch"),
            ("length", "restored log length mismatch"),
        ] {
            let result = restore(&|point| {
                if point != "check" {
                    return Ok(());
                }
                let stage = fs::read_dir(root.path())?
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.is_dir()
                            && path
                                .file_name()
                                .unwrap()
                                .as_encoded_bytes()
                                .starts_with(b".emda-stage-")
                    })
                    .expect("private restore stage");
                let file = stage.join("records.log");
                let mut bytes = fs::read(&file)?;
                let offset = crate::witness::LOG_HEADER_SIZE;
                match fault {
                    "checksum" => bytes[offset + crate::record::HEADER_SIZE + 1] ^= 1,
                    "value" | "key" => {
                        let (key, value): (&[u8], &[u8]) = if fault == "value" {
                            (b"a", b"BASE-data")
                        } else {
                            (b"b", b"base-data")
                        };
                        let record =
                            crate::record::encode(crate::record::Operation::Put, key, value)?;
                        bytes[offset..offset + record.len()].copy_from_slice(&record);
                    }
                    "length" => bytes.push(0),
                    _ => unreachable!(),
                }
                fs::write(file, bytes)
            });
            let Err(PublicationFailure::NotPublished(error)) = result else {
                panic!("restore published changed output: {result:?}");
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(error.to_string().contains(message), "{fault}: {error}");
            assert_eq!(names(), before);
        }
        restore(&|_| Ok(())).unwrap();
        let restored = Store::open(output).unwrap();
        assert_eq!(
            restored.get(b"a").unwrap().as_deref(),
            Some(&b"base-data"[..])
        );
        assert_eq!(
            restored.get(b"z").unwrap().as_deref(),
            Some(&b"suffix-data"[..])
        );
        assert!(restored.get(b"m").unwrap().is_none());
    }
}
