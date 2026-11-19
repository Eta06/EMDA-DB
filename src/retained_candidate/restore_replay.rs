use super::suffix_file::SuffixFile;
use super::{
    Limits, ReplayBudget, SuffixExpected, SuffixLimits, apply_suffix_entry, check_binding,
    check_files, copy_replay_bytes, invalid, open_regular, open_suffix, suffix_capacity,
};
use crate::retained_base_v2::index::OverlayValue;
use crate::retained_base_v2::{CapturedValue, FRAME_BYTES, StagingLimits, scan_value_locations};
use sha2::{Digest, Sha256};
use std::{fs::File, io, path::Path};

pub(super) struct RestoreReplay {
    base: File,
    base_len: u64,
    suffix: File,
    suffix_len: u64,
    pub(super) entries: Vec<(Vec<u8>, RestoreValue)>,
}

pub(super) struct RestoreValue {
    from_suffix: bool,
    location: CapturedValue,
}

impl RestoreReplay {
    pub(super) fn open(
        candidate: &Path,
        suffix: &Path,
        limits: Limits,
        staging: StagingLimits,
        suffix_limits: SuffixLimits,
        expected: &SuffixExpected,
    ) -> io::Result<Self> {
        let (suffix, len) = open_suffix(suffix, suffix_limits, expected.end)?;
        let source = SuffixFile {
            file: &suffix,
            start: 0,
            len,
        };
        // Maintain the detached replay contract: the whole suffix must
        // match the binding before base validation or replay admission.
        let mut hash = Sha256::new();
        source.copy_to(&mut hash)?;
        if <[u8; 32]>::from(hash.finalize()) != expected.digest
            || suffix.metadata()?.len() != expected.end
        {
            return Err(invalid());
        }
        check_files(candidate, &expected.candidate)?;
        let base = open_regular(&candidate.join("base"))?;
        let mut entries = Vec::new();
        let metadata = scan_value_locations(&base, limits, staging, |key, location| {
            entries.try_reserve(1).map_err(io::Error::other)?;
            entries.push((
                copy_replay_bytes(key)?,
                RestoreValue {
                    from_suffix: false,
                    location,
                },
            ));
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
        source.replay::<Sha256>(
            Some((len, expected.digest)),
            suffix_limits.operations,
            |key, value| {
                let replacement = value.map(|value| {
                    (value.length, move || {
                        Ok(RestoreValue {
                            from_suffix: true,
                            location: CapturedValue {
                                offset: value.offset,
                                length: value.length,
                                digest: value.digest,
                            },
                        })
                    })
                });
                apply_suffix_entry(
                    &mut entries,
                    key,
                    replacement,
                    |v| v.location.length,
                    &mut budget,
                )
            },
        )?;
        let replay = Self {
            base,
            base_len: metadata.file_bytes,
            suffix,
            suffix_len: expected.end,
            entries,
        };
        replay.check_lengths()?;
        Ok(replay)
    }

    pub(super) fn check_lengths(&self) -> io::Result<()> {
        if self.base.metadata()?.len() != self.base_len
            || self.suffix.metadata()?.len() != self.suffix_len
        {
            return Err(crate::record::invalid_data(
                "retained restore input length changed",
            ));
        }
        Ok(())
    }

    pub(super) fn read(&self, value: &RestoreValue) -> io::Result<Vec<u8>> {
        OverlayValue::Disk {
            file: if value.from_suffix {
                &self.suffix
            } else {
                &self.base
            },
            offset: value.location.offset,
            len: value.location.length,
            digest: &value.location.digest,
        }
        .to_owned_bytes()
    }

    pub(super) fn verify_output(&self, log: &crate::log::Log, end: u64) -> io::Result<()> {
        let mismatch = || crate::record::invalid_data("restored state mismatch");
        let mut expected = self.entries.iter();
        log.verify_restored_records(end, |key, length, digest| {
            let (expected_key, value) = expected.next().ok_or_else(mismatch)?;
            if key != expected_key || length != value.len() || digest != value.location.digest {
                return Err(mismatch());
            }
            Ok(())
        })?;
        if expected.next().is_some() {
            return Err(mismatch());
        }
        Ok(())
    }
}

impl RestoreValue {
    pub(super) fn len(&self) -> usize {
        self.location.length
    }
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
        let expected = store
            .build_retained_candidate(&candidate, [3; 16], [4; 16], export)
            .unwrap();
        store.put(b"z", b"suffix-data").unwrap();
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
        for (file, value) in [
            (candidate.join("base"), b"base-data".as_slice()),
            (suffix.clone(), b"suffix-data".as_slice()),
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
            assert!(
                error.to_string().contains("value digest mismatch"),
                "{error}"
            );
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
    }
}
