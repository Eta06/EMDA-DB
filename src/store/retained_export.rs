//! Non-authoritative, non-destructive retained-base export.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{Seek, SeekFrom, Write};

/// Explicit limits for full replay and the resulting file.
#[derive(Clone, Copy, Debug)]
pub struct RetainedExportLimits {
    pub max_log_bytes: u64,
    pub max_live_keys: usize,
    pub max_live_key_bytes: usize,
    pub max_output_bytes: u64,
}

impl Store {
    /// Exports an untrusted retained base. The caller supplies a nonzero fresh
    /// identifier; this operation does not establish authority or permit recovery.
    /// The caller must choose a destination outside the source directory and
    /// ensure the source and both directories remain stable and trusted against
    /// uncoordinated mutation for the entire operation. Store does not retain
    /// its source path, so it cannot enforce the destination precondition.
    pub fn export_retained_base(
        &self,
        destination: impl AsRef<Path>,
        base_id: [u8; 16],
        limits: RetainedExportLimits,
    ) -> Result<crate::retained_base_v2::Validated, crate::PublicationFailure> {
        self.export_retained_base_with_sync(
            destination.as_ref(),
            base_id,
            limits,
            crate::directory::sync_existing,
        )
    }

    fn export_retained_base_with_sync(
        &self,
        destination: &Path,
        base_id: [u8; 16],
        limits: RetainedExportLimits,
        sync: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<crate::retained_base_v2::Validated, crate::PublicationFailure> {
        self.export_retained_base_with_hook(destination, base_id, limits, sync, |_| Ok(()))
    }

    fn export_retained_base_with_hook(
        &self,
        destination: &Path,
        base_id: [u8; 16],
        limits: RetainedExportLimits,
        sync: impl FnOnce(&Path) -> io::Result<()>,
        mut hook: impl FnMut(&str) -> io::Result<()>,
    ) -> Result<crate::retained_base_v2::Validated, crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            let _writer = self
                .writes
                .write()
                .map_err(|_| io::Error::other("write barrier poisoned"))?;
            let inner = self.read()?;
            if self.commits.failed() {
                return Err(uncertain_error());
            }
            let source = self.log.id().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "retained export requires a witnessed store",
                )
            })?;
            if base_id == [0; 16] || base_id == source {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid base identifier",
                ));
            }
            let (peak, peak_bytes) = self.log.verify_index_peaks_with_limits(
                &inner.index,
                Some(crate::log::PeakVerificationLimits {
                    max_log_bytes: limits.max_log_bytes,
                    max_live_keys: limits.max_live_keys,
                    max_live_key_bytes: limits.max_live_key_bytes,
                }),
            )?;
            hook("after_replay")?;
            let boundary = self.log.len()?;
            let mut encoded = 0u64;
            let mut key_bytes = 0u64;
            for (key, location) in inner.index.iter() {
                crate::record::validate_key(key)?;
                if location.length > 16 * 1024 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "value too large",
                    ));
                }
                key_bytes = key_bytes
                    .checked_add(key.len() as u64)
                    .ok_or_else(|| io::Error::other("key bytes overflow"))?;
                encoded = encoded
                    .checked_add(8)
                    .and_then(|n| n.checked_add(key.len() as u64))
                    .and_then(|n| n.checked_add(location.length as u64))
                    .ok_or_else(|| io::Error::other("output overflow"))?;
            }
            let size = encoded
                .checked_add(144)
                .ok_or_else(|| io::Error::other("output overflow"))?;
            if size > limits.max_output_bytes || size == u64::MAX {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "output budget exceeded",
                ));
            }
            let mut header = [0u8; 112];
            header[..8].copy_from_slice(b"EMDARB02");
            header[8..12].copy_from_slice(&2u32.to_le_bytes());
            header[12..16].copy_from_slice(&112u32.to_le_bytes());
            header[16..32].copy_from_slice(&source);
            header[32..48].copy_from_slice(&base_id);
            for (offset, value) in [
                (48, boundary),
                (56, inner.index.len() as u64),
                (64, key_bytes),
                (72, peak as u64),
                (80, peak_bytes as u64),
                (88, encoded),
            ] {
                header[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            }
            let parent = backup::parent_directory(destination)?;
            let (mut stage, mut file) = crate::staging::Stage::file(parent)?;
            let mut hash = Sha256::new();
            hash.update(b"EMDA retained base v2\0");
            file.write_all(&header)?;
            hash.update(header);
            let mut buffer = [0u8; 65536];
            for (key, location) in inner.index.iter() {
                let mut lengths = [0u8; 8];
                lengths[..4].copy_from_slice(&(key.len() as u32).to_le_bytes());
                lengths[4..].copy_from_slice(&(location.length as u32).to_le_bytes());
                file.write_all(&lengths)?;
                hash.update(lengths);
                file.write_all(key)?;
                hash.update(key);
                let mut crc = crc32fast::Hasher::new();
                let mut offset = location.offset;
                let mut remaining = location.length;
                while remaining > 0 {
                    let n = remaining.min(buffer.len());
                    hook("value_read")?;
                    self.log.read_bytes(&mut buffer[..n], offset)?;
                    crc.update(&buffer[..n]);
                    hash.update(&buffer[..n]);
                    file.write_all(&buffer[..n])?;
                    offset = offset
                        .checked_add(n as u64)
                        .ok_or_else(|| io::Error::other("offset overflow"))?;
                    remaining -= n;
                }
                if crc.finalize() != location.checksum {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "stored value checksum mismatch",
                    ));
                }
            }
            let digest: [u8; 32] = hash.finalize().into();
            file.write_all(&digest)?;
            hook("output_sync")?;
            file.sync_all()?;
            file.seek(SeekFrom::Start(0))?;
            let result = crate::retained_base_v2::validate(
                &mut file,
                crate::retained_base_v2::Limits {
                    file_bytes: limits.max_output_bytes,
                    entries: limits.max_live_keys as u64,
                    key_bytes: limits.max_live_key_bytes as u64,
                    work_bytes: size + 1,
                },
            )?;
            if result.digest != digest || result.source_id != source || result.boundary != boundary
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "export verification mismatch",
                ));
            }
            // Recheck source witness after the final value read.
            hook("before_witness")?;
            self.log.verify_witness()?;
            tracker.begin_publication();
            stage.publish(destination)?;
            sync(parent)?;
            stage.publication_synchronized();
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::batch::BatchOperation;
    fn limits() -> RetainedExportLimits {
        RetainedExportLimits {
            max_log_bytes: 10_000_000,
            max_live_keys: 10,
            max_live_key_bytes: 100,
            max_output_bytes: 10_000_000,
        }
    }
    #[test]
    fn exports_batch_peak_and_chunked_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        store.put(b"aa", &vec![7; 150_000]).unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"long-key",
                    value: b"",
                },
                BatchOperation::Delete { key: b"long-key" },
            ])
            .unwrap();
        let path = dir.path().join("base");
        let result = store
            .export_retained_base(&path, [1; 16], limits())
            .unwrap();
        assert_eq!(
            (
                result.entries,
                result.key_bytes,
                result.peak_entries,
                result.peak_key_bytes
            ),
            (1, 2, 2, 10)
        );
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(&bytes[122..122 + 150_000], &vec![7; 150_000]);
    }
    #[test]
    fn rejects_budgets_conflicts_and_reports_uncertainty() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        store.put(b"key", b"value").unwrap();
        for limit in [
            RetainedExportLimits {
                max_log_bytes: 1,
                ..limits()
            },
            RetainedExportLimits {
                max_live_keys: 0,
                ..limits()
            },
            RetainedExportLimits {
                max_live_key_bytes: 2,
                ..limits()
            },
            RetainedExportLimits {
                max_output_bytes: 144,
                ..limits()
            },
        ] {
            assert!(matches!(
                store.export_retained_base(dir.path().join("out"), [1; 16], limit),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
        }
        let path = dir.path().join("out");
        std::fs::write(&path, b"existing").unwrap();
        assert!(
            store
                .export_retained_base(&path, [1; 16], limits())
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"existing");
        let path = dir.path().join("uncertain");
        assert!(matches!(
            store.export_retained_base_with_sync(&path, [1; 16], limits(), |_| Err(
                io::Error::other("sync failed")
            )),
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert!(path.exists());
    }
    #[test]
    fn rejects_corrupt_obsolete_history() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let store = Store::open(&source).unwrap();
        store.put(b"key", b"old").unwrap();
        let location = *store.read().unwrap().index.get(&b"key"[..]).unwrap();
        store.put(b"key", b"new").unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(source.join("records.log"))
            .unwrap();
        file.write_all_at(b"X", location.offset).unwrap();
        assert!(matches!(
            store.export_retained_base(dir.path().join("out"), [1; 16], limits()),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
    }
    #[test]
    fn exact_limits_ids_empty_and_max_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        let out = dir.path().join("out");
        for id in [[0; 16], store.log.id().unwrap()] {
            assert!(matches!(
                store.export_retained_base(&out, id, limits()),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
            assert!(!out.exists());
        }
        let empty = RetainedExportLimits {
            max_log_bytes: store.log.len().unwrap(),
            max_live_keys: 0,
            max_live_key_bytes: 0,
            max_output_bytes: 144,
        };
        store.export_retained_base(&out, [1; 16], empty).unwrap();
        std::fs::remove_file(&out).unwrap();
        store.put(b"k", &vec![42; 16 * 1024 * 1024]).unwrap();
        let exact = RetainedExportLimits {
            max_log_bytes: store.log.len().unwrap(),
            max_live_keys: 1,
            max_live_key_bytes: 1,
            max_output_bytes: 144 + 8 + 1 + 16 * 1024 * 1024,
        };
        for limit in [
            RetainedExportLimits {
                max_log_bytes: exact.max_log_bytes - 1,
                ..exact
            },
            RetainedExportLimits {
                max_live_keys: 0,
                ..exact
            },
            RetainedExportLimits {
                max_live_key_bytes: 0,
                ..exact
            },
            RetainedExportLimits {
                max_output_bytes: exact.max_output_bytes - 1,
                ..exact
            },
        ] {
            assert!(matches!(
                store.export_retained_base(&out, [1; 16], limit),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
            assert!(!out.exists());
        }
        store.export_retained_base(&out, [1; 16], exact).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlink_destination_is_not_replaced() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, b"keep").unwrap();
        let out = dir.path().join("out");
        symlink(&target, &out).unwrap();
        assert!(store.export_retained_base(&out, [1; 16], limits()).is_err());
        assert!(
            std::fs::symlink_metadata(&out)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read(target).unwrap(), b"keep");
    }

    #[test]
    fn changed_source_and_live_corruption_fail_without_publication() {
        use std::os::unix::fs::FileExt;
        for append in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            let store = Store::open(&source).unwrap();
            store.put(b"key", b"value").unwrap();
            let location = *store.read().unwrap().index.get(&b"key"[..]).unwrap();
            let file = OpenOptions::new()
                .write(true)
                .open(source.join("records.log"))
                .unwrap();
            file.write_all_at(
                b"X",
                if append {
                    store.log.len().unwrap()
                } else {
                    location.offset
                },
            )
            .unwrap();
            let before = std::fs::read(source.join("records.log")).unwrap();
            let out = dir.path().join("out");
            assert!(matches!(
                store.export_retained_base(&out, [1; 16], limits()),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
            assert!(!out.exists());
            assert_eq!(std::fs::read(source.join("records.log")).unwrap(), before);
        }
    }

    #[test]
    fn writer_waits_until_publication_sync_finishes() {
        use std::sync::{Arc, mpsc};
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("source")).unwrap());
        let out = dir.path().join("out");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let exporting = Arc::clone(&store);
        let export = std::thread::spawn(move || {
            exporting.export_retained_base_with_sync(&out, [1; 16], limits(), |parent| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                crate::directory::sync_existing(parent)
            })
        });
        entered_rx.recv().unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let writing = Arc::clone(&store);
        let writer = std::thread::spawn(move || {
            let result = writing.put(b"k", b"v");
            done_tx.send(result).unwrap();
        });
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        release_tx.send(()).unwrap();
        export.join().unwrap().unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        writer.join().unwrap();
    }
    #[test]
    fn failed_store_rejects_export() {
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let fault = Arc::new(Mutex::new(None));
        let store =
            super::super::fault_tests::open_versioned_fault_store(&source, Arc::clone(&fault))
                .unwrap();
        store.put(b"safe", b"value").unwrap();
        // A partial append poisons both V2 and V3 stores. V3 synchronizes via
        // its metadata witness rather than this adapter's data-sync hook.
        *fault.lock().unwrap() = Some(super::super::fault_tests::Fault::PartialAppend(1));
        assert!(store.put(b"uncertain", b"value").is_err());
        let before = std::fs::read(source.join("records.log")).unwrap();
        let out = dir.path().join("out");
        assert!(matches!(
            store.export_retained_base(&out, [1; 16], limits()),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        assert!(!out.exists());
        assert_eq!(std::fs::read(source.join("records.log")).unwrap(), before);
    }
    #[test]
    fn injected_read_and_staging_sync_failures_preserve_source() {
        use super::super::fault_tests::{Fault, open_versioned_fault_store};
        use std::sync::{Arc, Mutex};
        for point in ["replay", "value_read", "output_sync"] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            std::fs::create_dir(&source).unwrap();
            let fault = Arc::new(Mutex::new(None));
            let store = open_versioned_fault_store(&source, Arc::clone(&fault)).unwrap();
            store.put(b"key", b"value").unwrap();
            let log = std::fs::read(source.join("records.log")).unwrap();
            let witness = std::fs::read(source.join("commit.state")).ok();
            let out = dir.path().join("out");
            if point == "replay" {
                *fault.lock().unwrap() = Some(Fault::ShortRead(1));
            }
            let result = store.export_retained_base_with_hook(
                &out,
                [1; 16],
                limits(),
                crate::directory::sync_existing,
                |at| {
                    if point == "value_read" && at == point {
                        *fault.lock().unwrap() = Some(Fault::ShortRead(1));
                    }
                    if point == "output_sync" && at == point {
                        return Err(io::Error::other("injected output sync failure"));
                    }
                    Ok(())
                },
            );
            assert!(
                matches!(result, Err(crate::PublicationFailure::NotPublished(_))),
                "{point}"
            );
            assert!(!out.exists());
            assert_eq!(std::fs::read(source.join("records.log")).unwrap(), log);
            assert_eq!(std::fs::read(source.join("commit.state")).ok(), witness);
            assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        }
    }

    #[test]
    fn v2_witness_or_v3_anchor_changes_after_replay_and_staging_fail_closed() {
        for point in ["after_replay", "before_witness"] {
            for identity in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let source = dir.path().join("source");
                let store = Store::open(&source).unwrap();
                store.put(b"key", b"value").unwrap();
                let log = std::fs::read(source.join("records.log")).unwrap();
                let marker = source.join("commit.state");
                // V2 stores use this marker as the mutable witness. V3 stores
                // use it as the identity anchor, with a separate xattr witness.
                let original = std::fs::read(&marker).unwrap();
                let id = if identity {
                    [9; 16]
                } else {
                    store.log.id().unwrap()
                };
                let changed = crate::witness::encode_for_test(
                    &id,
                    if identity {
                        store.log.len().unwrap()
                    } else {
                        0
                    },
                );
                let out = dir.path().join("out");
                let result = store.export_retained_base_with_hook(
                    &out,
                    [1; 16],
                    limits(),
                    crate::directory::sync_existing,
                    |at| {
                        if at == point {
                            std::fs::write(&marker, &changed)?;
                        }
                        Ok(())
                    },
                );
                assert!(
                    matches!(result, Err(crate::PublicationFailure::NotPublished(_))),
                    "{point} {identity}"
                );
                assert!(!out.exists());
                assert_eq!(std::fs::read(source.join("records.log")).unwrap(), log);
                assert_eq!(std::fs::read(&marker).unwrap(), changed);
                std::fs::write(&marker, &original).unwrap();
                assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
            }
        }
    }

    #[test]
    fn appended_log_during_staging_is_not_published() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let store = Store::open(&source).unwrap();
        store.put(b"key", b"value").unwrap();
        let log_path = source.join("records.log");
        let original = std::fs::read(&log_path).unwrap();
        let witness = std::fs::read(source.join("commit.state")).ok();
        let out = dir.path().join("out");
        let result = store.export_retained_base_with_hook(
            &out,
            [1; 16],
            limits(),
            crate::directory::sync_existing,
            |at| {
                if at == "before_witness" {
                    OpenOptions::new()
                        .append(true)
                        .open(&log_path)?
                        .write_all(b"unexpected")?;
                }
                Ok(())
            },
        );
        assert!(matches!(
            result,
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        assert!(!out.exists());
        let mut expected = original;
        expected.extend_from_slice(b"unexpected");
        assert_eq!(std::fs::read(log_path).unwrap(), expected);
        assert_eq!(std::fs::read(source.join("commit.state")).ok(), witness);
    }
}
