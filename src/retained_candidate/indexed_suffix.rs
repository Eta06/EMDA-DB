use super::{
    IndexLimits, IndexedCandidate, Limits, SuffixExpected, SuffixLimits, allocation_point,
    charge_suffix_work, check_suffix_binding, open_suffix, read_suffix, suffix_capacity,
    visit_suffix,
};
use crate::record::batch::stream::{StreamValue, Update};
use crate::record::{self, Operation};
use crate::retained_base_v2::index::{EntryPart, IndexedBase, OverlayValue, reserve_index_slots};
use crate::retained_base_v2::{FRAME_BYTES, encoded_entry_bytes};
use sha2::{Digest, Sha256};
use std::{borrow::Cow, cmp::Ordering, fs::File, io, ops::Range, path::Path};

use super::suffix_file::{Changes, ReplayWork, SuffixFile};

/// Separate overlay bounds for a detached suffix. Distinct touched keys and
/// their bytes count even when their final operation is a deletion.
/// The limits also cap the number of vector slots requested for that overlay.
#[derive(Clone, Copy, Debug)]
pub struct OverlayLimits {
    pub entries: usize,
    pub key_bytes: usize,
}

impl OverlayLimits {
    fn slots(self) -> usize {
        // Every admitted key is nonempty.
        self.entries.min(self.key_bytes)
    }
}

/// Read-only, non-authoritative view of a retained base and its independently
/// bound detached suffix. The overlay stores only distinct touched keys and
/// live value locations. Use `open` to capture the suffix bytes in memory or
/// `open_file_backed` to retain the verified file descriptor.
pub struct IndexedSuffix {
    candidate: IndexedCandidate,
    suffix: DetachedSuffix,
    overlay: Overlay,
}

enum DetachedSuffix {
    Buffered(Vec<u8>),
    File { file: File, len: usize },
}

impl DetachedSuffix {
    fn source(&self) -> io::Result<OverlaySource<'_>> {
        match self {
            Self::Buffered(bytes) => Ok(OverlaySource::Inline(bytes)),
            Self::File { file, len } => {
                if file.metadata()?.len() != *len as u64 {
                    return Err(record::invalid_data("detached suffix file length changed"));
                }
                Ok(OverlaySource::Disk(SuffixFile {
                    file,
                    start: 0,
                    len: *len,
                }))
            }
        }
    }
}

#[derive(Default)]
pub(super) struct Overlay {
    entries: Vec<OverlayEntry>,
    key_bytes: usize,
    work: u64,
    operations: u64,
}

struct OverlayEntry {
    key: Vec<u8>,
    value: Option<ValueLocation>,
}

struct ValueLocation {
    range: Range<usize>,
    digest: [u8; 32],
}

#[derive(Clone, Copy)]
pub(super) enum OverlaySource<'a> {
    Inline(&'a [u8]),
    Disk(SuffixFile<'a>),
}

impl IndexedSuffix {
    /// Verify both inputs before returning a merged view. Trusted, stable
    /// inputs and a separately retained binding are required. The base remains
    /// immutable for this handle's lifetime. Opening scans base bytes without
    /// loading base values. Suffix work charges overlay entry visits and moves.
    /// Base totals are governed by `limits`; the overlay has independent
    /// limits. Together these limits do not impose a total process memory cap.
    pub fn open(
        directory: impl AsRef<Path>,
        suffix_path: impl AsRef<Path>,
        limits: Limits,
        index_limits: IndexLimits,
        suffix_limits: SuffixLimits,
        overlay_limits: OverlayLimits,
        expected: &SuffixExpected,
    ) -> io::Result<Self> {
        let base = IndexedCandidate::preflight(
            directory.as_ref(),
            limits,
            index_limits,
            &expected.candidate,
        )?;
        let suffix = read_suffix(suffix_path.as_ref(), suffix_limits, expected.end)?;
        check_suffix_binding(&suffix, suffix_limits, expected)?;
        let mut overlay = Overlay::default();
        overlay.apply(&suffix, 0, suffix_limits, overlay_limits)?;
        let candidate =
            IndexedCandidate::open_preflighted(base, limits, index_limits, &expected.candidate)?;
        Ok(Self {
            candidate,
            suffix: DetachedSuffix::Buffered(suffix),
            overlay,
        })
    }

    /// Verify the complete detached suffix and base without keeping a suffix
    /// byte buffer. Replay uses fixed-size I/O buffers and retains the
    /// admitted overlay and bounded metadata for one batch. Later reads verify
    /// the bytes of returned suffix values against their captured digests.
    ///
    /// Supply an independently retained binding. The opened base and suffix
    /// files must remain immutable for the view's lifetime. Replacing the
    /// suffix path does not redirect this view. The lengths of the files,
    /// overlay entries, and replay work have separate limits. Result buffers
    /// and allocator overhead have separate memory costs.
    pub fn open_file_backed(
        directory: impl AsRef<Path>,
        suffix_path: impl AsRef<Path>,
        limits: Limits,
        index_limits: IndexLimits,
        suffix_limits: SuffixLimits,
        overlay_limits: OverlayLimits,
        expected: &SuffixExpected,
    ) -> io::Result<Self> {
        let base = IndexedCandidate::preflight(
            directory.as_ref(),
            limits,
            index_limits,
            &expected.candidate,
        )?;
        let (file, len) = open_suffix(suffix_path.as_ref(), suffix_limits, expected.end)?;
        let source = SuffixFile {
            file: &file,
            start: 0,
            len,
        };
        let mut overlay = Overlay::default();
        overlay.replay_disk(
            source,
            (len, expected.digest),
            suffix_limits,
            overlay_limits,
        )?;
        let candidate =
            IndexedCandidate::open_preflighted(base, limits, index_limits, &expected.candidate)?;
        let suffix = DetachedSuffix::File { file, len };
        suffix.source()?;
        Ok(Self {
            candidate,
            suffix,
            overlay,
        })
    }

    /// Return the last suffix value unless the key was untouched, in which
    /// case the base block is checked and searched. A tombstone hides the base.
    pub fn get(&self, key: &[u8], max_value_bytes: usize) -> io::Result<Option<Vec<u8>>> {
        self.overlay.get(
            &self.candidate.base,
            self.suffix.source()?,
            key,
            max_value_bytes,
        )
    }

    /// Merge touched suffix keys with authenticated base blocks into one
    /// ordered page. Optional bounds are exclusive and the ordinary store's
    /// entry and byte limits apply across both sources.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<crate::Entry>> {
        self.overlay.scan(
            &self.candidate.base,
            self.suffix.source()?,
            after,
            before,
            limit,
        )
    }

    pub fn candidate_metadata(&self) -> &crate::retained_base_v2::Validated {
        self.candidate.metadata()
    }
}

impl Overlay {
    pub(super) fn visit_merged(
        &self,
        base: &IndexedBase,
        suffix: OverlaySource<'_>,
        visit: impl FnMut(EntryPart<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        let changes = self
            .entries
            .iter()
            .map(|entry| (entry.key.as_slice(), value_at(suffix, entry)));
        base.visit_merged(changes, visit)
    }

    pub(super) fn live_peaks(
        &self,
        base: &IndexedBase,
        suffix: SuffixFile<'_>,
        limits: SuffixLimits,
        expected: [u8; 32],
    ) -> io::Result<LivePeaks> {
        let mut present = Vec::new();
        present
            .try_reserve_exact(self.entries.len())
            .map_err(allocation)?;
        present.resize(self.entries.len(), false);
        let initial = base.metadata();
        let mut encoded_bytes = initial.file_bytes - FRAME_BYTES;
        let keys = self.entries.iter().map(|entry| entry.key.as_slice());
        let mut touched = keys.clone().enumerate().peekable();
        base.visit_selected_block_lengths(keys, |key, value_len| {
            // Selected base blocks arrive in key order, so each suffix key
            // can be discarded as soon as the scan has passed it.
            while let Some(&(position, candidate)) = touched.peek() {
                match candidate.cmp(key) {
                    Ordering::Less => {
                        touched.next();
                    }
                    Ordering::Equal => {
                        touched.next();
                        present[position] = true;
                        encoded_bytes = encoded_bytes
                            .checked_sub(encoded_entry_bytes(key, value_len))
                            .ok_or_else(|| {
                                record::invalid_data("retained encoded size underflow")
                            })?;
                        break;
                    }
                    Ordering::Greater => break,
                }
            }
            Ok(())
        })?;
        for entry in &self.entries {
            if let Some(value) = &entry.value {
                encoded_bytes = encoded_bytes
                    .checked_add(encoded_entry_bytes(&entry.key, value.range.len()))
                    .ok_or_else(|| record::invalid_data("retained encoded size overflow"))?;
            }
        }
        let mut peaks = LivePeaks {
            encoded_bytes,
            entries: initial.entries,
            key_bytes: initial.key_bytes,
            peak_entries: initial.peak_entries,
            peak_key_bytes: initial.peak_key_bytes,
        };
        let hash = suffix.replay::<()>(None, limits.operations, |key, location| {
            let position = find(&self.entries, key)
                .map_err(|_| record::invalid_data("overlay lost a suffix key"))?;
            let exists = location.is_some();
            if exists != present[position] {
                let size = key.len() as u64;
                let delta = |number: u64, amount| {
                    if exists {
                        number.checked_add(amount)
                    } else {
                        number.checked_sub(amount)
                    }
                    .ok_or_else(|| record::invalid_data("retained live total overflow"))
                };
                peaks.entries = delta(peaks.entries, 1)?;
                peaks.key_bytes = delta(peaks.key_bytes, size)?;
                peaks.peak_entries = peaks.peak_entries.max(peaks.entries);
                peaks.peak_key_bytes = peaks.peak_key_bytes.max(peaks.key_bytes);
                present[position] = exists;
            }
            Ok(())
        })?;
        if <[u8; 32]>::from(hash.finalize()) != expected {
            return Err(record::invalid_data(
                "retained suffix changed during rebase",
            ));
        }
        Ok(peaks)
    }

    pub(super) fn prepare_append(
        &mut self,
        record: &[u8],
        offset: usize,
        suffix_limits: SuffixLimits,
        overlay_limits: OverlayLimits,
    ) -> io::Result<OverlayAppend> {
        let mut prepared = OverlayAppend::new(self);
        let remaining = remaining_operations(suffix_limits, self.operations)?;
        visit_suffix(record, remaining, |operation, key, range| {
            prepared.stage(
                &self.entries,
                Cow::Borrowed(key),
                || value_location(operation, record, offset, range),
                suffix_limits.work,
                overlay_limits,
            )
        })?;
        reserve_index_slots(&mut self.entries, prepared.added, overlay_limits.slots())?;
        Ok(prepared)
    }

    pub(super) fn commit_append(&mut self, prepared: OverlayAppend) {
        // Keep direct updates for replacements, appends and sparse inserts.
        // Merge when repeated shifts cost more than two passes.
        if prepared.total_shifts <= prepared.shift_span.saturating_mul(2) {
            for change in prepared.changes {
                match find(&self.entries, &change.key) {
                    Ok(position) => self.entries[position].value = change.value,
                    Err(position) => self.entries.insert(position, change),
                }
            }
        } else {
            self.merge_append(prepared.changes, prepared.added);
        }
        self.operations = prepared.operations;
        self.key_bytes = prepared.key_bytes;
        self.work = prepared.work;
    }

    fn merge_append(&mut self, changes: Vec<OverlayEntry>, added: usize) {
        // Preparation reserved the extra slots before the durable append.
        // Merge from the end so each existing entry moves at most once.
        let mut old = self.entries.len();
        self.entries.resize_with(old + added, || OverlayEntry {
            key: Vec::new(),
            value: None,
        });
        let mut out = self.entries.len();
        for change in changes.into_iter().rev() {
            if out == old {
                // All inserts are placed. Update earlier replacements
                // directly instead of scanning the untouched prefix.
                let position = find(&self.entries[..old], &change.key)
                    .expect("prepared overlay replacement exists");
                self.entries[position].value = change.value;
                continue;
            }
            while old != 0 && self.entries[old - 1].key > change.key {
                old -= 1;
                out -= 1;
                self.entries.swap(old, out);
            }
            out -= 1;
            if old != 0 && self.entries[old - 1].key == change.key {
                old -= 1;
            }
            self.entries[out] = change;
        }
        debug_assert_eq!(old, out);
    }

    pub(super) fn replay_disk(
        &mut self,
        suffix: SuffixFile<'_>,
        committed: (usize, [u8; 32]),
        suffix_limits: SuffixLimits,
        overlay_limits: OverlayLimits,
    ) -> io::Result<Sha256> {
        let remaining = remaining_operations(suffix_limits, self.operations)?;
        let replay_work = |overlay: &Self| {
            Some(ReplayWork {
                remaining: suffix_limits.work.saturating_sub(overlay.work),
                entries: overlay.entries.len(),
            })
        };
        suffix.replay_records::<Sha256>(
            Some(committed),
            remaining.operations,
            replay_work(self),
            |record| {
                match record {
                    Changes::Single(key, location) => {
                        self.apply_update(
                            key,
                            || location.map(stream_location),
                            suffix_limits.work,
                            overlay_limits,
                        )?;
                        self.operations += 1;
                    }
                    Changes::Batch(updates) => {
                        self.replay_batch(updates, suffix_limits.work, overlay_limits)?;
                    }
                }
                Ok(replay_work(self))
            },
        )
    }

    fn replay_batch(
        &mut self,
        updates: Vec<Update<[u8; 32]>>,
        work_limit: u64,
        overlay_limits: OverlayLimits,
    ) -> io::Result<()> {
        let mut prepared = OverlayAppend::new(self);
        let sorted_tail = updates
            .first()
            .is_some_and(|first| self.entries.last().is_none_or(|last| last.key < first.key))
            && updates.windows(2).all(|pair| pair[0].key < pair[1].key);
        if sorted_tail {
            // Each key is distinct and past the overlay. Charge the complete
            // ordered batch before reserving space, then move the owned keys.
            for (added, update) in updates.iter().enumerate() {
                let current = self.entries.len() + added;
                charge_suffix_work(&mut prepared.work, work_limit, current)?;
                prepared.key_bytes =
                    admit_key(overlay_limits, current, prepared.key_bytes, &update.key)?;
            }
            reserve_index_slots(&mut self.entries, updates.len(), overlay_limits.slots())?;
            self.operations += updates.len() as u64;
            self.entries
                .extend(updates.into_iter().map(|update| OverlayEntry {
                    key: update.key,
                    value: update.location.map(stream_location),
                }));
            self.work = prepared.work;
            self.key_bytes = prepared.key_bytes;
            return Ok(());
        }
        for update in updates {
            prepared.stage(
                &self.entries,
                Cow::Owned(update.key),
                || update.location.map(stream_location),
                work_limit,
                overlay_limits,
            )?;
        }
        reserve_index_slots(&mut self.entries, prepared.added, overlay_limits.slots())?;
        self.commit_append(prepared);
        Ok(())
    }

    fn apply(
        &mut self,
        suffix: &[u8],
        offset: usize,
        suffix_limits: SuffixLimits,
        overlay_limits: OverlayLimits,
    ) -> io::Result<()> {
        let remaining = remaining_operations(suffix_limits, self.operations)?;
        let operations = visit_suffix(suffix, remaining, |operation, key, value| {
            self.apply_update(
                key,
                || value_location(operation, suffix, offset, value),
                suffix_limits.work,
                overlay_limits,
            )
        })?;
        self.operations += operations;
        Ok(())
    }

    fn apply_update(
        &mut self,
        key: &[u8],
        value: impl FnOnce() -> Option<ValueLocation>,
        work_limit: u64,
        overlay_limits: OverlayLimits,
    ) -> io::Result<()> {
        charge_suffix_work(&mut self.work, work_limit, self.entries.len())?;
        let value = value();
        match find(&self.entries, key) {
            Ok(position) => self.entries[position].value = value,
            Err(position) => {
                self.key_bytes =
                    admit_key(overlay_limits, self.entries.len(), self.key_bytes, key)?;
                insert(
                    &mut self.entries,
                    position,
                    key,
                    value,
                    overlay_limits.slots(),
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn get(
        &self,
        base: &IndexedBase,
        suffix: OverlaySource<'_>,
        key: &[u8],
        max_value_bytes: usize,
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        crate::retained_base_v2::validate_value_budget(max_value_bytes)?;
        self.get_admitted(base, suffix, key, |len| {
            if len > max_value_bytes {
                return Err(invalid_overlay("value budget exceeded"));
            }
            Ok(())
        })
    }

    pub(super) fn get_admitted(
        &self,
        base: &IndexedBase,
        suffix: OverlaySource<'_>,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let Ok(position) = self
            .entries
            .binary_search_by(|entry| entry.key.as_slice().cmp(key))
        else {
            return base.get_admitted(key, admit);
        };
        let Some(value) = value_at(suffix, &self.entries[position]) else {
            return Ok(None);
        };
        admit(value.len())?;
        Ok(Some(value.to_owned_bytes()?))
    }

    pub(super) fn scan(
        &self,
        base: &IndexedBase,
        suffix: OverlaySource<'_>,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<crate::Entry>> {
        self.scan_with(
            base,
            suffix,
            (after, before),
            limit,
            std::iter::empty(),
            |_, _| true,
        )
    }

    pub(super) fn scan_with<'a>(
        &'a self,
        base: &'a IndexedBase,
        suffix: OverlaySource<'a>,
        (after, before): (Option<&[u8]>, Option<&[u8]>),
        limit: usize,
        staged: impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)>,
        admit: impl FnMut(&[u8], usize) -> bool,
    ) -> io::Result<Vec<crate::Entry>> {
        let start = self
            .entries
            .partition_point(|entry| after.is_some_and(|key| entry.key.as_slice() <= key));
        let overlay = self.entries[start..]
            .iter()
            .take_while(|entry| before.is_none_or(|key| entry.key.as_slice() < key))
            .map(|entry| (entry.key.as_slice(), value_at(suffix, entry)));
        let mut overlay = overlay.peekable();
        let mut staged = staged
            .map(|(key, value)| (key, value.map(OverlayValue::Inline)))
            .peekable();
        let changes = std::iter::from_fn(|| match (overlay.peek(), staged.peek()) {
            (Some((old, _)), Some((new, _))) if old < new => overlay.next(),
            (Some((old, _)), Some((new, _))) if old == new => {
                overlay.next();
                staged.next()
            }
            (_, Some(_)) => staged.next(),
            _ => overlay.next(),
        });
        base.scan_merged(after, before, limit, changes, admit)
    }
}

pub(super) struct LivePeaks {
    pub encoded_bytes: u64,
    pub entries: u64,
    pub key_bytes: u64,
    pub peak_entries: u64,
    pub peak_key_bytes: u64,
}

pub(super) struct OverlayAppend {
    changes: Vec<OverlayEntry>,
    added: usize,
    total_shifts: usize,
    shift_span: usize,
    key_bytes: usize,
    work: u64,
    operations: u64,
}

impl OverlayAppend {
    fn new(overlay: &Overlay) -> Self {
        Self {
            changes: Vec::new(),
            added: 0,
            total_shifts: 0,
            shift_span: 0,
            key_bytes: overlay.key_bytes,
            work: overlay.work,
            operations: overlay.operations,
        }
    }

    fn stage(
        &mut self,
        entries: &[OverlayEntry],
        key: Cow<'_, [u8]>,
        value: impl FnOnce() -> Option<ValueLocation>,
        work_limit: u64,
        limits: OverlayLimits,
    ) -> io::Result<()> {
        let current = entries.len() + self.added;
        charge_suffix_work(&mut self.work, work_limit, current)?;
        let value = value();
        match find(&self.changes, &key) {
            Ok(position) => self.changes[position].value = value,
            Err(position) => {
                if let Err(position) = find(entries, &key) {
                    self.key_bytes = admit_key(limits, current, self.key_bytes, &key)?;
                    self.added += 1;
                    let shifts = entries.len() - position;
                    self.total_shifts = self.total_shifts.saturating_add(shifts);
                    self.shift_span = self.shift_span.max(shifts);
                }
                allocation_point()?;
                let key = match key {
                    Cow::Borrowed(bytes) => copy(bytes)?,
                    Cow::Owned(key) => key,
                };
                reserve_index_slots(
                    &mut self.changes,
                    1,
                    limits
                        .slots()
                        .min(record::batch::MAX_ATOMIC_BATCH_OPERATIONS),
                )?;
                self.changes.insert(position, OverlayEntry { key, value });
            }
        }
        self.operations += 1;
        Ok(())
    }
}

fn remaining_operations(limits: SuffixLimits, used: u64) -> io::Result<SuffixLimits> {
    let operations = limits
        .operations
        .checked_sub(used)
        .ok_or_else(|| suffix_capacity("suffix operation limit exceeded"))?;
    Ok(SuffixLimits {
        operations,
        ..limits
    })
}

fn find(entries: &[OverlayEntry], key: &[u8]) -> Result<usize, usize> {
    entries.binary_search_by(|entry| entry.key.as_slice().cmp(key))
}

pub(super) fn admit_key(
    limits: OverlayLimits,
    count: usize,
    key_bytes: usize,
    key: &[u8],
) -> io::Result<usize> {
    let key_bytes = key_bytes
        .checked_add(key.len())
        .filter(|&sum| sum <= limits.key_bytes)
        .ok_or_else(|| suffix_capacity("suffix overlay key byte limit exceeded"))?;
    if count >= limits.entries {
        return Err(suffix_capacity("suffix overlay entry limit exceeded"));
    }
    Ok(key_bytes)
}

fn insert(
    entries: &mut Vec<OverlayEntry>,
    position: usize,
    key: &[u8],
    value: Option<ValueLocation>,
    slots: usize,
) -> io::Result<()> {
    allocation_point()?;
    let key = copy(key)?;
    reserve_index_slots(entries, 1, slots)?;
    entries.insert(position, OverlayEntry { key, value });
    Ok(())
}

fn value_location(
    operation: Operation,
    bytes: &[u8],
    offset: usize,
    range: Range<usize>,
) -> Option<ValueLocation> {
    (operation == Operation::Put).then(|| ValueLocation {
        digest: Sha256::digest(&bytes[range.clone()]).into(),
        range: offset + range.start..offset + range.end,
    })
}

fn stream_location(location: StreamValue<[u8; 32]>) -> ValueLocation {
    // The parser bounds the enclosing record and the suffix fits usize.
    let start = location.offset as usize;
    ValueLocation {
        range: start..start + location.length,
        digest: location.digest,
    }
}

fn value_at<'a>(suffix: OverlaySource<'a>, entry: &'a OverlayEntry) -> Option<OverlayValue<'a>> {
    let value = entry.value.as_ref()?;
    Some(match suffix {
        OverlaySource::Inline(bytes) => OverlayValue::Inline(&bytes[value.range.clone()]),
        OverlaySource::Disk(suffix) => OverlayValue::Disk {
            file: suffix.file,
            offset: suffix.start + value.range.start as u64,
            len: value.range.len(),
            digest: &value.digest,
        },
    })
}

fn copy(bytes: &[u8]) -> io::Result<Vec<u8>> {
    let mut result = Vec::new();
    result.try_reserve_exact(bytes.len()).map_err(allocation)?;
    result.extend_from_slice(bytes);
    Ok(result)
}

fn allocation(_: std::collections::TryReserveError) -> io::Error {
    io::Error::other("suffix overlay allocation failed")
}

fn invalid_overlay(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
#[test]
fn retained_overlay_reservations_stay_within_entry_and_key_limits() {
    use crate::record::batch::{BatchOperation, encode_payload};
    use std::io::Write;

    let batch = |keys: &[&[u8]]| {
        let operations: Vec<_> = keys
            .iter()
            .map(|key| BatchOperation::Put {
                key,
                value: b"value",
            })
            .collect();
        record::encode(
            Operation::Batch,
            &[0],
            &encode_payload(&operations).unwrap(),
        )
        .unwrap()
    };
    let first = batch(&[b"b", b"c", b"d"]);
    let second = batch(&[b"e", b"a", b"d"]);
    let extra = record::encode(Operation::Put, b"f", b"value").unwrap();
    let suffix = [&first[..], &second].concat();
    let expected: [u8; 32] = Sha256::digest(&suffix).into();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&suffix).unwrap();
    let work = SuffixLimits {
        bytes: suffix.len() as u64,
        operations: 7,
        work: 128,
    };
    for limits in [
        OverlayLimits {
            entries: 5,
            key_bytes: 12,
        },
        OverlayLimits {
            entries: 12,
            key_bytes: 5,
        },
    ] {
        let mut inline = Overlay::default();
        inline.apply(&suffix, 0, work, limits).unwrap();

        let mut live = Overlay::default();
        for (record, position) in [(&first, 0), (&second, first.len())] {
            let prepared = live.prepare_append(record, position, work, limits).unwrap();
            assert!(prepared.changes.capacity() <= 5);
            assert!(live.entries.capacity() <= 5);
            live.commit_append(prepared);
        }
        let error = live
            .prepare_append(&extra, suffix.len(), work, limits)
            .err()
            .expect("an extra overlay key exceeds the configured limit");
        assert!(error.to_string().starts_with("suffix overlay"));

        let mut reopened = Overlay::default();
        let source = SuffixFile {
            file: &file,
            start: 0,
            len: suffix.len(),
        };
        reopened
            .replay_disk(source, (suffix.len(), expected), work, limits)
            .unwrap();
        for overlay in [inline, live, reopened] {
            assert!(overlay.entries.capacity() <= 5);
            let keys: Vec<_> = overlay.entries.iter().map(|entry| entry.key[0]).collect();
            assert_eq!(keys, b"abcde");
        }
    }
}
