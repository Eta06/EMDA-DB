use super::{
    IndexLimits, IndexedCandidate, Limits, SuffixExpected, SuffixLimits, allocation_point,
    charge_suffix_work, check_suffix_binding, read_suffix, visit_suffix,
};
use crate::record::{self, Operation};
use crate::retained_base_v2::index::IndexedBase;
use std::{io, ops::Range, path::Path};

/// Separate overlay bounds for a detached suffix. Distinct touched keys and
/// their bytes count even when their final operation is a deletion.
#[derive(Clone, Copy, Debug)]
pub struct OverlayLimits {
    pub entries: usize,
    pub key_bytes: usize,
}

/// Read-only, non-authoritative view of a retained base and its independently
/// bound detached suffix. The suffix buffer is kept privately under
/// `SuffixLimits::bytes`.
/// The overlay stores only distinct touched keys and live value offsets.
pub struct IndexedSuffix {
    candidate: IndexedCandidate,
    suffix: Vec<u8>,
    overlay: Overlay,
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
    value: Option<Range<usize>>,
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
        let suffix = read_suffix(suffix_path.as_ref(), suffix_limits, expected.end)?;
        check_suffix_binding(&suffix, suffix_limits, expected)?;
        let overlay = Overlay::build(&suffix, suffix_limits, overlay_limits, None)?;
        let candidate =
            IndexedCandidate::open(directory, limits, index_limits, &expected.candidate)?;
        Ok(Self {
            candidate,
            suffix,
            overlay,
        })
    }

    /// Return the last suffix value unless the key was untouched, in which
    /// case the base block is checked and searched. A tombstone hides the base.
    pub fn get(&self, key: &[u8], max_value_bytes: usize) -> io::Result<Option<Vec<u8>>> {
        self.overlay
            .get(&self.candidate.base, &self.suffix, key, max_value_bytes)
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
        self.overlay
            .scan(&self.candidate.base, &self.suffix, after, before, limit)
    }

    pub fn candidate_metadata(&self) -> &crate::retained_base_v2::Validated {
        self.candidate.metadata()
    }
}

impl Overlay {
    pub(super) fn build(
        suffix: &[u8],
        suffix_limits: SuffixLimits,
        overlay_limits: OverlayLimits,
        committed: Option<usize>,
    ) -> io::Result<Self> {
        let mut overlay = Self::default();
        overlay.apply(suffix, suffix_limits, overlay_limits, committed)?;
        Ok(overlay)
    }

    pub(super) fn prepare_append(
        &mut self,
        record: &[u8],
        offset: usize,
        suffix_limits: SuffixLimits,
        overlay_limits: OverlayLimits,
    ) -> io::Result<OverlayAppend> {
        let mut changes = Vec::<OverlayEntry>::new();
        let mut key_bytes = self.key_bytes;
        let mut work = self.work;
        let mut added = 0;
        let remaining = remaining_operations(suffix_limits, self.operations)?;
        let operations = visit_suffix(record, remaining, None, |operation, key, range| {
            let current = self.entries.len() + added;
            charge_suffix_work(&mut work, suffix_limits.work, current)?;
            let value =
                (operation == Operation::Put).then_some(offset + range.start..offset + range.end);
            match find(&changes, key) {
                Ok(position) => changes[position].value = value,
                Err(position) => {
                    if find(&self.entries, key).is_err() {
                        key_bytes = admit_key(overlay_limits, current, key_bytes, key)?;
                        added += 1;
                    }
                    insert(&mut changes, position, key, value)?;
                }
            }
            Ok(())
        })?;
        self.entries.try_reserve(added).map_err(allocation)?;
        Ok(OverlayAppend {
            changes,
            work,
            key_bytes,
            operations: self.operations + operations,
        })
    }

    pub(super) fn commit_append(&mut self, prepared: OverlayAppend) {
        for change in prepared.changes {
            match find(&self.entries, &change.key) {
                Ok(position) => self.entries[position].value = change.value,
                Err(position) => self.entries.insert(position, change),
            }
        }
        self.operations = prepared.operations;
        self.key_bytes = prepared.key_bytes;
        self.work = prepared.work;
    }

    fn apply(
        &mut self,
        suffix: &[u8],
        suffix_limits: SuffixLimits,
        overlay_limits: OverlayLimits,
        committed: Option<usize>,
    ) -> io::Result<()> {
        let remaining = remaining_operations(suffix_limits, self.operations)?;
        let operations = visit_suffix(suffix, remaining, committed, |operation, key, value| {
            charge_suffix_work(&mut self.work, suffix_limits.work, self.entries.len())?;
            let value = (operation == Operation::Put).then_some(value);
            match find(&self.entries, key) {
                Ok(position) => self.entries[position].value = value,
                Err(position) => {
                    self.key_bytes =
                        admit_key(overlay_limits, self.entries.len(), self.key_bytes, key)?;
                    insert(&mut self.entries, position, key, value)?;
                }
            }
            Ok(())
        })?;
        self.operations += operations;
        Ok(())
    }

    pub(super) fn get(
        &self,
        base: &IndexedBase,
        suffix: &[u8],
        key: &[u8],
        max_value_bytes: usize,
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        crate::retained_base_v2::validate_value_budget(max_value_bytes)?;
        let Ok(position) = self
            .entries
            .binary_search_by(|entry| entry.key.as_slice().cmp(key))
        else {
            return base.get(key, max_value_bytes);
        };
        let Some(value) = value_at(suffix, &self.entries[position]) else {
            return Ok(None);
        };
        if value.len() > max_value_bytes {
            return Err(invalid_overlay("value budget exceeded"));
        }
        Ok(Some(copy(value)?))
    }

    pub(super) fn scan(
        &self,
        base: &IndexedBase,
        suffix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<crate::Entry>> {
        let start = self
            .entries
            .partition_point(|entry| after.is_some_and(|key| entry.key.as_slice() <= key));
        let overlay = self.entries[start..]
            .iter()
            .take_while(|entry| before.is_none_or(|key| entry.key.as_slice() < key))
            .map(|entry| (entry.key.as_slice(), value_at(suffix, entry)));
        base.scan_merged(after, before, limit, overlay)
    }
}

pub(super) struct OverlayAppend {
    changes: Vec<OverlayEntry>,
    key_bytes: usize,
    work: u64,
    operations: u64,
}

fn remaining_operations(limits: SuffixLimits, used: u64) -> io::Result<SuffixLimits> {
    let operations = limits
        .operations
        .checked_sub(used)
        .ok_or_else(|| invalid_overlay("suffix operation limit exceeded"))?;
    Ok(SuffixLimits {
        operations,
        ..limits
    })
}

fn find(entries: &[OverlayEntry], key: &[u8]) -> Result<usize, usize> {
    entries.binary_search_by(|entry| entry.key.as_slice().cmp(key))
}

fn admit_key(
    limits: OverlayLimits,
    count: usize,
    key_bytes: usize,
    key: &[u8],
) -> io::Result<usize> {
    let key_bytes = key_bytes
        .checked_add(key.len())
        .filter(|&sum| sum <= limits.key_bytes)
        .ok_or_else(|| invalid_overlay("suffix overlay key byte limit exceeded"))?;
    if count >= limits.entries {
        return Err(invalid_overlay("suffix overlay entry limit exceeded"));
    }
    Ok(key_bytes)
}

fn insert(
    entries: &mut Vec<OverlayEntry>,
    position: usize,
    key: &[u8],
    value: Option<Range<usize>>,
) -> io::Result<()> {
    allocation_point()?;
    let key = copy(key)?;
    entries.try_reserve(1).map_err(allocation)?;
    entries.insert(position, OverlayEntry { key, value });
    Ok(())
}

fn value_at<'a>(suffix: &'a [u8], entry: &OverlayEntry) -> Option<&'a [u8]> {
    entry.value.clone().map(|range| &suffix[range])
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
