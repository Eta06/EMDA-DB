use super::{
    IndexLimits, IndexedCandidate, Limits, SuffixExpected, SuffixLimits, allocation_point,
    charge_suffix_work, check_suffix_binding, read_suffix, visit_suffix,
};
use crate::record::{self, Operation};
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
    overlay: Vec<OverlayEntry>,
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
        let mut overlay = Vec::<OverlayEntry>::new();
        let mut keys = 0usize;
        let mut work = 0u64;
        visit_suffix(&suffix, suffix_limits, |operation, key, value| {
            charge_suffix_work(&mut work, suffix_limits.work, overlay.len())?;
            let value = (operation == Operation::Put).then_some(value);
            match overlay.binary_search_by(|entry| entry.key.as_slice().cmp(key)) {
                Ok(position) => overlay[position].value = value,
                Err(position) => {
                    keys = keys
                        .checked_add(key.len())
                        .filter(|&sum| sum <= overlay_limits.key_bytes)
                        .ok_or_else(|| invalid_overlay("suffix overlay key byte limit exceeded"))?;
                    if overlay.len() >= overlay_limits.entries {
                        return Err(invalid_overlay("suffix overlay entry limit exceeded"));
                    }
                    allocation_point()?;
                    let mut owned_key = Vec::new();
                    owned_key
                        .try_reserve_exact(key.len())
                        .map_err(|_| io::Error::other("suffix overlay allocation failed"))?;
                    owned_key.extend_from_slice(key);
                    overlay
                        .try_reserve(1)
                        .map_err(|_| io::Error::other("suffix overlay allocation failed"))?;
                    overlay.insert(
                        position,
                        OverlayEntry {
                            key: owned_key,
                            value,
                        },
                    );
                }
            }
            Ok(())
        })?;
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
        record::validate_key(key)?;
        crate::retained_base_v2::validate_value_budget(max_value_bytes)?;
        let Ok(position) = self
            .overlay
            .binary_search_by(|entry| entry.key.as_slice().cmp(key))
        else {
            return self.candidate.get(key, max_value_bytes);
        };
        let Some(value) = self.value(&self.overlay[position]) else {
            return Ok(None);
        };
        if value.len() > max_value_bytes {
            return Err(invalid_overlay("value budget exceeded"));
        }
        let mut copy = Vec::new();
        copy.try_reserve_exact(value.len())
            .map_err(|_| io::Error::other("value allocation failed"))?;
        copy.extend_from_slice(value);
        Ok(Some(copy))
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
        let start = self
            .overlay
            .partition_point(|entry| after.is_some_and(|key| entry.key.as_slice() <= key));
        let overlay = self.overlay[start..]
            .iter()
            .take_while(|entry| before.is_none_or(|key| entry.key.as_slice() < key))
            .map(|entry| (entry.key.as_slice(), self.value(entry)));
        self.candidate
            .base
            .scan_merged(after, before, limit, overlay)
    }

    pub fn candidate_metadata(&self) -> &crate::retained_base_v2::Validated {
        self.candidate.metadata()
    }

    fn value<'a>(&'a self, entry: &'a OverlayEntry) -> Option<&'a [u8]> {
        entry.value.clone().map(|range| &self.suffix[range])
    }
}

fn invalid_overlay(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
