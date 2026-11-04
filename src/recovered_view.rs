//! Read-only checkpoint plus bounded, validated log-suffix replay.
use crate::checkpoint_view::CheckpointView;
use crate::log::ValueLocation;
use crate::record;
use crate::{Entry, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES};
use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::{Excluded, Unbounded};
use std::ops::ControlFlow;
use std::path::Path;

/// Maximum suffix bytes replayed by one recovered checkpoint view.
pub const MAX_RECOVERED_VIEW_TAIL_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum distinct keys recorded in one recovered checkpoint overlay.
pub const MAX_RECOVERED_VIEW_TAIL_KEYS: usize = 16_384;
/// Maximum combined key bytes recorded in one recovered checkpoint overlay.
pub const MAX_RECOVERED_VIEW_TAIL_KEY_BYTES: usize = 4 * 1024 * 1024;

/// A read-only checkpoint view with its witnessed log suffix replayed.
///
/// It keeps a bounded in-memory overlay of suffix keys, verifies every suffix
/// record, and holds a shared advisory store lock. Cooperating writers cannot
/// coexist with it. Opening rejects any bytes beyond the witnessed boundary;
/// use a writable store to recover an unacknowledged complete tail first.
/// The independently trusted checkpoint digest and full prefix-hashing costs
/// of `CheckpointView` still apply. This is not a writable disk-backed index.
pub struct RecoveredCheckpointView {
    pub(crate) base: CheckpointView,
    pub(crate) overlay: BTreeMap<Vec<u8>, Option<ValueLocation>>,
    pub(crate) tail_bytes: u64,
}

impl RecoveredCheckpointView {
    /// Opens using the public hard limits for suffix bytes, keys and key bytes.
    pub fn open(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
    ) -> io::Result<Self> {
        Self::open_with_limits(
            path,
            checkpoint,
            trusted_digest,
            MAX_RECOVERED_VIEW_TAIL_BYTES,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        )
    }

    /// Opens with caller-selected limits no higher than the public hard caps.
    /// Limits cover the raw suffix and distinct touched key bytes, including
    /// tombstones; they are not a total process-memory or recovery-time bound.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
    ) -> io::Result<Self> {
        Self::validate_limits(max_tail_bytes, max_tail_keys, max_tail_key_bytes)?;
        let base = CheckpointView::open(path, checkpoint, trusted_digest)?;
        Self::from_base(base, max_tail_bytes, max_tail_keys, max_tail_key_bytes)
    }

    pub(crate) fn validate_limits(
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
    ) -> io::Result<()> {
        if max_tail_bytes > MAX_RECOVERED_VIEW_TAIL_BYTES
            || max_tail_keys > MAX_RECOVERED_VIEW_TAIL_KEYS
            || max_tail_key_bytes > MAX_RECOVERED_VIEW_TAIL_KEY_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovered view limits exceed supported maximum",
            ));
        }
        Ok(())
    }

    pub(crate) fn from_base(
        base: CheckpointView,
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
    ) -> io::Result<Self> {
        Self::from_base_mode(
            base,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            false,
        )
    }

    // Only an exclusive writer may adopt a complete unacknowledged tail,
    // after this function validates it and before it publishes the view.
    pub(crate) fn from_base_for_writer(
        base: CheckpointView,
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
    ) -> io::Result<Self> {
        Self::from_base_mode(
            base,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            true,
        )
    }

    fn from_base_mode(
        base: CheckpointView,
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
        allow_unacknowledged: bool,
    ) -> io::Result<Self> {
        Self::validate_limits(max_tail_bytes, max_tail_keys, max_tail_key_bytes)?;
        let boundary = base.witnessed_boundary;
        let length = base.log.len()?;
        if length < boundary || (!allow_unacknowledged && length != boundary) {
            return Err(record::invalid_data(
                "log has an unacknowledged tail; open a writable store to recover it first",
            ));
        }
        let tail_bytes = length
            .checked_sub(base.log_boundary())
            .ok_or_else(|| record::invalid_data("checkpoint is beyond the witnessed boundary"))?;
        if tail_bytes > max_tail_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovered view suffix byte limit exceeded",
            ));
        }
        let mut overlay = BTreeMap::new();
        let mut key_bytes = 0_usize;
        base.log
            .visit_records(base.log_boundary(), Some(boundary), |key, location| {
                if !overlay.contains_key(&key) {
                    if overlay.len() >= max_tail_keys {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "recovered view suffix key limit exceeded",
                        ));
                    }
                    key_bytes = key_bytes
                        .checked_add(key.len())
                        .filter(|bytes| *bytes <= max_tail_key_bytes)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "recovered view suffix key byte limit exceeded",
                            )
                        })?;
                }
                overlay.insert(key, location);
                Ok(())
            })?;
        if base.log.len()? != length {
            return Err(record::invalid_data(
                "log length changed while opening recovered view",
            ));
        }
        Ok(Self {
            base,
            overlay,
            tail_bytes,
        })
    }

    /// Number of raw suffix bytes validated at open.
    pub fn tail_bytes(&self) -> u64 {
        self.tail_bytes
    }

    /// Number of checkpoint index blocks retained in the view's directory.
    pub fn checkpoint_block_count(&self) -> usize {
        self.base.block_count()
    }

    /// Number of distinct suffix keys retained, including tombstones.
    pub fn overlay_keys(&self) -> usize {
        self.overlay.len()
    }

    /// Reads one key from the recovered checkpoint and suffix.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        self.location(key)?
            .map(|location| self.base.log.read(location))
            .transpose()
    }

    pub(crate) fn location(&self, key: &[u8]) -> io::Result<Option<ValueLocation>> {
        match self.overlay.get(key) {
            Some(location) => Ok(*location),
            None => self.base.location(key),
        }
    }

    /// Scans recovered keys in bytewise order with exclusive optional bounds.
    /// Each page has the same entry and byte caps as `Store::scan`.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        for key in [after, before].into_iter().flatten() {
            record::validate_key(key)?;
        }
        if limit == 0 || limit > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scan limit must be between 1 and 1024",
            ));
        }
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        let mut bytes = 0_usize;
        self.visit_entries(after, before, |key, location| {
            if !push_entry(&self.base, &mut result, &mut bytes, limit, key, location)? {
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(result)
    }

    // Merge verified locations without loading values or materializing the
    // complete base index. Callers validate public scan bounds beforehand.
    pub(crate) fn visit_entries(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        mut visit: impl FnMut(&[u8], ValueLocation) -> io::Result<ControlFlow<()>>,
    ) -> io::Result<()> {
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut updates = self.overlay.range::<[u8], _>(bounds).peekable();
        let mut stopped = false;
        self.base
            .visit_entries(after, before, |base_key, base_location| {
                while updates
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() < base_key)
                {
                    let (key, location) = updates.next().expect("peeked overlay entry");
                    if let Some(location) = location {
                        if visit(key, *location)?.is_break() {
                            stopped = true;
                            return Ok(ControlFlow::Break(()));
                        }
                    }
                }
                let chosen = if updates
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() == base_key)
                {
                    updates
                        .next()
                        .expect("peeked overlay entry")
                        .1
                        .as_ref()
                        .copied()
                } else {
                    Some(base_location)
                };
                if let Some(location) = chosen {
                    if visit(base_key, location)?.is_break() {
                        stopped = true;
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            })?;
        if !stopped {
            for (key, location) in updates {
                if let Some(location) = location {
                    if visit(key, *location)?.is_break() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

fn push_entry(
    base: &CheckpointView,
    result: &mut Vec<Entry>,
    bytes: &mut usize,
    limit: usize,
    key: &[u8],
    location: ValueLocation,
) -> io::Result<bool> {
    if result.len() == limit || *bytes + key.len() + location.length > MAX_SCAN_BYTES {
        return Ok(false);
    }
    let value = base.log.read(location)?;
    *bytes += key.len() + location.length;
    result.push((key.to_vec(), value));
    Ok(true)
}
