//! Read-only checkpoint plus bounded, validated log-suffix replay.
use crate::checkpoint_view::{CheckpointView, CheckpointViewLimits, PreparedCheckpoint};
use crate::log::{Log, ValueLocation};
use crate::record;
use crate::{Entry, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::{Excluded, Unbounded};
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;

/// Maximum suffix bytes replayed by one recovered checkpoint view.
pub const MAX_RECOVERED_VIEW_TAIL_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum distinct keys recorded in one recovered checkpoint overlay.
pub const MAX_RECOVERED_VIEW_TAIL_KEYS: usize = 16_384;
/// Maximum combined key bytes recorded in one recovered checkpoint overlay.
pub const MAX_RECOVERED_VIEW_TAIL_KEY_BYTES: usize = 4 * 1024 * 1024;

/// Per-open logical admission limits for a read-only recovered checkpoint.
/// These do not cover all allocations, elapsed time or other project files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveredViewLimits {
    /// Maximum raw bytes in the replayed suffix.
    pub max_tail_bytes: u64,
    /// Maximum distinct suffix keys, including tombstones.
    pub max_tail_keys: usize,
    /// Maximum combined bytes of suffix keys.
    pub max_tail_key_bytes: usize,
    /// Optional maximum length of the entire log, including old history.
    pub max_log_bytes: Option<u64>,
    /// Maximum checkpoint file length and retained block-directory entries.
    pub checkpoint_view: CheckpointViewLimits,
}

impl Default for RecoveredViewLimits {
    fn default() -> Self {
        Self {
            max_tail_bytes: MAX_RECOVERED_VIEW_TAIL_BYTES,
            max_tail_keys: MAX_RECOVERED_VIEW_TAIL_KEYS,
            max_tail_key_bytes: MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            max_log_bytes: None,
            checkpoint_view: CheckpointViewLimits::default(),
        }
    }
}

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
    pub(crate) overlay: Arc<BTreeMap<Arc<[u8]>, Option<ValueLocation>>>,
    pub(crate) tail_bytes: u64,
    pub(crate) tail_operations: u64,
}

pub(crate) struct ReplayedSuffix {
    overlay: BTreeMap<Arc<[u8]>, Option<ValueLocation>>,
    bytes: u64,
    operations: u64,
    pub(crate) adoption_digest: Option<[u8; 32]>,
}

impl ReplayedSuffix {
    pub(crate) fn attach(self, base: CheckpointView) -> RecoveredCheckpointView {
        RecoveredCheckpointView {
            base,
            overlay: Arc::new(self.overlay),
            tail_bytes: self.bytes,
            tail_operations: self.operations,
        }
    }
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
        Self::open_with_options(
            path,
            checkpoint,
            trusted_digest,
            RecoveredViewLimits {
                max_tail_bytes,
                max_tail_keys,
                max_tail_key_bytes,
                ..Default::default()
            },
        )
    }

    /// Opens with suffix, checkpoint and optional whole-log admission limits.
    /// The whole-log cap and witnessed length are checked before opening the
    /// checkpoint. The suffix byte cap can reject from its declared boundary
    /// before entry traversal. An admitted open verifies the checkpoint and
    /// log. Limits must be reapplied on reopen and bound logical resources.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: RecoveredViewLimits,
    ) -> io::Result<Self> {
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            limits,
            None,
            None,
        )
    }

    /// Also limits the number of suffix mutations, counting every batch child.
    /// A zero limit admits an empty suffix. Rejected suffixes can skip hashing
    /// the covered prefix. Every successful open still verifies that prefix.
    pub fn open_with_suffix_operation_limit(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: RecoveredViewLimits,
        max_tail_operations: u64,
    ) -> io::Result<Self> {
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            limits,
            Some(max_tail_operations),
            None,
        )
    }

    /// Bound the combined logical checkpoint scan, log prefix verification
    /// and witnessed suffix replay. Admission precedes traversal of the
    /// checkpoint entries. An optional suffix operation cap counts batch
    /// children separately. Metadata, framing probes, physical I/O retries
    /// and later reads are outside the scan ceiling.
    pub fn open_with_total_recovery_scan_limit(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: RecoveredViewLimits,
        max_tail_operations: Option<u64>,
        max_total_scan_bytes: u64,
    ) -> io::Result<Self> {
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            limits,
            max_tail_operations,
            Some(max_total_scan_bytes),
        )
    }

    fn open_using(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        limits: RecoveredViewLimits,
        max_tail_operations: Option<u64>,
        max_total_scan_bytes: Option<u64>,
    ) -> io::Result<Self> {
        Self::validate_limits(
            limits.max_tail_bytes,
            limits.max_tail_keys,
            limits.max_tail_key_bytes,
        )?;
        let prepared = CheckpointView::prepare_shared(
            path,
            checkpoint,
            trusted_digest,
            limits.checkpoint_view,
            limits.max_log_bytes,
            crate::checkpoint_view::TailAdmission::Witnessed {
                max_tail_bytes: limits.max_tail_bytes,
                max_total_scan_bytes,
            },
        )?;
        let suffix = Self::replay_prepared(
            &prepared,
            limits.max_tail_bytes,
            limits.max_tail_keys,
            limits.max_tail_key_bytes,
            max_tail_operations,
        )?;
        let (base, _) = prepared.verify()?;
        Ok(suffix.attach(base))
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
        max_tail_operations: Option<u64>,
    ) -> io::Result<Self> {
        let boundary = base.witnessed_boundary;
        let length = base.log.len()?;
        crate::checkpoint::check_witnessed_length(length, boundary)?;
        let suffix = Self::replay_suffix(
            &base.log,
            base.log_boundary()..length,
            boundary,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            max_tail_operations,
        )?;
        Ok(suffix.attach(base))
    }

    // Admission failures stop before hashing older log bytes. The prepared
    // checkpoint admits the range and boundary for the caller's lock mode.
    // A writer still rebinds any unwitnessed bytes before adopting them.
    pub(crate) fn replay_prepared(
        prepared: &PreparedCheckpoint,
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
        max_tail_operations: Option<u64>,
    ) -> io::Result<ReplayedSuffix> {
        let (log, range, boundary) = prepared.suffix();
        Self::replay_suffix(
            log,
            range,
            boundary,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            max_tail_operations,
        )
    }

    fn replay_suffix(
        log: &Log,
        range: std::ops::Range<u64>,
        boundary: u64,
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
        max_tail_operations: Option<u64>,
    ) -> io::Result<ReplayedSuffix> {
        Self::validate_limits(max_tail_bytes, max_tail_keys, max_tail_key_bytes)?;
        let tail_bytes =
            crate::checkpoint::checked_tail_bytes(range.end, range.start, max_tail_bytes)?;
        let mut overlay = BTreeMap::<Arc<[u8]>, Option<ValueLocation>>::new();
        let mut key_bytes = 0_usize;
        let mut tail_operations = 0_u64;
        let visit = |key: Cow<'_, [u8]>, location| {
            // Each mutation consumes bytes in the admitted log range.
            tail_operations += 1;
            if let Some(existing) = overlay.get_mut(key.as_ref()) {
                *existing = location;
                return Ok(());
            }
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
            overlay.insert(Arc::from(key.as_ref()), location);
            Ok(())
        };
        let adoption_digest = log.visit_checkpoint_suffix(
            range.start,
            range.end,
            boundary,
            max_tail_operations,
            visit,
        )?;
        if log.len()? != range.end {
            return Err(record::invalid_data(
                "log length changed while opening recovered view",
            ));
        }
        Ok(ReplayedSuffix {
            overlay,
            bytes: tail_bytes,
            operations: tail_operations,
            adoption_digest,
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
        self.get_admitted(key, |_| Ok(()))
    }

    pub(crate) fn get_admitted(
        &self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        self.location(key)?
            .map(|location| {
                admit(location.length)?;
                self.base.log.read(location)
            })
            .transpose()
    }

    pub(crate) fn location(&self, key: &[u8]) -> io::Result<Option<ValueLocation>> {
        match self.overlay.get(key) {
            Some(location) => Ok(*location),
            None => self.base.location(key),
        }
    }

    // Public callers validate all key lengths and the key count before
    // capturing the view. Preserve input order for admission and errors.
    pub(crate) fn get_many(&self, keys: &[&[u8]]) -> io::Result<Vec<Option<Vec<u8>>>> {
        let mut size = 0_usize;
        let mut locations = Vec::with_capacity(keys.len());
        let mut lookup = self.base.lookup();
        for key in keys {
            let location = match self.overlay.get(*key) {
                Some(location) => *location,
                None => lookup.location(key)?,
            };
            size = size
                .checked_add(key.len() + location.map_or(0, |entry| entry.length))
                .filter(|bytes| *bytes <= MAX_SCAN_BYTES)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "bulk read exceeds 32 MiB")
                })?;
            locations.push(location);
        }
        drop(lookup);
        self.base.log.read_many_locations(&locations)
    }

    /// Scans recovered keys in bytewise order with exclusive optional bounds.
    /// Each page has the same entry and byte caps as `Store::scan`.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        self.scan_admitted(after, before, limit, |_, _| true)
    }

    pub(crate) fn scan_admitted(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
        mut admit: impl FnMut(&[u8], usize) -> bool,
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
        let mut read_ahead = crate::log::LogReadAhead::new(&self.base.log);
        self.visit_entries_with_peek(after, before, |key, location, peek| {
            let size = key.len() + location.length;
            if result.len() == limit
                || bytes + size > MAX_SCAN_BYTES
                || !admit(key, location.length)
            {
                return Ok(ControlFlow::Break(()));
            }
            let remaining = limit - result.len() - 1;
            let value = read_ahead.read_with_peek(location, |push| {
                let mut count = 0;
                peek(&mut |next| {
                    if count >= remaining {
                        return false;
                    }
                    count += 1;
                    push(next)
                });
            })?;
            bytes += size;
            result.push((key.to_vec(), value));
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
        self.visit_entries_with_peek(after, before, |key, location, _| visit(key, location))
    }

    pub(crate) fn visit_entries_with_peek(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        mut visit: impl FnMut(
            &[u8],
            ValueLocation,
            crate::log::LocationPeek<'_>,
        ) -> io::Result<ControlFlow<()>>,
    ) -> io::Result<()> {
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut updates = self.overlay.range::<[u8], _>(bounds).peekable();
        let mut stopped = false;
        self.base.visit_entries_with_peek(
            after,
            before,
            |base_key, base_location, base_peek| {
                while updates
                    .peek()
                    .is_some_and(|(key, _)| key.as_ref() < base_key)
                {
                    let (key, location) = updates.next().expect("peeked overlay entry");
                    if let Some(location) = location {
                        let mut peek = |push: &mut dyn FnMut(ValueLocation) -> bool| {
                            for (next_key, next_loc) in updates.clone() {
                                if next_key.as_ref() >= base_key {
                                    break;
                                }
                                if let Some(next_loc) = next_loc {
                                    if !push(*next_loc) {
                                        return;
                                    }
                                }
                            }
                            if push(base_location) {
                                base_peek(push);
                            }
                        };
                        if visit(key, *location, &mut peek)?.is_break() {
                            stopped = true;
                            return Ok(ControlFlow::Break(()));
                        }
                    }
                }
                let (chosen, from_overlay) = if updates
                    .peek()
                    .is_some_and(|(key, _)| key.as_ref() == base_key)
                {
                    (
                        updates
                            .next()
                            .expect("peeked overlay entry")
                            .1
                            .as_ref()
                            .copied(),
                        true,
                    )
                } else {
                    (Some(base_location), false)
                };
                if let Some(location) = chosen {
                    let mut peek = |push: &mut dyn FnMut(ValueLocation) -> bool| {
                        if from_overlay {
                            for (_, next_loc) in updates.clone() {
                                if let Some(next_loc) = next_loc {
                                    if !push(*next_loc) {
                                        return;
                                    }
                                }
                            }
                        } else {
                            base_peek(push);
                        }
                    };
                    if visit(base_key, location, &mut peek)?.is_break() {
                        stopped = true;
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if !stopped {
            while let Some((key, location)) = updates.next() {
                if let Some(location) = location {
                    let mut peek = |push: &mut dyn FnMut(ValueLocation) -> bool| {
                        for (_, next_loc) in updates.clone() {
                            if let Some(next_loc) = next_loc {
                                if !push(*next_loc) {
                                    break;
                                }
                            }
                        }
                    };
                    if visit(key, *location, &mut peek)?.is_break() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
