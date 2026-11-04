//! Bounded, process-local snapshots of a checkpoint and its suffix overlay.
use super::*;
use crate::checkpoint_view::MAX_CHECKPOINT_VIEW_BLOCKS;
use std::time::Instant;

/// Maximum simultaneously active checkpoint snapshots per open store.
pub const MAX_CHECKPOINT_ACTIVE_SNAPSHOTS: usize = 16;
/// Maximum combined copied checkpoint-directory blocks across active snapshots.
pub const MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS: usize = 262_144;
/// Maximum combined copied overlay keys, including tombstones, across snapshots.
pub const MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS: usize = 65_536;
/// Maximum combined copied overlay-key bytes across active snapshots.
pub const MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
pub(super) struct SnapshotBudget {
    active: usize,
    blocks: usize,
    keys: usize,
    key_bytes: usize,
}

impl SnapshotBudget {
    fn reserve(&mut self, blocks: usize, keys: usize, key_bytes: usize) -> io::Result<()> {
        if self.active >= MAX_CHECKPOINT_ACTIVE_SNAPSHOTS
            || blocks > MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS - self.blocks
            || keys > MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS - self.keys
            || key_bytes > MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES - self.key_bytes
        {
            return Err(io::Error::other(
                "aggregate checkpoint snapshot limit exceeded",
            ));
        }
        self.active += 1;
        self.blocks += blocks;
        self.keys += keys;
        self.key_bytes += key_bytes;
        Ok(())
    }

    fn release(&mut self, blocks: usize, keys: usize, key_bytes: usize) {
        self.active -= 1;
        self.blocks -= blocks;
        self.keys -= keys;
        self.key_bytes -= key_bytes;
    }
}

struct Reservation<'a> {
    store: &'a CheckpointStore,
    blocks: usize,
    keys: usize,
    key_bytes: usize,
    active: bool,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.store
                .snapshot_budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(self.blocks, self.keys, self.key_bytes);
        }
    }
}

/// A stable process-local checkpoint-backed view borrowing its store.
///
/// It copies the checkpoint block directory and bounded suffix overlay, not
/// the complete base index or values. Its original log locations remain valid
/// because the live source log is append-only. Snapshots cannot outlive the
/// store or survive reopening and do not provide write transactions. Limits
/// cover logical copied entries, not allocator overhead or total process memory.
/// An expired idle snapshot retains its reservation until cancellation or drop.
pub struct CheckpointSnapshot<'a> {
    store: &'a CheckpointStore,
    view: Option<RecoveredCheckpointView>,
    blocks: usize,
    keys: usize,
    key_bytes: usize,
    deadline: Option<Instant>,
}

impl CheckpointSnapshot<'_> {
    fn view(&self) -> io::Result<&RecoveredCheckpointView> {
        let view = self
            .view
            .as_ref()
            .ok_or_else(|| io::Error::other("checkpoint snapshot cancelled"))?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "checkpoint snapshot expired",
            ));
        }
        Ok(view)
    }

    /// Reads one key from the captured state.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let view = self.view()?;
        let _healthy = self.store.read()?;
        view.get(key)
    }

    /// Reads up to 1,024 keys, preserving order and duplicates. The combined
    /// key and value bytes are capped at 32 MiB; errors return no partial result.
    pub fn get_many(&self, keys: &[&[u8]]) -> io::Result<Vec<Option<Vec<u8>>>> {
        if keys.len() > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many keys in bulk read",
            ));
        }
        for key in keys {
            record::validate_key(key)?;
        }
        let view = self.view()?;
        let _healthy = self.store.read()?;
        let mut size = 0_usize;
        let mut locations = Vec::with_capacity(keys.len());
        for key in keys {
            let location = view.location(key)?;
            size = size
                .checked_add(key.len() + location.map_or(0, |entry| entry.length))
                .filter(|bytes| *bytes <= MAX_SCAN_BYTES)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "bulk read exceeds 32 MiB")
                })?;
            locations.push(location);
        }
        locations
            .into_iter()
            .map(|location| {
                location
                    .map(|location| view.base.log.read(location))
                    .transpose()
            })
            .collect()
    }

    /// Reads a stable ordered page. Bounds are exclusive; continue until empty.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        let view = self.view()?;
        let _healthy = self.store.read()?;
        view.scan(after, before, limit)
    }

    /// Releases the captured view and its reservation. Repeated calls are safe.
    pub fn cancel(&mut self) {
        if let Some(view) = self.view.take() {
            drop(view);
            self.store
                .snapshot_budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(self.blocks, self.keys, self.key_bytes);
        }
    }
}

impl Drop for CheckpointSnapshot<'_> {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl CheckpointStore {
    /// Captures a stable view with the checkpoint store's hard per-view caps.
    pub fn snapshot(&self) -> io::Result<CheckpointSnapshot<'_>> {
        self.snapshot_with_limits(
            MAX_CHECKPOINT_VIEW_BLOCKS,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        )
    }

    /// Captures a snapshot with lower per-view block, overlay-key and key-byte caps.
    pub fn snapshot_with_limits(
        &self,
        max_blocks: usize,
        max_keys: usize,
        max_key_bytes: usize,
    ) -> io::Result<CheckpointSnapshot<'_>> {
        self.snapshot_internal(max_blocks, max_keys, max_key_bytes, None)
    }

    /// Captures a snapshot with an operation-boundary monotonic deadline.
    /// Reads begun after expiry fail; in-flight reads and I/O are not interrupted.
    /// Expired snapshots retain reservations until cancellation or drop.
    pub fn snapshot_until(&self, deadline: Instant) -> io::Result<CheckpointSnapshot<'_>> {
        self.snapshot_internal(
            MAX_CHECKPOINT_VIEW_BLOCKS,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            Some(deadline),
        )
    }

    fn snapshot_internal(
        &self,
        max_blocks: usize,
        max_keys: usize,
        max_key_bytes: usize,
        deadline: Option<Instant>,
    ) -> io::Result<CheckpointSnapshot<'_>> {
        self.snapshot_using(max_blocks, max_keys, max_key_bytes, deadline, |base| {
            base.duplicate_for_snapshot()
        })
    }

    fn snapshot_using(
        &self,
        max_blocks: usize,
        max_keys: usize,
        max_key_bytes: usize,
        deadline: Option<Instant>,
        duplicate: impl FnOnce(&CheckpointView) -> io::Result<CheckpointView>,
    ) -> io::Result<CheckpointSnapshot<'_>> {
        if max_blocks > MAX_CHECKPOINT_VIEW_BLOCKS
            || max_keys > MAX_RECOVERED_VIEW_TAIL_KEYS
            || max_key_bytes > MAX_RECOVERED_VIEW_TAIL_KEY_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint snapshot limits exceed supported maximum",
            ));
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "checkpoint snapshot expired",
            ));
        }
        let state = self.read()?;
        let blocks = state.view.base.block_count();
        let keys = state.view.overlay.len();
        let key_bytes = state.overlay_key_bytes;
        if blocks > max_blocks || keys > max_keys || key_bytes > max_key_bytes {
            return Err(io::Error::other(
                "checkpoint snapshot per-view limit exceeded",
            ));
        }
        self.snapshot_budget
            .lock()
            .map_err(|_| io::Error::other("checkpoint snapshot budget poisoned"))?
            .reserve(blocks, keys, key_bytes)?;
        let mut reservation = Reservation {
            store: self,
            blocks,
            keys,
            key_bytes,
            active: true,
        };
        let view = RecoveredCheckpointView {
            base: duplicate(&state.view.base)?,
            overlay: state.view.overlay.clone(),
            tail_bytes: state.view.tail_bytes,
        };
        let snapshot = CheckpointSnapshot {
            store: self,
            view: Some(view),
            blocks,
            keys,
            key_bytes,
            deadline,
        };
        reservation.active = false;
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "checkpoint snapshot expired",
            ));
        }
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    fn fixture() -> (tempfile::TempDir, CheckpointStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let checkpoint = dir.path().join("checkpoint");
        let ordinary = Store::open(&path).unwrap();
        ordinary.put(b"base", b"value").unwrap();
        let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
        drop(ordinary);
        let store =
            crate::test_support::retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest))
                .unwrap();
        store.put(b"suffix", b"value").unwrap();
        (dir, store)
    }

    #[test]
    fn snapshot_creation_error_and_panic_release_all_reservations() {
        let (_dir, store) = fixture();
        let failed = store.snapshot_using(
            MAX_CHECKPOINT_VIEW_BLOCKS,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            None,
            |_| Err(io::Error::other("injected snapshot duplication error")),
        );
        assert!(failed.is_err());
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 0);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = store.snapshot_using(
                MAX_CHECKPOINT_VIEW_BLOCKS,
                MAX_RECOVERED_VIEW_TAIL_KEYS,
                MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                None,
                |_| panic!("injected snapshot duplication panic"),
            );
        }));
        assert!(panic.is_err());
        let budget = store.snapshot_budget.lock().unwrap();
        assert_eq!(
            (budget.active, budget.blocks, budget.keys, budget.key_bytes),
            (0, 0, 0, 0)
        );
        drop(budget);
        store.snapshot().unwrap();
    }

    #[test]
    fn aggregate_block_key_and_byte_limits_reject_without_leaking_capacity() {
        let (_dir, store) = fixture();
        for dimension in 0..3 {
            {
                let mut budget = store.snapshot_budget.lock().unwrap();
                match dimension {
                    0 => budget.blocks = MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS,
                    1 => budget.keys = MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
                    _ => budget.key_bytes = MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES,
                }
            }
            assert!(store.snapshot().is_err());
            let mut budget = store.snapshot_budget.lock().unwrap();
            assert_eq!(budget.active, 0);
            *budget = SnapshotBudget::default();
        }
        store.snapshot().unwrap();
    }

    #[test]
    fn expiry_during_snapshot_creation_releases_its_reservation() {
        use std::time::Duration;
        let (_dir, store) = fixture();
        let deadline = Instant::now() + Duration::from_millis(50);
        let result = store.snapshot_using(
            MAX_CHECKPOINT_VIEW_BLOCKS,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            Some(deadline),
            |base| {
                std::thread::sleep(
                    deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
                );
                base.duplicate_for_snapshot()
            },
        );
        assert_eq!(result.err().unwrap().kind(), io::ErrorKind::TimedOut);
        let budget = store.snapshot_budget.lock().unwrap();
        assert_eq!(
            (budget.active, budget.blocks, budget.keys, budget.key_bytes),
            (0, 0, 0, 0)
        );
        drop(budget);
        store.snapshot().unwrap();
    }
}
