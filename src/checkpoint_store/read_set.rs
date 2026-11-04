//! Bounded point observations and conservative scan validation.
use super::transaction::conflict;
use super::{CheckpointStore, State};
use crate::log::ValueLocation;
use crate::transaction_budget::PendingReservation;
use crate::{MAX_TRANSACTION_READ_CONSTRAINT_BYTES, MAX_TRANSACTION_READ_CONSTRAINTS};
use std::collections::BTreeMap;
use std::io;

pub(super) struct ReadSet {
    points: BTreeMap<Vec<u8>, Option<ValueLocation>>,
    key_bytes: usize,
    charged: usize,
    scan_generation: Option<u64>,
    validated_generation: u64,
    conflicted: bool,
}

impl ReadSet {
    pub(super) fn new(generation: u64) -> Self {
        Self {
            points: BTreeMap::new(),
            key_bytes: 0,
            charged: 0,
            scan_generation: None,
            validated_generation: generation,
            conflicted: false,
        }
    }

    // Revalidate earlier observations before exposing any new state. This
    // prevents a callback from seeing a fractured pair across a concurrent
    // atomic update, even when it ultimately aborts or has external effects.
    pub(super) fn check_state(&mut self, state: &State) -> io::Result<()> {
        if self.conflicted
            || self
                .scan_generation
                .is_some_and(|seen| seen != state.generation)
        {
            self.conflicted = true;
            return Err(conflict());
        }
        if self.validated_generation != state.generation {
            for (key, location) in &self.points {
                if state.view.location(key)? != *location {
                    self.conflicted = true;
                    return Err(conflict());
                }
            }
            self.validated_generation = state.generation;
        }
        Ok(())
    }

    pub(super) fn record_scan(&mut self, generation: u64) {
        self.scan_generation = Some(generation);
    }

    pub(super) fn record_point(
        &mut self,
        store: &CheckpointStore,
        key: &[u8],
        location: Option<ValueLocation>,
    ) -> io::Result<()> {
        if self.conflicted {
            return Err(conflict());
        }
        if let Some(previous) = self.points.get(key) {
            if *previous != location {
                self.conflicted = true;
                return Err(conflict());
            }
            return Ok(());
        }
        if self.points.len() >= MAX_TRANSACTION_READ_CONSTRAINTS
            || key.len() > MAX_TRANSACTION_READ_CONSTRAINT_BYTES - self.key_bytes
        {
            return Err(io::Error::other(
                "checkpoint transaction read-set limit exceeded",
            ));
        }
        // Account for retained key bytes and the fixed location value. Map and
        // allocator overhead remain outside this logical reservation.
        let charge = key.len() + std::mem::size_of::<Option<ValueLocation>>();
        let mut reservation = PendingReservation::acquire(&store.transaction_budget, charge)?;
        self.points.insert(key.to_vec(), location);
        self.key_bytes += key.len();
        self.charged += charge;
        reservation.finish();
        Ok(())
    }

    // The caller holds the state write lock, making these observations stable
    // until commit. Immutable log locations distinguish updates to present keys.
    pub(super) fn matches(&self, state: &State) -> io::Result<bool> {
        if self.conflicted
            || self
                .scan_generation
                .is_some_and(|seen| seen != state.generation)
        {
            return Ok(false);
        }
        if self.validated_generation == state.generation {
            return Ok(true);
        }
        for (key, location) in &self.points {
            if state.view.location(key)? != *location {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn clear(&mut self) -> usize {
        self.points.clear();
        std::mem::take(&mut self.charged)
    }
}
