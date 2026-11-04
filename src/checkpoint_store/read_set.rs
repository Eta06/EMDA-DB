//! Bounded point and complete-range observations over checkpoint-backed data.
use super::transaction::conflict;
use super::{CheckpointStore, State};
use crate::log::ValueLocation;
use crate::transaction_budget::PendingReservation;
use crate::{
    MAX_TRANSACTION_READ_CONSTRAINT_BYTES, MAX_TRANSACTION_READ_CONSTRAINTS,
    MAX_TRANSACTION_VALIDATION_ITEMS,
};
use std::collections::BTreeMap;
use std::io;
use std::ops::ControlFlow;

struct Range {
    after: Option<Vec<u8>>,
    before: Option<Vec<u8>>,
    entries: Vec<(Vec<u8>, ValueLocation)>,
}

pub(super) struct ReadSet {
    points: BTreeMap<Vec<u8>, Option<ValueLocation>>,
    ranges: Vec<Range>,
    key_bytes: usize,
    items: usize,
    charged: usize,
    validated_generation: u64,
    conflicted: bool,
}

impl ReadSet {
    pub(super) fn new(generation: u64) -> Self {
        Self {
            points: BTreeMap::new(),
            ranges: Vec::new(),
            key_bytes: 0,
            items: 0,
            charged: 0,
            validated_generation: generation,
            conflicted: false,
        }
    }

    // Before exposing another observation after a write, ensure every prior
    // point and complete requested range still matches this locked state.
    pub(super) fn check_state(&mut self, state: &State) -> io::Result<()> {
        if self.conflicted {
            return Err(conflict());
        }
        if self.validated_generation != state.generation {
            if !self.matches(state)? {
                self.conflicted = true;
                return Err(conflict());
            }
            self.validated_generation = state.generation;
        }
        Ok(())
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
        if self.points.len() + self.ranges.len() >= MAX_TRANSACTION_READ_CONSTRAINTS
            || key.len() > MAX_TRANSACTION_READ_CONSTRAINT_BYTES - self.key_bytes
            || self.items >= MAX_TRANSACTION_VALIDATION_ITEMS
        {
            return Err(io::Error::other(
                "checkpoint transaction read-set limit exceeded",
            ));
        }
        let charge = key.len() + std::mem::size_of::<Option<ValueLocation>>();
        let mut reservation = PendingReservation::acquire(&store.transaction_budget, charge)?;
        self.points.insert(key.to_vec(), location);
        self.key_bytes += key.len();
        self.items += 1;
        self.charged += charge;
        reservation.finish();
        Ok(())
    }

    pub(super) fn record_scan(
        &mut self,
        store: &CheckpointStore,
        state: &State,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
    ) -> io::Result<()> {
        if self.conflicted {
            return Err(conflict());
        }
        if self
            .ranges
            .iter()
            .any(|range| range.after.as_deref() == after && range.before.as_deref() == before)
        {
            return Ok(());
        }
        let bounds_bytes = after.map_or(0, <[u8]>::len) + before.map_or(0, <[u8]>::len);
        if self.points.len() + self.ranges.len() >= MAX_TRANSACTION_READ_CONSTRAINTS
            || bounds_bytes > MAX_TRANSACTION_READ_CONSTRAINT_BYTES - self.key_bytes
        {
            return Err(io::Error::other(
                "checkpoint transaction read-set limit exceeded",
            ));
        }
        let mut key_bytes = bounds_bytes;
        let mut charged = bounds_bytes;
        let mut reservation = PendingReservation::acquire(&store.transaction_budget, charged)?;
        let mut entries = Vec::new();
        state.view.visit_entries(after, before, |key, location| {
            if self.items + entries.len() >= MAX_TRANSACTION_VALIDATION_ITEMS
                || key.len() > MAX_TRANSACTION_READ_CONSTRAINT_BYTES - self.key_bytes - key_bytes
            {
                return Err(io::Error::other(
                    "checkpoint transaction range observation limit exceeded",
                ));
            }
            let charge = key.len() + std::mem::size_of::<ValueLocation>();
            reservation.grow(charge)?;
            charged += charge;
            key_bytes += key.len();
            entries.push((key.to_vec(), location));
            Ok(ControlFlow::Continue(()))
        })?;
        let items = entries.len();
        self.ranges.push(Range {
            after: after.map(<[u8]>::to_vec),
            before: before.map(<[u8]>::to_vec),
            entries,
        });
        self.key_bytes += key_bytes;
        self.items += items;
        self.charged += charged;
        reservation.finish();
        Ok(())
    }

    // The caller holds a state lock. Matching all immutable locations and
    // membership makes the observations valid for this complete state.
    pub(super) fn matches(&self, state: &State) -> io::Result<bool> {
        if self.conflicted {
            return Ok(false);
        }
        if self.validated_generation == state.generation {
            return Ok(true);
        }
        let mut work = 0_usize;
        for (key, location) in &self.points {
            work += 1;
            if state.view.location(key)? != *location {
                return Ok(false);
            }
        }
        for range in &self.ranges {
            let mut index = 0;
            let mut same = true;
            state.view.visit_entries(
                range.after.as_deref(),
                range.before.as_deref(),
                |key, location| {
                    let Some((previous_key, previous_location)) = range.entries.get(index) else {
                        same = false;
                        return Ok(ControlFlow::Break(()));
                    };
                    work += 1;
                    if work > MAX_TRANSACTION_VALIDATION_ITEMS {
                        return Err(io::Error::other(
                            "checkpoint transaction validation work limit exceeded",
                        ));
                    }
                    if previous_key.as_slice() != key || *previous_location != location {
                        same = false;
                        return Ok(ControlFlow::Break(()));
                    }
                    index += 1;
                    Ok(ControlFlow::Continue(()))
                },
            )?;
            if !same || index != range.entries.len() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn clear(&mut self) -> usize {
        self.points.clear();
        self.ranges.clear();
        std::mem::take(&mut self.charged)
    }
}
