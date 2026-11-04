//! Bounded read dependencies used by the opt-in serializable validator.
use super::*;
use std::collections::BTreeSet;

/// Maximum distinct point and range dependencies in one read-set transaction.
pub const MAX_TRANSACTION_READ_CONSTRAINTS: usize = 2048;
/// Maximum combined key and range-bound bytes in one read-set transaction.
pub const MAX_TRANSACTION_READ_CONSTRAINT_BYTES: usize = 2 * 1024 * 1024;
/// Maximum key comparisons during read-set validation while holding the writer barrier.
pub const MAX_TRANSACTION_VALIDATION_ITEMS: usize = 65_536;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Range {
    after: Option<Vec<u8>>,
    before: Option<Vec<u8>>,
}

#[derive(Default)]
pub(super) struct ReadSet {
    points: BTreeSet<Vec<u8>>,
    ranges: BTreeSet<Range>,
    bytes: usize,
}

impl ReadSet {
    fn admit(&self, bytes: usize) -> io::Result<()> {
        if self.points.len() + self.ranges.len() >= MAX_TRANSACTION_READ_CONSTRAINTS
            || bytes > MAX_TRANSACTION_READ_CONSTRAINT_BYTES - self.bytes
        {
            return Err(io::Error::other("transaction read-set limit exceeded"));
        }
        Ok(())
    }

    pub(super) fn point(&mut self, key: &[u8]) -> io::Result<()> {
        if !self.points.contains(key) {
            self.admit(key.len())?;
            self.points.insert(key.to_vec());
            self.bytes += key.len();
        }
        Ok(())
    }

    pub(super) fn range(&mut self, after: Option<&[u8]>, before: Option<&[u8]>) -> io::Result<()> {
        let candidate = Range {
            after: after.map(<[u8]>::to_vec),
            before: before.map(<[u8]>::to_vec),
        };
        if !self.ranges.contains(&candidate) {
            let bytes = after.map_or(0, <[u8]>::len) + before.map_or(0, <[u8]>::len);
            self.admit(bytes)?;
            self.ranges.insert(candidate);
            self.bytes += bytes;
        }
        Ok(())
    }

    // The caller holds the writer barrier, so `current` cannot change during
    // validation. Matching immutable locations and key membership establish
    // that every observed point and whole requested range matches at commit.
    pub(super) fn matches(
        &self,
        snapshot: &BTreeMap<Vec<u8>, ValueLocation>,
        current: &BTreeMap<Vec<u8>, ValueLocation>,
    ) -> io::Result<bool> {
        let mut inspected = 0_usize;
        for key in &self.points {
            inspected += 1;
            if inspected > MAX_TRANSACTION_VALIDATION_ITEMS {
                return Err(io::Error::other(
                    "transaction validation work limit exceeded",
                ));
            }
            if snapshot.get(key) != current.get(key) {
                return Ok(false);
            }
        }
        for range in &self.ranges {
            let bounds = (
                range.after.as_deref().map_or(Unbounded, Excluded),
                range.before.as_deref().map_or(Unbounded, Excluded),
            );
            let mut previous = snapshot.range::<[u8], _>(bounds);
            let mut now = current.range::<[u8], _>(bounds);
            loop {
                let left = previous.next();
                let right = now.next();
                if left.is_none() && right.is_none() {
                    break;
                }
                inspected += 1;
                if inspected > MAX_TRANSACTION_VALIDATION_ITEMS {
                    return Err(io::Error::other(
                        "transaction validation work limit exceeded",
                    ));
                }
                if left != right {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}
