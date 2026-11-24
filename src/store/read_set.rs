//! Bounded read dependencies used by the opt-in serializable validator.
use super::*;
use crate::log::GenerationLocation;
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

impl Range {
    fn contains(&self, key: &[u8]) -> bool {
        self.after.as_deref().is_none_or(|after| after < key)
            && self.before.as_deref().is_none_or(|before| key < before)
    }

    fn covers(&self, after: Option<&[u8]>, before: Option<&[u8]>) -> bool {
        self.after.as_deref() <= after
            && self
                .before
                .as_deref()
                .is_none_or(|upper| before.is_some_and(|end| end <= upper))
    }
}

#[derive(Default)]
pub(super) struct ReadSet {
    points: BTreeSet<Vec<u8>>,
    ranges: BTreeSet<Range>,
    bytes: usize,
    validation_limit: Option<u64>,
}

impl ReadSet {
    pub(super) fn with_validation_limit(validation_limit: Option<u64>) -> Self {
        Self {
            validation_limit,
            ..Self::default()
        }
    }

    fn inspect(&self, inspected: &mut usize) -> io::Result<()> {
        *inspected += 1;
        if *inspected > MAX_TRANSACTION_VALIDATION_ITEMS {
            return Err(io::Error::other(
                "transaction validation work limit exceeded",
            ));
        }
        if self
            .validation_limit
            .is_some_and(|limit| *inspected as u64 > limit)
        {
            return Err(io::Error::other(
                "transaction cumulative validation limit exceeded",
            ));
        }
        Ok(())
    }

    fn admit(&self, bytes: usize) -> io::Result<()> {
        if self.points.len() + self.ranges.len() >= MAX_TRANSACTION_READ_CONSTRAINTS
            || bytes > MAX_TRANSACTION_READ_CONSTRAINT_BYTES - self.bytes
        {
            return Err(io::Error::other("transaction read-set limit exceeded"));
        }
        Ok(())
    }

    pub(super) fn point(&mut self, key: &[u8]) -> io::Result<()> {
        if !self.points.contains(key) && !self.ranges.iter().any(|range| range.contains(key)) {
            self.admit(key.len())?;
            self.points.insert(key.to_vec());
            self.bytes += key.len();
        }
        Ok(())
    }

    pub(super) fn range(&mut self, after: Option<&[u8]>, before: Option<&[u8]>) -> io::Result<()> {
        if self.ranges.iter().any(|range| range.covers(after, before)) {
            return Ok(());
        }
        let bytes = after.map_or(0, <[u8]>::len) + before.map_or(0, <[u8]>::len);
        self.admit(bytes)?;
        self.ranges.insert(Range {
            after: after.map(<[u8]>::to_vec),
            before: before.map(<[u8]>::to_vec),
        });
        self.bytes += bytes;
        Ok(())
    }

    // The caller holds the writer barrier, so `current` cannot change during
    // validation. Matching immutable locations and key membership establish
    // that every observed point and whole requested range matches at commit.
    pub(super) fn matches(
        &self,
        snapshot: &BTreeMap<Vec<u8>, ValueLocation>,
        current: &BTreeMap<Vec<u8>, ValueLocation>,
        snapshot_generation: Option<GenerationId>,
        current_generation: Option<GenerationId>,
    ) -> io::Result<bool> {
        let mut inspected = 0_usize;
        for key in &self.points {
            self.inspect(&mut inspected)?;
            let qualify = |location: Option<&ValueLocation>, generation| {
                location.map(|location| GenerationLocation {
                    generation,
                    location: *location,
                })
            };
            if qualify(snapshot.get(key), snapshot_generation)
                != qualify(current.get(key), current_generation)
            {
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
                self.inspect(&mut inspected)?;
                fn qualify<'a>(
                    entry: Option<(&'a Vec<u8>, &ValueLocation)>,
                    generation: Option<GenerationId>,
                ) -> Option<(&'a Vec<u8>, GenerationLocation)> {
                    entry.map(|(key, location)| {
                        (
                            key,
                            GenerationLocation {
                                generation,
                                location: *location,
                            },
                        )
                    })
                }
                if qualify(left, snapshot_generation) != qualify(right, current_generation) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_complete_range_covers_later_pages_and_interior_point_reads() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        for key in [b"a", b"b", b"d", b"e"] {
            store.put(key, b"old").unwrap();
        }
        let run = |intervening_key: &[u8], observe_endpoint, receipt: &[u8]| {
            store.transaction_detailed_with_validation_limit(
                crate::TransactionMode::ReadSet,
                None,
                None,
                Some(2),
                |tx| {
                    assert_eq!(tx.scan(Some(b"a"), Some(b"e"), 1)?[0].0, b"b");
                    assert_eq!(tx.scan(Some(b"b"), Some(b"e"), 1)?[0].0, b"d");
                    assert_eq!(tx.get(b"b")?.as_deref(), Some(&b"old"[..]));
                    assert_eq!(tx.get(b"c")?, None);
                    if observe_endpoint {
                        assert!(tx.get(b"e")?.is_some());
                    }
                    store.put(intervening_key, b"concurrent")?;
                    tx.put(receipt, b"staged")
                },
            )
        };
        run(b"a", false, b"receipt").unwrap();
        assert_eq!(
            store.get(b"receipt").unwrap().as_deref(),
            Some(&b"staged"[..])
        );
        for (key, observe_endpoint) in [(b"e", true), (b"c", false)] {
            let result = run(key, observe_endpoint, b"rejected");
            assert!(
                matches!(result, Err(crate::TransactionFailure::Rejected(error))
                if crate::is_transaction_conflict(&error))
            );
            assert_eq!(store.get(b"rejected").unwrap(), None);
        }
    }

    #[test]
    fn witnessed_identity_qualifies_point_and_range_observations() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"value").unwrap();
        let original = store.read().unwrap().log_generation.expect("witnessed log");
        for range in [false, true] {
            let result = store.transaction_with_read_set(|tx| {
                if range {
                    tx.scan(None, None, 10)?;
                } else {
                    tx.get(b"a")?;
                }
                tx.put(b"staged", b"not committed")?;
                // Model a future atomic publication with identical locations but a
                // different witnessed identity. No actual log switch is implemented.
                store.inner.write().unwrap().log_generation = Some(GenerationId([42; 16]));
                Ok(())
            });
            store.inner.write().unwrap().log_generation = Some(original);
            let error = result.expect_err("identity change must conflict");
            assert!(crate::is_transaction_conflict(&error), "{error}");
            assert_eq!(store.get(b"staged").unwrap(), None);
        }
    }

    #[test]
    fn missing_observations_and_legacy_locations_keep_their_semantics() {
        let mut reads = ReadSet::default();
        reads.point(b"missing").unwrap();
        let empty = BTreeMap::new();
        assert!(
            reads
                .matches(
                    &empty,
                    &empty,
                    Some(GenerationId([1; 16])),
                    Some(GenerationId([2; 16]))
                )
                .unwrap()
        );
        let mut index = BTreeMap::new();
        index.insert(
            b"missing".to_vec(),
            ValueLocation {
                offset: 1,
                length: 2,
                checksum: 3,
            },
        );
        assert!(!reads.matches(&empty, &index, None, None).unwrap());
        assert!(reads.matches(&index, &index, None, None).unwrap());
    }
}
