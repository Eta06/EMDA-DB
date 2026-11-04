//! Bounded exact integer statistics over a transactional collection snapshot.
use super::{
    filter::{DocumentFilter, QueryPlan},
    index::IndexValue,
    key, schema,
    transaction::DocumentTransaction,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io;

const MAX_FIELDS: usize = 16;
const MAX_SCANNED: usize = 256;

/// A bounded collection scan computing statistics for signed integer fields.
#[derive(Clone, Debug)]
pub struct AggregateStatsRequest {
    /// Predicate evaluated before grouping.
    pub filter: DocumentFilter,
    /// Optional scalar grouping path. Missing and null are distinct.
    pub group_by: Option<Vec<String>>,
    /// One to sixteen distinct object paths to summarize, in result order.
    pub fields: Vec<Vec<String>>,
    /// Maximum documents examined, between 1 and 256.
    pub max_scanned: usize,
}

/// Exact signed-integer statistics for one requested field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumericFieldStats {
    /// Number of present, non-null values. This can be less than group count.
    pub count: u64,
    /// Exact checked sum. An empty field has sum zero.
    pub sum: i128,
    /// Smallest present, non-null value, or `None` if there are none.
    pub min: Option<i64>,
    /// Largest present, non-null value, or `None` if there are none.
    pub max: Option<i64>,
}

impl NumericFieldStats {
    /// Approximate floating-point mean, or `None` when there are no values.
    /// Use `sum` and `count` directly when an exact rational is required.
    pub fn mean(&self) -> Option<f64> {
        (self.count != 0).then(|| self.sum as f64 / self.count as f64)
    }
}

/// Statistics for one group, ordered by the documented typed scalar order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateStatsGroup {
    /// `None` represents a missing grouping field or an ungrouped result.
    pub key: Option<IndexValue>,
    /// Total matching documents in this group, including missing/null fields.
    pub count: u64,
    /// One entry for each requested field, in request order.
    pub fields: Vec<NumericFieldStats>,
}

/// Complete groups and the actual work performed by the collection scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateStatsResult {
    /// Requested paths, shared by every group's statistics in the same order.
    pub fields: Vec<Vec<String>>,
    /// All groups; an empty ungrouped scan still returns one empty group.
    pub groups: Vec<AggregateStatsGroup>,
    /// Statistics from the underlying complete scan, before grouping.
    pub plan: QueryPlan,
}

fn new_group(key: Option<IndexValue>, field_count: usize) -> AggregateStatsGroup {
    AggregateStatsGroup {
        key,
        count: 0,
        fields: (0..field_count)
            .map(|_| NumericFieldStats {
                count: 0,
                sum: 0,
                min: None,
                max: None,
            })
            .collect(),
    }
}

impl DocumentTransaction<'_, '_> {
    /// Computes count, exact sum, minimum and maximum for multiple fields.
    /// Missing and null values are skipped; any present non-integer fails the
    /// entire operation. Input, scan and group counts are explicitly bounded.
    pub fn aggregate_stats(
        &self,
        collection: &str,
        request: &AggregateStatsRequest,
    ) -> io::Result<AggregateStatsResult> {
        if request.max_scanned == 0 || request.max_scanned > MAX_SCANNED {
            return Err(key::invalid(
                "aggregate scan limit must be between 1 and 256",
            ));
        }
        if request.fields.is_empty() || request.fields.len() > MAX_FIELDS {
            return Err(key::invalid("aggregate statistics require 1 to 16 fields"));
        }
        let mut paths = BTreeSet::new();
        for path in &request.fields {
            schema::validate_path(path)?;
            if !paths.insert(path) {
                return Err(key::invalid(
                    "aggregate statistics contain a repeated field",
                ));
            }
        }
        if let Some(path) = &request.group_by {
            schema::validate_path(path)?;
        }
        let mut groups = BTreeMap::<Vec<u8>, AggregateStatsGroup>::new();
        let plan = self.visit_filtered_documents(
            collection,
            &request.filter,
            request.max_scanned,
            |document| {
                let value = request
                    .group_by
                    .as_ref()
                    .and_then(|path| schema::field(document, path))
                    .map(IndexValue::from_json)
                    .transpose()?;
                let mut encoded = Vec::new();
                if let Some(value) = &value {
                    value.encode(&mut encoded)?;
                }
                let group = groups
                    .entry(encoded)
                    .or_insert_with(|| new_group(value, request.fields.len()));
                group.count += 1;
                for (path, stats) in request.fields.iter().zip(&mut group.fields) {
                    let Some(value) = schema::field(document, path) else {
                        continue;
                    };
                    if value.is_null() {
                        continue;
                    }
                    let value = value.as_i64().ok_or_else(|| {
                        key::invalid("aggregate statistics require signed 64-bit integers")
                    })?;
                    stats.count += 1;
                    stats.sum = stats
                        .sum
                        .checked_add(i128::from(value))
                        .ok_or_else(|| key::invalid("aggregate statistics sum overflow"))?;
                    stats.min = Some(stats.min.map_or(value, |previous| previous.min(value)));
                    stats.max = Some(stats.max.map_or(value, |previous| previous.max(value)));
                }
                Ok(())
            },
        )?;
        if request.group_by.is_none() && groups.is_empty() {
            groups.insert(Vec::new(), new_group(None, request.fields.len()));
        }
        Ok(AggregateStatsResult {
            fields: request.fields.clone(),
            groups: groups.into_values().collect(),
            plan,
        })
    }
}
