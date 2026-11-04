//! Bounded aggregation and ID-based relationship lookup over one snapshot.
use super::{
    filter::{DocumentFilter, DocumentQuery, QueryPlan},
    index::IndexValue,
    key,
    query::DocumentEntry,
    schema,
    transaction::DocumentTransaction,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io;

/// A bounded count and optional signed integer sum, optionally grouped by a path.
#[derive(Clone, Debug)]
pub struct AggregateRequest {
    /// Predicate applied before grouping.
    pub filter: DocumentFilter,
    /// Group by this indexable scalar path. Missing and null are distinct.
    pub group_by: Option<Vec<String>>,
    /// Sum this signed integer field; missing and null values are skipped.
    pub sum: Option<Vec<String>>,
    /// Maximum documents examined, between 1 and 256.
    pub max_scanned: usize,
}

/// One aggregation group in typed scalar order, with missing first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateGroup {
    /// `None` represents a missing group field or the ungrouped result.
    pub key: Option<IndexValue>,
    /// Number of matching documents in this group.
    pub count: u64,
    /// Checked signed integer sum, when requested.
    pub sum: Option<i64>,
}

/// Complete aggregation results and actual collection-scan statistics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateResult {
    /// Bounded groups in scalar order.
    pub groups: Vec<AggregateGroup>,
    /// The scan used to produce the groups.
    pub plan: QueryPlan,
}

/// A source document and optional document referenced by its ID field.
#[derive(Clone, Debug, PartialEq)]
pub struct LookupEntry {
    /// The source document.
    pub source: DocumentEntry,
    /// The matching document, or `None` for missing, null, or absent IDs.
    pub related: Option<Value>,
}

/// Bounded results of an ID lookup and source query statistics.
#[derive(Clone, Debug, PartialEq)]
pub struct LookupResult {
    /// Source rows with related documents.
    pub documents: Vec<LookupEntry>,
    /// The source collection scan.
    pub plan: QueryPlan,
}

impl DocumentTransaction<'_, '_> {
    /// Aggregates a complete scan with at most 256 documents and groups.
    /// Non-integer sum fields and integer overflow fail the operation.
    pub fn aggregate(
        &self,
        collection: &str,
        request: &AggregateRequest,
    ) -> io::Result<AggregateResult> {
        if request.max_scanned == 0 || request.max_scanned > 256 {
            return Err(key::invalid(
                "aggregate scan limit must be between 1 and 256",
            ));
        }
        if let Some(path) = &request.group_by {
            schema::validate_path(path)?;
        }
        if let Some(path) = &request.sum {
            schema::validate_path(path)?;
        }
        super::filter::validate_filter(&request.filter)?;
        let query = DocumentQuery {
            filter: request.filter.clone(),
            limit: 256,
            max_scanned: request.max_scanned,
            ..DocumentQuery::default()
        };
        let result = self.query(collection, &query)?;
        let mut groups = BTreeMap::<Vec<u8>, AggregateGroup>::new();
        for row in result.documents {
            let value = match &request.group_by {
                None => None,
                Some(path) => schema::field(&row.document, path)
                    .map(IndexValue::from_json)
                    .transpose()?,
            };
            let mut encoded = Vec::new();
            if let Some(value) = &value {
                value.encode(&mut encoded)?;
            }
            let group = groups.entry(encoded).or_insert_with(|| AggregateGroup {
                key: value,
                count: 0,
                sum: request.sum.as_ref().map(|_| 0),
            });
            group.count += 1;
            if let Some(path) = &request.sum {
                if let Some(value) = schema::field(&row.document, path) {
                    if !value.is_null() {
                        let value = value.as_i64().ok_or_else(|| {
                            key::invalid("aggregate sum requires signed 64-bit integers")
                        })?;
                        group.sum = Some(
                            group
                                .sum
                                .expect("requested sum")
                                .checked_add(value)
                                .ok_or_else(|| key::invalid("aggregate sum overflow"))?,
                        );
                    }
                }
            }
        }
        if request.group_by.is_none() && groups.is_empty() {
            groups.insert(
                Vec::new(),
                AggregateGroup {
                    key: None,
                    count: 0,
                    sum: request.sum.as_ref().map(|_| 0),
                },
            );
        }
        Ok(AggregateResult {
            groups: groups.into_values().collect(),
            plan: result.plan,
        })
    }

    /// Executes a bounded source query and follows string IDs into another
    /// collection. Both reads share one transaction snapshot. Projection is
    /// rejected so the local lookup field cannot be silently removed.
    pub fn lookup(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_path: &[String],
        foreign_collection: &str,
    ) -> io::Result<LookupResult> {
        schema::validate_path(local_path)?;
        self.metadata(foreign_collection)?;
        if query.projection.is_some() {
            return Err(key::invalid(
                "lookup does not accept a projected source query",
            ));
        }
        let result = self.query(collection, query)?;
        let mut documents = Vec::with_capacity(result.documents.len());
        let mut bytes = 0_usize;
        for source in result.documents {
            let related = match schema::field(&source.document, local_path) {
                None | Some(Value::Null) => None,
                Some(Value::String(id)) => self.get(foreign_collection, id)?,
                Some(_) => {
                    return Err(key::invalid(
                        "lookup field must be a string ID, null, or missing",
                    ));
                }
            };
            if let Some(value) = &related {
                bytes = bytes
                    .checked_add(serde_json::to_vec(value).map_err(io::Error::other)?.len())
                    .ok_or_else(|| key::invalid("lookup response size overflow"))?;
                if bytes > 16 * 1024 * 1024 {
                    return Err(key::invalid("lookup response exceeds 16 MiB"));
                }
            }
            documents.push(LookupEntry { source, related });
        }
        Ok(LookupResult {
            documents,
            plan: result.plan,
        })
    }
}
