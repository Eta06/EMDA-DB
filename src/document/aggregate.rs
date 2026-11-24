//! Bounded aggregation and relationship lookups over one snapshot.
use super::{
    filter::{DocumentFilter, DocumentQuery, QueryPlan},
    index::{self, IndexValue, MAX_INDEX_FIELDS},
    key,
    planner::{DocumentQueryAccess, PlannedDocumentQueryResult},
    query::{DocumentEntry, index_definition},
    schema::{self, CollectionSchema},
    transaction::DocumentTransaction,
};
use crate::record::{MAX_KEY, invalid_data};
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

/// A source document and its optional related document.
#[derive(Clone, Debug, PartialEq)]
pub struct LookupEntry {
    /// The source document.
    pub source: DocumentEntry,
    /// The matching foreign document, if there is one.
    pub related: Option<Value>,
}

/// Bounded relationship lookup results and source query statistics.
#[derive(Clone, Debug, PartialEq)]
pub struct LookupResult {
    /// Source rows with related documents.
    pub documents: Vec<LookupEntry>,
    /// Actual work on the source collection.
    pub plan: QueryPlan,
}

/// Relationship lookup results with the chosen source access path.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedLookupResult {
    /// Rows and actual source work.
    pub result: LookupResult,
    /// Access path used to select source rows.
    pub access: DocumentQueryAccess,
}

struct LookupSource<'a> {
    collection: &'a str,
    query: &'a DocumentQuery,
    planned: bool,
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
        let mut groups = BTreeMap::<Vec<u8>, AggregateGroup>::new();
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
                let group = groups.entry(encoded).or_insert_with(|| AggregateGroup {
                    key: value,
                    count: 0,
                    sum: request.sum.as_ref().map(|_| 0),
                });
                group.count += 1;
                let Some(value) = request
                    .sum
                    .as_ref()
                    .and_then(|path| schema::field(document, path))
                    .filter(|value| !value.is_null())
                else {
                    return Ok(());
                };
                let value = value
                    .as_i64()
                    .ok_or_else(|| key::invalid("aggregate sum requires signed 64-bit integers"))?;
                let sum = group.sum.as_mut().expect("requested sum");
                *sum = sum
                    .checked_add(value)
                    .ok_or_else(|| key::invalid("aggregate sum overflow"))?;
                Ok(())
            },
        )?;
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
            plan,
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
        self.lookup_id_using(
            LookupSource {
                collection,
                query,
                planned: false,
            },
            local_path,
            foreign_collection,
        )
        .map(|lookup| lookup.result)
    }

    /// Follows string IDs after selecting source rows with the bounded query
    /// planner. Source and target reads share this transaction and its limits.
    pub fn lookup_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_path: &[String],
        foreign_collection: &str,
    ) -> io::Result<PlannedLookupResult> {
        self.lookup_id_using(
            LookupSource {
                collection,
                query,
                planned: true,
            },
            local_path,
            foreign_collection,
        )
    }

    fn lookup_id_using(
        &self,
        source: LookupSource<'_>,
        local_path: &[String],
        foreign_collection: &str,
    ) -> io::Result<PlannedLookupResult> {
        schema::validate_path(local_path)?;
        let foreign = self.metadata(foreign_collection)?;
        self.lookup_sources(source, |document, remaining| {
            match schema::field(document, local_path) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(id)) => {
                    self.read_lookup_target(foreign_collection, id, &foreign.schema, remaining)
                }
                Some(_) => Err(key::invalid(
                    "lookup field must be a string ID, null, or missing",
                )),
            }
        })
    }

    /// Resolve at most one related document per source row by a named unique
    /// index. Local paths correspond to the index's fields in order. A missing
    /// local field gives no match. Null is an exact indexed value. A partial
    /// index limits matches to the documents it covers.
    ///
    /// Source and target reads share this transaction. The source query uses
    /// a bounded collection scan and requires an unprojected result. Related
    /// document bytes share a 16 MiB ceiling, counting repeated references.
    pub fn lookup_unique_index(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_paths: &[Vec<String>],
        foreign_collection: &str,
        foreign_index: &str,
    ) -> io::Result<LookupResult> {
        self.lookup_unique_index_using(
            LookupSource {
                collection,
                query,
                planned: false,
            },
            local_paths,
            foreign_collection,
            foreign_index,
        )
        .map(|lookup| lookup.result)
    }

    /// Resolves complete unique index tuples after selecting source rows with
    /// the bounded query planner. Target validation and limits are the same as
    /// in [Self::lookup_unique_index].
    pub fn lookup_unique_index_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_paths: &[Vec<String>],
        foreign_collection: &str,
        foreign_index: &str,
    ) -> io::Result<PlannedLookupResult> {
        self.lookup_unique_index_using(
            LookupSource {
                collection,
                query,
                planned: true,
            },
            local_paths,
            foreign_collection,
            foreign_index,
        )
    }

    fn lookup_unique_index_using(
        &self,
        source: LookupSource<'_>,
        local_paths: &[Vec<String>],
        foreign_collection: &str,
        foreign_index: &str,
    ) -> io::Result<PlannedLookupResult> {
        let foreign = self.metadata(foreign_collection)?;
        let definition = index_definition(&foreign, foreign_index)?;
        if !definition.unique || local_paths.len() != definition.fields.len() {
            return Err(key::invalid(
                "lookup requires a unique index and one local path per indexed field",
            ));
        }
        for path in local_paths {
            schema::validate_path(path)?;
        }
        let mut tuple = key::index(b'U', foreign_collection, foreign_index)?;
        let mut entry = key::index(b'I', foreign_collection, foreign_index)?;
        let tuple_prefix = tuple.len();
        let entry_prefix = entry.len();
        self.lookup_sources(source, |document, remaining| {
            let mut values = [None; MAX_INDEX_FIELDS];
            for (slot, path) in values.iter_mut().zip(local_paths) {
                let Some(value) = schema::field(document, path) else {
                    return Ok(None);
                };
                *slot = Some(value);
            }
            tuple.truncate(tuple_prefix);
            for value in values.into_iter().flatten() {
                index::encode_json(value, &mut tuple)?;
            }
            if tuple.len() > MAX_KEY {
                return Ok(None);
            }
            let Some(owner) = self.read_unique_owner(&tuple)? else {
                return Ok(None);
            };
            let id = std::str::from_utf8(&owner)
                .map_err(|_| invalid_data("invalid unique index owner"))?;
            let target = self
                .read_lookup_target(foreign_collection, id, &foreign.schema, remaining)?
                .ok_or_else(|| invalid_data("unique index references a missing document"))?;
            entry.truncate(entry_prefix);
            let indexed = index::encode_indexed_tuple(definition, &target, &mut entry)
                .map_err(|_| invalid_data("document cannot be represented by its index"))?;
            if !indexed || entry[entry_prefix..] != tuple[tuple_prefix..] {
                return Err(invalid_data("unique index does not match its document"));
            }
            entry.extend_from_slice(&owner);
            if entry.len() > MAX_KEY {
                return Err(invalid_data("document cannot be represented by its index"));
            }
            if !self.has_index_entry(&entry)? {
                return Err(invalid_data("unique index has no matching document entry"));
            }
            Ok(Some(target))
        })
    }

    fn read_lookup_target(
        &self,
        collection: &str,
        id: &str,
        schema: &CollectionSchema,
        remaining: &mut usize,
    ) -> io::Result<Option<Value>> {
        self.read_document_bytes(&key::document(collection, id)?, |len| {
            *remaining = remaining
                .checked_sub(len)
                .ok_or_else(|| key::invalid("lookup response exceeds 16 MiB"))?;
            Ok(())
        })?
        .map(|encoded| Self::decode_document(&encoded, schema))
        .transpose()
    }

    fn lookup_sources(
        &self,
        source: LookupSource<'_>,
        mut resolve: impl FnMut(&Value, &mut usize) -> io::Result<Option<Value>>,
    ) -> io::Result<PlannedLookupResult> {
        let LookupSource {
            collection,
            query,
            planned,
        } = source;
        if query.projection.is_some() {
            return Err(key::invalid(
                "lookup does not accept a projected source query",
            ));
        }
        let PlannedDocumentQueryResult { result, access } = if planned {
            self.query_planned(collection, query)?
        } else {
            PlannedDocumentQueryResult {
                result: self.query(collection, query)?,
                access: DocumentQueryAccess::CollectionScan,
            }
        };
        let mut documents = Vec::with_capacity(result.documents.len());
        let mut remaining = 16 * 1024 * 1024_usize;
        for source in result.documents {
            let related = resolve(&source.document, &mut remaining)?;
            documents.push(LookupEntry { source, related });
        }
        Ok(PlannedLookupResult {
            result: LookupResult {
                documents,
                plan: result.plan,
            },
            access,
        })
    }
}
