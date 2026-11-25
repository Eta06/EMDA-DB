//! Atomic filter-driven updates and deletes of a bounded document set.
use super::{
    DocumentTransaction,
    filter::{DocumentFilter, DocumentQuery},
    key,
    update::DocumentUpdate,
};
use std::io;

/// Most documents one filter-driven mutation may change.
pub const MAX_MATCHED_DOCUMENTS: usize = 128;

/// How a filter-driven mutation finds the documents it changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum MatchSelection {
    /// Evaluate the filter over the complete collection, as
    /// [`DocumentTransaction::query`] does. This does not trust indexes.
    #[default]
    Scan,
    /// Use qualifying index probes, as [`DocumentTransaction::query_planned`]
    /// does. Like that query, this assumes declared indexes are maintained;
    /// a missing index entry would leave its document unchanged.
    Planned,
}

/// Limits and selection for a filter-driven mutation. Both limits are
/// required: `max_scanned` bounds document visits as for a query (1 to
/// 1,024), and `max_matched` bounds the documents changed (1 to 128).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MatchOptions {
    /// Maximum document visits while finding matches.
    pub max_scanned: usize,
    /// Maximum matching documents; more is an error, never a partial change.
    pub max_matched: usize,
    /// How matches are found.
    pub selection: MatchSelection,
}

impl MatchOptions {
    /// Complete-scan selection with the given limits.
    pub fn new(max_scanned: usize, max_matched: usize) -> Self {
        Self {
            max_scanned,
            max_matched,
            selection: MatchSelection::Scan,
        }
    }

    /// Uses index-planned selection instead of a complete scan.
    pub fn planned(mut self) -> Self {
        self.selection = MatchSelection::Planned;
        self
    }
}

impl DocumentTransaction<'_, '_> {
    // Finds the complete set of matching IDs on this transaction's view, or
    // fails. A result page larger than `max_matched` is requested so that a
    // complete count can be proven: the query counts every evaluated match
    // regardless of its page limit, and any truncated page is rejected.
    fn resolve_matches(
        &self,
        collection: &str,
        filter: &DocumentFilter,
        options: MatchOptions,
    ) -> io::Result<Vec<String>> {
        if !(1..=MAX_MATCHED_DOCUMENTS).contains(&options.max_matched) {
            return Err(key::invalid("max_matched must be between 1 and 128"));
        }
        // Admit the borrowed filter's depth and operands before copying it.
        super::filter::validate_filter(filter)?;
        let query = DocumentQuery {
            filter: filter.clone(),
            sort: None,
            // Return IDs only; the documents are reread when changed.
            projection: Some(Vec::new()),
            offset: 0,
            limit: options.max_matched + 1,
            max_scanned: options.max_scanned,
        };
        let result = match options.selection {
            MatchSelection::Scan => self.query(collection, &query)?,
            MatchSelection::Planned => self.query_planned(collection, &query)?.result,
        };
        if result.plan.matched > options.max_matched {
            return Err(key::invalid(
                "matching documents exceed the configured max_matched",
            ));
        }
        if result.documents.len() != result.plan.matched {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "matching documents could not be collected completely",
            ));
        }
        Ok(result.documents.into_iter().map(|entry| entry.id).collect())
    }

    /// Applies `updates` to every document of `collection` matching `filter`
    /// and returns how many were changed. Matches are resolved completely on
    /// this transaction's view before any write. Exceeding either limit, an
    /// invalid update (even with no matches) or any document's validation,
    /// unique-index or transaction failure fails the call, and the whole
    /// transaction then aborts with no change committed. Upsert is not
    /// supported. Writes and the matching reads use this transaction's mode.
    pub fn update_matching(
        &mut self,
        collection: &str,
        filter: &DocumentFilter,
        updates: &[DocumentUpdate],
        options: MatchOptions,
    ) -> io::Result<usize> {
        self.mutate(|this| {
            super::update::validate_inputs(&[], updates)?;
            let ids = this.resolve_matches(collection, filter, options)?;
            // One nested-comparison budget covers the whole call.
            let mut equality = super::equality::EqualityWork::default();
            for id in &ids {
                if !this.update_validated((collection, id), &[], updates, false, &mut equality)? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "a matched document disappeared before its update",
                    ));
                }
            }
            Ok(ids.len())
        })
    }

    /// Deletes every document of `collection` matching `filter` and returns
    /// how many were deleted, with the resolution, limit and abort rules of
    /// [`Self::update_matching`].
    pub fn delete_matching(
        &mut self,
        collection: &str,
        filter: &DocumentFilter,
        options: MatchOptions,
    ) -> io::Result<usize> {
        self.mutate(|this| {
            let ids = this.resolve_matches(collection, filter, options)?;
            for id in &ids {
                if !this.delete(collection, id)? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "a matched document disappeared before its deletion",
                    ));
                }
            }
            Ok(ids.len())
        })
    }
}
