//! Opt-in cost-based choice among eligible range paths. Results must match
//! complete scans, estimates must stay within their total budget, and the
//! default planner must keep its existing choice.
use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentEstimateLimits,
    DocumentFilter, DocumentQuery, DocumentQueryAccess, DocumentReadLimits,
    DocumentTransactionMode, IndexDefinition, IndexValue, RetainedExportLimits, Store,
};
use serde_json::json;
use std::io::ErrorKind;
use std::sync::Barrier;

mod support;
use support::retry_lock;

const DOCUMENTS: i64 = 400;

const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 4_000_000,
        entries: 10_000,
        key_bytes: 1_000_000,
        work_bytes: 4_000_001,
    },
    index: IndexLimits {
        blocks: 64,
        anchor_bytes: 8192,
    },
    suffix: SuffixLimits {
        bytes: 4_000_000,
        operations: 10_000,
        work: 40_000_000,
    },
    overlay: OverlayLimits {
        entries: 10_000,
        key_bytes: 1_000_000,
    },
};

const EXPORT: RetainedExportLimits = RetainedExportLimits {
    max_log_bytes: 4_000_000,
    max_live_keys: 10_000,
    max_live_key_bytes: 1_000_000,
    max_output_bytes: 4_000_000,
};

fn index(name: &str) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        fields: vec![vec![name.into()]],
        unique: false,
        partial: None,
    }
}

fn at_least(field: &str, value: i64) -> DocumentFilter {
    DocumentFilter::Compare {
        path: vec![field.into()],
        comparison: Comparison::GreaterOrEqual,
        value: IndexValue::Integer(value),
    }
}

fn below(field: &str, value: i64) -> DocumentFilter {
    DocumentFilter::Compare {
        path: vec![field.into()],
        comparison: Comparison::Less,
        value: IndexValue::Integer(value),
    }
}

fn entries(max_entries: usize) -> DocumentEstimateLimits {
    DocumentEstimateLimits {
        max_entries,
        max_scans: 64,
    }
}

fn query(filter: DocumentFilter, max_scanned: usize) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned,
        ..Default::default()
    }
}

fn range(name: &str) -> DocumentQueryAccess {
    DocumentQueryAccess::RangeIndex { index: name.into() }
}

// `age` is defined first and every document has 0 <= age < 400, so the
// default range order probes it. `score >= 390` matches ten documents.
fn broad_and_narrow() -> DocumentFilter {
    DocumentFilter::And(vec![at_least("age", 0), at_least("score", 390)])
}

fn seed(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index("age")).unwrap();
    db.create_index("c", index("score")).unwrap();
    for start in (0..DOCUMENTS).step_by(50) {
        db.transaction(|tx| {
            for value in start..(start + 50).min(DOCUMENTS) {
                tx.insert(
                    "c",
                    &format!("doc-{value:04}"),
                    &json!({"age": value, "score": value}),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
}

// Runs `exercise` against a seeded collection on every writable backend.
fn on_backends(exercise: impl Fn(&DocumentDatabase<'_>, &str)) {
    for backend in ["ordinary", "checkpoint", "retained"] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        seed(&DocumentDatabase::new(&store));
        match backend {
            "ordinary" => exercise(&DocumentDatabase::new(&store), backend),
            "checkpoint" => {
                let digest = store.checkpoint_to(&checkpoint).unwrap();
                drop(store);
                let store =
                    retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
                exercise(&DocumentDatabase::with_checkpoint_store(&store), backend);
            }
            _ => {
                let catalog = root.path().join("catalog");
                ManagedRetainedStore::copy_from_store(store, &source, &catalog, EXPORT, LIMITS)
                    .unwrap();
                let mut retained = ManagedRetainedStore::open(&catalog, LIMITS).unwrap();
                exercise(
                    &DocumentDatabase::with_managed_retained_store(&mut retained),
                    backend,
                );
            }
        }
    }
}

#[test]
fn estimated_choice_reads_fewer_entries_and_matches_the_scan() {
    on_backends(|db, backend| {
        let request = query(broad_and_narrow(), DOCUMENTS as usize);
        let scan = db.query("c", &request).unwrap();
        assert_eq!(scan.documents.len(), 10, "{backend}");
        // The default order probes the broad index and reads every document.
        let default = db.query_planned("c", &request).unwrap();
        assert_eq!(default.access, range("age"), "{backend}");
        assert_eq!(default.result.documents, scan.documents);
        assert_eq!(default.result.plan.scanned, DOCUMENTS as usize);
        // A tight candidate budget makes that default path fail.
        let tight = query(broad_and_narrow(), 20);
        let error = db.query_planned("c", &tight).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{backend}");
        // The estimated choice probes the narrow index within that budget.
        let (planned, estimates) = db
            .query_planned_estimated("c", &tight, entries(1_000))
            .unwrap();
        assert_eq!(planned.access, range("score"), "{backend}");
        assert_eq!(planned.result.documents, scan.documents);
        assert_eq!(planned.result.plan.scanned, 10);
        let counts = estimates
            .candidates
            .iter()
            .map(|estimate| (estimate.index.as_str(), estimate.entries))
            .collect::<Vec<_>>();
        assert_eq!(counts, [("age", None), ("score", Some(10))], "{backend}");
        // Age is counted to its 256-entry cap plus one; score to its 10.
        assert_eq!(estimates.entries_read, 257 + 10, "{backend}");
        // Two 256-entry pages for age, one page for score.
        assert_eq!(estimates.scans, 3, "{backend}");
        let total = estimates.entries_read + planned.result.plan.scanned;
        assert!(total < default.result.plan.scanned, "{backend}: {total}");
        // The explanation chooses the same path and reports the same counts.
        let (explained, explained_estimates) = db
            .explain_query_estimated("c", &tight, entries(1_000))
            .unwrap();
        assert_eq!(explained.access, planned.access);
        assert_eq!(explained_estimates, estimates);
        assert_eq!(db.explain_query("c", &tight).unwrap().access, range("age"));
    });
}

#[test]
fn the_total_estimate_budget_is_never_exceeded_and_falls_back_in_order() {
    on_backends(|db, backend| {
        let request = query(broad_and_narrow(), DOCUMENTS as usize);
        let scan = db.query("c", &request).unwrap();
        for budget in [0, 1, 5, 10, 11, 256, 257, 258, 267, 268, 10_000] {
            let (planned, estimates) = db
                .query_planned_estimated("c", &request, entries(budget))
                .unwrap();
            assert!(estimates.entries_read <= budget, "{backend} {budget}");
            assert_eq!(
                planned.result.documents, scan.documents,
                "{backend} {budget}"
            );
            // Only a budget that proves age exceeds score switches paths.
            let expected = if budget >= 268 { "score" } else { "age" };
            assert_eq!(planned.access, range(expected), "{backend} {budget}");
        }
    });
}

#[test]
fn staged_matching_rows_are_counted_and_returned() {
    on_backends(|db, backend| {
        // Narrow both ranges: age >= 395 has five rows, score >= 390 ten.
        let filter = DocumentFilter::And(vec![at_least("age", 395), at_least("score", 390)]);
        let request = query(filter, 64);
        let (planned, _) = db
            .query_planned_estimated("c", &request, entries(1_000))
            .unwrap();
        assert_eq!(planned.access, range("age"), "{backend}");
        db.transaction(|tx| {
            // Twenty staged rows widen the age range without matching, and
            // three staged rows match both predicates.
            for value in 0..20 {
                tx.insert(
                    "c",
                    &format!("wide-{value:02}"),
                    &json!({"age": 500, "score": 0}),
                )?;
            }
            for value in 0..3 {
                tx.insert(
                    "c",
                    &format!("new-{value}"),
                    &json!({"age": 600, "score": 600}),
                )?;
            }
            let (planned, estimates) = tx.query_planned_estimated("c", &request, entries(1_000))?;
            assert_eq!(planned.access, range("score"), "{backend}");
            let counts = estimates
                .candidates
                .iter()
                .map(|estimate| estimate.entries)
                .collect::<Vec<_>>();
            assert_eq!(counts, [Some(28), Some(13)], "{backend}");
            let scan = tx.query("c", &query(request.filter.clone(), 1_000))?;
            assert_eq!(scan.documents.len(), 8, "{backend}");
            assert_eq!(planned.result.documents, scan.documents, "{backend}");
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn estimate_reads_count_against_transaction_read_limits() {
    on_backends(|db, backend| {
        let request = query(broad_and_narrow(), 20);
        let limits = |units| DocumentReadLimits {
            max_read_units: units,
            max_read_bytes: 1 << 20,
        };
        // Enough for the chosen narrow probe alone is not enough once the
        // unchosen broad candidate has also been counted.
        let narrow = db.transaction_with_read_limits(
            limits(200),
            DocumentTransactionMode::Conservative,
            None,
            None,
            |tx| tx.query_planned("c", &query(at_least("score", 390), 20)),
        );
        assert!(narrow.is_ok(), "{backend}: {narrow:?}");
        let estimated = db.transaction_with_read_limits(
            limits(200),
            DocumentTransactionMode::Conservative,
            None,
            None,
            |tx| tx.query_planned_estimated("c", &request, entries(1_000)),
        );
        assert!(
            estimated.is_err(),
            "{backend}: estimate reads must be charged"
        );
        // A smaller total estimate budget keeps within the same read limit,
        // falling back to the default path, which the candidate limit rejects.
        let bounded = db.transaction_with_read_limits(
            limits(200),
            DocumentTransactionMode::Conservative,
            None,
            None,
            |tx| tx.query_planned_estimated("c", &request, entries(16)),
        );
        assert!(bounded.is_err(), "{backend}");
    });
}

fn conflict_case(db: &DocumentDatabase<'_>) {
    let run = |estimated: bool| {
        let ready = Barrier::new(2);
        let changed = Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                ready.wait();
                // Inside the broad age range only; it matches no query.
                let inserted = db.insert(
                    "c",
                    &format!("outside-{estimated}"),
                    &json!({"age": 1, "score": 0}),
                );
                changed.wait();
                inserted.unwrap();
            });
            db.transaction_with_read_set(|tx| {
                let planned = if estimated {
                    let (planned, estimates) = tx.query_planned_estimated(
                        "c",
                        &query(broad_and_narrow(), 20),
                        entries(1_000),
                    )?;
                    assert!(estimates.entries_read > 0);
                    planned
                } else {
                    tx.query_planned("c", &query(at_least("score", 390), 20))?
                };
                assert_eq!(planned.access, range("score"));
                ready.wait();
                changed.wait();
                tx.put_raw(format!("marker-{estimated}").as_bytes(), b"written")
            })
        })
    };
    // Reading only the narrow range does not conflict with the insert.
    run(false).unwrap();
    // Counting the unchosen broad candidate does, conservatively.
    assert_eq!(run(true).unwrap_err().kind(), ErrorKind::WouldBlock);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"marker-true")).unwrap(),
        None
    );
}

#[test]
fn estimate_reads_join_the_read_set_and_can_conflict_on_the_unchosen_index() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&source).unwrap();
    seed(&DocumentDatabase::new(&store));
    conflict_case(&DocumentDatabase::new(&store));
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    db.transaction(|tx| {
        tx.delete("c", "outside-false")?;
        tx.delete("c", "outside-true")?;
        tx.delete_raw(b"marker-false")
    })
    .unwrap();
    conflict_case(&db);
}

#[test]
fn a_single_eligible_range_needs_no_estimate() {
    on_backends(|db, backend| {
        let request = query(
            DocumentFilter::And(vec![at_least("score", 390), below("score", 400)]),
            20,
        );
        let (planned, estimates) = db
            .query_planned_estimated("c", &request, entries(1_000))
            .unwrap();
        assert_eq!(planned.access, range("score"), "{backend}");
        assert!(estimates.candidates.is_empty());
        assert_eq!(estimates.entries_read, 0);
    });
}

// Sixty-four absent, non-consecutive scores give a membership candidate
// with one seek per value; every seek returns nothing.
fn many_empty_probes(lower: i64) -> DocumentFilter {
    DocumentFilter::And(vec![
        at_least("age", lower),
        at_least("score", 1_000),
        DocumentFilter::In {
            path: vec!["score".into()],
            values: (0..64).map(|value| json!(1_000 + 2 * value)).collect(),
        },
    ])
}

#[test]
fn empty_seeks_are_charged_to_the_scan_budget() {
    on_backends(|db, backend| {
        let request = query(many_empty_probes(0), DOCUMENTS as usize);
        let scan = db.query("c", &request).unwrap();
        assert!(scan.documents.is_empty());
        // The broad age range is counted in two scans; the membership
        // candidate's 64 empty seeks then exhaust a small scan budget, so
        // it is inconclusive and the default order is kept.
        let small = DocumentEstimateLimits {
            max_entries: 1_000,
            max_scans: 16,
        };
        let (planned, estimates) = db.query_planned_estimated("c", &request, small).unwrap();
        assert_eq!(estimates.scans, 16, "{backend}");
        assert_eq!(estimates.entries_read, 257, "{backend}");
        let counts = estimates
            .candidates
            .iter()
            .map(|estimate| estimate.entries)
            .collect::<Vec<_>>();
        assert_eq!(counts, [None, None], "{backend}");
        assert_eq!(planned.access, range("age"), "{backend}");
        assert_eq!(planned.result.documents, scan.documents);
        // With room for every seek the empty membership candidate wins.
        let large = DocumentEstimateLimits {
            max_entries: 1_000,
            max_scans: 200,
        };
        let (planned, estimates) = db.query_planned_estimated("c", &request, large).unwrap();
        assert_eq!(estimates.scans, 2 + 64, "{backend}");
        assert_eq!(estimates.candidates[1].entries, Some(0), "{backend}");
        assert_ne!(planned.access, range("age"), "{backend}");
        assert_eq!(planned.result.plan.scanned, 0);
        assert!(planned.result.documents.is_empty());
    });
}

#[test]
fn an_empty_first_candidate_stops_estimation() {
    on_backends(|db, backend| {
        // No age is at least 1,000: one empty scan proves the first
        // candidate reads nothing, so the 64-seek candidate is never counted.
        let request = query(many_empty_probes(1_000), 10);
        let (planned, estimates) = db
            .query_planned_estimated("c", &request, DocumentEstimateLimits::default())
            .unwrap();
        assert_eq!(planned.access, range("age"), "{backend}");
        assert_eq!(
            (estimates.entries_read, estimates.scans),
            (0, 1),
            "{backend}"
        );
        let counts = estimates
            .candidates
            .iter()
            .map(|estimate| estimate.entries)
            .collect::<Vec<_>>();
        assert_eq!(counts, [Some(0), None], "{backend}");
        assert!(planned.result.documents.is_empty());
    });
}
