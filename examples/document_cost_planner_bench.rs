//! Disposable comparison of the default and opt-in estimated document
//! planners on a matched query where the default range order probes a broad
//! index. Reports logical work and elapsed time for each path; results are
//! checked against a complete collection scan.
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentEstimateLimits,
    DocumentFilter, DocumentQuery, IndexDefinition, IndexValue, Store,
};
use serde_json::json;
use std::error::Error;
use std::path::Path;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

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

// The query API admits at most this many candidates per query.
const MAX_SCANNED: usize = 1024;

fn measure(db: &DocumentDatabase<'_>, documents: i64, selective: i64) -> Result<serde_json::Value> {
    // `age` is defined first and covers every document; `score` matches
    // `selective` documents.
    let filter = DocumentFilter::And(vec![
        at_least("age", 0),
        at_least("score", documents - selective),
    ]);
    let request = DocumentQuery {
        filter,
        max_scanned: (documents as usize).min(MAX_SCANNED),
        ..Default::default()
    };
    // Exact expected IDs, checked against a complete scan where one fits.
    let expected = (documents - selective..documents)
        .map(|value| format!("doc-{value:07}"))
        .collect::<Vec<_>>();
    if documents as usize <= MAX_SCANNED {
        let scan = db.query("c", &request)?;
        let ids = scan
            .documents
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<Vec<_>>();
        if ids != expected {
            return Err("collection scan differs from the expected documents".into());
        }
    }
    let ids = |documents: &[database_engine::DocumentEntry]| {
        documents
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<Vec<_>>()
    };
    let start = Instant::now();
    let default = db.query_planned("c", &request);
    let default_us = start.elapsed().as_micros();
    let default = match default {
        Ok(planned) => {
            if ids(&planned.result.documents) != expected {
                return Err("default planned result differs".into());
            }
            json!({
                "access": format!("{:?}", planned.access),
                "candidates_read": planned.result.plan.scanned,
                "elapsed_us": default_us,
            })
        }
        Err(error) => json!({"error": error.to_string(), "elapsed_us": default_us}),
    };
    let start = Instant::now();
    let (estimated, estimates) =
        db.query_planned_estimated("c", &request, DocumentEstimateLimits::default())?;
    let estimated_us = start.elapsed().as_micros();
    if ids(&estimated.result.documents) != expected {
        return Err("estimated planned result differs".into());
    }
    Ok(json!({
        "matched": expected.len(),
        "max_scanned": request.max_scanned,
        "default": default,
        "estimated": {
            "access": format!("{:?}", estimated.access),
            "candidates_read": estimated.result.plan.scanned,
            "estimate_entries_read": estimates.entries_read,
            "estimate_scans": estimates.scans,
            "total_entries_and_candidates": estimates.entries_read + estimated.result.plan.scanned,
            "elapsed_us": estimated_us,
        },
    }))
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [parent, documents, selective] = args.as_slice() else {
        return Err(
            "usage: document_cost_planner_bench <directory> <documents> <selective>".into(),
        );
    };
    let (documents, selective): (i64, i64) = (documents.parse()?, selective.parse()?);
    if !Path::new(parent).is_dir()
        || !(1..=1_000_000).contains(&documents)
        || !(1..=documents).contains(&selective)
    {
        return Err("invalid directory or counts".into());
    }
    let root = tempfile::Builder::new()
        .prefix(".document-cost-planner-bench-")
        .tempdir_in(parent)?;
    let source = root.path().join("store");
    let store = Store::open(&source)?;
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())?;
    db.create_index("c", index("age"))?;
    db.create_index("c", index("score"))?;
    for start in (0..documents).step_by(50) {
        db.transaction(|tx| {
            for value in start..(start + 50).min(documents) {
                tx.insert(
                    "c",
                    &format!("doc-{value:07}"),
                    &json!({"age": value, "score": value}),
                )?;
            }
            Ok(())
        })?;
    }
    let ordinary = measure(&db, documents, selective)?;
    let digest = store.checkpoint_to(root.path().join("checkpoint"))?;
    drop(store);
    let checkpoint = CheckpointStore::open(&source, root.path().join("checkpoint"), &digest)?;
    let checkpointed = measure(
        &DocumentDatabase::with_checkpoint_store(&checkpoint),
        documents,
        selective,
    )?;
    println!(
        "{}",
        json!({
            "documents": documents,
            "selective": selective,
            "ordinary": ordinary,
            "checkpoint": checkpointed,
        })
    );
    Ok(())
}
