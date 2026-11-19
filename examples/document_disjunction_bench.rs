//! Single-client indexed OR comparison with a collection scan and SQLite JSON.
use database_engine::{
    CollectionSchema, Comparison, DocumentDatabase, DocumentEntry, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, IndexDefinition, IndexValue, PartialFilter, Store,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{error::Error, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const DOCUMENTS: i64 = 1024;
const SAMPLES: usize = 300;
const WARMUP: usize = 30;
const FILTER: &str = "SELECT id,body FROM docs WHERE
    (json_extract(body,'$.active')=1 AND json_extract(body,'$.code')=?1)
    OR (json_extract(body,'$.score') BETWEEN ?2 AND ?3)
    ORDER BY id";

fn document(number: i64) -> DocumentEntry {
    DocumentEntry {
        id: format!("doc-{number:04}"),
        document: json!({"code": number, "active": number % 2 == 0, "score": number}),
    }
}

fn equal(path: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: vec![path.into()],
        value,
    }
}

fn compare_score(comparison: Comparison, value: i64) -> DocumentFilter {
    DocumentFilter::Compare {
        path: vec!["score".into()],
        comparison,
        value: IndexValue::Integer(value),
    }
}

fn setup(path: &Path, sqlite: &mut Connection) -> Result<()> {
    let store = Store::open(path)?;
    let db = DocumentDatabase::new(&store);
    db.create_collection("docs", CollectionSchema::default())?;
    db.create_index(
        "docs",
        IndexDefinition {
            name: "code".into(),
            fields: vec![vec!["code".into()]],
            unique: true,
            partial: Some(PartialFilter::Equals {
                path: vec!["active".into()],
                value: IndexValue::Boolean(true),
            }),
        },
    )?;
    db.create_index(
        "docs",
        IndexDefinition {
            name: "score".into(),
            fields: vec![vec!["score".into()]],
            unique: false,
            partial: None,
        },
    )?;

    let mode: String = sqlite.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    sqlite.pragma_update(None, "synchronous", "FULL")?;
    let synchronous: i64 = sqlite.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if mode != "wal" || synchronous != 2 {
        return Err("SQLite WAL/FULL required".into());
    }
    sqlite.execute_batch(
        "CREATE TABLE docs(id TEXT PRIMARY KEY, body TEXT NOT NULL CHECK(json_valid(body))) WITHOUT ROWID;
         CREATE UNIQUE INDEX code ON docs(json_extract(body,'$.code')) WHERE json_extract(body,'$.active')=1;
         CREATE INDEX score ON docs(json_extract(body,'$.score'));",
    )?;
    for start in (0..DOCUMENTS).step_by(32) {
        let entries = (start..start + 32).map(document).collect::<Vec<_>>();
        db.transaction(|tx| {
            for entry in &entries {
                tx.insert("docs", &entry.id, &entry.document)?;
            }
            Ok(())
        })?;
        let tx = sqlite.transaction()?;
        for entry in entries {
            tx.prepare_cached("INSERT INTO docs(id,body) VALUES(?1,?2)")?
                .execute(params![entry.id, entry.document.to_string()])?;
        }
        tx.commit()?;
    }
    Ok(())
}

fn sqlite_query(sqlite: &Connection, code: i64, low: i64, high: i64) -> Result<Vec<DocumentEntry>> {
    let mut stmt = sqlite.prepare_cached(FILTER)?;
    let mut rows = stmt.query(params![code, low, high])?;
    let mut entries = Vec::new();
    while let Some(row) = rows.next()? {
        let body: String = row.get(1)?;
        entries.push(DocumentEntry {
            id: row.get(0)?,
            document: serde_json::from_str(&body)?,
        });
    }
    Ok(entries)
}

fn summary(samples: &[u128]) -> Value {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let percentile = |p: usize| ordered[(p * ordered.len()).div_ceil(100) - 1] as f64 / 1000.0;
    json!({"p50_us": percentile(50), "p95_us": percentile(95), "p99_us": percentile(99),
           "samples_ns": samples})
}

fn main() -> Result<()> {
    let parent = std::env::args_os()
        .nth(1)
        .ok_or("usage: document_disjunction_bench <existing-directory>")?;
    let fixture = tempfile::tempdir_in(parent)?;
    let source = fixture.path().join("store");
    let sqlite_path = fixture.path().join("sqlite.db");
    let mut sqlite = Connection::open(&sqlite_path)?;
    setup(&source, &mut sqlite)?;
    drop(sqlite);

    let store = Store::open(&source)?;
    let db = DocumentDatabase::new(&store);
    let sqlite = Connection::open(&sqlite_path)?;
    sqlite.pragma_update(None, "synchronous", "FULL")?;
    let plan = sqlite
        .prepare(&format!("EXPLAIN QUERY PLAN {FILTER}"))?
        .query_map([0, 0, 2], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !["MULTI-INDEX OR", "USING INDEX code", "USING INDEX score"]
        .into_iter()
        .all(|expected| plan.iter().any(|line| line.contains(expected)))
    {
        return Err(format!("unexpected SQLite plan: {plan:?}").into());
    }
    let access = DocumentQueryAccess::DisjunctionIndex {
        indexes: vec!["code".into(), "score".into()],
        probes: 2,
    };
    let mut times = [Vec::new(), Vec::new(), Vec::new()];
    let mut visits = 0;
    for sample in 0..SAMPLES + WARMUP {
        let low = (sample as i64 * 13) % (DOCUMENTS - 2);
        let high = low + 2;
        let code = if sample % 3 == 0 {
            low
        } else {
            (low + 271) % DOCUMENTS
        };
        let query = DocumentQuery {
            filter: DocumentFilter::Or(vec![
                DocumentFilter::And(vec![
                    equal("active", json!(true)),
                    equal("code", json!(code)),
                ]),
                DocumentFilter::And(vec![
                    compare_score(Comparison::GreaterOrEqual, low),
                    compare_score(Comparison::LessOrEqual, high),
                ]),
            ]),
            max_scanned: DOCUMENTS as usize,
            ..Default::default()
        };
        let expected = (0..DOCUMENTS)
            .filter(|&n| (low..=high).contains(&n) || (n == code && n % 2 == 0))
            .map(document)
            .collect::<Vec<_>>();
        // Rotate among paths so the same engine does not always read first.
        for step in 0..3 {
            let path = (sample + step) % 3;
            let start = Instant::now();
            let actual = match path {
                0 => db.query("docs", &query)?.documents,
                1 => {
                    let planned = db.query_planned("docs", &query)?;
                    if planned.access != access {
                        return Err("document engine did not pick the disjunction".into());
                    }
                    if sample >= WARMUP {
                        visits += planned.result.plan.scanned;
                    }
                    planned.result.documents
                }
                _ => sqlite_query(&sqlite, code, low, high)?,
            };
            let elapsed = start.elapsed().as_nanos();
            if actual != expected {
                return Err(
                    format!("query result mismatch for path {path} at sample {sample}").into(),
                );
            }
            if sample >= WARMUP {
                times[path].push(elapsed);
            }
        }
    }
    println!(
        "{}",
        json!({
            "documents": DOCUMENTS, "warmup": WARMUP, "samples": SAMPLES,
            "candidate_visits": visits, "sqlite_version": rusqlite::version(),
            "sqlite_access": plan, "collection_scan": summary(&times[0]),
            "indexed_disjunction": summary(&times[1]), "sqlite": summary(&times[2]),
        })
    );
    Ok(())
}
