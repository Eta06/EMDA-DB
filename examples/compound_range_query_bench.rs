//! Single-client, read-only comparison of a compound equality-prefix range query.
use database_engine::{
    CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, IndexDefinition, IndexQuery, IndexValue, Store,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Row = (String, Value);
const WARMUP: usize = 10;

fn document(number: usize, matches: usize, total: usize) -> Row {
    (
        format!("id-{number:05}"),
        json!({
            "tenant": if number < matches || number % 3 == 0 { "target" } else { "other" },
            "number": total - 1 - number,
            "payload": "small document value",
        }),
    )
}
fn prototype_rows(
    db: &DocumentDatabase<'_>,
    query: &DocumentQuery,
    planned: bool,
) -> Result<Vec<Row>> {
    let result = if planned {
        let planned = db.query_planned("docs", query)?;
        if planned.access
            != (DocumentQueryAccess::CompoundRangeIndex {
                index: "tenant_number".into(),
                equality_fields: 1,
            })
        {
            return Err("unexpected prototype planner access path".into());
        }
        planned.result
    } else {
        db.query("docs", query)?
    };
    Ok(result
        .documents
        .into_iter()
        .map(|row| (row.id, row.document))
        .collect())
}
fn sqlite_rows(connection: &Connection, indexed: bool, threshold: i64) -> Result<Vec<Row>> {
    let sql = if indexed {
        "SELECT id, body FROM docs INDEXED BY docs_tenant_number WHERE json_extract(body, '$.tenant') = ?1 AND json_extract(body, '$.number') >= ?2 ORDER BY id"
    } else {
        "SELECT id, body FROM docs INDEXED BY sqlite_autoindex_docs_1 WHERE json_extract(body, '$.tenant') = ?1 AND json_extract(body, '$.number') >= ?2 ORDER BY id"
    };
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map(params!["target", threshold], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.map(|row| {
        let (id, body) = row?;
        Ok((id, serde_json::from_str(&body)?))
    })
    .collect()
}
fn peak_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with("VmHWM:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err("usage: compound_range_query_bench DIRECTORY DOCUMENTS MATCHES CYCLES".into());
    }
    let parent = Path::new(&args[1]);
    let documents: usize = args[2].parse()?;
    let matches: usize = args[3].parse()?;
    let cycles: usize = args[4].parse()?;
    if !(1..=1024).contains(&documents)
        || !(1..=256).contains(&matches)
        || matches > documents
        || cycles == 0
    {
        return Err("invalid document, match or cycle count".into());
    }
    let root = tempfile::tempdir_in(parent)?;
    let source = root.path().join("store");
    let sqlite_path = root.path().join("sqlite.db");
    let store = Store::open(&source)?;
    let db = DocumentDatabase::new(&store);
    db.create_collection("docs", CollectionSchema::default())?;
    db.create_index(
        "docs",
        IndexDefinition {
            name: "tenant_number".into(),
            fields: vec![vec!["tenant".into()], vec!["number".into()]],
            unique: false,
            partial: None,
        },
    )?;
    let mut sqlite = Connection::open(&sqlite_path)?;
    let mode: String = sqlite.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    sqlite.pragma_update(None, "synchronous", "FULL")?;
    let synchronous: i64 = sqlite.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if mode != "wal" || synchronous != 2 {
        return Err("SQLite WAL/FULL required".into());
    }
    sqlite.execute_batch("CREATE TABLE docs(id TEXT PRIMARY KEY, body TEXT NOT NULL CHECK(json_valid(body))) WITHOUT ROWID; CREATE INDEX docs_tenant_number ON docs(json_extract(body,'$.tenant'),json_extract(body,'$.number')); ")?;
    let index_plan: String = sqlite.query_row(
        "EXPLAIN QUERY PLAN SELECT id,body FROM docs INDEXED BY docs_tenant_number WHERE json_extract(body,'$.tenant')=?1 AND json_extract(body,'$.number')>=?2 ORDER BY id",
        params!["target", (documents - matches) as i64], |row| row.get(3),
    )?;
    if !index_plan.contains("USING INDEX docs_tenant_number") {
        return Err("SQLite did not select its compound range index".into());
    }
    let full_index_plan = {
        let mut statement = sqlite.prepare("EXPLAIN QUERY PLAN SELECT id,body FROM docs INDEXED BY docs_tenant_number WHERE json_extract(body,'$.tenant')=?1 AND json_extract(body,'$.number')>=?2 ORDER BY id")?;
        let rows = statement.query_map(params!["target", (documents - matches) as i64], |row| {
            row.get::<_, String>(3)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };
    let scan_plan: String = sqlite.query_row(
        "EXPLAIN QUERY PLAN SELECT id,body FROM docs INDEXED BY sqlite_autoindex_docs_1 WHERE json_extract(body,'$.tenant')=?1 AND json_extract(body,'$.number')>=?2 ORDER BY id",
        params!["target", (documents - matches) as i64],
        |row| row.get(3),
    )?;
    if !scan_plan.contains("SCAN docs") || scan_plan.contains("USING INDEX") {
        return Err("SQLite scan did not use the expected access path".into());
    }
    let sqlite_version: String =
        sqlite.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    let all = (0..documents)
        .map(|n| document(n, matches, documents))
        .collect::<Vec<_>>();
    for chunk in all.chunks(32) {
        db.transaction(|tx| {
            for (id, doc) in chunk {
                tx.insert("docs", id, doc)?;
            }
            Ok(())
        })?;
        let transaction = sqlite.transaction()?;
        for (id, doc) in chunk {
            transaction.execute(
                "INSERT INTO docs(id,body) VALUES(?1,?2)",
                params![id, serde_json::to_string(doc)?],
            )?;
        }
        transaction.commit()?;
    }
    let expected = all[..matches].to_vec();
    let query = DocumentQuery {
        filter: DocumentFilter::And(vec![
            DocumentFilter::Equal {
                path: vec!["tenant".into()],
                value: json!("target"),
            },
            DocumentFilter::Compare {
                path: vec!["number".into()],
                comparison: Comparison::GreaterOrEqual,
                value: IndexValue::Integer((documents - matches) as i64),
            },
        ]),
        max_scanned: 1024,
        limit: 256,
        ..Default::default()
    };
    let labels = [
        "prototype_scan",
        "prototype_planned",
        "sqlite_scan",
        "sqlite_index",
    ];
    let mut samples = BTreeMap::<&str, Vec<u128>>::new();
    for cycle in 0..cycles + WARMUP {
        for offset in 0..labels.len() {
            let variant = (cycle + offset) % labels.len();
            let start = Instant::now();
            let rows = match variant {
                0 => prototype_rows(&db, &query, false)?,
                1 => prototype_rows(&db, &query, true)?,
                2 => sqlite_rows(&sqlite, false, (documents - matches) as i64)?,
                _ => sqlite_rows(&sqlite, true, (documents - matches) as i64)?,
            };
            let elapsed = start.elapsed().as_nanos();
            if rows != expected {
                return Err(format!("{} produced incorrect rows", labels[variant]).into());
            }
            if cycle >= WARMUP {
                samples.entry(labels[variant]).or_default().push(elapsed);
            }
        }
    }
    drop(sqlite);
    drop(store);
    let reopened = Store::open(&source)?;
    let db = DocumentDatabase::new(&reopened);
    let mut index_query = IndexQuery {
        limit: 256,
        ..Default::default()
    };
    let mut found = BTreeMap::new();
    loop {
        let page = db.query_index("docs", "tenant_number", &index_query)?;
        if page.documents.is_empty() {
            break;
        }
        for row in page.documents {
            found.insert(row.id, row.document);
        }
        index_query.after = page.next;
    }
    if found != all.iter().cloned().collect::<BTreeMap<_, _>>() {
        return Err("prototype reopen verification failed".into());
    }
    let sqlite = Connection::open(&sqlite_path)?;
    let integrity: String = sqlite.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err("SQLite integrity check failed".into());
    }
    let mut statement = sqlite.prepare("SELECT id,body FROM docs ORDER BY id")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let restored = rows
        .map(|row| {
            let (id, body) = row?;
            Ok((id, serde_json::from_str(&body)?))
        })
        .collect::<Result<Vec<Row>>>()?;
    if restored != all {
        return Err("SQLite reopen verification failed".into());
    }
    println!(
        "{}",
        json!({
            "documents":documents,"matches":matches,"cycles":cycles,"warmup_cycles":WARMUP,
        "samples_ns":samples,"sqlite_query_plan":index_plan,"sqlite_full_plan":full_index_plan,"sqlite_scan_plan":scan_plan,
        "sqlite_version":sqlite_version,"sqlite_journal":mode,
            "sqlite_synchronous":synchronous,"peak_rss_kib":peak_rss_kib(),
            "reopen_verified":true,
        })
    );
    Ok(())
}
