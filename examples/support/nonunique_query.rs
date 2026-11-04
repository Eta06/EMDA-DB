//! Shared verified workload for nonunique equality and membership comparisons.
use database_engine::{
    CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery, DocumentQueryAccess,
    IndexDefinition, IndexQuery, Store,
};
use rusqlite::{Connection, params, params_from_iter};
use serde_json::{Value, json};
use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Row = (String, Value);
const WARMUP: usize = 10;

fn document(number: usize, matches: usize, membership: bool) -> Row {
    let group = if number >= matches {
        "other"
    } else if membership && number % 2 == 0 {
        "target-a"
    } else if membership {
        "target-b"
    } else {
        "target"
    };
    (
        format!("id-{number:05}"),
        json!({
            "group": group,
            "number": number,
            "payload": "small document value",
        }),
    )
}
fn prototype_rows(
    db: &DocumentDatabase<'_>,
    query: &DocumentQuery,
    planned: bool,
    membership: bool,
) -> Result<Vec<Row>> {
    let result = if planned {
        let planned = db.query_planned("docs", query)?;
        let expected = if membership {
            DocumentQueryAccess::MembershipIndex {
                index: "group".into(),
                probes: 2,
            }
        } else {
            DocumentQueryAccess::EqualityIndex {
                index: "group".into(),
            }
        };
        if planned.access != expected {
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
fn sqlite_sql(indexed: bool, membership: bool) -> &'static str {
    match (indexed, membership) {
        (true, false) => {
            "SELECT id, body FROM docs WHERE json_extract(body, '$.group') = ?1 ORDER BY id"
        }
        (false, false) => {
            "SELECT id, body FROM docs INDEXED BY sqlite_autoindex_docs_1 WHERE json_extract(body, '$.group') = ?1 ORDER BY id"
        }
        (true, true) => {
            "SELECT id, body FROM docs WHERE json_extract(body, '$.group') IN (?1, ?2) ORDER BY id"
        }
        (false, true) => {
            "SELECT id, body FROM docs INDEXED BY sqlite_autoindex_docs_1 WHERE json_extract(body, '$.group') IN (?1, ?2) ORDER BY id"
        }
    }
}

fn sqlite_values(membership: bool) -> Vec<&'static str> {
    if membership {
        vec!["target-a", "target-b"]
    } else {
        vec!["target"]
    }
}

fn sqlite_rows(connection: &Connection, indexed: bool, membership: bool) -> Result<Vec<Row>> {
    let mut statement = connection.prepare_cached(sqlite_sql(indexed, membership))?;
    let rows = statement.query_map(params_from_iter(sqlite_values(membership)), |row| {
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
pub fn run(membership: bool) -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err("usage: query_bench DIRECTORY DOCUMENTS MATCHES CYCLES".into());
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
            name: "group".into(),
            fields: vec![vec!["group".into()]],
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
    sqlite.execute_batch("CREATE TABLE docs(id TEXT PRIMARY KEY, body TEXT NOT NULL CHECK(json_valid(body))) WITHOUT ROWID; CREATE INDEX docs_group ON docs(json_extract(body,'$.group')); ")?;
    let explain = format!("EXPLAIN QUERY PLAN {}", sqlite_sql(true, membership));
    let full_index_plan = {
        let mut statement = sqlite.prepare(&explain)?;
        let rows = statement.query_map(params_from_iter(sqlite_values(membership)), |row| {
            row.get::<_, String>(3)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };
    let index_plan = full_index_plan
        .first()
        .ok_or("SQLite returned no index plan")?
        .clone();
    if !index_plan.contains("USING INDEX docs_group") {
        return Err("SQLite did not select its group index".into());
    }
    let scan_plan: String = sqlite.query_row(
        &format!("EXPLAIN QUERY PLAN {}", sqlite_sql(false, membership)),
        params_from_iter(sqlite_values(membership)),
        |row| row.get(3),
    )?;
    if !scan_plan.contains("SCAN docs") || scan_plan.contains("USING INDEX") {
        return Err("SQLite scan did not use the expected access path".into());
    }
    let sqlite_version: String =
        sqlite.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    let all = (0..documents)
        .map(|n| document(n, matches, membership))
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
        filter: if membership {
            DocumentFilter::In {
                path: vec!["group".into()],
                values: vec![json!("target-a"), json!("target-b")],
            }
        } else {
            DocumentFilter::Equal {
                path: vec!["group".into()],
                value: json!("target"),
            }
        },
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
                0 => prototype_rows(&db, &query, false, membership)?,
                1 => prototype_rows(&db, &query, true, membership)?,
                2 => sqlite_rows(&sqlite, false, membership)?,
                _ => sqlite_rows(&sqlite, true, membership)?,
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
        let page = db.query_index("docs", "group", &index_query)?;
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
        "samples_ns":samples,"sqlite_query_plan":index_plan,"sqlite_full_query_plan":full_index_plan,"sqlite_scan_plan":scan_plan,
        "sqlite_version":sqlite_version,"sqlite_journal":mode,
            "predicate":if membership { "membership" } else { "equality" },
            "sqlite_synchronous":synchronous,"peak_rss_kib":peak_rss_kib(),
            "reopen_verified":true,
        })
    );
    Ok(())
}
