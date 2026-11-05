//! Compare a bounded same-element array scan with SQLite JSON1.
use database_engine::{
    CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery, IndexValue,
    Store,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Row = (String, Value);
const SQL: &str = "SELECT d.id, d.body FROM docs AS d WHERE EXISTS (SELECT 1 FROM json_each(d.body, '$.items') AS e WHERE json_extract(e.value, '$.color') = 'red' AND json_extract(e.value, '$.qty') >= 5) ORDER BY d.id";
const WARMUP: usize = 10;
fn prototype(db: &DocumentDatabase<'_>, q: &DocumentQuery) -> Result<Vec<Row>> {
    Ok(db
        .query("docs", q)?
        .documents
        .into_iter()
        .map(|row| (row.id, row.document))
        .collect())
}
fn sqlite(db: &Connection) -> Result<Vec<Row>> {
    let mut statement = db.prepare_cached(SQL)?;
    let rows = statement.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    rows.map(|r| {
        let (id, body) = r?;
        Ok((id, serde_json::from_str(&body)?))
    })
    .collect()
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 5 {
        return Err("usage: array_element_bench DIRECTORY DOCUMENTS MATCHES CYCLES".into());
    }
    let (n, matches, cycles) = (
        args[2].parse::<usize>()?,
        args[3].parse::<usize>()?,
        args[4].parse::<usize>()?,
    );
    if !(1..=1024).contains(&n) || matches > n || matches > 256 || cycles == 0 {
        return Err("invalid workload limits".into());
    }
    let root = tempfile::tempdir_in(Path::new(&args[1]))?;
    let source = root.path().join("store");
    let sqlite_path = root.path().join("sqlite.db");
    let store = Store::open(&source)?;
    let db = DocumentDatabase::new(&store);
    db.create_collection("docs", CollectionSchema::default())?;
    let mut sql = Connection::open(&sqlite_path)?;
    let mode: String = sql.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    sql.pragma_update(None, "synchronous", "FULL")?;
    let sync: i64 = sql.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if mode != "wal" || sync != 2 {
        return Err("SQLite WAL/FULL required".into());
    }
    sql.execute_batch("CREATE TABLE docs(id TEXT PRIMARY KEY, body TEXT NOT NULL CHECK(json_valid(body))) WITHOUT ROWID")?;
    let version: String = sql.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    let plan = {
        let mut statement = sql.prepare(&format!("EXPLAIN QUERY PLAN {SQL}"))?;
        let rows = statement.query_map([], |row| row.get::<_, String>(3))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };
    if !plan.iter().any(|p| p.contains("SCAN d")) {
        return Err("SQLite did not use a table scan".into());
    }
    let all = (0..n).map(|i| (format!("id-{i:05}"), json!({"items":[{"color":"red","qty":if i < matches {7} else {1}},{"color":"blue","qty":9}],"payload":"small value"}))).collect::<Vec<_>>();
    for chunk in all.chunks(32) {
        db.transaction(|tx| {
            for (id, doc) in chunk {
                tx.insert("docs", id, doc)?;
            }
            Ok(())
        })?;
        let tx = sql.transaction()?;
        for (id, doc) in chunk {
            tx.execute(
                "INSERT INTO docs(id,body) VALUES(?1,?2)",
                params![id, serde_json::to_string(doc)?],
            )?;
        }
        tx.commit()?;
    }
    let q = DocumentQuery {
        filter: DocumentFilter::ArrayElementMatch {
            path: vec!["items".into()],
            filter: Box::new(DocumentFilter::And(vec![
                DocumentFilter::Equal {
                    path: vec!["color".into()],
                    value: json!("red"),
                },
                DocumentFilter::Compare {
                    path: vec!["qty".into()],
                    comparison: Comparison::GreaterOrEqual,
                    value: IndexValue::Integer(5),
                },
            ])),
        },
        max_scanned: 1024,
        limit: 256,
        ..Default::default()
    };
    let expected = &all[..matches];
    let mut samples = BTreeMap::<&str, Vec<u128>>::new();
    for cycle in 0..cycles + WARMUP {
        for offset in 0..2 {
            let variant = (cycle + offset) % 2;
            let start = Instant::now();
            let rows = if variant == 0 {
                prototype(&db, &q)?
            } else {
                sqlite(&sql)?
            };
            let elapsed = start.elapsed().as_nanos();
            if rows != expected {
                return Err("benchmark returned incorrect rows".into());
            }
            if cycle >= WARMUP {
                samples
                    .entry(if variant == 0 {
                        "prototype_scan"
                    } else {
                        "sqlite_scan"
                    })
                    .or_default()
                    .push(elapsed);
            }
        }
    }
    drop(sql);
    drop(store);
    let recovered = Store::open(&source)?;
    let db = DocumentDatabase::new(&recovered);
    let mut observed = Vec::new();
    for offset in (0..n).step_by(256) {
        let page = DocumentQuery {
            offset,
            limit: 256,
            max_scanned: 1024,
            ..Default::default()
        };
        observed.extend(prototype(&db, &page)?);
    }
    if observed != all || prototype(&db, &q)? != expected {
        return Err("prototype recovery mismatch".into());
    }
    let sql = Connection::open(&sqlite_path)?;
    let integrity: String = sql.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" || sqlite(&sql)? != expected {
        return Err("SQLite recovery mismatch".into());
    }
    let mut statement = sql.prepare("SELECT id, body FROM docs ORDER BY id")?;
    let rows = statement.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    let restored = rows
        .map(|r| {
            let (id, body) = r?;
            Ok((id, serde_json::from_str(&body)?))
        })
        .collect::<Result<Vec<Row>>>()?;
    if restored != all {
        return Err("SQLite content mismatch".into());
    }
    println!(
        "{}",
        json!({"documents":n,"matches":matches,"cycles":cycles,"warmup_cycles":WARMUP,"samples_ns":samples,"sqlite_version":version,"sqlite_query_plan":plan,"sqlite_journal":mode,"sqlite_synchronous":sync,"reopen_verified":true})
    );
    Ok(())
}
