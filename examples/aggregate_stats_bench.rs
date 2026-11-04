//! Verified in-process grouped statistics versus SQLite's normalized table scan.
use database_engine::{
    AggregateStatsRequest, CollectionSchema, DocumentDatabase, DocumentFilter, IndexValue, Store,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{error::Error, path::Path, time::Instant};

type Field = (i64, i128, Option<i64>, Option<i64>);
type Group = (String, i64, Field, Field);

fn prototype(
    db: &DocumentDatabase<'_>,
    request: &AggregateStatsRequest,
) -> Result<Vec<Group>, Box<dyn Error>> {
    Ok(db
        .aggregate_stats("c", request)?
        .groups
        .into_iter()
        .map(|group| {
            let Some(IndexValue::String(key)) = group.key else {
                panic!("unexpected group");
            };
            let fields = group.fields;
            let field = |index: usize| {
                (
                    fields[index].count as i64,
                    fields[index].sum,
                    fields[index].min,
                    fields[index].max,
                )
            };
            (key, group.count as i64, field(0), field(1))
        })
        .collect())
}

fn sqlite(statement: &mut rusqlite::Statement<'_>) -> Result<Vec<Group>, Box<dyn Error>> {
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            (
                row.get::<_, i64>(2)?,
                i128::from(row.get::<_, i64>(3)?),
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ),
            (
                row.get::<_, i64>(6)?,
                i128::from(row.get::<_, i64>(7)?),
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<i64>>(9)?,
            ),
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err("usage: aggregate_stats_bench DIRECTORY DOCUMENTS CYCLES".into());
    }
    let documents = args[2].parse::<usize>()?;
    let cycles = args[3].parse::<usize>()?;
    if !(1..=256).contains(&documents) || cycles == 0 || cycles > 100_000 {
        return Err("invalid workload limits".into());
    }
    let root = tempfile::tempdir_in(Path::new(&args[1]))?;
    let path = root.path().join("store");
    let store = Store::open(&path)?;
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())?;
    let mut connection = Connection::open(root.path().join("sqlite.db"))?;
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE docs(id TEXT PRIMARY KEY, g TEXT NOT NULL, n INTEGER NOT NULL, m INTEGER);")?;
    let transaction = connection.transaction()?;
    for id in 0..documents {
        let group = format!("g{:02}", id % 8);
        let n = (id as i64) - 128;
        let m = (id % 3 != 0).then_some((id as i64) * 2 - 64);
        let value = json!({"g":group,"n":n,"m":m});
        db.insert("c", &format!("{id:03}"), &value)?;
        transaction.execute(
            "INSERT INTO docs VALUES (?1,?2,?3,?4)",
            params![format!("{id:03}"), group, n, m],
        )?;
    }
    transaction.commit()?;
    drop(store);
    let store = Store::open(&path)?;
    let db = DocumentDatabase::new(&store);
    let request = AggregateStatsRequest {
        filter: DocumentFilter::All,
        group_by: Some(vec!["g".into()]),
        fields: vec![vec!["n".into()], vec!["m".into()]],
        max_scanned: 256,
    };
    let query = "SELECT g,COUNT(*),COUNT(n),COALESCE(SUM(n),0),MIN(n),MAX(n),COUNT(m),COALESCE(SUM(m),0),MIN(m),MAX(m) FROM docs GROUP BY g ORDER BY g";
    let plan = connection
        .prepare(&format!("EXPLAIN QUERY PLAN {query}"))?
        .query_map([], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .join("; ");
    let mut statement = connection.prepare(query)?;
    let expected = sqlite(&mut statement)?;
    assert_eq!(
        expected.iter().map(|group| group.1).sum::<i64>(),
        documents as i64
    );
    assert_eq!(prototype(&db, &request)?, expected);
    let mut samples_proto = Vec::with_capacity(cycles);
    let mut samples_sqlite = Vec::with_capacity(cycles);
    for cycle in 0..cycles + 5 {
        for variant in if cycle % 2 == 0 { [0, 1] } else { [1, 0] } {
            let start = Instant::now();
            let actual = if variant == 0 {
                prototype(&db, &request)?
            } else {
                sqlite(&mut statement)?
            };
            let elapsed = u64::try_from(start.elapsed().as_nanos())?;
            assert_eq!(actual, expected);
            if cycle >= 5 {
                if variant == 0 {
                    samples_proto.push(elapsed)
                } else {
                    samples_sqlite.push(elapsed)
                }
            }
        }
    }
    let journal: String = connection.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
    let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
    let version: String = connection.query_row("SELECT sqlite_version()", [], |r| r.get(0))?;
    let output: Value = json!({"documents":documents,"cycles":cycles,"prototype_reopen_verified":true,"sqlite_journal":journal,"sqlite_synchronous":synchronous,"sqlite_version":version,"sqlite_query_plan":plan,"groups":expected.len(),"samples_ns":{"prototype":samples_proto,"sqlite":samples_sqlite}});
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
