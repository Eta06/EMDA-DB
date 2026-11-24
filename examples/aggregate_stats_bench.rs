//! Verified in-process grouped statistics versus SQLite's normalized table scan.
use database_engine::{
    AggregateStatsRequest, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQueryAccess,
    IndexDefinition, IndexValue, Store,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{error::Error, path::Path, time::Instant};

type Field = (i64, i128, Option<i64>, Option<i64>);
type Group = (String, i64, Field, Field);

fn prototype(
    db: &DocumentDatabase<'_>,
    request: &AggregateStatsRequest,
    planned: bool,
) -> Result<Vec<Group>, Box<dyn Error>> {
    let result = if planned {
        let planned = db.aggregate_stats_planned("c", request)?;
        match (&request.filter, planned.access) {
            (DocumentFilter::Equal { .. }, DocumentQueryAccess::EqualityIndex { index })
            | (
                DocumentFilter::In { .. },
                DocumentQueryAccess::MembershipIndex { index, probes: 2 },
            ) if index == "g" => {}
            _ => panic!("unexpected aggregate access path"),
        }
        planned.result
    } else {
        db.aggregate_stats("c", request)?
    };
    Ok(result
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
    let selection = match args.get(4).map(String::as_str) {
        None if args.len() == 4 => None,
        Some(mode @ ("selective" | "membership")) if args.len() == 5 => Some(mode),
        _ => {
            return Err(
                "usage: aggregate_stats_bench DIRECTORY DOCUMENTS CYCLES [selective|membership]"
                    .into(),
            );
        }
    };
    let selective = selection.is_some();
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
    if selective {
        db.create_index(
            "c",
            IndexDefinition {
                name: "g".into(),
                fields: vec![vec!["g".into()]],
                unique: false,
                partial: None,
            },
        )?;
        connection.execute_batch("CREATE INDEX docs_g ON docs(g)")?;
    }
    drop(store);
    let store = Store::open(&path)?;
    let db = DocumentDatabase::new(&store);
    let request = AggregateStatsRequest {
        filter: match selection {
            Some("membership") => DocumentFilter::In {
                path: vec!["g".into()],
                values: vec![json!("g00"), json!("g01")],
            },
            Some(_) => DocumentFilter::Equal {
                path: vec!["g".into()],
                value: json!("g00"),
            },
            None => DocumentFilter::All,
        },
        group_by: Some(vec!["g".into()]),
        fields: vec![vec!["n".into()], vec!["m".into()]],
        max_scanned: 256,
    };
    let restriction = match selection {
        Some("membership") => " WHERE g IN ('g00', 'g01')",
        Some(_) => " WHERE g='g00'",
        None => "",
    };
    let query = format!(
        "SELECT g,COUNT(*),COUNT(n),COALESCE(SUM(n),0),MIN(n),MAX(n),COUNT(m),COALESCE(SUM(m),0),MIN(m),MAX(m) FROM docs{restriction} GROUP BY g ORDER BY g"
    );
    let plan = connection
        .prepare(&format!("EXPLAIN QUERY PLAN {query}"))?
        .query_map([], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .join("; ");
    let mut statement = connection.prepare(&query)?;
    let expected = sqlite(&mut statement)?;
    let matched = match selection {
        Some("membership") => documents.div_ceil(8) + (documents - 1).div_ceil(8),
        Some(_) => documents.div_ceil(8),
        None => documents,
    };
    assert_eq!(
        expected.iter().map(|group| group.1).sum::<i64>(),
        matched as i64
    );
    assert_eq!(prototype(&db, &request, false)?, expected);
    let variants = if selective {
        &["prototype", "planned", "sqlite"][..]
    } else {
        &["prototype", "sqlite"][..]
    };
    let mut samples = std::collections::BTreeMap::new();
    for cycle in 0..cycles + 5 {
        for step in 0..variants.len() {
            let variant = variants[(step + cycle) % variants.len()];
            let start = Instant::now();
            let actual = match variant {
                "sqlite" => sqlite(&mut statement)?,
                "planned" => prototype(&db, &request, true)?,
                _ => prototype(&db, &request, false)?,
            };
            let elapsed = u64::try_from(start.elapsed().as_nanos())?;
            assert_eq!(actual, expected);
            if cycle >= 5 {
                samples
                    .entry(variant)
                    .or_insert_with(|| Vec::with_capacity(cycles))
                    .push(elapsed);
            }
        }
    }
    let journal: String = connection.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
    let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
    let version: String = connection.query_row("SELECT sqlite_version()", [], |r| r.get(0))?;
    let output: Value = json!({"documents":documents,"cycles":cycles,"selective":selective,"membership":selection==Some("membership"),"matched":matched,"prototype_reopen_verified":true,"sqlite_journal":journal,"sqlite_synchronous":synchronous,"sqlite_version":version,"sqlite_query_plan":plan,"groups":expected.len(),"samples_ns":samples});
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
