//! Times SQL joins on the ordinary and checkpoint-backed stores against
//! SQLite (WAL, synchronous=FULL, fullfsync). Every engine has the same
//! secondary indexes on `orders(user_id)` and `users(team)`. Prints one JSON
//! line per backend.
//!
//! usage: sql_join_bench <directory> <users> <operations>
//!
//! Tables: `users(id, name, team)`, `teams(id, title)` with 50 rows, and
//! `orders(id, user_id, total)` with two orders per user. Workloads:
//! - `point`: one user joined to its team by primary key (`WHERE u.id = ?`);
//! - `fanout`: one user's orders (`JOIN orders ON o.user_id = u.id WHERE u.id = ?`);
//! - `team_scan`: every user of one team with its team, ordered by name;
//! - `three_way`: orders over a threshold with user and team, LIMIT 20.
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlOutcome, SqlValue, Store,
};
use std::error::Error;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

trait Backend {
    fn rows(&self, sql: &str, params: &[SqlValue]) -> Result<usize>;
}

impl Backend for SqlDatabase<'_> {
    fn rows(&self, sql: &str, params: &[SqlValue]) -> Result<usize> {
        Ok(match self.execute(sql, params)? {
            SqlOutcome::Rows { rows, .. } => rows.len(),
            SqlOutcome::Inserted { rows } => rows,
            _ => 0,
        })
    }
}

struct Sqlite(rusqlite::Connection);

impl Backend for Sqlite {
    fn rows(&self, sql: &str, params: &[SqlValue]) -> Result<usize> {
        let values = params
            .iter()
            .map(|value| match value {
                SqlValue::Integer(n) => rusqlite::types::Value::Integer(*n),
                SqlValue::Real(f) => rusqlite::types::Value::Real(*f),
                SqlValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
                SqlValue::Boolean(b) => rusqlite::types::Value::Integer(*b as i64),
                SqlValue::Null => rusqlite::types::Value::Null,
            })
            .collect::<Vec<_>>();
        let mut statement = self.0.prepare_cached(sql)?;
        if statement.readonly() {
            let mut rows = statement.query(rusqlite::params_from_iter(values))?;
            let mut count = 0;
            while rows.next()?.is_some() {
                count += 1;
            }
            Ok(count)
        } else {
            Ok(statement.execute(rusqlite::params_from_iter(values))?)
        }
    }
}

fn load(db: &dyn Backend, users: i64) -> Result<()> {
    for ddl in [
        "CREATE TABLE teams (id INTEGER PRIMARY KEY, title TEXT NOT NULL)",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, team INTEGER)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)",
        // Both engines get the same secondary indexes.
        "CREATE INDEX orders_user ON orders (user_id)",
        "CREATE INDEX users_team ON users (team)",
    ] {
        db.rows(ddl, &[])?;
    }
    let insert = |table: &str, rows: Vec<String>| -> Result<()> {
        for chunk in rows.chunks(100) {
            db.rows(
                &format!("INSERT INTO {table} VALUES {}", chunk.join(", ")),
                &[],
            )?;
        }
        Ok(())
    };
    insert(
        "teams",
        (0..50).map(|id| format!("({id}, 'team-{id}')")).collect(),
    )?;
    insert(
        "users",
        (0..users)
            .map(|id| format!("({id}, 'user-{:08}', {})", (id * 7919) % users, id % 50))
            .collect(),
    )?;
    insert(
        "orders",
        (0..users * 2)
            .map(|id| format!("({id}, {}, {})", id / 2, (id % 97) as f64 / 4.0))
            .collect(),
    )?;
    Ok(())
}

fn sample(
    samples: &mut Vec<f64>,
    f: impl FnOnce() -> Result<usize>,
    expect: Option<usize>,
) -> Result<()> {
    let started = Instant::now();
    let rows = f()?;
    samples.push(started.elapsed().as_secs_f64() * 1e6);
    if let Some(expect) = expect {
        if rows != expect {
            return Err(format!("expected {expect} rows, got {rows}").into());
        }
    }
    Ok(())
}

fn summary(mut samples: Vec<f64>) -> String {
    samples.sort_by(f64::total_cmp);
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
    format!(
        "{{\"n\":{},\"p50_us\":{:.1},\"p99_us\":{:.1}}}",
        samples.len(),
        at(0.5),
        at(0.99)
    )
}

fn workload(name: &str, db: &dyn Backend, users: i64, ops: i64) -> Result<String> {
    let started = Instant::now();
    load(db, users)?;
    let load_ms = started.elapsed().as_secs_f64() * 1e3;
    let (mut point, mut fanout, mut team, mut three) = (vec![], vec![], vec![], vec![]);
    for op in 0..ops {
        let id = SqlValue::Integer((op * 7919) % users);
        sample(
            &mut point,
            || {
                db.rows(
                    "SELECT u.name, t.title FROM users u JOIN teams t ON t.id = u.team WHERE u.id = ?",
                    std::slice::from_ref(&id),
                )
            },
            Some(1),
        )?;
        sample(
            &mut fanout,
            || {
                db.rows(
                    "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = ?",
                    std::slice::from_ref(&id),
                )
            },
            Some(2),
        )?;
    }
    for op in 0..ops.min(20) {
        let team_id = SqlValue::Integer(op % 50);
        sample(
            &mut team,
            || {
                db.rows(
                    "SELECT u.name, t.title FROM users u JOIN teams t ON t.id = u.team \
                     WHERE u.team = ? ORDER BY u.name",
                    std::slice::from_ref(&team_id),
                )
            },
            None,
        )?;
        sample(
            &mut three,
            || {
                db.rows(
                    "SELECT o.id, u.name, t.title FROM orders o JOIN users u ON u.id = o.user_id \
                     JOIN teams t ON t.id = u.team WHERE o.total > ? ORDER BY o.id LIMIT 20",
                    &[SqlValue::Real(20.0)],
                )
            },
            Some(20),
        )?;
    }
    Ok(format!(
        "{{\"backend\":\"{name}\",\"users\":{users},\"load_ms\":{load_ms:.1},\"point\":{},\
\"fanout\":{},\"team_scan\":{},\"three_way\":{}}}",
        summary(point),
        summary(fanout),
        summary(team),
        summary(three)
    ))
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [dir, users, ops] = args.as_slice() else {
        return Err("usage: sql_join_bench <directory> <users> <operations>".into());
    };
    let (users, ops): (i64, i64) = (users.parse()?, ops.parse()?);
    let root = tempfile::Builder::new()
        .prefix(".sql-join-bench-")
        .tempdir_in(dir)?;
    let root = root.path();

    let store = Store::open(root.join("store"))?;
    println!(
        "{}",
        workload(
            "emda-store",
            &SqlDatabase::new(DocumentDatabase::new(&store)),
            users,
            ops
        )?
    );
    drop(store);

    let initial = Store::open(root.join("checkpoint-log"))?;
    let digest = initial.checkpoint_to(root.join("checkpoint"))?;
    drop(initial);
    let store = CheckpointStore::open(
        root.join("checkpoint-log"),
        root.join("checkpoint"),
        &digest,
    )?;
    println!(
        "{}",
        workload(
            "emda-checkpoint",
            &SqlDatabase::new(DocumentDatabase::with_checkpoint_store(&store)),
            users,
            ops
        )?
    );
    drop(store);

    let conn = rusqlite::Connection::open(root.join("sqlite.db"))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON;",
    )?;
    println!("{}", workload("sqlite", &Sqlite(conn), users, ops)?);
    Ok(())
}
