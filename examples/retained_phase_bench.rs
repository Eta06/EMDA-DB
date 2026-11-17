//! Process-isolated retained candidate experiment. See retained_phase_bench.md.
use database_engine::{
    RetainedExportLimits, Store,
    retained_base_v2::{Limits, StagingLimits},
    retained_candidate::{self, Expected, RestoreLimits, SuffixExpected, SuffixLimits},
};
use std::{error::Error, fs, path::Path};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn limits() -> (
    RetainedExportLimits,
    Limits,
    StagingLimits,
    SuffixLimits,
    RestoreLimits,
) {
    (
        RetainedExportLimits {
            max_log_bytes: 1 << 30,
            max_live_keys: 1_000_000,
            max_live_key_bytes: 128 << 20,
            max_output_bytes: 1 << 30,
        },
        Limits {
            file_bytes: 1 << 30,
            entries: 1_000_000,
            key_bytes: 128 << 20,
            work_bytes: 2 << 30,
        },
        StagingLimits {
            value_bytes: 64 << 20,
            staging_bytes: 1 << 30,
        },
        SuffixLimits {
            bytes: 1 << 30,
            operations: 2_000_000,
            work: 100_000_000_000,
        },
        RestoreLimits {
            log_bytes: 1 << 30,
            live_keys: 1_000_000,
            live_key_bytes: 128 << 20,
        },
    )
}
fn key(i: u64) -> Vec<u8> {
    format!("k{i:016x}").into_bytes()
}
fn value(i: u64, seed: u64, rev: u8) -> Vec<u8> {
    format!("{:016x}-{rev}", i.wrapping_mul(0x9e3779b97f4a7c15) ^ seed).into_bytes()
}
fn write_expected(p: &Path, e: &Expected) -> Result<()> {
    let mut b = e.header.to_vec();
    b.extend(e.witness);
    b.extend(e.base_digest);
    fs::write(p, b)?;
    Ok(())
}
fn read_expected(p: &Path) -> Result<Expected> {
    let b = fs::read(p)?;
    if b.len() != 176 {
        return Err("invalid candidate binding".into());
    }
    Ok(Expected {
        header: b[..112].try_into()?,
        witness: b[112..144].try_into()?,
        base_digest: b[144..176].try_into()?,
    })
}
fn read_suffix(p: &Path, e: Expected) -> Result<SuffixExpected> {
    let b = fs::read(p)?;
    if b.len() != 40 {
        return Err("invalid suffix binding".into());
    }
    Ok(SuffixExpected {
        candidate: e,
        end: u64::from_le_bytes(b[..8].try_into()?),
        digest: b[8..].try_into()?,
    })
}
fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    if a.len() != 3 {
        return Err("usage: retained_phase_bench <setup|build|mutate|export|open|restore|verify> <fresh-root>".into());
    }
    let phase = &a[1];
    let r = Path::new(&a[2]);
    let (ex, base, stage, suf, out) = limits();
    if phase == "setup" {
        fs::create_dir(r)?;
        let n: u64 = std::env::var("KEYS").unwrap_or("16385".into()).parse()?;
        let s: u64 = std::env::var("SUFFIX").unwrap_or("16385".into()).parse()?;
        let seed: u64 = std::env::var("SEED").unwrap_or("42".into()).parse()?;
        if n == 0 {
            return Err("KEYS must be positive".into());
        }
        fs::write(r.join("settings"), format!("{n} {s} {seed}\n"))?;
        fs::create_dir(r.join("source"))?;
        fs::write(
            r.join("source/records.log"),
            include_bytes!("../tests/fixtures/v2-empty-records.log"),
        )?;
        fs::write(
            r.join("source/commit.state"),
            include_bytes!("../tests/fixtures/v2-empty-commit.state"),
        )?;
        let db = Store::open(r.join("source"))?;
        for i in 0..n {
            db.put(&key(i), &value(i, seed, 0))?;
        }
        return Ok(());
    }
    let settings = fs::read_to_string(r.join("settings"))?;
    let v: Vec<u64> = settings
        .split_whitespace()
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    let (n, s, seed) = (v[0], v[1], v[2]);
    match phase.as_str() {
        "build" => {
            let db = Store::open(r.join("source"))?;
            let e = db.build_retained_candidate(r.join("candidate"), [1; 16], [2; 16], ex)?;
            write_expected(&r.join("candidate.binding"), &e)?;
        }
        "mutate" => {
            let db = Store::open(r.join("source"))?;
            for i in 0..s {
                let k = key(i % n);
                if i % 7 == 0 {
                    db.delete(&k)?
                } else {
                    db.put(&k, &value(i, seed, 1))?
                }
            }
        }
        "export" => {
            let e = read_expected(&r.join("candidate.binding"))?;
            let db = Store::open(r.join("source"))?;
            let b = db.export_retained_suffix(
                r.join("candidate"),
                &e,
                r.join("suffix"),
                base,
                stage,
                suf,
                ex,
            )?;
            let mut x = b.end.to_le_bytes().to_vec();
            x.extend(b.digest);
            fs::write(r.join("suffix.binding"), x)?;
        }
        "open" => {
            let e = read_suffix(
                &r.join("suffix.binding"),
                read_expected(&r.join("candidate.binding"))?,
            )?;
            let rows = retained_candidate::open_exported_suffix(
                r.join("candidate"),
                r.join("suffix"),
                base,
                stage,
                suf,
                &e,
            )?;
            check(rows, n, s, seed)?;
        }
        "restore" => {
            let e = read_suffix(
                &r.join("suffix.binding"),
                read_expected(&r.join("candidate.binding"))?,
            )?;
            retained_candidate::restore_exported_suffix(
                r.join("candidate"),
                r.join("suffix"),
                r.join("restored"),
                base,
                stage,
                suf,
                &e,
                out,
            )?;
        }
        "verify" => {
            for name in ["source", "restored"] {
                let db = Store::open(r.join(name))?;
                let mut rows = Vec::new();
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let page = db.scan(after.as_deref(), None, 1024)?;
                    if page.is_empty() {
                        break;
                    }
                    after = Some(page.last().unwrap().0.clone());
                    rows.extend(page);
                }
                check(rows, n, s, seed)?;
            }
        }
        _ => return Err("unknown phase".into()),
    }
    println!("{phase}: ok; initial_keys={n} suffix_operations={s} seed={seed}");
    Ok(())
}
fn check(rows: Vec<(Vec<u8>, Vec<u8>)>, n: u64, s: u64, seed: u64) -> Result<()> {
    let mut m = std::collections::BTreeMap::new();
    for i in 0..n {
        m.insert(key(i), value(i, seed, 0));
    }
    for i in 0..s {
        if i % 7 == 0 {
            m.remove(&key(i % n));
        } else {
            m.insert(key(i % n), value(i, seed, 1));
        }
    }
    if rows != m.into_iter().collect::<Vec<_>>() {
        return Err("independent model mismatch".into());
    }
    Ok(())
}
