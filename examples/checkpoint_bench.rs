//! Paired warm-cache recovery measurements; the checkpoint digest is kept in memory.
use database_engine::{BatchOperation, Store};
use std::error::Error;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const VALUE_BYTES: usize = 64;

fn main() -> BenchResult<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [parent, writes, keys, rounds] = args.as_slice() else {
        return Err("usage: checkpoint_bench <existing-directory> <writes> <keys> <rounds>".into());
    };
    let writes: usize = writes.parse()?;
    let keys: usize = keys.parse()?;
    let rounds: usize = rounds.parse()?;
    if !Path::new(parent).is_dir()
        || !(1..=1_000_000).contains(&writes)
        || !(1..=16_000).contains(&keys)
        || keys > writes
        || !(2..=100).contains(&rounds)
    {
        return Err("invalid parent, writes, keys or rounds".into());
    }
    let directory = tempfile::Builder::new()
        .prefix(".checkpoint-bench-")
        .tempdir_in(parent)?;
    let path = directory.path().join("store");
    let checkpoint = directory.path().join("index");
    let store = Store::open(&path)?;
    for start in (0..writes).step_by(256) {
        let end = (start + 256).min(writes);
        let values = (start..end)
            .map(|i| {
                ((i % keys) as u64)
                    .to_be_bytes()
                    .into_iter()
                    .chain(std::iter::repeat_n((i % 251) as u8, VALUE_BYTES))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let ops = values
            .iter()
            .map(|item| BatchOperation::Put {
                key: &item[..8],
                value: &item[8..],
            })
            .collect::<Vec<_>>();
        store.write_batch(&ops)?;
    }
    let checkpoint_started = Instant::now();
    let digest = store.checkpoint_to(&checkpoint)?;
    let create_us = checkpoint_started.elapsed().as_micros();
    drop(store);
    let log_bytes = std::fs::metadata(path.join("records.log"))?.len();
    let index_bytes = std::fs::metadata(&checkpoint)?.len();
    println!(
        "writes={writes} keys={keys} value_bytes={VALUE_BYTES} rounds={rounds} log_bytes={log_bytes} index_bytes={index_bytes} checkpoint_create_us={create_us}"
    );
    if std::env::var_os("EMDA_CHECKPOINT_BENCH_GATE").is_some() {
        println!("READY_FOR_MEASUREMENT");
        io::stdout().flush()?;
        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            return Err("measurement gate closed".into());
        }
    }
    for round in 0..rounds {
        let modes = if round % 2 == 0 {
            ["full", "checkpoint"]
        } else {
            ["checkpoint", "full"]
        };
        for mode in modes {
            let start = Instant::now();
            let reopened = if mode == "full" {
                Store::open(&path)?
            } else {
                Store::open_with_checkpoint(&path, &checkpoint, &digest)?
            };
            let elapsed_us = start.elapsed().as_micros();
            for key in [0, keys / 2, keys - 1] {
                let last = writes - 1 - ((writes - 1 - key) % keys);
                let value = reopened
                    .get(&(key as u64).to_be_bytes())?
                    .ok_or("missing recovered key")?;
                if value.len() != VALUE_BYTES || value.iter().any(|v| *v != (last % 251) as u8) {
                    return Err("recovered value mismatch".into());
                }
            }
            drop(reopened);
            println!("round={} mode={mode} open_us={elapsed_us}", round + 1);
        }
    }
    Ok(())
}
