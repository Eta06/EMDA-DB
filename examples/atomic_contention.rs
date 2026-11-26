//! Disposable paired workload: ordinary writers to distinct keys alongside
//! compare-exchange threads advancing one hot counter. Every write is
//! synchronized. Prints put latency percentiles, the number of successful
//! exchanges and wall time as JSON. One run per process; compare revisions
//! on the same host and filesystem.
use database_engine::Store;
use std::sync::Mutex;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const WRITERS: usize = 4;
const EXCHANGERS: usize = 2;
const PUTS_PER_WRITER: usize = 200;
const EXCHANGES_PER_THREAD: usize = 100;

fn percentile(sorted: &[Duration], fraction: f64) -> u128 {
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index].as_micros()
}

fn main() -> Result<()> {
    let parent = std::env::args()
        .nth(1)
        .ok_or("usage: atomic_contention <directory>")?;
    let root = tempfile::Builder::new()
        .prefix(".atomic-contention-")
        .tempdir_in(parent)?;
    let store = Store::open(root.path())?;
    store.put(b"counter", &0_u64.to_le_bytes())?;
    let latencies = Mutex::new(Vec::with_capacity(WRITERS * PUTS_PER_WRITER));
    let start = Instant::now();
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..EXCHANGERS {
            handles.push(scope.spawn(|| -> Result<()> {
                for _ in 0..EXCHANGES_PER_THREAD {
                    loop {
                        let current = store.get(b"counter")?.ok_or("counter missing")?;
                        let next = u64::from_le_bytes(current.as_slice().try_into()?) + 1;
                        if store.compare_exchange(
                            b"counter",
                            Some(&current),
                            Some(&next.to_le_bytes()),
                        )? {
                            break;
                        }
                    }
                }
                Ok(())
            }));
        }
        for writer in 0..WRITERS {
            let (store, latencies) = (&store, &latencies);
            handles.push(scope.spawn(move || -> Result<()> {
                let mut local = Vec::with_capacity(PUTS_PER_WRITER);
                for index in 0..PUTS_PER_WRITER {
                    let key = format!("writer-{writer}-{index:04}");
                    let begun = Instant::now();
                    store.put(key.as_bytes(), b"value")?;
                    local.push(begun.elapsed());
                }
                latencies.lock().map_err(|_| "poisoned")?.extend(local);
                Ok(())
            }));
        }
        for handle in handles {
            handle.join().map_err(|_| "worker panicked")??;
        }
        Ok(())
    })?;
    let wall = start.elapsed();
    let counter = u64::from_le_bytes(
        store
            .get(b"counter")?
            .ok_or("counter missing")?
            .as_slice()
            .try_into()?,
    );
    if counter != (EXCHANGERS * EXCHANGES_PER_THREAD) as u64 {
        return Err(format!("counter is {counter}").into());
    }
    let mut latencies = latencies.into_inner().map_err(|_| "poisoned")?;
    latencies.sort();
    println!(
        "{{\"writers\":{WRITERS},\"exchangers\":{EXCHANGERS},\"puts\":{},\"exchanges\":{counter},\
\"put_p50_us\":{},\"put_p90_us\":{},\"put_p99_us\":{},\"wall_ms\":{}}}",
        latencies.len(),
        percentile(&latencies, 0.5),
        percentile(&latencies, 0.9),
        percentile(&latencies, 0.99),
        wall.as_millis()
    );
    Ok(())
}
