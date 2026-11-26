//! Workload for the R1 reference deployment in `docs/REFERENCE_DEPLOYMENT.md`.
//! One worker process owns one managed retained catalog of 1,048,576 keys
//! under a persisted catalog quota. It mixes point reads with synchronized
//! eight-mutation batches and rotates with a `RetainedRotationJob` when any
//! suffix limit is half used.
//!
//! - `setup <dir>` builds the catalog.
//! - `run <dir> <first> <batches>` runs global batches `first..first+batches`.
//!   It prints `ACK <batch>` on stdout after each batch returns, rotation
//!   events on stderr, and a JSON summary on stdout at the end.
//! - `open <dir>` removes abandoned stages, opens the catalog, reclaims the
//!   inactive slot and reads one key, and reports the times.
//! - `verify <dir> <acked>` recovers the same way and scans every entry. Batches below `acked`
//!   must be present. Batch `acked` may have been in flight, so it must be
//!   entirely present or entirely absent. Later batches must be absent.
use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    CatalogStorageLimits, IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits,
    SuffixLimits,
};
use database_engine::{BatchOperation, RetainedExportLimits, Store};
use std::error::Error;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const KEYS: u64 = 1 << 20;
const BATCH: u64 = 8;
const READS_PER_BATCH: u64 = 4;
const ROTATE_AT: f64 = 0.5;
const VALUE_BYTES: usize = 96;
const INITIAL: u64 = u64::MAX;

const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 128 << 20,
        entries: KEYS,
        key_bytes: KEYS * 8,
        work_bytes: (128 << 20) + 1,
    },
    index: IndexLimits {
        blocks: 1024,
        anchor_bytes: 16 << 10,
    },
    suffix: SuffixLimits {
        bytes: 12 << 20,
        operations: 131_072,
        work: 12_000_000_000,
    },
    overlay: OverlayLimits {
        entries: 32_768,
        key_bytes: 32_768 * 8,
    },
};

/// Active and inactive generations, one rotation stage and metadata.
const QUOTA: CatalogStorageLimits = CatalogStorageLimits {
    bytes: 512 << 20,
    entries: 256,
};

fn mix(mut word: u64) -> u64 {
    word = word.wrapping_add(0x9e37_79b9_7f4a_7c15);
    word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    word ^ (word >> 31)
}

fn key(index: u64) -> Vec<u8> {
    format!("k{index:07}").into_bytes()
}

fn value(index: u64, tag: u64) -> [u8; VALUE_BYTES] {
    let mut bytes = [0; VALUE_BYTES];
    let mut word = index ^ tag.rotate_left(21);
    for part in bytes.chunks_exact_mut(8) {
        word = mix(word);
        part.copy_from_slice(&word.to_le_bytes());
    }
    bytes
}

/// Eight percent of keys receive eighty percent of accesses.
fn skewed(word: u64) -> u64 {
    if word % 10 < 8 {
        (word >> 8) % (KEYS / 12)
    } else {
        (word >> 8) % KEYS
    }
}

/// Distinct keys of one batch with a delete roughly one time in sixteen.
fn batch_ops(batch: u64) -> Vec<(u64, bool)> {
    let mut ops: Vec<(u64, bool)> = Vec::with_capacity(BATCH as usize);
    for slot in 0..BATCH {
        let seq = batch * BATCH + slot;
        let mut index = skewed(mix(seq));
        while ops.iter().any(|(other, _)| *other == index) {
            index = (index + 1) % KEYS;
        }
        ops.push((index, mix(seq ^ 0x5555) % 16 == 0));
    }
    ops
}

/// Bytes this process has read from and written to storage devices.
#[cfg(target_os = "macos")]
fn disk_io() -> (u64, u64) {
    // The leading fields of `struct rusage_info_v2` from <sys/resource.h>.
    #[repr(C)]
    struct RusageInfoV2 {
        uuid: [u8; 16],
        counters: [u64; 16],
        diskio_bytesread: u64,
        diskio_byteswritten: u64,
    }
    unsafe extern "C" {
        fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut RusageInfoV2) -> i32;
    }
    let mut info = std::mem::MaybeUninit::<RusageInfoV2>::zeroed();
    // SAFETY: flavor 2 (RUSAGE_INFO_V2) fills exactly this layout.
    let status = unsafe { proc_pid_rusage(std::process::id() as i32, 2, info.as_mut_ptr()) };
    if status != 0 {
        return (0, 0);
    }
    // SAFETY: the call succeeded and initialized the structure.
    let info = unsafe { info.assume_init() };
    (info.diskio_bytesread, info.diskio_byteswritten)
}

#[cfg(not(target_os = "macos"))]
fn disk_io() -> (u64, u64) {
    let text = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    };
    (field("read_bytes:"), field("write_bytes:"))
}

fn io_since(start: (u64, u64)) -> String {
    let (read, written) = disk_io();
    format!(
        "\"disk_read_bytes\":{},\"disk_written_bytes\":{}",
        read - start.0,
        written - start.1
    )
}

fn catalog_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total += if metadata.is_dir() {
            catalog_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(total)
}

fn setup(root: &Path) -> Result<()> {
    std::fs::create_dir(root)?;
    let started = Instant::now();
    let source = Store::open(root.join("source"))?;
    for chunk in (0..KEYS).collect::<Vec<_>>().chunks(1024) {
        let keys = chunk.iter().map(|index| key(*index)).collect::<Vec<_>>();
        let values = chunk
            .iter()
            .map(|index| value(*index, INITIAL))
            .collect::<Vec<_>>();
        let batch = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| BatchOperation::Put { key, value })
            .collect::<Vec<_>>();
        source.write_batch(&batch)?;
    }
    let expected = source.build_retained_candidate(
        root.join("candidate"),
        [7; 16],
        [8; 16],
        RetainedExportLimits {
            max_log_bytes: 1 << 30,
            max_live_keys: KEYS as usize,
            max_live_key_bytes: KEYS as usize * 8,
            max_output_bytes: 1 << 30,
        },
    )?;
    drop(source);
    ManagedRetainedStore::create_with_storage_limits(
        root.join("candidate"),
        root.join("catalog"),
        LIMITS,
        QUOTA,
        &expected,
    )?;
    // The source and candidate are setup inputs, not part of the deployment.
    std::fs::remove_dir_all(root.join("source"))?;
    std::fs::remove_dir_all(root.join("candidate"))?;
    println!(
        "{{\"phase\":\"setup\",\"keys\":{KEYS},\"setup_ms\":{:.1},\"catalog_bytes\":{}}}",
        started.elapsed().as_secs_f64() * 1e3,
        catalog_bytes(&root.join("catalog"))?
    );
    Ok(())
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * fraction).round() as usize]
}

fn summary(mut samples: Vec<f64>) -> String {
    samples.sort_by(f64::total_cmp);
    format!(
        "{{\"count\":{},\"p50_ms\":{:.3},\"p99_ms\":{:.3},\"p999_ms\":{:.3},\"max_ms\":{:.3}}}",
        samples.len(),
        percentile(&samples, 0.5),
        percentile(&samples, 0.99),
        percentile(&samples, 0.999),
        samples.last().copied().unwrap_or(0.0)
    )
}

fn ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1e3
}

type Preparing = std::thread::JoinHandle<
    Result<(
        f64,
        database_engine::retained_candidate::RetainedRotationJob,
    )>,
>;

fn run(root: &Path, first: u64, batches: u64) -> Result<()> {
    let catalog = root.join("catalog");
    let started = Instant::now();
    let mut store = ManagedRetainedStore::open(&catalog, LIMITS)?;
    let open_ms = ms(started);
    // A rotation completed before an earlier exit leaves the old slot.
    let reclaimed_at_open = store.reclaim_inactive()?;
    let limits = store.limits()?;
    let mut stdout = std::io::stdout().lock();
    let (mut reads, mut writes, mut writes_preparing) = (Vec::new(), Vec::new(), Vec::new());
    let mut rotations = Vec::new();
    let mut preparing: Option<(Preparing, f64, u64)> = None;
    let mut peak_fraction: f64 = 0.0;
    let mut peak_catalog = catalog_bytes(&catalog)?;
    let mut read_word = first.wrapping_mul(0x1234_5678);
    let workload = Instant::now();
    let workload_io = disk_io();
    let mut rotation_io = (0, 0);
    for batch in first..first + batches {
        for _ in 0..READS_PER_BATCH {
            read_word = mix(read_word);
            let started = Instant::now();
            let found = store.get(&key(skewed(read_word)), VALUE_BYTES)?;
            reads.push(ms(started));
            if found
                .as_ref()
                .is_some_and(|value| value.len() != VALUE_BYTES)
            {
                return Err("read returned a value of the wrong size".into());
            }
        }
        let ops = batch_ops(batch);
        let keys = ops.iter().map(|(index, _)| key(*index)).collect::<Vec<_>>();
        let values = ops
            .iter()
            .map(|(index, _)| value(*index, batch))
            .collect::<Vec<_>>();
        let operations = ops
            .iter()
            .zip(keys.iter().zip(&values))
            .map(|((_, delete), (key, value))| {
                if *delete {
                    BatchOperation::Delete { key }
                } else {
                    BatchOperation::Put { key, value }
                }
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        store.write_batch(&operations)?;
        let elapsed = ms(started);
        writes.push(elapsed);
        if preparing.is_some() {
            writes_preparing.push(elapsed);
        }
        writeln!(stdout, "ACK {batch}")?;
        stdout.flush()?;

        let fraction = store.suffix_usage()?.max_fraction_used(&limits);
        peak_fraction = peak_fraction.max(fraction);
        if batch % 64 == 0 {
            peak_catalog = peak_catalog.max(catalog_bytes(&catalog)?);
        }
        if preparing.is_none() && fraction >= ROTATE_AT {
            eprintln!("EVENT begin {batch}");
            rotation_io = disk_io();
            let started = Instant::now();
            let mut job = store.begin_rotation()?;
            let begin_ms = ms(started);
            eprintln!("EVENT prepare {batch}");
            let handle = std::thread::spawn(move || {
                let started = Instant::now();
                job.prepare()?;
                Ok((ms(started), job))
            });
            preparing = Some((handle, begin_ms, batch));
        } else if preparing
            .as_ref()
            .is_some_and(|(handle, _, _)| handle.is_finished())
        {
            let (handle, begin_ms, begun) = preparing.take().unwrap();
            let (prepare_ms, job) = handle.join().map_err(|_| "preparation panicked")??;
            peak_catalog = peak_catalog.max(catalog_bytes(&catalog)?);
            eprintln!("EVENT complete {batch}");
            let started = Instant::now();
            store.complete_rotation(job)?;
            let complete_ms = ms(started);
            peak_catalog = peak_catalog.max(catalog_bytes(&catalog)?);
            eprintln!("EVENT reclaim {batch}");
            let started = Instant::now();
            store.reclaim_inactive()?;
            let reclaim_ms = ms(started);
            eprintln!("EVENT done {batch}");
            rotations.push(format!(
                "{{\"begun_after\":{begun},\"completed_after\":{batch},\"begin_ms\":{begin_ms:.1},\
\"prepare_ms\":{prepare_ms:.1},\"batches_during_prepare\":{},\"complete_ms\":{complete_ms:.1},\
\"reclaim_ms\":{reclaim_ms:.1},\"fraction_after\":{:.4},{}}}",
                batch - begun,
                store.suffix_usage()?.max_fraction_used(&limits),
                io_since(rotation_io)
            ));
        }
    }
    let workload_ms = ms(workload);
    let workload_io = io_since(workload_io);
    if let Some((handle, _, _)) = preparing.take() {
        // The job is dropped unfinished; dropping removes its stage.
        let _ = handle.join();
    }
    peak_catalog = peak_catalog.max(catalog_bytes(&catalog)?);
    writeln!(
        stdout,
        "{{\"phase\":\"run\",\"first\":{first},\"batches\":{batches},\"open_ms\":{open_ms:.1},\
\"reclaimed_at_open\":{reclaimed_at_open},\"workload_ms\":{workload_ms:.1},\
\"batches_per_s\":{:.1},\"reads\":{},\"writes\":{},\"writes_during_prepare\":{},\
\"rotations\":[{}],\"peak_suffix_fraction\":{peak_fraction:.4},\
\"peak_catalog_bytes\":{peak_catalog},\"final_catalog_bytes\":{},\"quota_bytes\":{},{workload_io}}}",
        batches as f64 / (workload_ms / 1e3),
        summary(reads),
        summary(writes),
        summary(writes_preparing),
        rotations.join(","),
        catalog_bytes(&catalog)?,
        QUOTA.bytes
    )?;
    Ok(())
}

/// Marks a deleted key in the model.
const DELETED: u64 = u64::MAX - 1;

/// Per-key tag of the last write through `batches`.
fn model(batches: u64) -> Vec<u64> {
    let mut state = vec![INITIAL; KEYS as usize];
    for batch in 0..batches {
        for (index, delete) in batch_ops(batch) {
            state[index as usize] = if delete { DELETED } else { batch };
        }
    }
    state
}

fn expected(tag: u64, index: u64) -> Option<[u8; VALUE_BYTES]> {
    (tag != DELETED).then(|| value(index, tag))
}

/// Removes abandoned stages, opens the catalog and reclaims the inactive
/// slot, as a worker does after an unclean exit.
fn recover(catalog: &Path) -> Result<(ManagedRetainedStore, String)> {
    let started = Instant::now();
    let cleanup = database_engine::cleanup_abandoned_staging(catalog)?;
    let cleanup_ms = ms(started);
    let started = Instant::now();
    let mut store = ManagedRetainedStore::open(catalog, LIMITS)?;
    let open_ms = ms(started);
    let started = Instant::now();
    let reclaimed = store.reclaim_inactive()?;
    let reclaim_ms = ms(started);
    let started = Instant::now();
    store.get(&key(0), VALUE_BYTES)?;
    let first_read_ms = ms(started);
    Ok((
        store,
        format!(
            "\"removed_stages\":{},\"cleanup_ms\":{cleanup_ms:.1},\"open_ms\":{open_ms:.1},\
\"reclaimed_inactive\":{reclaimed},\"reclaim_ms\":{reclaim_ms:.1},\"first_read_ms\":{first_read_ms:.3}",
            cleanup.removed_payloads
        ),
    ))
}

/// Recovery only, so the process measures the engine without the model.
fn open(root: &Path) -> Result<()> {
    let catalog = root.join("catalog");
    let io = disk_io();
    let started = Instant::now();
    let (store, details) = recover(&catalog)?;
    let ready_ms = ms(started);
    let io = io_since(io);
    drop(store);
    println!(
        "{{\"phase\":\"open\",\"ready_ms\":{ready_ms:.1},{details},{io},\"catalog_bytes\":{}}}",
        catalog_bytes(&catalog)?
    );
    Ok(())
}

fn verify(root: &Path, acked: u64) -> Result<()> {
    let catalog = root.join("catalog");
    let (store, details) = recover(&catalog)?;
    let mut reference = model(acked);
    // The batch after the last acknowledgement may have committed. It is
    // atomic, so all of its keys must agree.
    let inflight = batch_ops(acked);
    let mut applied = 0;
    for (index, delete) in &inflight {
        let found = store.get(&key(*index), VALUE_BYTES)?;
        let tag = if *delete { DELETED } else { acked };
        if found.as_deref() == expected(tag, *index).as_ref().map(|value| &value[..]) {
            applied += 1;
        }
    }
    let inflight_applied = applied == inflight.len();
    if inflight_applied {
        for (index, delete) in &inflight {
            reference[*index as usize] = if *delete { DELETED } else { acked };
        }
    }
    let started = Instant::now();
    let mut after: Option<Vec<u8>> = None;
    let (mut next, mut live) = (0u64, 0u64);
    loop {
        let page = store.scan(after.as_deref(), None, 1024)?;
        let Some(last) = page.last() else { break };
        after = Some(last.0.clone());
        for (found_key, found_value) in &page {
            let text = std::str::from_utf8(found_key)?;
            let index: u64 = text.strip_prefix('k').ok_or("bad key")?.parse()?;
            for absent in next..index {
                if reference[absent as usize] != DELETED {
                    return Err(format!("key {absent} is missing").into());
                }
            }
            if expected(reference[index as usize], index)
                .as_ref()
                .map(|value| &value[..])
                != Some(&found_value[..])
            {
                return Err(format!("key {index} does not match the acknowledged model").into());
            }
            next = index + 1;
            live += 1;
        }
    }
    for absent in next..KEYS {
        if reference[absent as usize] != DELETED {
            return Err(format!("key {absent} is missing").into());
        }
    }
    let scan_ms = ms(started);
    if !inflight_applied && applied > 0 {
        // Some keys may match by coincidence, such as a repeated delete;
        // the full comparison above already checked the absent case.
        eprintln!("note: {applied} in-flight keys matched without the batch");
    }
    println!(
        "{{\"phase\":\"verify\",\"acked\":{acked},\"inflight_applied\":{inflight_applied},\
\"live_keys\":{live},{details},\"scan_ms\":{scan_ms:.1},\"catalog_bytes\":{},\"verified\":true}}",
        catalog_bytes(&catalog)?
    );
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let usage = "usage: reference_deployment <setup <dir>|run <dir> <first> <batches>|open <dir>|verify <dir> <acked>>";
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["setup", dir] => setup(Path::new(dir)),
        ["run", dir, first, batches] => run(Path::new(dir), first.parse()?, batches.parse()?),
        ["open", dir] => open(Path::new(dir)),
        ["verify", dir, acked] => verify(Path::new(dir), acked.parse()?),
        _ => Err(usage.into()),
    }
}
