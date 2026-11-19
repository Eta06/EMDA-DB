//! Phase runner for the paired full-log and authority-backed recovery workload.
use database_engine::{
    BatchOperation, Entry, RetainedExportLimits, Store,
    retained_base_v2::Limits,
    retained_candidate::{
        AuthorizedRetainedStore, FileRetainedAuthority, IndexLimits, OverlayLimits,
        RetainedAuthority, RetainedStoreLimits, SuffixLimits,
    },
};
use std::{error::Error, fs, num::NonZeroU64, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;

const BATCH: u64 = 128;
const CHURN: u64 = 4096;
const MAX_KEYS: u64 = 262_144;
const THRESHOLD: NonZeroU64 = NonZeroU64::new(640 * 1024).unwrap();
const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 32 << 20,
        entries: MAX_KEYS,
        key_bytes: MAX_KEYS * 8,
        work_bytes: (32 << 20) + 1,
    },
    index: IndexLimits {
        blocks: 1024,
        anchor_bytes: 16 << 10,
    },
    suffix: SuffixLimits {
        bytes: 768 << 10,
        operations: 8192,
        work: 1_000_000,
    },
    overlay: OverlayLimits {
        entries: 8192,
        key_bytes: 64 << 10,
    },
};

enum Backend {
    Ordinary(Box<Store>),
    Retained(Box<Catalog>),
}

impl Backend {
    fn open(root: &Path, name: &str, limits: RetainedStoreLimits) -> Result<Self> {
        match name {
            "ordinary" => Ok(Self::Ordinary(Box::new(Store::open(root.join(name))?))),
            "retained" => {
                let provider = FileRetainedAuthority::open(root.join("authority"))?;
                let store =
                    Catalog::open_with_auto_reclaim(root.join(name), limits, provider, THRESHOLD)?;
                Ok(Self::Retained(Box::new(store)))
            }
            _ => Err("invalid backend".into()),
        }
    }

    fn write(&mut self, operations: &[BatchOperation<'_>]) -> Result<()> {
        match self {
            Self::Ordinary(store) => store.write_batch(operations)?,
            Self::Retained(store) => store.write_batch(operations)?,
        }
        Ok(())
    }

    fn page(&mut self, after: Option<&[u8]>) -> Result<Vec<Entry>> {
        match self {
            Self::Ordinary(store) => Ok(store.scan(after, None, 1024)?),
            Self::Retained(store) => Ok(store.scan(after, None, 1024)?),
        }
    }
}

fn value(key: u64, epoch: u64) -> [u8; 96] {
    let mut bytes = [0; 96];
    let mut word = key ^ epoch.rotate_left(21);
    for part in bytes.chunks_exact_mut(8) {
        word = word.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mixed = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        part.copy_from_slice(&mixed.to_le_bytes());
    }
    bytes
}

fn deleted(key: u64, epoch: u64) -> bool {
    epoch != 0 && (key + epoch) % 11 == 0
}

fn apply(store: &mut Backend, count: u64, epoch: u64) -> Result<()> {
    for start in (0..count).step_by(BATCH as usize) {
        let rows = (start..(start + BATCH).min(count))
            .map(|key| (key, key.to_be_bytes(), value(key, epoch)))
            .collect::<Vec<_>>();
        let operations = rows
            .iter()
            .map(|(id, key, value)| {
                if deleted(*id, epoch) {
                    BatchOperation::Delete { key }
                } else {
                    BatchOperation::Put { key, value }
                }
            })
            .collect::<Vec<_>>();
        store.write(&operations)?;
    }
    Ok(())
}

fn verify(store: &mut Backend, keys: u64, epoch: u64) -> Result<()> {
    let mut expected = (0..keys).filter_map(|id| {
        let revision = if id < CHURN { epoch } else { 0 };
        (!deleted(id, revision)).then(|| (id.to_be_bytes(), value(id, revision)))
    });
    let mut after = None;
    let mut checked = 0;
    loop {
        let page = store.page(after.as_deref())?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            let (expected_key, expected_value) = expected.next().ok_or("extra entry")?;
            if key.as_slice() != expected_key || value.as_slice() != expected_value {
                return Err(format!("model mismatch after {checked} verified entries").into());
            }
            checked += 1;
        }
        after = page.last().map(|(key, _)| key.clone());
    }
    if expected.next().is_some() {
        return Err("missing entry".into());
    }
    println!("verified_entries={checked}");
    Ok(())
}

fn authority_state(root: &Path) -> Result<(u64, u64, [u8; 16])> {
    let record = FileRetainedAuthority::open(root.join("authority"))?
        .read()?
        .ok_or("missing authority record")?;
    if record.pending.is_some() {
        return Err("pending authority at successful benchmark phase".into());
    }
    Ok((
        record.revision,
        record.committed.suffix_end,
        record.committed.generation,
    ))
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let [_, phase, root, name, keys, epoch] = args.as_slice() else {
        return Err("usage: authorized_recovery_bench <setup|cutover|mutate|reopen|verify> <root> <ordinary|retained|both> <keys> <epoch>".into());
    };
    let root = Path::new(root);
    let keys: u64 = keys.parse()?;
    let epoch: u64 = epoch.parse()?;
    if !(CHURN..=MAX_KEYS).contains(&keys) || epoch > 1000 {
        return Err("keys must be 4096–262144 and epoch must be 0–1000".into());
    }
    let mut limits = LIMITS;
    match std::env::var("EMDA_BENCH_SUFFIX_WORK") {
        Ok(work) => limits.suffix.work = work.parse()?,
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    if limits.suffix.work == 0 {
        return Err("EMDA_BENCH_SUFFIX_WORK must be positive".into());
    }

    match phase.as_str() {
        "setup" => {
            fs::create_dir(root)?;
            for name in ["ordinary", "retained"] {
                let mut store = Backend::Ordinary(Box::new(Store::open(root.join(name))?));
                apply(&mut store, keys, 0)?;
            }
        }
        "cutover" => {
            let path = root.join("retained");
            let source = Store::open(&path)?;
            let record = Catalog::cutover_from_store(
                source,
                &path,
                RetainedExportLimits {
                    max_log_bytes: 64 << 20,
                    max_live_keys: MAX_KEYS as usize,
                    max_live_key_bytes: MAX_KEYS as usize * 8,
                    max_output_bytes: LIMITS.base.file_bytes,
                },
                limits,
            )?;
            let mut provider = FileRetainedAuthority::create(root.join("authority"))?;
            if !provider.compare_and_swap(None, record)? {
                return Err("failed to register fresh authority".into());
            }
        }
        "mutate" | "reopen" | "verify" => {
            if phase == "mutate" && epoch == 0 {
                return Err("mutation epochs start at 1".into());
            }
            let start = Instant::now();
            let mut store = Backend::open(root, name, limits)?;
            println!("open_ns={}", start.elapsed().as_nanos());
            if phase == "mutate" {
                let before = (name == "retained")
                    .then(|| authority_state(root))
                    .transpose()?;
                let start = Instant::now();
                apply(&mut store, CHURN, epoch)?;
                let write_time = start.elapsed().as_nanos();
                println!(
                    "mutate_ns={write_time} batches={} operations={CHURN}",
                    CHURN / BATCH
                );
                if let Some((revision, _, _)) = before {
                    let (end, _, _) = authority_state(root)?;
                    let revisions = end
                        .checked_sub(revision + 2 * (CHURN / BATCH))
                        .ok_or("unexpected authority revision count")?;
                    if revisions % 2 != 0 {
                        return Err("unpaired rotation and reclamation revisions".into());
                    }
                    println!("rotations={}", revisions / 2);
                }
            } else if phase == "verify" {
                verify(&mut store, keys, epoch)?;
            }
            if name == "retained" {
                let (revision, suffix, generation) = authority_state(root)?;
                assert!(suffix <= limits.suffix.bytes);
                println!("revision={revision} suffix_bytes={suffix} generation={generation:02x?}");
            } else {
                println!(
                    "log_bytes={}",
                    fs::metadata(root.join("ordinary/records.log"))?.len()
                );
            }
        }
        _ => return Err("invalid phase".into()),
    }
    println!("suffix_work={}", limits.suffix.work);
    println!("phase={phase} backend={name} keys={keys} epoch={epoch}");
    Ok(())
}
