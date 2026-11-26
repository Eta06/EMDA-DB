//! Disposable demonstration of the durable-storage exit criteria in one
//! workload: independent concurrent clients, transactional atomicity,
//! recovery of every acknowledged write after process kills, and memory
//! measured against configured limits.
//!
//! `demo <directory> <rounds> <clients>` prepares a managed checkpoint store
//! under `<directory>/fixture` with automatic checkpoints, small suffix
//! limits that force frequent rollovers, and a persistent disk budget. Each
//! round starts a child process whose client threads run read-set
//! transactions moving balance between accounts and advancing a per-client
//! counter in the same transaction. A client prints an acknowledgement only
//! after its commit returns. The parent kills the child with SIGKILL partway
//! through, reopens the store and checks that:
//!
//! - the account total is unchanged, so no transfer was partly applied;
//! - every acknowledged counter value is present;
//! - at most one unacknowledged commit per client became durable;
//! - the disk budget has nothing pending and charges no more than its limit.
//!
//! Run the whole demonstration under an address-space limit, such as
//! `prlimit --as`, to bound each process, and under `/usr/bin/time` to
//! record peak resident memory. Process kills on a running OS are not
//! power-loss tests.
use database_engine::{
    BatchOperation, CheckpointStore, DiskBudgetPending, OnlineCheckpointStore, Store,
    is_transaction_conflict,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const ACCOUNTS: u64 = 16;
const START: u64 = 1_000;
const BUDGET: u64 = 64 * 1024 * 1024;

fn account(index: u64) -> Vec<u8> {
    format!("account-{index:02}").into_bytes()
}

fn counter(client: u64) -> Vec<u8> {
    format!("client-{client:02}").into_bytes()
}

fn number(bytes: Option<Vec<u8>>) -> Result<u64> {
    match bytes {
        None => Ok(0),
        Some(bytes) => Ok(u64::from_le_bytes(
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| "malformed number")?,
        )),
    }
}

fn open(fixture: &Path) -> io::Result<CheckpointStore> {
    // Small suffix limits make automatic rollovers frequent.
    CheckpointStore::open_managed_with_resource_limits(
        fixture.join("store"),
        16 * 1024,
        64,
        4096,
        None,
    )
}

fn prepare(fixture: &Path) -> Result<()> {
    std::fs::create_dir(fixture)?;
    let source = fixture.join("store");
    let ordinary = Store::open(&source)?;
    let balances = (0..ACCOUNTS)
        .map(|index| (account(index), START.to_le_bytes().to_vec()))
        .collect::<Vec<_>>();
    let batch = balances
        .iter()
        .map(|(key, value)| BatchOperation::Put { key, value })
        .collect::<Vec<_>>();
    ordinary.write_batch(&batch)?;
    let digest = ordinary.checkpoint_to(fixture.join("base"))?;
    drop(ordinary);
    let store = CheckpointStore::open(&source, fixture.join("base"), &digest)?;
    store.rollover_managed(fixture.join("managed"))?;
    let online = OnlineCheckpointStore::from_store(store)?;
    online.set_disk_budget(Some(BUDGET))?;
    online.with_store(CheckpointStore::enable_automatic_checkpoints)?;
    Ok(())
}

// One client: transfer between two accounts and advance its own counter in
// one serializable transaction, then acknowledge.
fn client(store: &CheckpointStore, id: u64, out: &std::sync::Mutex<io::Stdout>) -> Result<()> {
    let mut state = 0x2545_f491_4f6c_dd1d_u64 ^ (id + 1).wrapping_mul(0x9e37_79b9);
    loop {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let from = state % ACCOUNTS;
        let to = (from + 1 + (state >> 8) % (ACCOUNTS - 1)) % ACCOUNTS;
        let amount = 1 + (state >> 16) % 10;
        let committed = store.transaction_with_read_set(|tx| {
            let from_balance = number(tx.get(&account(from))?).map_err(io::Error::other)?;
            let to_balance = number(tx.get(&account(to))?).map_err(io::Error::other)?;
            let next = number(tx.get(&counter(id))?).map_err(io::Error::other)? + 1;
            let moved = amount.min(from_balance);
            tx.put(&account(from), &(from_balance - moved).to_le_bytes())?;
            tx.put(&account(to), &(to_balance + moved).to_le_bytes())?;
            tx.put(&counter(id), &next.to_le_bytes())?;
            Ok(next)
        });
        match committed {
            Ok(next) => {
                let mut out = out.lock().map_err(|_| "stdout poisoned")?;
                writeln!(out, "ack {id} {next}")?;
                out.flush()?;
            }
            Err(error) if is_transaction_conflict(&error) => {}
            // A pinned slot or a budget refusal changes nothing; retry.
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::StorageFull
                ) =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn clients(fixture: &Path, count: u64) -> Result<()> {
    let store = open(fixture)?;
    let out = std::sync::Mutex::new(io::stdout());
    std::thread::scope(|scope| -> Result<()> {
        let handles = (0..count)
            .map(|id| {
                let (store, out) = (&store, &out);
                scope.spawn(move || client(store, id, out))
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().map_err(|_| "client panicked")??;
        }
        Ok(())
    })
}

// Checks the recovered store against the acknowledged counters.
fn verify(fixture: &Path, acknowledged: &BTreeMap<u64, u64>, count: u64) -> Result<(u64, u64)> {
    let store = open(fixture)?;
    let mut total = 0;
    for index in 0..ACCOUNTS {
        total += number(store.get(&account(index))?)?;
    }
    if total != ACCOUNTS * START {
        return Err(format!("account total {total} differs; a transfer was partly applied").into());
    }
    let mut unacknowledged = 0;
    for id in 0..count {
        let stored = number(store.get(&counter(id))?)?;
        let acked = acknowledged.get(&id).copied().unwrap_or(0);
        if stored < acked {
            return Err(format!("client {id}: acknowledged {acked} but recovered {stored}").into());
        }
        if stored > acked + 1 {
            return Err(format!("client {id}: {stored} exceeds {acked} plus one in flight").into());
        }
        unacknowledged += stored - acked;
    }
    let status = store
        .disk_budget_status()?
        .ok_or("disk budget record disappeared")?;
    if status.pending != DiskBudgetPending::None
        || status.automatic_reserved != 0
        || status.charged > status.limit
    {
        return Err(format!("disk budget after recovery: {status:?}").into());
    }
    if !store.automatic_checkpoints_enabled()? {
        return Err("automatic checkpoints were not recovered".into());
    }
    Ok((unacknowledged, status.charged))
}

fn demo(directory: &Path, rounds: u64, count: u64) -> Result<()> {
    let fixture = directory.join("fixture");
    if fixture.exists() {
        return Err("fixture already exists".into());
    }
    prepare(&fixture)?;
    let executable = std::env::current_exe()?;
    let mut acknowledged = BTreeMap::<u64, u64>::new();
    let mut total_acks = 0_u64;
    let mut unacknowledged_durable = 0;
    let mut charged = 0;
    let start = Instant::now();
    for round in 0..rounds {
        let mut child = Command::new(&executable)
            .arg("clients")
            .arg(directory)
            .arg(count.to_string())
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = child.stdout.take().ok_or("no child stdout")?;
        // Kill after a varying number of acknowledgements.
        let target = 40 + round * 23 % 160;
        let mut seen = 0;
        let deadline = Instant::now() + Duration::from_secs(30);
        for line in BufReader::new(stdout).lines() {
            let line = line?;
            let mut parts = line.split_whitespace();
            if parts.next() != Some("ack") {
                return Err(format!("unexpected child output: {line}").into());
            }
            let id: u64 = parts.next().ok_or("missing client")?.parse()?;
            let value: u64 = parts.next().ok_or("missing value")?.parse()?;
            let entry = acknowledged.entry(id).or_default();
            *entry = (*entry).max(value);
            seen += 1;
            if seen >= target || Instant::now() > deadline {
                child.kill()?;
                break;
            }
        }
        let status = child.wait()?;
        if status.success() {
            return Err("client process exited before it was killed".into());
        }
        total_acks += seen;
        let (unacknowledged, bytes) = verify(&fixture, &acknowledged, count)?;
        unacknowledged_durable += unacknowledged;
        charged = bytes;
    }
    // Automatic rollovers publish engine-owned slots.
    let slots = std::fs::read_dir(fixture.join("store/.emda-auto-checkpoints-v1"))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("slot-"))
        .count();
    if slots == 0 {
        return Err("no automatic rollover happened".into());
    }
    println!(
        "{{\"automatic_slots\":{slots},\"rounds\":{rounds},\"clients\":{count},\"acknowledged\":{total_acks},\
\"recoveries_verified\":{rounds},\"unacknowledged_durable\":{unacknowledged_durable},\
\"budget_limit\":{BUDGET},\"final_charged\":{charged},\"elapsed_ms\":{}}}",
        start.elapsed().as_millis()
    );
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [mode, directory, rounds, count] if mode == "demo" => {
            demo(Path::new(directory), rounds.parse()?, count.parse()?)
        }
        [mode, directory, count] if mode == "clients" => {
            clients(&Path::new(directory).join("fixture"), count.parse()?)
        }
        _ => Err("usage: exit_demonstration demo <directory> <rounds> <clients>".into()),
    }
}
