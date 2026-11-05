#![no_main]
use database_engine::{
    CheckpointSnapshot, CheckpointStore, CheckpointView, MAX_ACTIVE_SNAPSHOTS,
    MAX_CHECKPOINT_ACTIVE_SNAPSHOTS, MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS,
    MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES, MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
    MAX_SNAPSHOT_TOTAL_KEY_BYTES, MAX_SNAPSHOT_TOTAL_KEYS, Snapshot, Store,
};
use libfuzzer_sys::fuzz_target;
use std::io::{self, ErrorKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct State {
    active: usize,
    blocks: usize,
    keys: usize,
    bytes: usize,
    active_limit: usize,
    block_limit: usize,
    key_limit: usize,
    byte_limit: usize,
}
enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
enum Held<'a> {
    Ordinary(Snapshot<'a>),
    Checkpoint(CheckpointSnapshot<'a>),
}
struct Reservation<'a> {
    snapshot: Held<'a>,
    active: bool,
}
impl Held<'_> {
    fn get(&self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(s) => s.get(b"a"),
            Self::Checkpoint(s) => s.get(b"a"),
        }
    }
    fn cancel(&mut self) {
        match self {
            Self::Ordinary(s) => s.cancel(),
            Self::Checkpoint(s) => s.cancel(),
        }
    }
}
impl Backend {
    fn snapshot(&self) -> io::Result<Held<'_>> {
        match self {
            Self::Ordinary(s) => s.snapshot(16, 128).map(Held::Ordinary),
            Self::Checkpoint(s) => s.snapshot().map(Held::Checkpoint),
        }
    }
    fn set(&self, active: usize, blocks: usize, keys: usize, bytes: usize) -> io::Result<()> {
        match self {
            Self::Ordinary(s) => s.set_snapshot_budget_limits(active, keys, bytes),
            Self::Checkpoint(s) => s.set_snapshot_budget_limits(active, blocks, keys, bytes),
        }
    }
    fn get(&self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(s) => s.get(b"bb"),
            Self::Checkpoint(s) => s.get(b"bb"),
        }
    }
    fn status(&self) -> State {
        match self {
            Self::Ordinary(s) => {
                let s = s.snapshot_budget_status().unwrap();
                State {
                    active: s.active,
                    blocks: 0,
                    keys: s.keys,
                    bytes: s.key_bytes,
                    active_limit: s.active_limit,
                    block_limit: usize::MAX,
                    key_limit: s.key_limit,
                    byte_limit: s.key_byte_limit,
                }
            }
            Self::Checkpoint(s) => {
                let s = s.snapshot_budget_status().unwrap();
                State {
                    active: s.active,
                    blocks: s.blocks,
                    keys: s.keys,
                    bytes: s.key_bytes,
                    active_limit: s.active_limit,
                    block_limit: s.block_limit,
                    key_limit: s.key_limit,
                    byte_limit: s.key_byte_limit,
                }
            }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"bb", b"two").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
    let blocks = view.block_count();
    drop(view);
    let checkpoint_mode = data[0] & 1 != 0;
    let backend = if checkpoint_mode {
        Backend::Checkpoint(CheckpointStore::open(&path, &checkpoint, &digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(&path).unwrap())
    };
    let (charge_blocks, charge_keys, charge_bytes) = if checkpoint_mode {
        (blocks, 0, 0)
    } else {
        (0, 2, 3)
    };
    let mut limits = if checkpoint_mode {
        (
            MAX_CHECKPOINT_ACTIVE_SNAPSHOTS,
            MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS,
            MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
            MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES,
        )
    } else {
        (
            MAX_ACTIVE_SNAPSHOTS,
            usize::MAX,
            MAX_SNAPSHOT_TOTAL_KEYS,
            MAX_SNAPSHOT_TOTAL_KEY_BYTES,
        )
    };
    let mut held = Vec::<Reservation<'_>>::new();
    for chunk in data[1..].chunks_exact(3) {
        let active = held.iter().filter(|r| r.active).count();
        match chunk[0] % 6 {
            0 => {
                let proposed = (
                    usize::from(chunk[1] % 5),
                    if checkpoint_mode {
                        usize::from(chunk[2] % 5)
                    } else {
                        usize::MAX
                    },
                    usize::from((chunk[1] >> 2) % 9),
                    usize::from((chunk[2] >> 2) % 13),
                );
                let possible = proposed.0 >= active
                    && proposed.1 >= active * charge_blocks
                    && proposed.2 >= active * charge_keys
                    && proposed.3 >= active * charge_bytes;
                let result = backend.set(proposed.0, proposed.1, proposed.2, proposed.3);
                if possible {
                    result.unwrap();
                    limits = proposed;
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            1 if held.len() < 16 => {
                let allowed = active < limits.0
                    && (active + 1) * charge_blocks <= limits.1
                    && (active + 1) * charge_keys <= limits.2
                    && (active + 1) * charge_bytes <= limits.3;
                let result = backend.snapshot();
                if allowed {
                    let snapshot = result.unwrap();
                    assert_eq!(snapshot.get().unwrap().as_deref(), Some(&b"one"[..]));
                    held.push(Reservation {
                        snapshot,
                        active: true,
                    });
                } else {
                    assert_eq!(result.err().unwrap().kind(), ErrorKind::Other);
                }
            }
            2 if !held.is_empty() => {
                let position = usize::from(chunk[1]) % held.len();
                held[position].snapshot.cancel();
                held[position].active = false;
                assert!(held[position].snapshot.get().is_err());
            }
            3 if !held.is_empty() => {
                held.remove(usize::from(chunk[1]) % held.len());
            }
            4 => {
                let before = backend.status();
                let hard = if checkpoint_mode {
                    MAX_CHECKPOINT_ACTIVE_SNAPSHOTS
                } else {
                    MAX_ACTIVE_SNAPSHOTS
                };
                assert_eq!(
                    backend.set(hard + 1, 0, 0, 0).unwrap_err().kind(),
                    ErrorKind::InvalidInput
                );
                assert_eq!(backend.status(), before);
            }
            _ => {
                assert_eq!(backend.get().unwrap().as_deref(), Some(&b"two"[..]));
            }
        }
        let active = held.iter().filter(|r| r.active).count();
        assert_eq!(
            backend.status(),
            State {
                active,
                blocks: active * charge_blocks,
                keys: active * charge_keys,
                bytes: active * charge_bytes,
                active_limit: limits.0,
                block_limit: limits.1,
                key_limit: limits.2,
                byte_limit: limits.3
            }
        );
        for reservation in &held {
            if reservation.active {
                assert_eq!(
                    reservation.snapshot.get().unwrap().as_deref(),
                    Some(&b"one"[..])
                );
            } else {
                assert!(reservation.snapshot.get().is_err());
            }
        }
    }
    drop(held);
    assert_eq!(backend.status().active, 0);
    drop(backend);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(reopened.get(b"bb").unwrap().as_deref(), Some(&b"two"[..]));
});
