use super::*;
use crate::retained_base_v2::Limits;
use crate::retained_candidate::{IndexLimits, OverlayLimits, SuffixLimits};
use std::{
    fs::OpenOptions,
    sync::{Arc, Barrier, Mutex},
};

const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 8192,
        entries: 32,
        key_bytes: 256,
        work_bytes: 8193,
    },
    index: IndexLimits {
        blocks: 4,
        anchor_bytes: 64,
    },
    suffix: SuffixLimits {
        bytes: 8192,
        operations: 64,
        work: 4096,
    },
    overlay: OverlayLimits {
        entries: 32,
        key_bytes: 256,
    },
};

#[derive(Clone, Default)]
struct Provider {
    record: Arc<Mutex<Option<AuthorityRecord>>>,
    fault: Option<(u64, bool)>,
    competing_append: Option<Arc<Barrier>>,
}

impl RetainedAuthority for Provider {
    fn read(&mut self) -> io::Result<Option<AuthorityRecord>> {
        Ok(*self.record.lock().unwrap())
    }

    fn compare_and_swap(
        &mut self,
        previous: Option<AuthorityRecord>,
        next: AuthorityRecord,
    ) -> io::Result<bool> {
        if next.revision == 1 {
            if let Some(barrier) = &self.competing_append {
                barrier.wait();
            }
        }
        let mut current = self.record.lock().unwrap();
        if self.fault == Some((next.revision, false)) {
            return Err(io::Error::other(
                "injected authority failure before replacement",
            ));
        }
        if *current != previous {
            return Ok(false);
        }
        *current = Some(next);
        if self.fault == Some((next.revision, true)) {
            return Err(io::Error::other(
                "injected authority failure after replacement",
            ));
        }
        Ok(true)
    }
}

struct Fixture {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    provider: Provider,
    initial: AuthorityRecord,
}

impl Fixture {
    fn prepare() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let root = tmp.path().join("authorized");
        let store = Store::open(&source).unwrap();
        store.put(b"base", b"imported").unwrap();
        let initial = AuthorizedRetainedStore::<Provider>::prepare_from_store(
            store,
            &source,
            &root,
            RetainedExportLimits {
                max_log_bytes: 8192,
                max_live_keys: 32,
                max_live_key_bytes: 256,
                max_output_bytes: 8192,
            },
            LIMITS,
        )
        .unwrap();
        Self {
            _tmp: tmp,
            root,
            provider: Provider::default(),
            initial,
        }
    }

    fn register(&mut self) {
        assert!(self.provider.compare_and_swap(None, self.initial).unwrap());
    }

    fn open(&self) -> AuthorizedRetainedStore<Provider> {
        AuthorizedRetainedStore::open(&self.root, LIMITS, self.provider.clone()).unwrap()
    }
}

#[test]
fn prepared_catalog_requires_authority_and_a_witnessed_role() {
    let mut f = Fixture::prepare();
    assert!(Store::open(&f.root).is_err());
    assert!(ManagedRetainedStore::open(&f.root, LIMITS).is_err());
    assert!(AuthorizedRetainedStore::open(&f.root, LIMITS, f.provider.clone()).is_err());
    fs::write(
        f.root.join(CATALOG_FILE),
        catalog_bytes(f.initial.committed.catalog, CATALOG_MAGIC),
    )
    .unwrap();
    assert!(ManagedRetainedStore::open(&f.root, LIMITS).is_err());
    fs::write(
        f.root.join(CATALOG_FILE),
        catalog_bytes(f.initial.committed.catalog, AUTHORIZED_MAGIC),
    )
    .unwrap();
    f.register();
    let mut store = f.open();
    assert_eq!(store.get(b"base", 8).unwrap().unwrap(), b"imported");
    store
        .write_batch(&[
            BatchOperation::Delete { key: b"base" },
            BatchOperation::Put {
                key: b"new",
                value: b"acknowledged",
            },
        ])
        .unwrap();
    assert_eq!(f.provider.read().unwrap().unwrap().revision, 2);
    assert_eq!(f.provider.read().unwrap().unwrap().pending, None);
    drop(store);
    let mut store = f.open();
    assert_eq!(store.get(b"base", 8).unwrap(), None);
    assert_eq!(store.get(b"new", 12).unwrap().unwrap(), b"acknowledged");
}

#[test]
fn reopening_resolves_either_side_of_uncertain_authority_updates() {
    for (revision, after, present) in [
        (1, false, false),
        (1, true, false),
        (2, false, true),
        (2, true, true),
    ] {
        let mut f = Fixture::prepare();
        f.register();
        let mut store = f.open();
        store.authority.fault = Some((revision, after));
        let failure = store.put(b"new", b"candidate").unwrap_err();
        if revision == 1 {
            assert!(matches!(failure, MutationFailure::Rejected(_)));
        } else {
            assert!(matches!(failure, MutationFailure::OutcomeUnknown(_)));
        }
        assert!(store.get(b"new", 9).is_err());
        drop(store);
        let mut recovered = f.open();
        assert_eq!(recovered.get(b"new", 9).unwrap().is_some(), present);
        assert!(f.provider.read().unwrap().unwrap().pending.is_none());
        recovered.delete(b"base").unwrap();
    }
}

#[test]
fn authority_rejects_rolled_back_and_unregistered_tails_without_adopting_them() {
    let mut f = Fixture::prepare();
    f.register();
    let slot = Slot::A.path(&f.root);
    let log_path = slot.join("records.log");
    let witness_path = slot.join("commit.state");
    let base_log = fs::read(&log_path).unwrap();
    let witness = fs::read(&witness_path).unwrap();
    let mut log = OpenOptions::new().append(true).open(&log_path).unwrap();
    log.write_all(&record::encode(Operation::Put, b"new", b"unregistered").unwrap())
        .unwrap();
    assert!(AuthorizedRetainedStore::open(&f.root, LIMITS, f.provider.clone()).is_err());
    assert_eq!(fs::read(&witness_path).unwrap(), witness);
    fs::write(&log_path, &base_log).unwrap();
    let mut store = f.open();
    store.put(b"new", b"acknowledged").unwrap();
    drop(store);
    fs::write(&log_path, base_log).unwrap();
    fs::write(&witness_path, &witness).unwrap();
    assert!(AuthorizedRetainedStore::open(&f.root, LIMITS, f.provider.clone()).is_err());
    assert_eq!(fs::read(&witness_path).unwrap(), witness);
}

#[test]
fn recovery_adopts_an_exact_proposed_tail_before_finalizing_a_pending_intent() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.authority.fault = Some((1, true));
    assert!(matches!(
        store.put(b"pending", b"proposed"),
        Err(MutationFailure::Rejected(_))
    ));
    drop(store);
    let log_path = Slot::A.path(&f.root).join("records.log");
    let encoded = record::encode(Operation::Put, b"pending", b"proposed").unwrap();
    let mut log = OpenOptions::new().append(true).open(&log_path).unwrap();
    log.write_all(&encoded).unwrap();
    let mut recovered = f.open();
    assert_eq!(recovered.get(b"pending", 8).unwrap().unwrap(), b"proposed");
    let current = f.provider.read().unwrap().unwrap();
    assert_eq!(current.committed.suffix_end, encoded.len() as u64);
    assert!(current.pending.is_none());
    drop(recovered);
    assert_eq!(f.open().get(b"pending", 8).unwrap().unwrap(), b"proposed");
}

#[test]
fn competing_copies_can_acknowledge_only_one_append() {
    let mut f = Fixture::prepare();
    f.register();
    let copy = f._tmp.path().join("copy");
    fs::create_dir(&copy).unwrap();
    for file in ["store.lock", CATALOG_FILE, ACTIVE_FILE] {
        fs::copy(f.root.join(file), copy.join(file)).unwrap();
    }
    fs::create_dir(Slot::A.path(&copy)).unwrap();
    for file in ["store.lock", "records.log", "commit.state", "base"] {
        fs::copy(
            Slot::A.path(&f.root).join(file),
            Slot::A.path(&copy).join(file),
        )
        .unwrap();
    }
    let mut first = f.open();
    let mut second = AuthorizedRetainedStore::open(&copy, LIMITS, f.provider.clone()).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    first.authority.competing_append = Some(barrier.clone());
    second.authority.competing_append = Some(barrier);
    let (left, right) = std::thread::scope(|scope| {
        let left = scope.spawn(|| first.put(b"answer", b"first"));
        let right = scope.spawn(|| second.put(b"answer", b"second"));
        (left.join().unwrap(), right.join().unwrap())
    });
    let (winner, loser, expected) = match (left, right) {
        (Ok(()), Err(MutationFailure::Rejected(_))) => (&f.root, &copy, &b"first"[..]),
        (Err(MutationFailure::Rejected(_)), Ok(())) => (&copy, &f.root, &b"second"[..]),
        other => panic!("expected a single winner, got {other:?}"),
    };
    drop((first, second));
    assert!(AuthorizedRetainedStore::open(loser, LIMITS, f.provider.clone()).is_err());
    assert_eq!(
        AuthorizedRetainedStore::open(winner, LIMITS, f.provider.clone())
            .unwrap()
            .get(b"answer", 8)
            .unwrap()
            .unwrap(),
        expected
    );
}

#[test]
fn selected_rotation_resets_the_suffix_and_reuses_both_slots() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.put(b"ack", b"kept").unwrap();
    let old = store.current.committed;
    let next = store.compact().unwrap();
    assert_ne!(next, old.generation);
    assert_eq!(f.provider.read().unwrap().unwrap().committed.suffix_end, 0);
    assert!(
        matches!(store.compact(), Err(PublicationFailure::NotPublished(error))
        if error.kind() == io::ErrorKind::AlreadyExists)
    );
    fs::remove_file(f.root.join(ACTIVE_FILE)).unwrap();
    drop(store);
    let mut store = f.open();
    assert_eq!(store.current.committed.generation, next);
    assert_eq!(store.get(b"ack", 4).unwrap().unwrap(), b"kept");
    assert!(store.reclaim_inactive().unwrap());
    assert!(!Slot::A.path(&f.root).exists());
    store.delete(b"base").unwrap();
    store.compact().unwrap();
    assert!(store.reclaim_inactive().unwrap());
    assert!(!Slot::B.path(&f.root).exists());
    drop(store);
    let mut reopened = f.open();
    assert_eq!(reopened.get(b"base", 8).unwrap(), None);
    assert_eq!(reopened.get(b"ack", 4).unwrap().unwrap(), b"kept");
}

#[test]
fn rotation_recovers_exact_authority_choice_after_uncertain_selection() {
    for selected in [false, true] {
        let mut f = Fixture::prepare();
        f.register();
        let mut store = f.open();
        store.put(b"ack", b"retained").unwrap();
        let prior = store.current.committed;
        store.authority.fault = Some((3, selected));
        let result = store.compact();
        assert!(matches!(result, Err(PublicationFailure::OutcomeUnknown(_))));
        assert!(store.get(b"ack", 8).is_err());
        assert!(Slot::A.path(&f.root).is_dir());
        assert!(Slot::B.path(&f.root).is_dir());
        drop(store);
        let mut recovered = f.open();
        assert_eq!(recovered.current.committed != prior, selected);
        assert_eq!(recovered.get(b"ack", 8).unwrap().unwrap(), b"retained");
        assert!(recovered.reclaim_inactive().unwrap());
        assert_eq!(Slot::B.path(&f.root).exists(), selected);
        assert_eq!(Slot::A.path(&f.root).exists(), !selected);
        recovered.put(b"next", b"latest").unwrap();
    }
}

#[test]
fn selected_missing_or_corrupt_successor_cannot_fall_back() {
    for missing in [false, true] {
        let mut f = Fixture::prepare();
        f.register();
        let mut store = f.open();
        store.put(b"ack", b"retained").unwrap();
        store.compact().unwrap();
        drop(store);
        let selected = Slot::B.path(&f.root).join("base");
        if missing {
            fs::rename(&selected, selected.with_extension("saved")).unwrap();
        } else {
            fs::write(&selected, b"corrupt").unwrap();
        }
        let old = Slot::A.path(&f.root).join("commit.state");
        let witness = fs::read(&old).unwrap();
        assert!(AuthorizedRetainedStore::open(&f.root, LIMITS, f.provider.clone()).is_err());
        assert_eq!(fs::read(&old).unwrap(), witness);
    }
}

#[test]
fn reclamation_fence_failure_keeps_the_inactive_generation() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.compact().unwrap();
    store.authority.fault = Some((2, true));
    assert!(store.reclaim_inactive().is_err());
    assert!(Slot::A.path(&f.root).exists());
    assert!(store.get(b"base", 8).is_err());
    drop(store);
    assert!(f.open().reclaim_inactive().unwrap());
    assert!(!Slot::A.path(&f.root).exists());
}

#[test]
fn file_provider_fences_catalog_rollback_after_rotation() {
    let f = Fixture::prepare();
    let directory = f._tmp.path().join("independent-authority");
    let mut provider = FileRetainedAuthority::create(&directory).unwrap();
    assert!(provider.compare_and_swap(None, f.initial).unwrap());
    let before = Slot::A.path(&f.root);
    let log = fs::read(before.join("records.log")).unwrap();
    let witness = fs::read(before.join("commit.state")).unwrap();
    let mut store = AuthorizedRetainedStore::open(&f.root, LIMITS, provider).unwrap();
    store.put(b"accepted", b"new").unwrap();
    store.compact().unwrap();
    drop(store);
    let mut store = AuthorizedRetainedStore::open(
        &f.root,
        LIMITS,
        FileRetainedAuthority::open(&directory).unwrap(),
    )
    .unwrap();
    assert_eq!(store.get(b"accepted", 3).unwrap().unwrap(), b"new");
    drop(store);
    let selected = Slot::B.path(&f.root);
    fs::rename(&selected, f.root.join("saved-generation")).unwrap();
    fs::write(before.join("records.log"), log).unwrap();
    fs::write(before.join("commit.state"), witness).unwrap();
    assert!(
        AuthorizedRetainedStore::open(
            &f.root,
            LIMITS,
            FileRetainedAuthority::open(&directory).unwrap()
        )
        .is_err()
    );
}
