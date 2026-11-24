use super::*;
use crate::record::Operation;
use crate::retained_base_v2::Limits;
use crate::retained_candidate::writable::decode_marker;
use crate::retained_candidate::{IndexLimits, OverlayLimits, SuffixLimits};
use std::{
    fs::OpenOptions,
    sync::{Arc, Barrier, Mutex},
};

pub(super) mod crash;

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
        Self::prepare_with_limits(LIMITS)
    }

    fn prepare_with_limits(limits: RetainedStoreLimits) -> Self {
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
            limits,
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

    fn open_auto(&self, threshold: NonZeroU64) -> AuthorizedRetainedStore<Provider> {
        AuthorizedRetainedStore::open_with_auto_reclaim(
            &self.root,
            LIMITS,
            self.provider.clone(),
            threshold,
        )
        .unwrap()
    }
}

#[test]
fn uncertain_backup_restore_keeps_its_verified_registration_record() {
    type Catalog = AuthorizedRetainedStore<Provider>;
    let mut fixture = Fixture::prepare();
    fixture.register();
    let mut source = fixture.open();
    source.put(b"suffix", b"acknowledged").unwrap();
    let original = fixture.provider.read().unwrap();
    let parent = fixture.root.parent().unwrap();
    let backup = parent.join("backup");
    let binding = source.backup_to(&backup).unwrap();
    let destination = parent.join("restored");
    let result = Catalog::prepare_from_backup_using(
        &backup,
        &destination,
        LIMITS.into(),
        &binding,
        |_| Err(io::Error::other("injected restored parent sync error")),
        |_| Ok(()),
    );
    let prepared = match result {
        Err(AuthorizedRestoreFailure::OutcomeUnknown { error, prepared }) => {
            assert_eq!(error.to_string(), "injected restored parent sync error");
            *prepared
        }
        other => panic!("expected a verified uncertain publication, got {other:?}"),
    };
    assert!(Catalog::open(&destination, LIMITS, Provider::default()).is_err());
    assert!(ManagedRetainedStore::open(&destination, LIMITS).is_err());
    assert!(
        Catalog::register_prepared(&destination, LIMITS, fixture.provider.clone(), prepared)
            .is_err()
    );
    assert_eq!(fixture.provider.read().unwrap(), original);

    let mut provider = Provider::default();
    let mut lost_response = provider.clone();
    lost_response.fault = Some((0, true));
    assert!(Catalog::register_prepared(&destination, LIMITS, lost_response, prepared).is_err());
    assert_eq!(provider.read().unwrap(), Some(prepared));
    let mut restored =
        Catalog::register_prepared(&destination, LIMITS, provider.clone(), prepared).unwrap();
    assert_eq!(
        restored.get(b"suffix", 12).unwrap().unwrap(),
        b"acknowledged"
    );
    restored.put(b"suffix", b"restored only").unwrap();
    assert_eq!(source.get(b"suffix", 12).unwrap().unwrap(), b"acknowledged");
    assert_eq!(fixture.provider.read().unwrap(), original);
    drop(restored);
    assert!(Catalog::register_prepared(&destination, LIMITS, provider, prepared).is_err());
}

#[test]
fn backup_restore_rejects_bad_inputs_and_conflicts_without_registering() {
    type Catalog = AuthorizedRetainedStore<Provider>;
    let mut fixture = Fixture::prepare();
    fixture.register();
    let mut source = fixture.open();
    source.put(b"suffix", b"value").unwrap();
    let parent = fixture.root.parent().unwrap();
    let backup = parent.join("backup");
    let binding = source.backup_to(&backup).unwrap();
    let destination = parent.join("restored");
    let mut bad = binding.clone();
    bad.digest[0] ^= 1;
    assert!(matches!(
        Catalog::prepare_from_backup(&backup, &destination, LIMITS, &bad),
        Err(AuthorizedRestoreFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    assert!(matches!(
        Catalog::prepare_from_backup_with_storage_limits(
            &backup, &destination, LIMITS, CatalogStorageLimits { bytes: 1, entries: 8 }, &binding
        ),
        Err(AuthorizedRestoreFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(!destination.exists());
    let mut recorded = None;
    let result = Catalog::prepare_from_backup_recorded(
        &backup,
        &destination,
        LIMITS,
        None,
        &binding,
        |prepared| {
            recorded = Some(prepared);
            Err(io::Error::other("injected receipt persistence failure"))
        },
    );
    assert!(matches!(
        result,
        Err(AuthorizedRestoreFailure::NotPublished(error))
            if error.to_string() == "injected receipt persistence failure"
    ));
    assert!(recorded.is_some());
    assert!(!destination.exists());
    let prepared = Catalog::prepare_from_backup(&backup, &destination, LIMITS, &binding).unwrap();
    let wrong = match Catalog::prepare_from_backup(&backup, &destination, LIMITS, &binding) {
        Err(AuthorizedRestoreFailure::OutcomeUnknown { prepared, .. }) => *prepared,
        other => panic!("expected a no-clobber conflict, got {other:?}"),
    };
    let mut provider = Provider::default();
    assert_ne!(prepared, wrong);
    assert!(Catalog::register_prepared(&destination, LIMITS, provider.clone(), wrong).is_err());
    assert_eq!(provider.read().unwrap(), None);
    let log = Slot::A.path(&destination).join("records.log");
    let original = fs::read(&log).unwrap();
    let mut damaged = original.clone();
    *damaged.last_mut().unwrap() ^= 1;
    fs::write(&log, damaged).unwrap();
    assert!(Catalog::register_prepared(&destination, LIMITS, provider.clone(), prepared).is_err());
    assert_eq!(provider.read().unwrap(), None);
    fs::write(&log, original).unwrap();
    let mut registered =
        Catalog::register_prepared(&destination, LIMITS, provider, prepared).unwrap();
    assert_eq!(registered.get(b"suffix", 5).unwrap().unwrap(), b"value");
}

#[test]
fn catalog_storage_reserves_headroom_for_automatic_authorized_reclamation() {
    use crate::retained_candidate::writable::managed::tests::catalog_usage;
    use crate::staging::Stage;
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.put(b"suffix", b"kept").unwrap();
    let selection = store.current;
    let base_bytes = 144
        + store
            .scan(None, None, 8)
            .unwrap()
            .iter()
            .map(|(key, value)| (8 + key.len() + value.len()) as u64)
            .sum::<u64>();
    drop(store);
    let used = catalog_usage(&f.root);
    let fixed = (crate::retained_candidate::writable::MANAGED_HEADER_BYTES
        + crate::retained_candidate::writable::WITNESS_BYTES) as u64;
    let exact = CatalogStorageLimits {
        bytes: used.bytes + base_bytes + fixed + Stage::RETAINED_MARKER_BYTES,
        entries: used.entries + 7,
    };
    let mut store = AuthorizedRetainedStore::open_with_storage_limits(
        &f.root,
        LIMITS,
        f.provider.clone(),
        CatalogStorageLimits {
            entries: exact.entries - 1,
            ..exact
        },
        Some(NonZeroU64::MIN),
    )
    .unwrap();
    assert!(
        matches!(store.put(b"new", b"ack"), Err(MutationFailure::Rejected(ref e))
        if e.kind() == io::ErrorKind::InvalidInput)
    );
    assert_eq!(store.authority.read().unwrap(), Some(selection));
    assert_eq!(catalog_usage(&f.root), used);
    drop(store);
    let mut store = AuthorizedRetainedStore::open_with_storage_limits(
        &f.root,
        LIMITS,
        f.provider.clone(),
        exact,
        Some(NonZeroU64::MIN),
    )
    .unwrap();
    store.put(b"new", b"ack").unwrap();
    assert!(!Slot::A.path(&f.root).exists());
    assert_ne!(
        store.current.committed.generation,
        selection.committed.generation
    );
    drop(store);
    let mut store = AuthorizedRetainedStore::open_with_storage_limits(
        &f.root,
        LIMITS,
        f.provider.clone(),
        exact,
        None,
    )
    .unwrap();
    assert_eq!(store.get(b"suffix", 4).unwrap().unwrap(), b"kept");
    assert_eq!(store.get(b"new", 3).unwrap().unwrap(), b"ack");
}

#[test]
fn persisted_catalog_storage_preserves_room_for_manual_retirement() {
    use crate::retained_candidate::writable::managed::tests::catalog_usage;
    use crate::retained_candidate::writable::{QUOTA_HEADER_BYTES, WITNESS_BYTES};
    use crate::staging::Stage;

    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.put(b"suffix", b"kept").unwrap();
    let before = store.current;
    let used = catalog_usage(&f.root);
    let base_bytes = 144
        + store
            .scan(None, None, 8)
            .unwrap()
            .iter()
            .map(|(key, value)| (8 + key.len() + value.len()) as u64)
            .sum::<u64>();
    let exact = CatalogStorageLimits {
        bytes: used.bytes
            + base_bytes
            + (QUOTA_HEADER_BYTES + WITNESS_BYTES) as u64
            + Stage::RETAINED_MARKER_BYTES,
        entries: used.entries + 7,
    };
    for (limits, message) in [
        (
            CatalogStorageLimits {
                entries: exact.entries - 1,
                ..exact
            },
            "catalog entry limit exceeded",
        ),
        (
            CatalogStorageLimits {
                bytes: exact.bytes - 1,
                ..exact
            },
            "compacted base byte limit exceeded",
        ),
    ] {
        assert!(matches!(
            store.compact_with_persisted_storage_limits_detailed(limits),
            Err(RetainedRotationFailure::NotPublished(ref error))
                if error.kind() == io::ErrorKind::InvalidInput && error.to_string() == message
        ));
        assert_eq!(store.authority.read().unwrap(), Some(before));
        assert_eq!(catalog_usage(&f.root), used);
        assert!(!Slot::B.path(&f.root).exists());
    }
    let selected = store.compact_with_persisted_storage_limits(exact).unwrap();
    assert_ne!(selected, before.committed.generation);
    let selection = store.current;
    assert!(Slot::A.path(&f.root).is_dir());
    drop(store);

    let mut store = f.open();
    assert_eq!(store.inner.store.header.storage_limits(), Some(exact));
    let usage = catalog_usage(&f.root);
    assert_eq!(usage.bytes + Stage::RETAINED_MARKER_BYTES, exact.bytes);
    assert_eq!(usage.entries + 2, exact.entries);
    let active = Slot::B.path(&f.root);
    let log = fs::read(active.join("records.log")).unwrap();
    let witness = fs::read(active.join("commit.state")).unwrap();
    assert!(matches!(
        store.put(b"new", b"ack"),
        Err(MutationFailure::Rejected(ref error))
            if error.kind() == io::ErrorKind::InvalidInput
                && error.to_string() == "catalog byte limit exceeded"
    ));
    assert_eq!(store.authority.read().unwrap(), Some(selection));
    assert_eq!(catalog_usage(&f.root), usage);
    assert_eq!(fs::read(active.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(active.join("commit.state")).unwrap(), witness);
    assert_eq!(store.get(b"new", 3).unwrap(), None);

    assert!(store.reclaim_inactive().unwrap());
    assert!(!Slot::A.path(&f.root).exists());
    store.put(b"new", b"ack").unwrap();
    drop(store);

    let mut store = f.open();
    assert_eq!(store.inner.store.header.storage_limits(), Some(exact));
    assert_eq!(store.get(b"suffix", 4).unwrap().unwrap(), b"kept");
    assert_eq!(store.get(b"new", 3).unwrap().unwrap(), b"ack");
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
fn authority_and_local_witness_bind_the_same_appended_suffix() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    let mut observed = Sha256::new();
    let mut end = 0;
    for (key, value) in [(b"first", b"one"), (b"other", b"two")] {
        store.put(key, value).unwrap();
        let encoded = record::encode(Operation::Put, key, value).unwrap();
        observed.update(&encoded);
        end += encoded.len() as u64;
        let digest: [u8; 32] = observed.clone().finalize().into();
        let record = f.provider.read().unwrap().unwrap();
        assert!(record.pending.is_none());
        assert_eq!(record.committed.suffix_end, end);
        assert_eq!(record.committed.suffix_digest, digest);
        let generation = &store.inner.store;
        assert_eq!(
            decode_marker(
                &read_marker(&generation.witness).unwrap(),
                generation.header.bytes(),
            )
            .unwrap(),
            (end, digest)
        );
    }
    drop(store);
    let mut reopened = f.open();
    assert_eq!(
        reopened.get(b"first", 3).unwrap().as_deref(),
        Some(&b"one"[..])
    );
    assert_eq!(
        reopened.get(b"other", 3).unwrap().as_deref(),
        Some(&b"two"[..])
    );
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
fn document_commit_recovers_index_and_receipt_together_after_authority_faults() {
    use crate::{
        CollectionSchema, DocumentDatabase, DocumentTransactionMode, IndexDefinition, IndexQuery,
        TransactionFailure,
    };
    use serde_json::json;

    for (phase, after) in [(1, false), (1, true), (2, false), (2, true)] {
        let mut f = Fixture::prepare();
        f.register();
        let mut store = f.open();
        let db = DocumentDatabase::with_authorized_retained_store(&mut store);
        db.create_collection("items", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "items",
            IndexDefinition {
                name: "score".into(),
                fields: vec![vec!["n".into()]],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
        let revision = store.current.revision;
        store.authority.fault = Some((revision + phase, after));
        let db = DocumentDatabase::with_authorized_retained_store(&mut store);
        let result = db.transaction_detailed(DocumentTransactionMode::ReadSet, None, None, |tx| {
            tx.insert("items", "a", &json!({"n": 1}))?;
            tx.put_raw(b"receipt", b"written")
        });
        let committed = phase == 2;
        match result {
            Err(TransactionFailure::Rejected(error)) if !committed => {
                assert!(error.to_string().contains("injected authority failure"));
            }
            Err(TransactionFailure::OutcomeUnknown(error)) if committed => {
                assert!(error.to_string().contains("injected authority failure"));
            }
            _ => panic!("unexpected document transaction outcome: {result:?}"),
        }
        assert!(db.get("items", "a").is_err());
        drop(store);

        let mut recovered = f.open();
        let db = DocumentDatabase::with_authorized_retained_store(&mut recovered);
        assert_eq!(
            db.get("items", "a").unwrap(),
            committed.then(|| json!({"n": 1}))
        );
        assert_eq!(
            db.query_index("items", "score", &IndexQuery::default())
                .unwrap()
                .documents
                .len(),
            usize::from(committed)
        );
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
            committed.then(|| b"written".to_vec())
        );
        db.verify_collection("items").unwrap();
    }
}

#[test]
fn document_read_rejects_an_authority_change_during_the_callback() {
    use crate::{DocumentDatabase, DocumentTransactionMode, TransactionFailure};

    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    let db = DocumentDatabase::with_authorized_retained_store(&mut store);
    let next = f.initial.finalized(f.initial.committed).unwrap();
    assert!(
        matches!(db.transaction_detailed(DocumentTransactionMode::ReadSet, None, None, |tx| {
        assert_eq!(tx.get_raw(b"base")?.unwrap(), b"imported");
        assert!(f.provider.compare_and_swap(Some(f.initial), next)?);
        Ok(())
    }), Err(TransactionFailure::Rejected(error)) if error.to_string() == "retained authority changed")
    );
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
fn recovery_admits_only_authorized_ends_and_authenticates_the_accepted_length() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.put(b"earlier", b"acknowledged").unwrap();
    store.authority.fault = Some((store.current.revision + 1, true));
    assert!(matches!(
        store.put(b"pending", b"proposed"),
        Err(MutationFailure::Rejected(_))
    ));
    drop(store);

    let slot = Slot::A.path(&f.root);
    let path = slot.join("records.log");
    let witness_path = slot.join("commit.state");
    let witness = fs::read(&witness_path).unwrap();
    let prefix = fs::read(&path).unwrap();
    let authority = f.provider.read().unwrap().unwrap();
    let proposed = record::encode(Operation::Put, b"pending", b"proposed").unwrap();
    let substitute = record::encode(Operation::Put, b"pending", b"replaced").unwrap();
    assert_eq!(proposed.len(), substitute.len());
    assert_eq!(
        authority.pending.unwrap().suffix_end,
        authority.committed.suffix_end + proposed.len() as u64
    );
    // Keep the base frame intact while making a full base scan fail. An
    // unauthorized end or digest should be rejected before that scan.
    let base_path = slot.join("base");
    let base = fs::read(&base_path).unwrap();
    let mut corrupt_base = base.clone();
    corrupt_base[base.len() - 33] ^= 1;
    fs::write(&base_path, &corrupt_base).unwrap();
    for (tail, message) in [
        (
            &proposed[..proposed.len() - 1],
            "retained suffix length does not match independent authority",
        ),
        (
            substitute.as_slice(),
            "retained state does not match independent authority",
        ),
    ] {
        let mut log = prefix.clone();
        log.extend_from_slice(tail);
        fs::write(&path, &log).unwrap();
        let error = AuthorizedRetainedStore::open(&f.root, LIMITS, f.provider.clone())
            .err()
            .expect("the unapproved suffix must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), message);
        assert_eq!(fs::read(&path).unwrap(), log);
        assert_eq!(fs::read(&witness_path).unwrap(), witness);
        assert_eq!(fs::read(&base_path).unwrap(), corrupt_base);
        assert_eq!(f.provider.read().unwrap(), Some(authority));
    }

    fs::write(&path, [&prefix[..], &proposed].concat()).unwrap();
    let error = AuthorizedRetainedStore::open(&f.root, LIMITS, f.provider.clone())
        .err()
        .expect("a permitted suffix cannot authorize a damaged base");
    assert_eq!(error.to_string(), "digest mismatch");
    assert_eq!(fs::read(&witness_path).unwrap(), witness);
    assert_eq!(f.provider.read().unwrap(), Some(authority));
    fs::write(&base_path, base).unwrap();
    let mut reopened = f.open();
    assert_eq!(
        reopened.get(b"earlier", 12).unwrap().as_deref(),
        Some(b"acknowledged".as_slice())
    );
    assert_eq!(
        reopened.get(b"pending", 8).unwrap().as_deref(),
        Some(b"proposed".as_slice())
    );
    let resolved = f.provider.read().unwrap().unwrap();
    assert!(resolved.pending.is_none());
    assert_eq!(resolved.committed, authority.pending.unwrap());
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
    let witnessed_path = Slot::A.path(&f.root).join("commit.state");
    let witness = fs::read(&witnessed_path).unwrap();
    let original_log = fs::read(&log_path).unwrap();
    let authority = f.provider.read().unwrap();
    let open = |cap| {
        AuthorizedRetainedStore::open_with_suffix_scan_limit(
            &f.root,
            LIMITS,
            f.provider.clone(),
            cap,
            None,
        )
    };
    let error = open((2 * encoded.len() - 1) as u64).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "retained recovery suffix scan byte limit exceeded"
    );
    assert_eq!(fs::read(&log_path).unwrap(), original_log);
    assert_eq!(fs::read(&witnessed_path).unwrap(), witness);
    assert_eq!(f.provider.clone().read().unwrap(), authority);

    let base_bytes = fs::metadata(Slot::A.path(&f.root).join("base"))
        .unwrap()
        .len();
    let open_combined = |cap| {
        AuthorizedRetainedStore::open_with_recovery_scan_limit(
            &f.root,
            LIMITS,
            f.provider.clone(),
            cap,
            None,
        )
    };
    let cap = base_bytes + (2 * encoded.len()) as u64;
    assert_eq!(
        open_combined(cap - 1).err().unwrap().to_string(),
        "retained recovery scan byte limit exceeded"
    );
    assert_eq!(fs::read(&log_path).unwrap(), original_log);
    assert_eq!(fs::read(&witnessed_path).unwrap(), witness);
    assert_eq!(f.provider.clone().read().unwrap(), authority);
    let mut recovered = open_combined(cap).unwrap();
    assert_eq!(recovered.get(b"pending", 8).unwrap().unwrap(), b"proposed");
    let current = f.provider.read().unwrap().unwrap();
    assert_eq!(current.committed.suffix_end, encoded.len() as u64);
    assert!(current.pending.is_none());
    drop(recovered);
    assert_eq!(f.open().get(b"pending", 8).unwrap().unwrap(), b"proposed");
}

#[test]
fn pending_recovery_requires_local_sync_even_with_an_advanced_witness() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.authority.fault = Some((2, false));
    assert!(matches!(
        store.put(b"pending", b"proposed"),
        Err(MutationFailure::OutcomeUnknown(_))
    ));
    drop(store);

    let pending = f.provider.read().unwrap().unwrap();
    let proposed = pending.pending.unwrap();
    let slot = Slot::A.path(&f.root);
    let log = fs::read(slot.join("records.log")).unwrap();
    let witness = fs::read(slot.join("commit.state")).unwrap();
    assert_eq!(
        u64::from_le_bytes(witness[40..48].try_into().unwrap()),
        proposed.suffix_end
    );
    let error = AuthorizedRetainedStore::open_using(
        &f.root,
        LIMITS,
        f.provider.clone(),
        (None, None),
        |store| {
            assert_eq!(
                AuthorityPoint::from_store(proposed.catalog, store),
                proposed
            );
            store.log.sync_data()?;
            Err(io::Error::other("injected recovery witness sync failure"))
        },
    )
    .err()
    .expect("recovery must return the sync failure before advancing authority");
    assert_eq!(error.to_string(), "injected recovery witness sync failure");
    assert_eq!(f.provider.read().unwrap(), Some(pending));
    assert_eq!(fs::read(slot.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(slot.join("commit.state")).unwrap(), witness);

    let error = AuthorizedRetainedStore::open_with_suffix_scan_limit(
        &f.root,
        LIMITS,
        f.provider.clone(),
        proposed.suffix_end - 1,
        None,
    )
    .err()
    .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(f.provider.read().unwrap(), Some(pending));
    let mut reopened = AuthorizedRetainedStore::open_with_suffix_scan_limit(
        &f.root,
        LIMITS,
        f.provider.clone(),
        proposed.suffix_end,
        None,
    )
    .unwrap();
    assert_eq!(reopened.get(b"pending", 8).unwrap().unwrap(), b"proposed");
    let committed = f.provider.read().unwrap().unwrap();
    assert_eq!(committed.committed, proposed);
    assert!(committed.pending.is_none());
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
fn automatic_rotation_recycles_slots_at_threshold_and_hard_admission_limits() {
    for (limits, threshold) in [
        (LIMITS, NonZeroU64::MIN),
        (
            RetainedStoreLimits {
                suffix: SuffixLimits {
                    operations: 1,
                    ..LIMITS.suffix
                },
                ..LIMITS
            },
            NonZeroU64::MAX,
        ),
    ] {
        let mut f = Fixture::prepare_with_limits(limits);
        f.register();
        let mut store = f.open_auto(threshold);
        store.put(b"ack", b"kept").unwrap();
        assert_eq!(
            store.current.committed.generation,
            f.initial.committed.generation
        );
        store.delete(b"base").unwrap();
        assert_eq!(store.inner.active.slot, Slot::B);
        assert!(!Slot::A.path(&f.root).exists());
        store.put(b"next", b"latest").unwrap();
        assert_eq!(store.inner.active.slot, Slot::A);
        assert!(!Slot::B.path(&f.root).exists());
        assert_eq!(f.provider.read().unwrap().unwrap().revision, 10);
        drop(store);
        assert_eq!(
            f.open_auto(threshold).scan(None, None, 4).unwrap(),
            [
                (b"ack".to_vec(), b"kept".to_vec()),
                (b"next".to_vec(), b"latest".to_vec()),
            ]
        );
    }
}

#[test]
fn automatic_rotation_rejects_impossible_writes_without_changing_authority() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open_auto(NonZeroU64::MIN);
    store.put(b"ack", b"kept").unwrap();
    let prior = store.current;
    assert!(matches!(store.put(b"oversize", &[0; 8192]),
        Err(MutationFailure::Rejected(error))
            if error.get_ref().is_some_and(|cause| cause.is::<SuffixCapacity>())));
    assert_eq!(f.provider.read().unwrap(), Some(prior));
    assert!(!Slot::B.path(&f.root).exists());
    assert_eq!(store.get(b"ack", 4).unwrap().unwrap(), b"kept");
}

#[test]
fn automatic_rotation_rechecks_admission_after_maintenance_before_intent() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open_auto(NonZeroU64::MIN);
    store.put(b"ack", b"kept").unwrap();
    let ack = store.current;
    let mut inspect = f.provider.clone();
    let result = store.write_batch_checking(
        &[BatchOperation::Put {
            key: b"new",
            value: b"cancelled",
        }],
        || {
            if inspect.record.lock().unwrap().unwrap().revision > ack.revision {
                Err(io::Error::other("admission closed during maintenance"))
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(result, Err(MutationFailure::Rejected(error))
        if error.to_string() == "admission closed during maintenance"));
    let record = inspect.read().unwrap().unwrap();
    assert_eq!(record.revision, ack.revision + 2);
    assert_eq!(record.committed.suffix_end, 0);
    assert_eq!(record.pending, None);
    assert!(!Slot::A.path(&f.root).exists());
    assert_eq!(store.get(b"new", 9).unwrap(), None);
    drop(store);
    assert_eq!(f.open().get(b"ack", 4).unwrap().unwrap(), b"kept");
}

#[test]
fn automatic_rotation_recovers_after_uncertain_selection_or_reclamation_fence() {
    for (revision, after) in [(3, false), (3, true), (4, false), (4, true)] {
        let mut f = Fixture::prepare();
        f.register();
        let mut store = f.open_auto(NonZeroU64::MIN);
        store.put(b"ack", b"kept").unwrap();
        let prior = store.current.committed;
        store.authority.fault = Some((revision, after));
        assert!(
            matches!(store.put(b"next", b"unsubmitted"), Err(MutationFailure::Rejected(error))
            if error.to_string().contains("injected authority failure"))
        );
        assert!(store.get(b"ack", 4).is_err());
        assert!(Slot::A.path(&f.root).is_dir());
        assert!(Slot::B.path(&f.root).is_dir());
        drop(store);
        let mut store = f.open_auto(NonZeroU64::MIN);
        let rotated = revision == 4 || after;
        assert_eq!(store.current.committed != prior, rotated);
        assert_eq!(Slot::A.path(&f.root).exists(), !rotated);
        assert_eq!(Slot::B.path(&f.root).exists(), rotated);
        assert!(store.current.pending.is_none());
        assert_eq!(store.get(b"next", 12).unwrap(), None);
        assert_eq!(store.get(b"ack", 4).unwrap().unwrap(), b"kept");
        store.put(b"next", b"retried").unwrap();
        drop(store);
        assert_eq!(f.open().get(b"next", 7).unwrap().unwrap(), b"retried");
    }
}

#[test]
fn detailed_rotation_reports_successor_sync_failure_before_authority_advance() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.put(b"ack", b"kept").unwrap();
    let prior = store.current;
    let result = store.compact_detailed_using(LIMITS, None, |root| {
        assert!(Slot::B.path(root).is_dir());
        assert_eq!(*f.provider.record.lock().unwrap(), Some(prior));
        Err(io::Error::other("successor sync"))
    });
    assert!(matches!(
        result,
        Err(RetainedRotationFailure::SuccessorOutcomeUnknown(error))
            if error.to_string() == "successor sync"
    ));
    assert_eq!(*f.provider.record.lock().unwrap(), Some(prior));
    assert!(matches!(
        store.compact_detailed(),
        Err(RetainedRotationFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::AlreadyExists
    ));
    store.put(b"later", b"still active").unwrap();
    drop(store);
    let mut reopened = f.open();
    assert_eq!(
        reopened.current.committed.generation,
        prior.committed.generation
    );
    assert_eq!(reopened.get(b"ack", 4).unwrap().unwrap(), b"kept");
    assert_eq!(
        reopened.get(b"later", 12).unwrap().unwrap(),
        b"still active"
    );
    assert!(reopened.reclaim_inactive().unwrap());
    assert_ne!(
        reopened.compact_detailed().unwrap(),
        prior.committed.generation
    );
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
        let result = store.compact_detailed();
        assert!(
            matches!(result, Err(RetainedRotationFailure::SelectionOutcomeUnknown(error))
            if error.to_string().contains("injected authority failure"))
        );
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
fn reclamation_rejects_unavailable_inactive_slot_before_advancing_authority() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.compact().unwrap();
    let inactive = Slot::A.path(&f.root);
    let before = f.provider.read().unwrap().unwrap();
    let witness = fs::read(inactive.join("commit.state")).unwrap();

    let lock = lock_directory(&inactive).unwrap();
    assert!(matches!(
        store.reclaim_inactive_with_outcome(),
        Err(AuthorizedReclamationFailure {
            fence: ReclamationFenceOutcome::NotAttempted,
            cleanup: StagingCleanupFailure::NotStarted(error),
        }) if error.kind() == io::ErrorKind::WouldBlock
    ));
    assert_eq!(f.provider.read().unwrap(), Some(before));
    drop(lock);

    let unknown = inactive.join("unknown");
    fs::write(&unknown, b"not catalog data").unwrap();
    let error = store.reclaim_inactive().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "unknown file in inactive retained generation"
    );
    assert_eq!(f.provider.read().unwrap(), Some(before));
    assert_eq!(fs::read(&unknown).unwrap(), b"not catalog data");
    assert_eq!(fs::read(inactive.join("commit.state")).unwrap(), witness);
    assert!(!crate::staging::has_retained_stage(&f.root).unwrap());

    fs::remove_file(unknown).unwrap();
    let cost = store.inner.store.base.metadata().file_bytes + store.inner.store.suffix_len as u64;
    assert!(matches!(
        store.reclaim_inactive_with_scan_limit(cost - 1),
        Err(AuthorizedReclamationFailure {
            fence: ReclamationFenceOutcome::NotAttempted,
            cleanup: StagingCleanupFailure::NotStarted(error),
        }) if error.kind() == io::ErrorKind::InvalidInput
            && error.to_string() == "retained reclamation scan byte limit exceeded"
    ));
    assert_eq!(f.provider.read().unwrap(), Some(before));
    assert_eq!(fs::read(inactive.join("commit.state")).unwrap(), witness);
    assert!(store.reclaim_inactive_with_scan_limit(cost).unwrap());
    assert_eq!(
        f.provider.read().unwrap(),
        Some(before.finalized(before.committed).unwrap())
    );
    assert!(!inactive.exists());
}

#[test]
fn reclamation_fence_failure_keeps_the_inactive_generation() {
    for applied in [false, true] {
        let mut f = Fixture::prepare();
        f.register();
        let mut store = f.open();
        store.compact().unwrap();
        store.authority.fault = Some((2, applied));
        assert!(matches!(
            store.reclaim_inactive_with_outcome(),
            Err(AuthorizedReclamationFailure {
                fence: ReclamationFenceOutcome::Unconfirmed,
                cleanup: StagingCleanupFailure::NotStarted(_),
            })
        ));
        assert!(Slot::A.path(&f.root).exists());
        assert_eq!(
            f.provider.read().unwrap().unwrap().revision,
            1 + u64::from(applied)
        );
        assert!(store.get(b"base", 8).is_err());
        drop(store);
        let mut reopened = f.open();
        assert!(reopened.reclaim_inactive_with_outcome().unwrap());
        assert!(!Slot::A.path(&f.root).exists());
        assert_eq!(reopened.get(b"base", 8).unwrap().unwrap(), b"imported");
    }
}

#[test]
fn confirmed_fence_can_precede_a_validation_failure_without_removal() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.compact().unwrap();
    let base = Slot::B.path(&f.root).join("base");
    let original = fs::read(&base).unwrap();
    let mut damaged = original.clone();
    *damaged.last_mut().unwrap() ^= 1;
    fs::write(&base, damaged).unwrap();

    assert!(matches!(
        store.reclaim_inactive_with_outcome(),
        Err(AuthorizedReclamationFailure {
            fence: ReclamationFenceOutcome::Confirmed,
            cleanup: StagingCleanupFailure::NotStarted(error),
        }) if error.kind() == io::ErrorKind::InvalidData
    ));
    assert_eq!(f.provider.read().unwrap().unwrap().revision, 2);
    assert!(Slot::A.path(&f.root).exists());
    assert!(!crate::staging::has_retained_stage(&f.root).unwrap());
    fs::write(base, original).unwrap();
    drop(store);
    assert!(f.open().reclaim_inactive_with_outcome().unwrap());
}

#[test]
fn confirmed_fence_and_uncertain_removal_resume_after_restart() {
    let mut f = Fixture::prepare();
    f.register();
    let mut store = f.open();
    store.put(b"ack", b"keep").unwrap();
    store.compact().unwrap();
    let before = f.provider.read().unwrap().unwrap();
    let error =
        store.reclaim_inactive_using(None, |_| Err(io::Error::other("injected retirement sync")));
    assert!(matches!(
        error,
        Err(AuthorizedReclamationFailure {
            fence: ReclamationFenceOutcome::Confirmed,
            cleanup: StagingCleanupFailure::OutcomeUnknown(ref error),
        }) if error.to_string() == "injected retirement sync"
    ));
    let fence = before.finalized(before.committed).unwrap();
    assert_eq!(f.provider.read().unwrap(), Some(fence));
    assert!(!Slot::A.path(&f.root).exists());
    assert!(crate::staging::has_retained_stage(&f.root).unwrap());
    drop(store);

    let mut reopened = f.open();
    assert_eq!(reopened.get(b"ack", 4).unwrap().unwrap(), b"keep");
    let cost =
        reopened.inner.store.base.metadata().file_bytes + reopened.inner.store.suffix_len as u64;
    assert!(matches!(
        reopened.reclaim_inactive_with_scan_limit(cost - 1),
        Err(AuthorizedReclamationFailure {
            fence: ReclamationFenceOutcome::NotAttempted,
            cleanup: StagingCleanupFailure::NotStarted(error),
        }) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(crate::staging::has_retained_stage(&f.root).unwrap());
    assert_eq!(f.provider.read().unwrap(), Some(fence));
    assert!(reopened.reclaim_inactive_with_scan_limit(cost).unwrap());
    assert!(!crate::staging::has_retained_stage(&f.root).unwrap());
    assert_eq!(f.provider.read().unwrap(), Some(fence));
    assert!(!reopened.reclaim_inactive_with_outcome().unwrap());
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
