use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    AuthorizedRetainedStore, FileRetainedAuthority, IndexLimits, ManagedRetainedStore,
    OverlayLimits, RetainedStoreLimits, SuffixLimits,
};
use std::{io, path::Path};

type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;
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

#[cfg(feature = "current")]
fn setup(root: &Path) {
    use database_engine::{
        RetainedExportLimits, Store,
        retained_candidate::{CatalogStorageLimits, RetainedAuthority},
    };
    let export = RetainedExportLimits {
        max_log_bytes: 8192,
        max_live_keys: 32,
        max_live_key_bytes: 256,
        max_output_bytes: 8192,
    };
    let quota = CatalogStorageLimits {
        bytes: 16384,
        entries: 64,
    };
    let seed = |path: &Path| {
        let store = Store::open(path).unwrap();
        store.put(b"k", b"v").unwrap();
        store
    };
    let source = root.join("source");
    let managed = root.join("managed");
    ManagedRetainedStore::copy_from_store(seed(&source), &source, &managed, export, LIMITS)
        .unwrap();
    ManagedRetainedStore::open(&managed, LIMITS)
        .unwrap()
        .compact_with_persisted_storage_limits(quota)
        .unwrap();
    let cutover = root.join("cutover");
    let initial = Catalog::cutover_from_store(seed(&cutover), &cutover, export, LIMITS).unwrap();
    let mut authority = FileRetainedAuthority::create(root.join("authority")).unwrap();
    assert!(authority.compare_and_swap(None, initial).unwrap());
    Catalog::open(&cutover, LIMITS, authority)
        .unwrap()
        .compact_with_persisted_storage_limits(quota)
        .unwrap();
}

fn main() {
    let arg = std::env::args().nth(1).unwrap();
    let root = Path::new(&arg);
    #[cfg(feature = "current")]
    if std::env::args().nth(2).as_deref() == Some("setup") {
        setup(root);
    }
    let managed = ManagedRetainedStore::open(root.join("managed"), LIMITS);
    let authority = FileRetainedAuthority::open(root.join("authority")).unwrap();
    let authorized = Catalog::open(root.join("cutover"), LIMITS, authority);
    if cfg!(feature = "current") {
        assert_eq!(
            managed.unwrap().get(b"k", 1).unwrap().as_deref(),
            Some(&b"v"[..])
        );
        assert_eq!(
            authorized.unwrap().get(b"k", 1).unwrap().as_deref(),
            Some(&b"v"[..])
        );
        println!("Current reader opened both selected generations");
    } else {
        for (role, error) in [
            (
                "managed",
                managed.err().expect("old reader opened managed catalog"),
            ),
            (
                "authorized",
                authorized
                    .err()
                    .expect("old reader opened authorized catalog"),
            ),
        ] {
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            println!("Old reader rejected {role}: {error}");
        }
    }
}
