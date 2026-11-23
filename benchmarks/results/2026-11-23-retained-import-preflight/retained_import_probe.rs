use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    Expected, IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStore, RetainedStoreLimits,
    SuffixLimits,
};
use database_engine::{PublicationFailure, RetainedExportLimits, Store};
use std::{env, fs, path::Path};

const VALUE_BYTES: usize = 8 * 1024 * 1024;

fn main() {
    let args: Vec<_> = env::args().collect();
    let root = Path::new(&args[1]);
    let mode = args[2].as_str();
    let candidate = root.join("candidate");
    let binding = root.join("binding");
    if mode == "prepare" {
        let store = Store::open(root.join("source")).unwrap();
        store.put(b"base", &vec![0x42; VALUE_BYTES]).unwrap();
        let expected = store
            .build_retained_candidate(
                &candidate,
                [3; 16],
                [5; 16],
                RetainedExportLimits {
                    max_log_bytes: (VALUE_BYTES + 1024) as u64,
                    max_live_keys: 1,
                    max_live_key_bytes: 4,
                    max_output_bytes: (VALUE_BYTES + 1024) as u64,
                },
            )
            .unwrap();
        fs::write(
            binding,
            [
                &expected.header[..],
                &expected.witness,
                &expected.base_digest,
            ]
            .concat(),
        )
        .unwrap();
        return;
    }
    let bytes = fs::read(binding).unwrap();
    let expected = Expected {
        header: bytes[..112].try_into().unwrap(),
        witness: bytes[112..144].try_into().unwrap(),
        base_digest: bytes[144..].try_into().unwrap(),
    };
    let file_bytes = u64::from_le_bytes(expected.header[48..56].try_into().unwrap());
    let accepted = mode.ends_with("accept");
    let limits = RetainedStoreLimits {
        base: Limits {
            file_bytes,
            work_bytes: file_bytes + 1,
            entries: 1,
            key_bytes: 4,
        },
        index: IndexLimits {
            blocks: usize::from(accepted),
            anchor_bytes: 4,
        },
        suffix: SuffixLimits {
            bytes: 0,
            operations: 0,
            work: 0,
        },
        overlay: OverlayLimits {
            entries: 0,
            key_bytes: 0,
        },
    };
    let destination = root.join(mode);
    if mode.starts_with("managed") {
        let result = ManagedRetainedStore::create(&candidate, &destination, limits, &expected);
        if accepted {
            let id = result.unwrap();
            let store = ManagedRetainedStore::open(&destination, limits).unwrap();
            assert_eq!(store.generation_id().unwrap(), id);
            assert_eq!(
                store.get(b"base", VALUE_BYTES).unwrap().unwrap(),
                vec![0x42; VALUE_BYTES]
            );
        } else {
            check_rejection(result, &destination);
        }
    } else {
        let result = RetainedStore::create(&candidate, &destination, limits, &expected);
        if accepted {
            let id = result.unwrap();
            let store = RetainedStore::open(&destination, limits, &expected).unwrap();
            assert_eq!(store.generation_id(), id);
            assert_eq!(
                store.get(b"base", VALUE_BYTES).unwrap().unwrap(),
                vec![0x42; VALUE_BYTES]
            );
        } else {
            check_rejection(result, &destination);
        }
    }
    println!("{mode} verified; base bytes {file_bytes}");
}

fn check_rejection(result: Result<[u8; 16], PublicationFailure>, destination: &Path) {
    assert!(
        matches!(result, Err(PublicationFailure::NotPublished(ref error))
        if error.to_string() == "retained base block limit exceeded")
    );
    assert!(!destination.exists());
}
