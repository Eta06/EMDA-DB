#![no_main]
use database_engine::{
    CancellationToken, CheckpointStore, CollectionSchema, ControlledTransactionError,
    DocumentDatabase, DocumentTransactionMode, IndexDefinition, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};
use std::io::{self, ErrorKind};
use std::path::Path;
use std::time::{Duration, Instant};

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn db(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }
}
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], checkpoint_mode: bool) -> Backend {
    if checkpoint_mode {
        Backend::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(path).unwrap())
    }
}
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let checkpoint_mode = data[0] & 1 != 0;
    let mut store = open(&path, &checkpoint, &digest, checkpoint_mode);
    store
        .db()
        .create_collection("c", CollectionSchema::default())
        .unwrap();
    store
        .db()
        .create_index(
            "c",
            IndexDefinition {
                name: "unique".into(),
                fields: vec![vec!["id".into()]],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
    let mut model = vec![None::<Value>; 8];
    let mut raw = vec![None::<Vec<u8>>; 8];
    for bytes in data[1..].chunks_exact(4) {
        let flags = bytes[0];
        let id = usize::from(bytes[1] % 8);
        let document = json!({"id":id, "value":bytes[2]});
        let key = format!("raw-{id}");
        let value = vec![bytes[3]; usize::from(bytes[2] % 8)];
        let expired = flags & 1 != 0;
        let pre_cancelled = flags & 2 != 0;
        let cancel_inside = flags & 4 != 0;
        let mode = match (flags >> 3) % 3 {
            0 => DocumentTransactionMode::Conservative,
            1 => DocumentTransactionMode::ReadSet,
            _ => DocumentTransactionMode::ReadSetFifo,
        };
        let callback_error = flags & 32 != 0;
        let token = CancellationToken::new();
        if pre_cancelled {
            token.cancel();
        }
        let deadline = if expired {
            Some(Instant::now() - Duration::from_secs(1))
        } else if flags & 64 != 0 {
            Some(Instant::now() + Duration::from_secs(3600))
        } else {
            None
        };
        let result = store
            .db()
            .transaction_controlled(&token, deadline, mode, |tx| {
                tx.replace("c", &id.to_string(), &document, true)?;
                tx.put_raw(key.as_bytes(), &value)?;
                if cancel_inside {
                    token.cancel();
                }
                if callback_error {
                    return Err(io::Error::new(
                        ErrorKind::Interrupted,
                        "external callback error",
                    ));
                }
                if cancel_inside && flags & 128 != 0 {
                    tx.get_raw(key.as_bytes())?;
                }
                Ok(())
            });
        if expired {
            assert!(matches!(
                result,
                Err(ControlledTransactionError::DeadlineExceeded)
            ));
        } else if pre_cancelled {
            assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
        } else if callback_error {
            assert!(
                matches!(result, Err(ControlledTransactionError::Operation(error)) if error.kind()==ErrorKind::Interrupted)
            );
        } else if cancel_inside {
            assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
        } else {
            result.unwrap();
            model[id] = Some(document);
            raw[id] = Some(value);
        }
        if bytes[3] & 1 != 0 {
            drop(store);
            store = open(&path, &checkpoint, &digest, checkpoint_mode);
        }
        for index in 0..8 {
            assert_eq!(
                store.db().get("c", &index.to_string()).unwrap(),
                model[index]
            );
            let observed = store
                .db()
                .transaction(|tx| tx.get_raw(format!("raw-{index}").as_bytes()))
                .unwrap();
            assert_eq!(observed, raw[index]);
        }
        assert!(store.db().verify_collection("c").is_ok());
    }
});
