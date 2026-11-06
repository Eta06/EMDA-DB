#![no_main]
use database_engine::{BatchOperation, CheckpointStore, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::io;

trait Writer {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn delete(&self, key: &[u8]) -> io::Result<()>;
    fn batch(&self, operations: &[BatchOperation<'_>]) -> io::Result<()>;
    fn transaction(&self, first: &[u8], second: &[u8], value: &[u8]) -> io::Result<()>;
    fn backup(&self, path: &std::path::Path) -> io::Result<()>;
}

impl Writer for Store {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        Store::put(self, key, value)
    }
    fn delete(&self, key: &[u8]) -> io::Result<()> {
        Store::delete(self, key)
    }
    fn batch(&self, operations: &[BatchOperation<'_>]) -> io::Result<()> {
        self.write_batch(operations)
    }
    fn transaction(&self, first: &[u8], second: &[u8], value: &[u8]) -> io::Result<()> {
        Store::transaction(self, |tx| {
            tx.put(first, value)?;
            tx.delete(second)?;
            Ok(())
        })
    }
    fn backup(&self, path: &std::path::Path) -> io::Result<()> {
        Store::backup(self, path)
    }
}

impl Writer for CheckpointStore {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        CheckpointStore::put(self, key, value)
    }
    fn delete(&self, key: &[u8]) -> io::Result<()> {
        CheckpointStore::delete(self, key)
    }
    fn batch(&self, operations: &[BatchOperation<'_>]) -> io::Result<()> {
        self.write_batch(operations)
    }
    fn transaction(&self, first: &[u8], second: &[u8], value: &[u8]) -> io::Result<()> {
        CheckpointStore::transaction(self, |tx| {
            tx.put(first, value)?;
            tx.delete(second)?;
            Ok(())
        })
    }
    fn backup(&self, path: &std::path::Path) -> io::Result<()> {
        CheckpointStore::backup(self, path)
    }
}

fn key(byte: u8) -> Vec<u8> {
    let index = byte % 8;
    vec![b'a' + index; usize::from(index % 3) + 1]
}

fn apply(writer: &impl Writer, model: &mut BTreeMap<Vec<u8>, Vec<u8>>, bytes: &[u8]) {
    let first = key(bytes[1]);
    let second = key(bytes[2]);
    let value = vec![bytes[3]; usize::from(bytes[3] % 6)];
    match bytes[0] % 4 {
        0 => {
            writer.put(&first, &value).expect("put");
            model.insert(first, value);
        }
        1 => {
            writer.delete(&first).expect("delete");
            model.remove(&first);
        }
        2 => {
            writer
                .batch(&[
                    BatchOperation::Put {
                        key: &first,
                        value: &value,
                    },
                    BatchOperation::Delete { key: &second },
                ])
                .expect("batch");
            model.insert(first, value);
            model.remove(&second);
        }
        _ => {
            writer
                .transaction(&first, &second, &value)
                .expect("transaction");
            model.insert(first, value);
            model.remove(&second);
        }
    }
}

fn scan_model(model: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 || data.len() > 98 {
        return;
    }
    let root = tempfile::tempdir().expect("temporary directory");
    let source = root.path().join("source");
    let checkpoint = root.path().join("checkpoint");
    let backup = root.path().join("backup");
    let restored = root.path().join("restored");
    if data[0] & 1 == 0 {
        std::fs::create_dir(&source).expect("create v2 source");
        std::fs::write(
            source.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .expect("v2 log");
        std::fs::write(
            source.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .expect("v2 witness");
    }
    let operations = data[2..].chunks_exact(4).collect::<Vec<_>>();
    let split = usize::from(data[1]) % (operations.len() + 1);
    let mut model = BTreeMap::new();
    let ordinary = Store::open(&source).expect("open source");
    for bytes in &operations[..split] {
        apply(&ordinary, &mut model, bytes);
    }
    let digest = ordinary.checkpoint_to(&checkpoint).expect("checkpoint");
    drop(ordinary);

    fn finish(
        writer: &impl Writer,
        model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        operations: &[&[u8]],
        backup: &std::path::Path,
    ) -> BTreeMap<Vec<u8>, Vec<u8>> {
        for bytes in operations {
            apply(writer, model, bytes);
        }
        writer.backup(backup).expect("backup");
        let expected_backup = model.clone();
        // Mutate the source after the backup returns. This key cannot collide
        // with generated keys and must not appear in the restored snapshot.
        writer
            .put(b"\xffafter-backup", b"newer")
            .expect("later source write");
        model.insert(b"\xffafter-backup".to_vec(), b"newer".to_vec());
        expected_backup
    }

    let expected_backup = if data[0] & 2 == 0 {
        let writer = Store::open(&source).expect("reopen ordinary source");
        finish(&writer, &mut model, &operations[split..], &backup)
    } else {
        let writer =
            CheckpointStore::open(&source, &checkpoint, &digest).expect("open checkpoint source");
        finish(&writer, &mut model, &operations[split..], &backup)
    };
    Store::restore(&backup, &restored).expect("restore");
    let output = Store::open(&restored).expect("open restored store");
    assert_eq!(
        output.scan(None, None, 100).expect("restored scan"),
        scan_model(&expected_backup)
    );
    let restored_checkpoint = root.path().join("restored-checkpoint");
    let restored_digest = output
        .checkpoint_to(&restored_checkpoint)
        .expect("restored checkpoint");
    drop(output);
    let output = CheckpointStore::open(&restored, &restored_checkpoint, &restored_digest)
        .expect("open restored checkpoint");
    assert_eq!(
        output
            .scan(None, None, 100)
            .expect("restored checkpoint scan"),
        scan_model(&expected_backup)
    );
    drop(output);
    let source_store = CheckpointStore::open(&source, &checkpoint, &digest).expect("reopen source");
    assert_eq!(
        source_store.scan(None, None, 100).expect("source scan"),
        scan_model(&model)
    );
});
