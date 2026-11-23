use database_engine::{BatchOperation, RecoveredCheckpointView, RecoveredViewLimits, Store};
use std::fs;
use std::io;
use std::path::Path;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let root = Path::new(&args[2]);
    let path = root.join("store");
    let checkpoint = root.join("checkpoint.bin");
    let digest_path = root.join("checkpoint.sha256");
    if args[1] == "prepare" {
        fs::create_dir(root).unwrap();
        let store = Store::open(&path).unwrap();
        for start in (0..65_536_u64).step_by(256) {
            let keys: Vec<_> = (start..start + 256)
                .map(|key| {
                    let mut bytes = [b'x'; 64];
                    bytes[..8].copy_from_slice(&key.to_be_bytes());
                    bytes
                })
                .collect();
            let batch: Vec<_> = keys
                .iter()
                .map(|key| BatchOperation::Put { key, value: &[b'v'; 16] })
                .collect();
            store.write_batch(&batch).unwrap();
        }
        fs::write(digest_path, store.checkpoint_to(&checkpoint).unwrap()).unwrap();
        let prefix = fs::metadata(path.join("records.log")).unwrap().len();
        store.put(b"suffix", b"visible").unwrap();
        let tail = fs::metadata(path.join("records.log")).unwrap().len() - prefix;
        fs::write(root.join("tail"), tail.to_string()).unwrap();
        return;
    }
    let digest = fs::read(digest_path).unwrap().try_into().unwrap();
    let tail = fs::read_to_string(root.join("tail")).unwrap().parse::<u64>().unwrap();
    let rejected = args[1] == "reject";
    let result = RecoveredCheckpointView::open_with_options(
        &path,
        &checkpoint,
        &digest,
        RecoveredViewLimits {
            max_tail_bytes: tail - u64::from(rejected),
            ..Default::default()
        },
    );
    if rejected {
        let error = result.err().expect("suffix above the configured cap");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("suffix byte limit exceeded"));
    } else {
        let view = result.unwrap();
        let mut first = [b'x'; 64];
        first[..8].fill(0);
        assert_eq!(view.get(&first).unwrap(), Some(vec![b'v'; 16]));
        assert_eq!(view.get(b"suffix").unwrap(), Some(b"visible".to_vec()));
    }
}
