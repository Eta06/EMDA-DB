use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    Expected, IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits,
    SuffixExpected, SuffixLimits,
};
use std::{fs, path::Path, time::Instant};

const MIB: usize = 1024 * 1024;
const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 4 * MIB as u64,
        entries: 100,
        key_bytes: 1024,
        work_bytes: 4 * MIB as u64 + 1,
    },
    index: IndexLimits {
        blocks: 64,
        anchor_bytes: 1024,
    },
    suffix: SuffixLimits {
        bytes: 64 * MIB as u64,
        operations: 96,
        work: 1024,
    },
    overlay: OverlayLimits {
        entries: 16,
        key_bytes: 1024,
    },
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args_os().collect::<Vec<_>>();
    let [_, phase, source, output] = args.as_slice() else {
        return Err("expected phase, source path and output path".into());
    };
    let source = Path::new(source);
    let output = Path::new(output);
    match phase.to_str().ok_or("phase must be UTF-8")? {
        "backup" => {
            let store = ManagedRetainedStore::open(source, LIMITS)?;
            fs::create_dir(output)?;
            let expected = store.backup_to(output.join("backup"))?;
            fs::write(
                output.join("binding"),
                [
                    expected.candidate.header.as_slice(),
                    &expected.candidate.witness,
                    &expected.candidate.base_digest,
                    &expected.end.to_le_bytes(),
                    &expected.digest,
                ]
                .concat(),
            )?;
            println!("suffix_bytes={}", expected.end);
        }
        "restore" => {
            let binding = fs::read(source.join("binding"))?;
            let expected = SuffixExpected {
                candidate: Expected {
                    header: binding[..112].try_into()?,
                    witness: binding[112..144].try_into()?,
                    base_digest: binding[144..176].try_into()?,
                },
                end: u64::from_le_bytes(binding[176..184].try_into()?),
                digest: binding[184..].try_into()?,
            };
            let start = Instant::now();
            ManagedRetainedStore::restore_backup(source.join("backup"), output, LIMITS, &expected)?;
            println!("restore_ns={}", start.elapsed().as_nanos());
        }
        _ => return Err("unknown phase".into()),
    }
    Ok(())
}
