use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    Expected, IndexLimits, OverlayLimits, RetainedStoreLimits, SuffixLimits,
};

pub const BASE: &[u8] = include_bytes!("retained-v1-base");
pub const LOG: &[u8] = include_bytes!("retained-v1-records.log");
pub const WITNESS: &[u8] = include_bytes!("retained-v1-commit.state");
const BINDING: &[u8; 176] = include_bytes!("retained-v1-binding.bin");
pub const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 4096,
        entries: 8,
        key_bytes: 128,
        work_bytes: 4097,
    },
    index: IndexLimits {
        blocks: 4,
        anchor_bytes: 128,
    },
    suffix: SuffixLimits {
        bytes: 4096,
        operations: 16,
        work: 1024,
    },
    overlay: OverlayLimits {
        entries: 8,
        key_bytes: 128,
    },
};

pub fn binding() -> Expected {
    Expected {
        header: BINDING[..112].try_into().unwrap(),
        witness: BINDING[112..144].try_into().unwrap(),
        base_digest: BINDING[144..].try_into().unwrap(),
    }
}
