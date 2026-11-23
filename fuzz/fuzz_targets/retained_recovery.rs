#![no_main]
use libfuzzer_sys::fuzz_target;

#[path = "../../tests/fixtures/retained_v1.rs"]
mod fixture;
#[path = "retained_recovery/model.rs"]
mod model;

fuzz_target!(|data: &[u8]| model::check(data));
