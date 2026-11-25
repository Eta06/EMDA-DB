//! The initial-copy source read ceiling of online checkpoint compaction.
//! It admits one pass over the captured log prefix before reading it, then
//! each copied live value. Catch-up and final verification are not charged.
use super::*;
use crate::{CheckpointCompactionLimits, DiskBudgetPending};
use std::os::unix::fs::FileExt;

fn limited(max: u64) -> OnlineCheckpointCompactionOptions {
    OnlineCheckpointCompactionOptions {
        copy_limits: CheckpointCompactionLimits {
            max_source_log_read_bytes: Some(max),
            max_target_log_bytes: None,
        },
        ..Default::default()
    }
}

fn state(fixture: &Fixture) -> (Vec<String>, Vec<u8>, Vec<u8>, Vec<u8>) {
    (
        parent_entries(fixture),
        fs::read(fixture.source.join("records.log")).unwrap(),
        fs::read(fixture.source.join("commit.state")).unwrap(),
        fs::read(
            fixture
                .source
                .join(super::super::super::disk_budget::BUDGET_FILE),
        )
        .unwrap(),
    )
}

fn refused(result: Result<OnlineCompactionReport, OnlineReclaimFailure>) -> io::Error {
    match result {
        Err(OnlineReclaimFailure::Compaction(OnlineHandoffFailure::NotPublished(error))) => error,
        other => panic!("expected a refusal before publication: {other:?}"),
    }
}

#[test]
fn an_under_budget_initial_pass_is_refused_before_reading_or_reserving() {
    let (fixture, online, model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    let initial = fs::metadata(fixture.source.join("records.log"))
        .unwrap()
        .len();
    // Corrupt one payload byte in the middle of the log, beyond the header,
    // anchor and witness probes taken while capturing the boundary.
    let log = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.source.join("records.log"))
        .unwrap();
    let offset = initial / 2;
    let mut original = [0];
    log.read_exact_at(&mut original, offset).unwrap();
    log.write_all_at(&[original[0] ^ 0x5a], offset).unwrap();
    let before = state(&fixture);
    let charged = online.disk_budget_status().unwrap();

    let error =
        refused(online.compact_and_reclaim_detailed(&fixture.previous, &limited(initial - 1)));
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
    assert!(error.to_string().contains("source log read"), "{error}");
    // Nothing was reserved, staged or published.
    assert_eq!(state(&fixture), before);
    assert_eq!(online.disk_budget_status().unwrap(), charged);

    // An admitted attempt reads the prefix and detects the corruption.
    let error = refused(online.compact_and_reclaim_detailed(&fixture.previous, &limited(u64::MAX)));
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert!(!fixture.previous.exists());
    assert_eq!(
        online.disk_budget_status().unwrap().unwrap().pending,
        DiskBudgetPending::None
    );

    log.write_all_at(&original, offset).unwrap();
    drop(log);
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    assert_eq!(live(&online), model);
}

#[test]
fn the_ceiling_covers_the_initial_pass_plus_copied_live_values() {
    for short in [true, false] {
        let (fixture, online, model) = fixture(false);
        online.set_disk_budget(Some(u64::MAX)).unwrap();
        let initial = fs::metadata(fixture.source.join("records.log"))
            .unwrap()
            .len();
        let values = model.values().map(|value| value.len() as u64).sum::<u64>();
        let exact = initial + values;
        let before = state(&fixture);
        let charged = online.disk_budget_status().unwrap();
        let result = online
            .compact_and_reclaim_detailed(&fixture.previous, &limited(exact - u64::from(short)));
        if short {
            // The initial pass is admitted; the last copied value is not.
            let error = refused(result);
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
            assert!(error.to_string().contains("source log read"), "{error}");
            assert_eq!(state(&fixture), before);
            assert_eq!(online.disk_budget_status().unwrap(), charged);
        } else {
            result.unwrap();
            assert!(!fixture.previous.exists());
        }
        assert_eq!(live(&online), model, "short={short}");
        drop(online);
        assert_eq!(reopened(&fixture.source), model, "short={short}");
    }
}
