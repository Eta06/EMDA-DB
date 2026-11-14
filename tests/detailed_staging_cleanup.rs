use database_engine::{StagingCleanupFailure, cleanup_abandoned_staging_detailed};
use std::error::Error;
use std::fs;

#[test]
fn external_caller_can_classify_a_rejection_and_clean_a_recognized_stage() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("missing");
    let error = cleanup_abandoned_staging_detailed(&missing).unwrap_err();
    assert!(matches!(error, StagingCleanupFailure::NotStarted(_)));
    assert!(error.source().is_some());
    assert!(!missing.exists());

    let marker = root.path().join(".emda-stage-v1-f-abcdefghijklmnop.owner");
    let payload = root
        .path()
        .join(".emda-stage-v1-f-abcdefghijklmnop.owner.data");
    let unrelated = root.path().join("unrelated");
    fs::write(&marker, b"EMDA-STAGING-V1-FILE\n").unwrap();
    fs::write(&payload, b"abandoned").unwrap();
    fs::write(&unrelated, b"preserve").unwrap();
    let report = cleanup_abandoned_staging_detailed(root.path()).unwrap();
    assert_eq!((report.removed_payloads, report.removed_markers), (1, 1));
    assert!(!marker.exists() && !payload.exists());
    assert_eq!(fs::read(unrelated).unwrap(), b"preserve");
}
