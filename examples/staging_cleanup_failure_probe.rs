//! Public cleanup outcome probe for disposable filesystem-error campaigns.
use database_engine::{StagingCleanupFailure, cleanup_abandoned_staging_detailed};
use serde_json::json;
use std::io;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let command = args.next().ok_or("missing command")?;
    let path = args.next().ok_or("missing directory")?;
    let operation = args.next().ok_or("missing operation")?;
    if command != "attempt" || operation != "cleanup" || args.next().is_some() {
        return Err("unexpected arguments".into());
    }
    let result = cleanup_abandoned_staging_detailed(Path::new(&path));
    match result {
        Ok(report) => println!(
            "{}",
            json!({"status":"success", "operation":"cleanup", "classification":null,
                   "removed_payloads":report.removed_payloads,
                   "removed_markers":report.removed_markers,"skipped":report.skipped})
        ),
        Err(error) => {
            let (classification, error) = match error {
                StagingCleanupFailure::NotStarted(error) => ("not_started", error),
                StagingCleanupFailure::OutcomeUnknown(error) => ("unknown", error),
                other => ("unknown", io::Error::other(other)),
            };
            println!(
                "{}",
                json!({"status":"error", "operation":"cleanup", "classification":classification,
                       "kind":format!("{:?}",error.kind()),"message":error.to_string()})
            );
        }
    }
    Ok(())
}
