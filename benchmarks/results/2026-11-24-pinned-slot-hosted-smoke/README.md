# Hosted smoke benchmarks after automatic slot protection

Revision `5c006e1` ran three [cross-engine smoke cases](https://github.com/Eta06/EMDA-DB/actions/runs/45308074968): compound-range reads, large-value recovery, and concurrent document updates. All three commands exited successfully. The [raw archive](raw-artifacts.tar.gz) preserves every command log, attempt record, sample report, and workflow metadata file. All 12 source files were verified against the included SHA-256 manifest after extraction.

The [Linux CI run](https://github.com/Eta06/EMDA-DB/actions/runs/45308074889) passed on Rust 1.85.0. The stable job failed on the size of two test enums and stopped before executing benchmark commands. The archive preserves the lint and artifact-upload diagnostics and the workflow metadata.
