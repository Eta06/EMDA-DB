# Managed rebuild witness update faults

Linux ARM64, Rust 1.85.0, revision `15858ad`. The ordinary detailed
offline rebuild starts with an existing managed selection and a complete
tail. Each fixture restores the witness captured before the tail was written.
The control trace locates the actual witness update and sync. Fresh fixtures
receive EIO at each location with strace.

The [four results](faults.jsonl) cover V2 witness write and sync, and V3
metadata-witness update and inode sync. Each result reports uncertain tail
adoption, preserves the prior selection and log bytes, leaves the new
checkpoint absent, and passes both source readers after the failure.
These injections model syscall errors on the running filesystem.

To reproduce on Linux with strace and xattr support, build
`cargo +1.85.0 build --locked --example sync_failure_probe` and run
`python3 benchmarks/results/2026-11-24-managed-rebuild-witness-faults/reproduce.py`
from the repository root.
