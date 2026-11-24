# Standard Linux checks alongside the expanded memory gate

Both Rust toolchains passed at `0057f3d`, along with the writable checkpoint
and retained memory gate. The stable Rust job also passed the optimized
suite and all 45 benchmark smoke cases.

The [raw archive](raw-artifacts.tar.gz) preserves all 45 attempt records,
outputs, structured measurements and hosted job metadata. All 114 file
hashes verified after extraction. The [expanded memory gate archive](../2026-11-24-writable-recovery-linux-ceiling/README.md)
records the separate four-backend workload from the same CI run.

[Hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45347413883)
