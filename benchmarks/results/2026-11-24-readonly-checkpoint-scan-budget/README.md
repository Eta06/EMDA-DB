# Read-only checkpoint scan budgets

Revision `9146b416` adds an inclusive combined logical scan ceiling to
historical and recovered read-only checkpoint opens. A historical open
charges the encoded checkpoint and its covered log prefix. Recovered open
also charges the witnessed suffix and optionally caps suffix operations,
counting each atomic batch child. The remaining whole-log and checkpoint
limits still apply.

## Boundary verification

The regression opens the same checkpoint both ways after an acknowledged
batch changes the log. A one-byte-short ceiling rejects a checkpoint with
a damaged trailer before full index validation. At the exact ceiling, the
bad trailer still rejects. With the restored checkpoint, historical open
returns the old value and recovered open returns both batch values. The
exact suffix operation count succeeds and one fewer fails. Both APIs
reject corruption in the covered log, and recovered open also rejects
corruption after the checkpoint boundary. These cases exercise the
forced V2 fixture and the platform's default witness format.

These are per-open logical read ceilings. Framing probes, metadata reads,
physical I/O retries, later reads and process memory have separate costs.

## Validation and raw data

Native macOS Rust 1.85 passed 24 focused tests and 1,369 tests across
112 nonempty binaries in the full library and integration run. Lint and
Rustdoc passed with warnings denied. All five hosted workflows passed. The [raw archive](raw-artifacts.tar.gz)
contains the local results and logs and artifacts from the [Linux CI],
[macOS correctness], [cross-engine smoke], [ENOSPC], and [filesystem EIO]
runs. It preserves the benchmark attempts with their raw results and
contains a SHA-256 manifest and verification scripts.

[Linux CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45455762649
[macOS correctness]: https://github.com/Eta06/EMDA-DB/actions/runs/45455762664
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45455762951
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45455762603
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45455762749
