# Document transaction process-kill races — 2026-11-05

On test revision `a23990a`, a child process commits three document transactions. Each inserts a document, a unique-index entry and a raw-key receipt in one atomic transaction. The child signals after all three calls return, and pauses while the parent saves their log prefix. After the child is released to submit a fourth transaction, the parent kills it at one of four delays. Successful full-log recovery checks the three acknowledged transactions and verifies that the fourth transaction's document, index entry and receipt are all present or all absent. The test then reopens the checkpoint-backed path and checks the same logical results. A failed open is accepted only for invalid/incomplete data with the interrupted log left byte-for-byte unchanged; this path confirms the previously acknowledged prefix remains on disk, not that its logical state can be recovered.

Each writer backend was tested for 50 repetitions per delay (0, 50, 500 and 2,000 microseconds) on each filesystem. The measured outcomes were:

| Filesystem | Writer | Recovered | Interrupted transaction present | Failed closed |
| --- | --- | ---: | ---: | ---: |
| `/tmp` tmpfs | ordinary | 200 | 125 | 0 |
| `/tmp` tmpfs | checkpoint-backed | 200 | 118 | 0 |
| workspace overlay | ordinary | 200 | 124 | 0 |
| workspace overlay | checkpoint-backed | 200 | 115 | 0 |

In all 800 cases the open succeeded, all acknowledged operations were found, and both recovery paths agreed. The other 318 interrupted transactions were absent. The [raw record](../benchmarks/results/2026-11-05-document-kill-races.json) contains the test binary hash and outputs. The stable and minimum supported Rust toolchains also passed the default short test; stable Clippy reported no warnings.

To repeat with a private temporary directory on a selected filesystem:

```sh
TMPDIR=/path/to/private/existing/directory EMDA_DOCUMENT_KILL_REPETITIONS=50 \
  cargo test --locked --test document_kill_races -- \
  --exact killed_document_writers_recover_acknowledged_transactions_or_fail_closed \
  --nocapture --test-threads=1
```

The workspace overlay's backing device was not independently verified. This is a finite set of process terminations on a running Linux VM, not a simulation of lost OS/device caches or physical power, and it does not exercise off-host restore. The test does not force every byte or metadata interruption, nor does it prove every acknowledged transaction is recoverable after an actual device failure. Separate fault-injection suites cover specific write and metadata failures.
