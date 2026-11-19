# Retained document histories and recovery races

Tested revision `44eb46e`. The macOS and Linux runs used production code
and test inputs matching the [SHA-256 manifest](source.sha256). See the
[platform and compiler record](environment.txt).

The serial-history checker starts six concurrent operations per history.
It evaluates transfer and reset operations, indexed observations, competing
unique claims, and cancellation under real-time ordering. It records
admission conflicts separately from a retained catalog's documented busy
response. Each retained history crosses a generation change and compares
the recovered documents, index results and raw-key mirror with the state
observed before close.

The process-kill campaign waits for three acknowledged document, index
and raw-key transactions, then releases one additional transaction and
kills the process at one of four delays. A recovered catalog must contain
all three acknowledgements, and the fourth transaction must be present or
absent across all three kinds of data. Retained cases enable automatic
reclamation and use delays of 0, 50, 2,000 and 200,000 microseconds. A
fail-closed result requires invalid or incomplete bytes and preservation
of the interrupted selector, log and witness files.

The controlled cancellation regression exercises an operation
interrupted during a callback and a callback that returns genuine
`Interrupted` I/O. The detailed API still retains callback errors.

| Retained campaign | Histories | Process kills | Fourth transaction present | Fail-closed cases |
| --- | ---: | ---: | ---: | ---: |
| macOS data volume | 128 | 32 | 8 | 0 |
| Linux ARM64 tmpfs | 512 | 128 | 64 | 0 |
| Linux ARM64 ext4 | 128 | 64 | 16 | 0 |

Each recovered catalog retained all three acknowledged transactions.
The ext4 experiments ran on the same Linux VM as the tmpfs tests.
See the [filesystem record](linux-filesystems.txt). The full library
suites passed 509 tests on macOS and 671 on Linux. Ten integration tests
passed on each platform, including the ordinary and checkpoint-backed
history and process-kill regressions. Formatting and all-target Clippy
with warnings denied passed on macOS.

Repeat the integration campaigns with
`cargo test --locked --test document_retained --test document_transaction_model --test document_kill_races -- --nocapture --test-threads=1`.
The following four variables control the history counts per backend and
repetitions per kill delay.

| Setting | macOS | Linux tmpfs |
| --- | ---: | ---: |
| `EMDA_DOCUMENT_MODEL_HISTORIES` | 128 | 512 |
| `EMDA_RETAINED_DOCUMENT_MODEL_HISTORIES` | 128 | 512 |
| `EMDA_DOCUMENT_KILL_REPETITIONS` | 8 | 16 |
| `EMDA_RETAINED_DOCUMENT_KILL_REPETITIONS` | 8 | 32 |

For ext4, set `TMPDIR` to an existing directory on that volume and run the
retained history and kill tests by their exact names. Use 128 histories and
16 repetitions. These are finite schedules and process stops on a running
OS. Physical power-loss testing and independent off-host restore remain
deferred.

Raw logs include the [macOS library](macos-lib.log.gz),
[integration](macos-integration.log.gz), [Clippy](macos-clippy.log.gz),
[Linux library](linux-lib.log.gz), [tmpfs integration](linux-integration.log.gz),
[ext4 histories](linux-ext4-model.log.gz), and [ext4 kills](linux-ext4-kill.log.gz).
