# Managed rollover model and failed-handle checks

Revision `fa16ba1ef42de361de27193cb35cf33d6f5dacd8` extends the managed
checkpoint model with the detailed rollover API. It mixes writes, deletes,
snapshots, successful rollovers and reopens with an existing-destination
collision and a missing-parent rejection. The model checks the returned
classification, unchanged selection and sentinel, snapshot values and
recovered contents against a `BTreeMap`. It does not inject selection I/O
failures into the fuzz target itself.

An AddressSanitizer-instrumented, 45-second local fuzz run on the default
Linux V3 format completed 4,673 inputs without a reported crash. Its configured
input limit was 128 bytes; the last reported mutation length limit and largest
retained input were 33 bytes.
The [metadata](../benchmarks/results/2026-11-14-managed-rollover-fuzz.json),
[raw log](../benchmarks/results/2026-11-14-managed-rollover-fuzz.log.gz) and
[353-file corpus](../benchmarks/results/2026-11-14-managed-rollover-fuzz-corpus.tar.gz)
are retained. A separate [replay](../benchmarks/results/2026-11-14-managed-rollover-fuzz-replay.log.gz)
loaded all 353 files and completed 354 reported runs without a crash. This is
a short randomized run, not exhaustive model checking or a power-loss test.
The existing scheduled GitHub fuzz workflow runs this target with its committed
seed corpus.

The same revision extends the real-syscall probe to try both a read and a
write on the original handle after a selection-uncertain failure. The
[retained fault bundle](../benchmarks/results/2026-11-14-managed-rollover-handle-faults.json.gz)
contains 424 one-shot EIO/EAGAIN cases with an acknowledged suffix, V2/V3,
first-time and existing selections, and both recovery orders. It includes
136 not-published results, 88 checkpoint-uncertain results, 168
selection-uncertain results and 32 successes. All 168 selection-uncertain
cases rejected both later operations on the handle, and subsequent recovery
checks preserved the acknowledged suffix. Initial handle-open errors are
classified by the probe as not published before it calls the detailed API.
`managed_handle_check: 1` identifies reports with this additional check.

The stable and Rust 1.85 probe suites and example Clippy checks passed, as did
132 Python tests. The previous production-code revision's full stable, Rust
1.85 and optimized stable test coverage is documented in the
[managed-rollover report](MANAGED_ROLLOVER_OUTCOMES_2026_11_14.md).
The injected errors are selected returned errors on a running Linux VM with an
overlay filesystem of unknown backing; they do not prove physical power-loss
safety, independent off-host recovery or all possible schedules.

To replay the retained corpus from the repository root:

```sh
corpus_dir=$(mktemp -d)
tar -xzf benchmarks/results/2026-11-14-managed-rollover-fuzz-corpus.tar.gz -C "$corpus_dir"
cargo +nightly-2026-11-03 fuzz run managed_checkpoint "$corpus_dir" -- -runs=353 -max_len=128 -timeout=15
```

To recheck the fault reports:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_one_shot_fs_results import verify
with gzip.open('benchmarks/results/2026-11-14-managed-rollover-handle-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
