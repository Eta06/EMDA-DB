# Longer recovery inputs and stricter fuzz outcomes — 6 November 2026

The ordinary-log and checkpoint-recovery fuzz targets now stop on an
unclassified error. A genuine I/O error can have an uncertain outcome, so the
test reports it for investigation instead of assuming files are unchanged or
silently counting the input as checked. Expected malformed-input errors still
check that the candidate log and witnesses are preserved.

Four new seeds exercise longer valid data from the start: an 8,116-byte V2
log of atomic batches, an 8,022-byte V3 log, and complete 256-byte checkpoint
tails for each format. The [seed tests](../tests/recovery_fuzz_seeds.rs) confirm
that the long logs actually open and that both checkpoint tails are adopted
and then reopen through ordinary recovery. This guards against a large seed
that is rejected immediately and never exercises deep replay.

On clean revision `24e425ff0a9e57a07e0110b45edd4d8ba2fed41f`, two
address-sanitized, 180-second libFuzzer campaigns ran concurrently on the
workspace overlay with no finding:

| Target | Starting maximum input | Configured maximum | Executions |
| --- | ---: | ---: | ---: |
| Ordinary recovery | 8,116 bytes | 8,192 bytes | 67,581 |
| Checkpoint recovery | 256 bytes | 256 bytes | 13,430 |

The [result metadata](../benchmarks/results/2026-11-06-long-recovery-fuzz.json)
records seeds, source and binary hashes, versions and filesystem; the
[archive](../benchmarks/results/2026-11-06-long-recovery-fuzz-corpus.tar.gz)
retains the complete logs and resulting corpora. The overlay's backing device
was not independently verified. These finite, serial-input fuzzers do not
exhaust malformed inputs, model concurrent writers or I/O faults, simulate
physical power loss, or perform an off-host restore.

The [hosted recovery-fuzz workflow](https://github.com/Eta06/EMDA-DB/actions/runs/42163263653)
also passed all four shards on revision `a124651`, including the updated
recovery targets and long starting seeds. Hosted and local runs are separate
finite campaigns.
