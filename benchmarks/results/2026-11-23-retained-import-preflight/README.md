# Retained import admission before copying the base

Standalone writable creation and managed catalog creation check the retained
base frame, size and bound trailer before copying its body. Header totals can
reject insufficient entry, key, block or anchor limits. Creation still
verifies every admitted staged base entry and value before publication.

## Linux ARM64 ext4 trace

Rust 1.85.0 debug builds with separate targets. Baseline revision
`8f560d65599a52d276cfc59a500c3903c5589b27`. The candidate replaces
`src/retained_candidate/writable.rs`. Executable SHA-256 hashes are
`69b2a7634cb987ff36b9751ac822fc498c655a5e45fb9e5c1539f2d05a32f01b` and
`4e7ef10e1d2fe3d3077643cc3e9e11d305ff5475e93ef08acb2c62d3174ad4ca`.

The same independently bound candidate held one four-byte key and an
8 MiB value. Both create APIs rejected the candidate under a zero-block
limit without publishing a destination. The four trace excerpts retain
all traced calls referencing the candidate's base descriptor.

| Create path | Baseline logical bytes copied to staging | Updated logical bytes copied | Updated frame bytes read |
| --- | ---: | ---: | ---: |
| Standalone | 8,388,764 | 0 | 144 |
| Managed | 8,388,764 | 0 | 144 |

The two reads are the 112-byte header and 32-byte trailer. The baseline
transfer used Linux `copy_file_range`. These are syscall byte counts,
with no estimate of physical storage traffic or elapsed-time improvement.
Both updated create APIs accepted the exact limit of one block, reopened
successfully and returned the complete expected value. Candidate-base,
source-log and source-witness hashes matched before and after the runs.

On macOS, 106 writable retained tests passed, including exact and
insufficient base limits, frame mismatches and same-length body damage.
Thirteen focused Linux integration tests passed.

## Reproduction

Copy [the probe](retained_import_probe.rs) into `examples/` in both source
copies. Build each with `cargo +1.85.0 build --locked --example retained_import_probe`
and separate targets. Set `baseline_bin` and `candidate_bin` to the
executables and `fixture` to a fresh ext4 directory.

```sh
"$baseline_bin" "$fixture" prepare
for bin in "$baseline_bin" "$candidate_bin"; do
  for mode in standalone-reject managed-reject; do
    strace -qq -yy -s 0 -e trace=read,pread64,write,copy_file_range,sendfile,splice \
      "$bin" "$fixture" "$mode"
  done
done
"$candidate_bin" "$fixture" standalone-accept
"$candidate_bin" "$fixture" managed-accept
```
