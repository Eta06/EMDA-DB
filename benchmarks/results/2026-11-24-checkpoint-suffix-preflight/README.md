# Checkpoint log reads when a suffix exceeds its operation limit

Linux ARM64 tmpfs, Rust 1.85.0 debug binaries, V3 witnesses. Both
executables used the same fixtures. Historical puts overwrote one
key, then a checkpoint covered the resulting log prefix. One
acknowledged DB02 batch after the checkpoint deleted and replaced
the key. An operation limit of one rejected the batch. A limit of
two admitted it and verified the resulting value.

There were 72 traced opens, with three repeats per history size,
reader mode, limit and build. The order alternated for the second
repeat. Each cell counts bytes returned by read syscalls on
`records.log`. The three repeats agreed in every case.

| History payload | Rejected, before | Rejected, after | Admitted reader, either build | Admitted writer, either build |
| ---: | ---: | ---: | ---: | ---: |
| 1 MiB | 1,048,745 B | 117 B | 1,048,752 B | 1,048,788 B |
| 8 MiB | 8,388,945 B | 117 B | 8,388,952 B | 8,388,988 B |
| 32 MiB | 33,555,345 B | 117 B | 33,555,352 B | 33,555,388 B |

Rejected reader and writer opens read the same volume. The admitted
opens still read all covered bytes. Fixture file hashes matched
before and after. The trace counts log I/O requested by the
application. Checkpoint and witness-file reads are separate, and
tmpfs does not measure physical storage I/O or recovery latency.

The [raw archive](raw-results.tar.gz) contains the baseline revision,
candidate source diff, probe, commands' output, 72 syscall traces,
binary fingerprints, fixture hashes and a summary verifier. Build
the archived probe as a Rust example with `cargo +1.85.0 build
--offline --locked --example suffix_admission_probe`. Use its
`setup <fixture> <MiB>` command once for each fixture, then pass
`reader <fixture> <limit>` or `writer <fixture> <limit>` under
`strace -s 0 -y`.
