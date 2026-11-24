# Writable checkpoint header admission on a larger index

The release probe opened the same acknowledged macOS ARM64 V2 fixture
with Rust 1.85.0 at baseline `170f8a9` and fixed `8fed2d4`. It used an
explicit checkpoint with 262,144 keys, a 6,815,844-byte checkpoint file
and a 10,754,588-byte log. Each shuffled round started one fresh process
per case; four warmup samples per case were excluded from the table.

| Recovery budget | Baseline median, ms | Fixed median, ms |
| --- | ---: | ---: |
| One byte below the log length | 6.012 | 0.055 |
| Exact log length | 12.985 | 13.122 |

The timed region is the open call. Each short call checks the exact
rejection. Every exact call opens and verifies a stored value after the
timer. Fixture file hashes match before and after both attempts. The
numbers are warm-cache samples from one machine. They measure the rejected
and admitted open paths and do not measure pending-tail adoption.

The [raw archive](raw-artifacts.tar.gz) contains the harness, build and
attempt logs, every sample including warmups, filesystem hashes and binary
identities. The first attempt is retained as invalid because Cargo
reused a single artifact across source trees. The reported rerun used
separate build directories and independently confirmed distinct binaries.
See the [pending-tail regression](../2026-11-24-checkpoint-header-admission/README.md)
for the integrity and recovery checks.
