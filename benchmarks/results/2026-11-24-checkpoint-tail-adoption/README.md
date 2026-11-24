# Log reads during checkpoint tail adoption

Linux ARM64 tmpfs, Rust 1.85.0 debug, V3 witnesses. Both builds
opened the same verified checkpoint and log. Each fixture had
one DB02 batch after the checkpoint. The probe restored the
earlier witness before each adoption run and the later witness
before each already-acknowledged control. Each open verified the
new value and the probe checked the final witness.

The two campaigns captured 72 opens. Counts are bytes returned
by read syscalls on `records.log`. Each campaign used three
repetitions per case with alternating build order. All repeats
agreed.

| History payload | Pending before | Pending after | Already acknowledged, either build |
| ---: | ---: | ---: | ---: |
| 1 MiB | 2,097,497 B | 1,048,869 B | 1,048,788 B |
| 8 MiB | 16,777,897 B | 8,389,069 B | 8,388,988 B |
| 32 MiB | 67,110,697 B | 33,555,469 B | 33,555,388 B |

Each pending run still wrote the V3 witness and synchronized the
log after that write. The removed reads match the covered prefix
length. The first campaign's shell glob omitted the checkpoint
and digest from its hash manifest. The second campaign checked
all fixture files, including the checkpoint and trusted digest,
and their hashes matched before and after. The raw archive keeps
both attempts.

The [raw results](raw-results.tar.gz) include the probe, binary
fingerprints, baseline revision, candidate diff, both sets of
traces and output, and the aggregation script. Build the probe as
the example `adoption_scan_probe` for the baseline and candidate.
Use `setup <fixture> <MiB>` on the baseline to prepare the fixtures,
then use the archived `repeat.sh` to reset and check the witness
around each traced open. These counts capture application reads
on a running OS. They exclude checkpoint reads and do not
measure physical storage latency.
