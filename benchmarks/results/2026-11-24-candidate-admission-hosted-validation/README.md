# Hosted candidate admission validation

At source revision `0dff313`, Linux and macOS passed on stable Rust and
Rust 1.85.0. Linux stable also passed the optimized suite and all 45
standard benchmark cases. The single-writer recovery gate passed across
four epochs with the checkpoint and retained phases capped at 48 MiB
address space. The Linux EIO and disposable ENOSPC campaigns passed.

The three additional cross-engine cases preserve compound range,
document contention, and large process-crash samples. The
[raw archive](raw-artifacts.tar.gz) preserves all 49 benchmark attempt
records, the available raw samples and logs, the filesystem fault
artifacts and the job reports. All 306 file hashes verified after extraction.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45350903589) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45350903696) ·
[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45350903670) ·
[Filesystem EIO](https://github.com/Eta06/EMDA-DB/actions/runs/45350903564) ·
[Disposable ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45350903579)
