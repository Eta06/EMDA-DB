# Linux fault and cross-engine checks after header admission

At `8fed2d4` the Linux EIO and disposable ENOSPC jobs passed.
The witness campaign covered 38 injected returned errors. Its
V2 and V3 explicit and managed checkpoints each admitted the exact
181-byte pending-log scan budget. At 180 bytes they read only the
two fixed log identity ranges totaling 36 bytes and preserved the
log, witness and selection.

The [raw archive](raw-artifacts.tar.gz) includes the fault reports,
run metadata and the three successful compound-range, document
contention and large process-crash comparison attempts. These traces
exercise a running OS and measure requested logical log reads.

[Filesystem EIO](https://github.com/Eta06/EMDA-DB/actions/runs/45353497370) ·
[Disposable ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45353497375) ·
[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45353497451)
