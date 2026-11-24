# Authorized reclamation under returned filesystem errors

At `21928f0`, a successful Linux recovery probe established the fence
and retirement syscall ordinals. The campaign then used a fresh
authorized catalog for each injected EIO.

| Injected boundary | Fence reported | Removal reported | Verified retry |
| --- | --- | --- | --- |
| Sync of the authority directory after replacing the record | Unconfirmed | Not started | The handle required reopening. The old slot was preserved. Retry removed it with a new fence at revision 3. |
| Sync of the retirement directory after moving the inactive slot | Confirmed | Outcome unknown | The handle stayed readable. Reopen verified the acknowledged value and resumed cleanup at revision 2. |

Each retry finished with no inactive or retired files remaining and a
subsequent reclaim reported no work. These are faults returned by
the running kernel. The [raw archive](raw-artifacts.tar.gz) preserves
the exact traces, job reports, the remaining filesystem campaign and
the three hosted cross-engine comparison attempts.

[Linux fault job](https://github.com/Eta06/EMDA-DB/actions/runs/45358112126) ·
[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45358112053) ·
[Local authority tests](../2026-11-24-authorized-reclamation-outcomes/README.md)
