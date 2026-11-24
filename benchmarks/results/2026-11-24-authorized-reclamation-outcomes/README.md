# Independent authorized reclamation outcomes

At `21928f0`, 67 focused macOS ARM64 tests passed under Rust 1.85.0,
along with formatting, warnings-denied Clippy and the disposable probe
smoke check. The tests cover an unavailable inactive slot, provider
errors before and after replacement, corruption detected after the
fence, and an interrupted move with successful restart cleanup. Resuming
a staged retirement leaves the acknowledged value and the authority
revision intact.

The [raw archive](raw-artifacts.tar.gz) contains the macOS tests,
lint output and source diff. The [Linux fault job](https://github.com/Eta06/EMDA-DB/actions/runs/45358112126)
exercises errors returned at the authority fence and after moving the
inactive slot.
