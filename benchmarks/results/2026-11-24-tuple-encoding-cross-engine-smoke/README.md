# Cross-engine smoke cases after tuple encoding optimization

The [hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45330110396)
at `597783e` recorded three successful smoke cases covering compound
ranges, large-value crash recovery, and concurrent document updates.
All ten files in the [raw archive](raw-artifacts.tar.gz) matched the
SHA-256 manifest after extraction.

The [paired timing report](../2026-11-24-tuple-encoding-pairs/README.md)
covers a separate workload with a SQLite control.
