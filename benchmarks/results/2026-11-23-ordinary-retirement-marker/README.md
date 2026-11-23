# Ordinary retired-store marker publication

Ordinary online reclamation writes the ownership marker under a
`.pending` name, synchronizes it, publishes it as `.owner` with a
non-overwriting rename, and synchronizes the parent before staging
the old directory. An interrupted preparation cannot authorize payload
removal. Generic staging cleanup can remove recognized abandoned
preparations under the exclusive parent lock. Published and legacy
incomplete owner files retain their existing scoped cleanup rules.

On macOS, 37 staging tests and 84 managed retained tests passed. The
staging tests killed a subprocess with a partial ordinary retirement
marker at the pre-publication boundary. Generic cleanup was excluded
until the process exited, then removed the preparation. Separate tests
cover an owner-name conflict, an existing payload under the corresponding
name, symlinks, and oversized files.

On Linux ARM64 ext4, the [namespace fault report](linux-namespace-eio.json.gz)
contains two controls and 64 injected `EIO` returns across every observed
fixture namespace call, for V2 and V3 starting stores and both live-first
and backup-first verification orders. Four injections targeted the marker
rename. All four preserved the original store at the previous path,
reported reclamation failure after successful handoff, and left no
abandoned marker. Live-state and local backup verification passed before
and after cleanup. Four focused Linux integration tests passed.

Run the report verifier from the repo root.

```sh
python3 scripts/verify_reclamation_eio_results.py \
  benchmarks/results/2026-11-23-ordinary-retirement-marker/linux-namespace-eio.json.gz
```

The subprocess termination and syscall tests run on a live OS. Physical
power-loss testing and independent off-host restore remain deferred.
