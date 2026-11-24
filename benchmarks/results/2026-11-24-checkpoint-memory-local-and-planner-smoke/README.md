# Writable checkpoint comparison and hosted smoke evidence

The [raw archive](raw-artifacts.tar.gz) contains 50 successful workload
attempts plus environment, build and hosted-run records. All 176 file
hashes verified after extraction.

- `local-macos` records two workloads from `bc6264f` with the included
  runner patch. A four-backend workload started with 32,768 keys and
  ran two epochs. The checkpoint advanced during both epochs. A separate
  run exercised the existing three-backend invocation. All fresh-process
  point checks and complete ordered scans passed. Both runs were uncapped
  on macOS ARM64 with Rust 1.85.0. The first Clippy attempt resolved an older
  dependency artifact from the shared target. The isolated target build
  and warnings-denied Clippy passed. Both logs are preserved.
- `planner-cross-engine` records all three hosted compound-range,
  document-contention, and process-crash cases at `bc6264f`.
  [Hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45346761849).
- `baseline-ci` records all 45 hosted Linux smoke cases from `0177f26`.
  [Hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45345432651).

The cases use different durability scopes and resource settings.
The [benchmark guide](../../../examples/authorized_recovery_bench.md)
gives the command for the separate 48 MiB Linux gate.
