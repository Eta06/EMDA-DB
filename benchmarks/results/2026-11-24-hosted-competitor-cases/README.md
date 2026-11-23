# Hosted cross-engine benchmark archive

This snapshot preserves the artifacts from all 206 runs returned by the
compound-range workflow history on November 24, 2026. The runs began
between November 4 and November 23 UTC. The archive includes the three
measured campaigns, 203 push-triggered smoke runs, and both workflow
failures. Four failed cases remain in the original driver reports.
The 1,994 case records include the compound-range and process-restart
reports, with the workload and result schema of each driver preserved.

The [compressed archive](raw-artifacts.tar.gz) contains one directory per
GitHub run ID. Open `RUNS.tsv` inside the archive to locate the tested
revision, original workflow URL, outcome and report files. Report and
command-log bytes are unchanged from the downloaded artifacts. Each
directory also contains the hosted run record. The latest smoke run has
the new recorder's attempt metadata, combined output and per-case samples.
Earlier runs retain the report files published by their workflow revisions.

```sh
archive=benchmarks/results/2026-11-24-hosted-competitor-cases/raw-artifacts.tar.gz
tar -xOzf "$archive" RUNS.tsv
directory=$(mktemp -d "${TMPDIR:-/tmp}/emda-hosted-cases.XXXXXX")
tar -xzf "$archive" -C "$directory"
(cd "$directory" && shasum -a 256 -c SHA256SUMS)
```

The [compound-range methodology](../../../docs/BENCHMARKS.md#compound-equality-prefix-and-range-comparison)
defines the adapters and scope. Use each stored report's case settings,
verification outcome and runner metadata before comparing timings across
revisions. The archive preserves the available hosted workload artifacts
as of its collection date. Future workflow artifacts and locally run cases
are recorded under the [benchmark recording policy](../../../docs/BENCHMARKS.md#competitor-benchmark-records).
