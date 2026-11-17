# Retained phase experiment

Build before timing: `cargo build --release --example retained_phase_bench`. Use a fresh, trusted directory and keep it for evidence. The commands below require macOS `/usr/bin/time -l`; its maximum resident set size is bytes. `setup` creates the fixture directory and refuses an existing path. Run each phase exactly once, in order; a failed publication may leave artifacts and must be inspected, not blindly retried. Bindings are stored separately from the candidate and suffix, but on the same machine; protect them independently in real use.

```sh
set -e
run=$(mktemp -d "${TMPDIR:-/tmp}/retained-bench.XXXXXX")
bin="$PWD/target/release/examples/retained_phase_bench"
uname -a > "$run/environment.txt"
rustc --version >> "$run/environment.txt"
git rev-parse HEAD >> "$run/environment.txt"
git status --short >> "$run/environment.txt"
shasum -a 256 examples/retained_phase_bench.rs >> "$run/environment.txt"
KEYS=16385 SUFFIX=16385 SEED=42 /usr/bin/time -l "$bin" setup "$run/fixture" > "$run/setup.log" 2>&1
find "$run/fixture/source" -type f -exec shasum -a 256 {} + | sort > "$run/source-before-build.sha256"
/usr/bin/time -l "$bin" build "$run/fixture" > "$run/build.log" 2>&1
find "$run/fixture/source" -type f -exec shasum -a 256 {} + | sort > "$run/source-after-build.sha256"
diff "$run/source-before-build.sha256" "$run/source-after-build.sha256"
/usr/bin/time -l "$bin" mutate "$run/fixture" > "$run/mutate.log" 2>&1
find "$run/fixture/source" -type f -exec shasum -a 256 {} + | sort > "$run/source-before.sha256"
/usr/bin/time -l "$bin" export "$run/fixture" > "$run/export.log" 2>&1
/usr/bin/time -l "$bin" open "$run/fixture" > "$run/open.log" 2>&1
/usr/bin/time -l "$bin" restore "$run/fixture" > "$run/restore.log" 2>&1
find "$run/fixture/source" -type f -exec shasum -a 256 {} + | sort > "$run/source-after.sha256"
diff "$run/source-before.sha256" "$run/source-after.sha256"
/usr/bin/time -l "$bin" verify "$run/fixture" > "$run/verify.log" 2>&1
du -sk "$run/fixture"/* > "$run/disk-kib.txt"
printf '%s\n' "$run"
```

Check every exit status before proceeding. The open phase includes construction and comparison of the independent model, so its elapsed time and RSS are not replay-only. Build and export include opening the source store; restore excludes the later verification process. Verify scans every source and restored entry against a separately generated BTreeMap. The hash comparison covers source files across export, open and restore, before verification opens the source. Setup uses the repository's empty v2 fixture and deterministic small keys and values; suffix operations cycle across keys and delete every seventh operation. Configure sizes and seed at setup only.

Logical limits are fixed in the example: 1 GiB log, candidate, suffix and staging limits; 1 million live keys; 128 MiB key bytes; 64 MiB staged values; 2 million suffix operations and 100 billion replay work units. These are admission budgets, not measured memory or disk caps. `/usr/bin/time -l` measures process peak RSS, not system-wide memory. `du` records allocated disk at the end only: short-lived staging files, simultaneous peaks, filesystem metadata, compression, and copy-on-write effects are not measured. No transient disk peak can be inferred from it. The experiment is local and does not simulate power loss or off-host restore.

One [preserved run](../benchmarks/results/2026-11-18-macos-retained-phase-run.tar.gz) used commit `a38e742`, 16,385 initial keys, 16,385 suffix operations, and seed 42; it ended with 14,044 live keys. The run used an earlier copy of this harness, whose output labels `keys` as the initial count. The full ordered states matched the generated model. Source file hashes matched across export through restore, but were not captured before candidate build. Restore timing includes replay, per-entry writes, staged reopen, comparison and publication. A single run is not a latency distribution, cold-cache study, or total resource bound. The archive contains raw environment and phase logs and the fixtures; the harness itself was uncommitted at measurement time.

The [exact harness used](../benchmarks/results/2026-11-18-macos-retained-phase-harness.rs) matches the hash in the run's environment log. On that one macOS ARM64 run, elapsed seconds and per-process peak RSS were: setup 109.06 s / 4,161,536 bytes; candidate build 0.53 s / 7,061,504 bytes; mutation 117.59 s / 4,751,360 bytes; suffix export 0.22 s / 9,879,552 bytes; detached open plus model check 0.14 s / 7,946,240 bytes; restore 93.57 s / 6,635,520 bytes; source and restore verification 0.03 s / 9,912,320 bytes. End-of-run allocated sizes were 700 KiB candidate, 856 KiB suffix, 1,756 KiB source and 776 KiB restored store. These numbers include the work described above and exclude transient disk peaks. Replay's vector scans and shifts can amplify work quadratically; the configured work budget is not a time limit.
