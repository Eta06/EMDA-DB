# Request 001: compound document range comparison

**Status:** requested; awaiting results.

**Measured revision:** `0edfebaeb0be8693813a66e4740e7537127ab2db`.

**Goal:** repeat the existing document-query comparison on a less noisy host
and investigate the gap between the prototype's indexed reads and SQLite's.
The previous [VM results](../docs/COMPOUND_RANGE_CURRENT_2026_11_05.md) are a
baseline, not an expected outcome. Please retain slower and failed runs too.

For the Mac mini, first follow the [Apple silicon VM setup](001-mac-mini.md).
Run the commands below **inside the Linux VM**, with its test parent on the
guest's local virtual disk. This is a Linux/ARM64 VM test, not a native macOS
benchmark.

The test compares one equality-prefix plus numeric-range query with the same
synthetic documents and expected results on the prototype, SQLite, PostgreSQL,
and MongoDB. The runner checks results and index plans, reopens the data, rotates
execution order, and saves raw samples, versions, settings, hashes, failures,
and available noise counters. The measured queries are **single-client,
warm-cache reads**; setup writes are outside their timers. They do not measure
write throughput, large datasets, scale-to-zero, or physical power-loss safety.

The prototype and SQLite run in one Rust process; PostgreSQL and MongoDB use
Python drivers and loopback-connected containers. The PostgreSQL path forces a
bitmap index plan and MongoDB uses an index hint. These adapter, planner,
transport, and model differences are part of the comparison, so the results
cannot establish a universal engine ranking. See the
[benchmark description](../docs/BENCHMARKS.md#compound-equality-prefix-and-range-comparison)
for details.

## Machine and prerequisites

- A disposable Linux machine with at least two allowed logical CPUs and
  enough free disk space for Rust builds, container images, and test data.
  A quiet dedicated host with separate physical cores and a local SSD or NVMe
  filesystem is preferred. Record if the machine is a VM or shares storage.
- Git, Python 3 with `venv` and `pip`, Rust/Cargo 1.85 or later, `findmnt`,
  `lscpu`, and access to the **local** Docker daemon at
  `/var/run/docker.sock`. The images used below need a compatible CPU and
  kernel; MongoDB 8 requires AVX on x86-64 and has a known incompatibility with
  some Linux 7.0.0 kernels. If a prerequisite fails, return the error instead
  of disabling the database's startup checks.
- Choose a test directory on a local filesystem that supports the engine's
  xattrs and synchronization, preferably ext4 or XFS. Do not use tmpfs to
  draw durable-storage conclusions. Record the filesystem and storage type.
- Run only synthetic data on a dedicated test Docker daemon. The commands
  below set the local `postgres:18-bookworm`, `mongo:8.0`, and
  `redis:8.2-bookworm` tags to pinned images; they therefore change those local
  tags. The runner creates disposable containers, binds their ports to
  localhost, and removes its own data and containers. The Redis image is used
  for cleanup if container-owned files cannot be removed by the calling user.
  An interrupted run can leave its own `emda-compare-*` resources behind;
  inspect those specific resources before removing them.
- Prepare dependencies and builds before measurement. Avoid running other
  demanding work during the benchmark. Do not change system-wide scheduling,
  drop caches, stop unrelated services, or weaken database durability to make
  results look better.

## Run

Use a fresh Bash session. If any command fails, stop and keep the output and
any partial result. Change `EMDA_TEST_PARENT` to a writable location on the
chosen disposable filesystem; the commands create a uniquely named work
directory underneath it. A completed benchmark can take several minutes, plus
the time needed to download and build its dependencies.

```bash
set -euo pipefail
export EMDA_TEST_PARENT="${EMDA_TEST_PARENT:-$HOME/emda-external-tests}"
mkdir -p -- "$EMDA_TEST_PARENT"
export EMDA_TEST_DIR="$(mktemp -d "$EMDA_TEST_PARENT/emda-range.XXXXXXXX")"
printf 'Test files and results: %s\n' "$EMDA_TEST_DIR"

git clone https://github.com/Eta06/EMDA-DB.git "$EMDA_TEST_DIR/repo"
cd "$EMDA_TEST_DIR/repo"
git checkout --detach 0edfebaeb0be8693813a66e4740e7537127ab2db

python3 -m venv "$EMDA_TEST_DIR/venv"
"$EMDA_TEST_DIR/venv/bin/python" -m pip install \
  'psycopg[binary]==3.3.7' 'pymongo==4.15.5'

export CARGO_TARGET_DIR="$EMDA_TEST_DIR/target"
cargo build --release --locked --example compound_range_query_bench

docker pull postgres@sha256:5c4783a8cc3f0074912164f94dfa1be4e043438f0f707cf968d7af9a0d31a2c8
docker tag postgres@sha256:5c4783a8cc3f0074912164f94dfa1be4e043438f0f707cf968d7af9a0d31a2c8 postgres:18-bookworm
docker pull mongo@sha256:b9924c330a8bbc5800fe9c69f04a84e221eca7b28fd049a0354147d9908fa142
docker tag mongo@sha256:b9924c330a8bbc5800fe9c69f04a84e221eca7b28fd049a0354147d9908fa142 mongo:8.0
docker pull redis@sha256:99ffb973380cc2f47bca06bd094f7f8c7138a0879a63cb8939896a22b7a65bba
docker tag redis@sha256:99ffb973380cc2f47bca06bd094f7f8c7138a0879a63cb8939896a22b7a65bba redis:8.2-bookworm

mkdir -p "$EMDA_TEST_DIR/data"
```

The following selects two CPUs available to the current process. If possible,
inspect `lscpu -e=CPU,CORE,SOCKET,ONLINE` and replace them with two allowed
logical CPUs on separate physical cores. Pinning limits where these processes
run; it does not reserve cores or eliminate host/storage interference.

```bash
EMDA_CPU_PAIR="$(python3 -c 'import os; c = sorted(os.sched_getaffinity(0)); assert len(c) >= 2, "at least two allowed CPUs are required"; print(c[0], c[1])')"
read -r EMDA_DRIVER_CPU EMDA_SERVER_CPU <<< "$EMDA_CPU_PAIR"
printf 'Driver CPU: %s; server CPU: %s\n' "$EMDA_DRIVER_CPU" "$EMDA_SERVER_CPU"

{
  date -u
  uname -srvm
  lscpu
  free -h
  df -h "$EMDA_TEST_DIR/data"
  findmnt -T "$EMDA_TEST_DIR/data" -o TARGET,FSTYPE,SOURCE,OPTIONS
  rustc -Vv
  cargo --version
  "$EMDA_TEST_DIR/venv/bin/python" --version
  "$EMDA_TEST_DIR/venv/bin/python" -m pip freeze
  docker version --format '{{.Server.Version}}'
  git rev-parse HEAD
  git status --short
} > "$EMDA_TEST_DIR/host.txt"
```

First run a small smoke test. Then run the full comparison twice with separate
result files. Each full run has four rounds, three dataset/match sizes
(`100:10`, `1000:10`, `1000:100`), ten warmups and 50 timed queries per path.
The output paths must not already exist; use a new name if retrying a failed
attempt so that its evidence is preserved.

```bash
"$EMDA_TEST_DIR/venv/bin/python" scripts/compound_range_external_compare.py \
  --directory "$EMDA_TEST_DIR/data" \
  --binary "$CARGO_TARGET_DIR/release/examples/compound_range_query_bench" \
  --output "$EMDA_TEST_DIR/smoke.json" --cases 16:4 --rounds 2 --cycles 2 \
  --driver-cpus "$EMDA_DRIVER_CPU" --server-cpus "$EMDA_SERVER_CPU"

for EMDA_RUN in primary repeat; do
  "$EMDA_TEST_DIR/venv/bin/python" scripts/compound_range_external_compare.py \
    --directory "$EMDA_TEST_DIR/data" \
    --binary "$CARGO_TARGET_DIR/release/examples/compound_range_query_bench" \
    --output "$EMDA_TEST_DIR/$EMDA_RUN.json" \
    --cases 100:10,1000:10,1000:100 --rounds 4 --cycles 50 \
    --driver-cpus "$EMDA_DRIVER_CPU" --server-cpus "$EMDA_SERVER_CPU"
done
```

The runner exits unsuccessfully if a recorded case fails. A successful full
run reports **36 verified backend/case/round entries**; each local entry
contains the prototype and SQLite paths. A passing quiet check does not prove
the host or disk was idle. Keep raw samples and noise observations even when a
run looks noisy; note the disturbance and, if practical, repeat separately.

## Return

Please provide `primary.json`, `repeat.json`, and `host.txt`, plus `smoke.json`
and any error output if something failed. Include a short note describing the
CPU, memory, SSD/NVMe or other storage, filesystem, whether the host was shared
or virtualized, CPU choices, and any background activity. Check the files for
sensitive paths or machine details before sharing; redact sensitive information
and describe any redactions. Do not include credentials or customer data.

Results can be attached in chat or committed separately in the repository.
The data is synthetic. A maintainer will review the samples, correctness
checks and noise before making performance claims or selecting optimizations.
